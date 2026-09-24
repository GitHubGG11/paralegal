import asyncio
import copy
import json
import sys
import unittest
from pathlib import Path
from xml.etree import ElementTree

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from streaming import SSEDecoder, ChatDeltas
from relay import RelayConversation
from conversation import new_state
from core import relay_xml


class StreamDecoderTests(unittest.TestCase):
    def test_every_byte_boundary_including_unicode_and_crlf(self):
        body = ('data: ' + json.dumps({'word': 'café'}, ensure_ascii=False) + '\r\n\r\n'
                ': heartbeat\r\n\r\ndata: [DONE]\r\n\r\n').encode()
        parser = SSEDecoder()
        output = []
        for byte in body:
            output.extend(parser.feed(bytes([byte])))
        self.assertEqual(output, [{'word': 'café'}])
        self.assertTrue(parser.done)

    def test_fragmented_tool_arguments_and_preserved_spaces(self):
        tokens = []
        parts = ChatDeltas(tokens.append)
        parts.accept({'choices': [{'delta': {'tool_calls': [{'index': 0, 'id': 'c1', 'function': {'name': 'search_records', 'arguments': '{"query":'}}]}}]})
        parts.accept({'choices': [{'delta': {'tool_calls': [{'index': 0, 'function': {'arguments': '"Gene"}'}}]}, 'finish_reason': 'tool_calls'}]})
        call = parts.message()['tool_calls'][0]
        self.assertEqual(json.loads(call['function']['arguments']), {'query': 'Gene'})
        self.assertEqual(tokens, [])
        parts = ChatDeltas(tokens.append)
        for text in ('Hello', ' there', '!'):
            parts.accept({'choices': [{'delta': {'content': text}}]})
        parts.accept({'choices': [{'delta': {}, 'finish_reason': 'stop'}]})
        self.assertEqual(parts.message()['content'], 'Hello there!')
        self.assertEqual(tokens, ['Hello', ' there', '!'])

    def test_truncated_or_overlarge_stream_rejected(self):
        with self.assertRaises(ValueError):
            ChatDeltas(lambda text: None).message()
        with self.assertRaises(ValueError):
            SSEDecoder().feed(b'x' * 262145)

    def test_relay_twiml(self):
        xml = ElementTree.fromstring(relay_xml('https://example.com', 'CA'+'a'*32, 'voice-flash_v2_5'))
        relay = xml.find('Connect/ConversationRelay')
        self.assertEqual(relay.get('ttsProvider'), 'ElevenLabs')
        self.assertEqual(relay.get('interruptible'), 'speech')
        self.assertEqual(relay.get('partialPrompts'), 'true')
        self.assertEqual(relay.get('speechModel'), 'flux')
        self.assertTrue(relay.get('url').startswith('wss://'))
        self.assertIsNone(xml.find('Start/Transcription'))
        self.assertEqual(xml.find('Connect').get('action'), 'https://example.com/relay/ended')


class RelayTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tasks, self.tokens, self.events, self.requests = [], [], [], []
        self.gate = asyncio.Event()
        self.engine = RelayConversation(new_state('CA'+'a'*32, 'AC'+'b'*32), lambda s: None,
            self.generate, None, self.emit, self.spawn, lambda token,last: self.tokens.append((token,last)))

    def spawn(self, coroutine):
        task = asyncio.create_task(coroutine)
        self.tasks.append(task)
        return task

    async def emit(self, kind, **fields):
        self.events.append((kind, fields))

    async def generate(self, history, on_text):
        self.requests.append(copy.deepcopy(history))
        on_text('First words ')
        await self.gate.wait()
        on_text('and the rest.')
        return {'text': 'First words and the rest.', 'sources': []}

    def send(self, sequence, text, final):
        self.engine.receive(dict(session='VX'+'c'*32,sequence=sequence,track='inbound_track',
            type='transcription-content',text=text,final=final,receivedAt=self.engine.clock()))

    async def settle(self):
        for _ in range(10):
            await asyncio.sleep(0)

    async def asyncTearDown(self):
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)

    async def test_sends_first_text_before_generation_finishes(self):
        self.send(1, 'Hello', True)
        await self.settle()
        self.assertEqual(self.tokens, [('First words ', False)])
        self.assertFalse(self.engine.reply_generation.done())
        self.gate.set()
        await self.settle()
        self.assertEqual(self.tokens[-2:], [('and the rest.', False), ('', True)])
        self.assertTrue(self.events[-1][1]['streaming'])

    async def test_interrupt_aborts_suffix_and_records_only_heard_text(self):
        self.send(1, 'Hello', True)
        await self.settle()
        self.engine.barge_in('First')
        self.gate.set()
        await self.settle()
        self.assertEqual(self.tokens, [('First words ', False)])
        self.assertEqual(self.engine.s['history'][-1], {'role': 'assistant', 'content': 'First'})
        self.send(2, 'New question', True)
        await self.settle()
        self.assertEqual(self.requests[-1][-1]['content'], 'New question')

    async def test_speculative_stream_stays_silent_until_matching_final(self):
        self.send(1, 'Can you find my date', False)
        await asyncio.sleep(.38)
        self.assertEqual(self.tokens, [])
        self.send(2, 'Can you find my date?', True)
        await self.settle()
        self.assertEqual(self.tokens, [('First words ', False)])
        self.assertEqual(len(self.requests), 1)
        self.gate.set()
        await self.settle()
        self.assertTrue(self.events[-1][1]['speculativeHit'])

    async def test_revised_partial_never_releases_old_draft(self):
        self.send(1, 'Can you find my date', False)
        await asyncio.sleep(.38)
        self.send(2, 'Find something else', True)
        await self.settle()
        self.assertEqual(len(self.requests), 2)
        self.assertEqual(self.tokens, [('First words ', False)])

    async def test_stream_failure_closes_talk_cycle_with_fallback(self):
        async def failed(history, on_text):
            on_text('One moment.')
            raise RuntimeError('Synthetic upstream failure')
        self.engine.generate = failed
        self.send(1, 'Hello', True)
        await self.settle()
        self.assertEqual(self.tokens[-1], ('', True))
        self.assertIn("couldn't finish", self.tokens[-2][0])
        self.assertEqual(self.events[-1][0], 'agent-error')
