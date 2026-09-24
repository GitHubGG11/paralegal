import asyncio
import copy
import sys
import unittest
from pathlib import Path
from xml.etree import ElementTree

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from conversation import Conversation, new_state
from core import listening_xml, voice_xml


class ConversationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tasks, self.requests, self.playback, self.events = [], [], [], []
        self.gate = None
        self.update_gate = None
        self.saved = None
        self.engine = Conversation(new_state('CA'+'a'*32, 'AC'+'b'*32), self.save,
                                   self.generate, self.update, self.emit, self.spawn)

    def save(self, state):
        self.saved = copy.deepcopy(state)

    def spawn(self, coro):
        task = asyncio.create_task(coro)
        self.tasks.append(task)
        return task

    async def generate(self, history):
        self.requests.append(history)
        if self.gate:
            await self.gate.wait()
        return {'text': 'Reply to ' + history[-1]['content'], 'sources': []}

    async def update(self, text, revision):
        if text and self.update_gate:
            await self.update_gate.wait()
        self.playback.append((text, revision))

    async def emit(self, kind, **fields):
        self.events.append((kind, fields))

    def send(self, sequence, text='', final=False, kind='transcription-content', track='inbound_track'):
        self.engine.receive(dict(session='GT'+'c'*32, sequence=sequence, type=kind,
                                 text=text, final=final, track=track, receivedAt=0))

    async def settle(self):
        for _ in range(10):
            await asyncio.sleep(0)

    async def asyncTearDown(self):
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)

    async def test_final_retry_and_late_events_only_reply_once(self):
        self.send(2, 'Hello', True)
        await self.settle()
        self.send(2, 'Hello', True)
        self.send(1, 'Late', True)
        self.send(3, 'Echo', True, track='outbound_track')
        await self.settle()
        self.assertEqual(len(self.requests), 1)
        self.assertEqual(len(self.playback), 1)

    async def test_speculation_reused_only_on_matching_final(self):
        self.send(1, 'Tell me about the file')
        await asyncio.sleep(.38)
        self.assertEqual(len(self.requests), 1)
        self.assertEqual(self.playback, [])
        self.send(2, 'Tell me about the file.', True)
        await self.settle()
        self.assertEqual(len(self.requests), 1)
        self.assertTrue(self.events[-1][1]['speculativeHit'])

    async def test_changed_final_discards_draft(self):
        self.send(1, 'I want the first file')
        await asyncio.sleep(.38)
        self.send(2, 'I want the second file', True)
        await self.settle()
        self.assertEqual(len(self.requests), 2)
        self.assertIn('second', self.playback[0][0])

    async def test_new_speech_cancels_pending_reply(self):
        self.gate = asyncio.Event()
        self.send(1, 'Old question', True)
        await self.settle()
        self.send(2, 'Actually')
        self.gate.set()
        await self.settle()
        self.assertEqual(self.playback, [])

    async def test_barge_in_waits_for_inflight_update_then_clears_it(self):
        self.update_gate = asyncio.Event()
        self.send(1, 'Hello', True)
        await self.settle()
        self.send(2, 'Stop')
        self.update_gate.set()
        await self.settle()
        self.assertEqual(len(self.playback), 2)
        self.assertEqual(self.playback[-1], ('', None))
        self.assertEqual(self.engine.s['speaking'], 0)
        self.assertIn('Interrupted', self.engine.s['history'][-1]['content'])

    async def test_hangup_discards_pending_generation(self):
        self.gate = asyncio.Event()
        self.send(1, 'Hello', True)
        await self.settle()
        self.send(2, kind='transcription-stopped')
        self.gate.set()
        await self.settle()
        self.assertEqual(self.playback, [])

    async def test_completion_and_stale_callback(self):
        self.send(1, 'Hello', True)
        await self.settle()
        revision = self.engine.s['speaking']
        self.engine.played(revision-1)
        self.assertEqual(self.engine.s['speaking'], revision)
        self.engine.played(revision)
        self.assertEqual(self.engine.s['history'][-1]['role'], 'assistant')

    async def test_budget_and_deadline(self):
        self.engine.s['generations'] = 100
        self.send(1, 'Hello', True)
        await self.settle()
        self.assertEqual(self.requests, [])
        self.engine.s['deadline'] = 0
        self.send(2, 'More', True)
        await self.settle()
        self.assertEqual(len(self.playback), 1)

    def test_twiml_escapes_reply_and_uses_default_voice(self):
        root = ElementTree.fromstring(listening_xml(12, '<Hangup/> & hello', 'https://example.com/done?a=1&b=2'))
        self.assertEqual(root.find('Say').text, '<Hangup/> & hello')
        self.assertEqual(root.find('Say').attrib, {})
        self.assertIsNone(root.find('Hangup'))
        self.assertEqual(root.find('Redirect').get('method'), 'POST')
        self.assertIsNotNone(ElementTree.fromstring(listening_xml(0)).find('Hangup'))
        self.assertEqual(ElementTree.fromstring(voice_xml('https://example.com', agent=True)).find('Say').text, 'How may I help you?')
