"""Call-scoped Durable Object, OpenAI tools, and serialized Twilio playback updates."""
import asyncio
import base64
import json
import math
from html import escape
from urllib.parse import urlencode, urlsplit

from js import AbortController, Object, Response, WebSocketPair, fetch
from pyodide.ffi import create_proxy, to_js
from workers import DurableObject

from conversation import Conversation, SYSTEM_PROMPT, new_state
from core import listening_xml, now_ms
from relay import RelayConversation
from streaming import ChatDeltas, SSEDecoder

SEARCH_TOOL = {"type": "function", "function": {
    "name": "search_records", "description": "Search indexed emails and JSON records. Use specific names, dates, subjects or identifiers. Read only.",
    "strict": True, "parameters": {"type": "object", "properties": {
        "query": {"type": "string", "description": "Specific search terms, at most 500 characters"}},
        "required": ["query"], "additionalProperties": False}}}


def options(value):
    return to_js(value, dict_converter=Object.fromEntries)


async def request_json(url, headers, body, timeout=8):
    """Abort provider I/O on timeout or when newer speech invalidates the turn."""
    controller = AbortController.new()
    try:
        async with asyncio.timeout(timeout):
            response = await fetch(url, options({"method": "POST", "headers": headers,
                "body": body, "signal": controller.signal}))
            if not response.ok:
                if response.body:
                    await response.body.cancel()
                raise RuntimeError("Provider HTTP " + str(response.status))
            # All requests bound the model response; enforce a transport bound too.
            reader = response.body.getReader()
            data = bytearray()
            try:
                while True:
                    part = await reader.read()
                    if part.done:
                        break
                    data.extend(bytes(part.value.to_py()))
                    if len(data) > 65536:
                        await reader.cancel()
                        raise ValueError("Provider response too large")
            finally:
                reader.releaseLock()
            return json.loads(data.decode())
    finally:
        controller.abort()


async def request_chat_stream(headers, payload, on_text):
    controller = AbortController.new()
    reader = None
    try:
        async with asyncio.timeout(8):
            response = await fetch("https://api.openai.com/v1/chat/completions", options({
                "method": "POST", "headers": headers, "body": json.dumps({**payload, "stream": True}),
                "signal": controller.signal}))
            if not response.ok:
                if response.body:
                    await response.body.cancel()
                raise RuntimeError("Provider HTTP " + str(response.status))
            decoder, deltas = SSEDecoder(), ChatDeltas(on_text)
            reader = response.body.getReader()
            while not decoder.done:
                part = await reader.read()
                chunk = b"" if part.done else bytes(part.value.to_py())
                for event in decoder.feed(chunk, final=part.done):
                    deltas.accept(event)
                if part.done:
                    break
            if not decoder.done:
                raise ValueError("Model stream disconnected")
            return deltas.message()
    finally:
        controller.abort()
        if reader:
            reader.releaseLock()


class CallAgent(DurableObject):
    def __init__(self, ctx, env):
        super().__init__(ctx, env)
        self.ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS call_state (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)")
        rows = self.ctx.storage.sql.exec("SELECT payload FROM call_state WHERE id=1").toArray()
        self.engine = self.make_engine(json.loads(rows[0].payload)) if len(rows) else None

    def make_engine(self, state):
        if state.get("transport") == "relay":
            return RelayConversation(state, self.save, self.generate, self.update_call, self.emit,
                                     self.spawn, self.send_text)
        return Conversation(state, self.save, self.generate, self.update_call, self.emit, self.spawn)

    def save(self, state):
        self.ctx.storage.sql.exec("INSERT OR REPLACE INTO call_state VALUES (1, ?)", json.dumps(state))

    def spawn(self, coroutine):
        task = asyncio.create_task(coroutine)
        # Consume cancellation/errors in the lifetime promise; consumers still see task failures.
        async def lifetime():
            try:
                await asyncio.shield(task)
            except (asyncio.CancelledError, Exception):
                pass
        lifetime_task = asyncio.create_task(lifetime())
        proxy = create_proxy(lifetime_task)
        lifetime_task.add_done_callback(lambda done: proxy.destroy())
        self.ctx.waitUntil(proxy)
        return task

    async def start(self, call, account, transport="say"):
        if not self.engine:
            state = new_state(call, account)
            state["transport"] = transport
            state["relay_sequence"] = 0
            self.engine = self.make_engine(state)
            self.save(self.engine.s)
            await self.ctx.storage.setAlarm(self.engine.s["deadline"])

    async def receive(self, payload):
        if self.engine and self.engine.s.get("transport") != "relay":
            self.engine.receive(json.loads(payload))

    async def check(self):
        started = now_ms()
        first_token = None
        def received(token):
            nonlocal first_token
            if first_token is None:
                first_token = now_ms()
        result = await self.generate([{"role": "user", "content": "Say hello in one short sentence. This is a connectivity test."}], received)
        return json.dumps({"ok": True, "model": getattr(self.env, "OPENAI_MODEL", "gpt-4.1-nano"),
                           "latencyMs": now_ms() - started, "firstTokenMs": (first_token or now_ms()) - started,
                           "streaming": True, "reply": result["text"]})

    async def fetch(self, request):
        # Only the signature-validating Worker forwards this route to this call object.
        if (not self.engine or self.engine.s.get("transport") != "relay" or not self.engine.live()):
            return Response.new("Call not active", options({"status": 409}))
        if len(self.ctx.getWebSockets()):
            return Response.new("Call already connected", options({"status": 409}))
        if (request.headers.get("Upgrade") or "").lower() != "websocket":
            return Response.new("Expected WebSocket", options({"status": 426}))
        client, server = Object.values(WebSocketPair.new())
        self.ctx.acceptWebSocket(server)
        server.serializeAttachment(options({"setup": False}))
        return Response.new(None, options({"status": 101, "webSocket": client}))

    def send_text(self, token, last):
        sockets = self.ctx.getWebSockets()
        if not len(sockets):
            raise RuntimeError("Relay disconnected")
        # Plain model text must not become SSML. Preserve whitespace between tokens.
        sockets[0].send(json.dumps({"type": "text", "token": escape(token, quote=False),
                                   "last": last, "interruptible": True, "preemptible": False}))

    def relay_event(self, kind, **fields):
        state = self.engine.s
        state["relay_sequence"] += 1
        event = dict(call=state["call"], session=state["session"], sequence=state["relay_sequence"],
                     track="inbound_track", receivedAt=now_ms(), type=kind, **fields)
        return event

    async def publish_relay(self, event):
        host = urlsplit(self.env.PUBLIC_VOICE_URL).hostname
        await self.env.TRANSCRIPTS.getByName(host).publish(json.dumps(event))

    def webSocketMessage(self, ws, message):
        try:
            if not isinstance(message, str) or len(message) > 16384:
                raise ValueError("Invalid message")
            data = json.loads(message)
            if not isinstance(data, dict) or not self.engine or not self.engine.live():
                raise ValueError("Call not active")
            state = self.engine.s
            if data.get("type") == "setup":
                if ws.deserializeAttachment().setup:
                    raise ValueError("Repeated setup")
                if data.get("callSid") != state["call"] or data.get("accountSid") != state["account"]:
                    raise ValueError("Wrong call")
                session = data.get("sessionId")
                if not isinstance(session, str) or not session.startswith("VX") or len(session) != 34:
                    raise ValueError("Invalid session")
                state["session"] = session
                ws.serializeAttachment(options({"setup": True}))
                event = self.relay_event("transcription-started")
                self.engine.receive(event)
                self.spawn(self.publish_relay(event))
                return
            if not ws.deserializeAttachment().setup:
                raise ValueError("Setup required")
            if data.get("type") == "prompt":
                text, last = data.get("voicePrompt"), data.get("last")
                if not isinstance(text, str) or len(text) > 4000 or not isinstance(last, bool):
                    raise ValueError("Invalid prompt")
                event = self.relay_event("transcription-content", text=text, final=last)
                self.engine.receive(event)
                self.spawn(self.publish_relay(event))
            elif data.get("type") == "interrupt":
                heard = data.get("utteranceUntilInterrupt", "")
                if not isinstance(heard, str):
                    raise ValueError("Invalid interrupt")
                self.engine.barge_in(heard)
            elif data.get("type") == "error":
                self.engine.barge_in()
                self.spawn(self.emit("agent-error", error="Twilio ConversationRelay reported an error. Check Twilio call diagnostics."))
                # Close so Twilio's Connect action can select the standard-voice fallback.
                ws.close(1011, "Relay error")
        except (ValueError, TypeError, KeyError):
            ws.close(1008, "Invalid relay message")

    def webSocketClose(self, ws, code, reason, was_clean):
        if self.engine and self.engine.s.get("transport") == "relay" and not self.engine.s["ended"]:
            event = self.relay_event("transcription-stopped")
            self.engine.receive(event)
            self.spawn(self.publish_relay(event))
        ws.close(code, "")

    def webSocketError(self, ws, error):
        self.webSocketClose(ws, 1011, "", False)

    async def fallback(self):
        if not self.engine or self.remaining() <= 0:
            return False
        if self.engine.s.get("transport") != "relay":
            return False
        self.engine.cancel_draft()
        if self.engine.reply_generation:
            self.engine.reply_generation.cancel()
        state = self.engine.s
        state.update(transport="say", ended=False, speaking=0, pending_text="", session="", sequence=-1, speculated=False)
        state["revision"] += 1
        self.engine = self.make_engine(state)
        self.save(state)
        await self.emit("agent-error", error="ElevenLabs relay unavailable; continuing with the standard Twilio voice. Verify ConversationRelay is enabled in Twilio.")
        return True

    async def played(self, revision):
        if not self.engine:
            return listening_xml(0)
        self.engine.played(int(revision))
        return listening_xml(self.remaining())

    def remaining(self):
        return max(0, math.ceil((self.engine.s["deadline"] - now_ms()) / 1000))

    async def update_call(self, text, revision):
        state = self.engine.s
        public = urlsplit(self.env.PUBLIC_VOICE_URL)
        callback = (f"{public.scheme}://{public.netloc}/agent/listen?turn={revision}" if text else "")
        twiml = listening_xml(self.remaining(), text, callback)
        credentials = base64.b64encode((state["account"] + ":" + self.env.TWILIO_AUTH_TOKEN).encode()).decode()
        await request_json(f'https://api.twilio.com/2010-04-01/Accounts/{state["account"]}/Calls/{state["call"]}.json',
            {"Authorization": "Basic " + credentials, "Content-Type": "application/x-www-form-urlencoded"},
            urlencode({"Twiml": twiml}), timeout=5)

    async def generate(self, history, on_text=None):
        messages = [{"role": "system", "content": SYSTEM_PROMPT}] + history[-12:]
        sources = []
        # At most two search rounds, and three short model calls per generation.
        async with asyncio.timeout(15):
            for round_number in range(3):
                headers = {"Authorization": "Bearer " + self.env.OPENAI_API_KEY, "Content-Type": "application/json"}
                payload = {"model": getattr(self.env, "OPENAI_MODEL", "gpt-4.1-nano"),
                        "messages": messages, "max_completion_tokens": 160, "temperature": 0.3,
                        "store": False, "tools": [SEARCH_TOOL], "parallel_tool_calls": False,
                        "tool_choice": "auto" if round_number < 2 else "none"}
                if on_text:
                    message = await request_chat_stream(headers, payload, on_text)
                else:
                    result = await request_json("https://api.openai.com/v1/chat/completions", headers, json.dumps(payload))
                    message = result["choices"][0]["message"]
                calls = message.get("tool_calls") or []
                if not calls:
                    text = (message.get("content") or "").strip()
                    if not text:
                        raise ValueError("Empty reply")
                    return {"text": text[:800], "sources": sources}
                messages.append({"role": "assistant", "content": message.get("content"), "tool_calls": calls})
                for call in calls[:1]:
                    args = json.loads(call["function"]["arguments"])
                    query = args.get("query")
                    if call["function"]["name"] != "search_records" or not isinstance(query, str) or not 0 < len(query) <= 500:
                        raise ValueError("Invalid search tool call")
                    host = urlsplit(self.env.PUBLIC_VOICE_URL).hostname
                    try:
                        async with asyncio.timeout(2):
                            records = json.loads(await self.env.KNOWLEDGE.getByName(host).search(query))
                        result_data = {"records": records, "status": "found" if records else "no_matches"}
                        sources.extend({"id": r["id"], "title": r["title"], "source": r["source"]} for r in records)
                    except Exception:
                        result_data = {"records": [], "status": "search_unavailable"}
                    messages.append({"role": "tool", "tool_call_id": call["id"], "content": json.dumps(result_data)})
        raise ValueError("Tool round limit")

    async def emit(self, kind, **fields):
        state = self.engine.s
        state["event_sequence"] += 1
        self.save(state)
        event = dict(call=state["call"], session="agent-" + state["call"],
            sequence=state["event_sequence"], track="agent", receivedAt=now_ms(), type=kind, **fields)
        host = urlsplit(self.env.PUBLIC_VOICE_URL).hostname
        await self.env.TRANSCRIPTS.getByName(host).publish(json.dumps(event))

    async def alarm(self):
        if self.engine and not self.engine.s["ended"]:
            self.engine.s["ended"] = True
            self.engine.cancel_draft()
            if self.engine.reply_generation:
                self.engine.reply_generation.cancel()
            self.save(self.engine.s)
            try:
                async with self.engine.lock:
                    await self.update_call("", None)
            except Exception:
                await self.emit("agent-error", error="Could not end the call at the session limit.")
            await self.ctx.storage.setAlarm(now_ms() + 3600000)
        else:
            self.ctx.storage.sql.exec("DELETE FROM call_state")
            self.engine = None
