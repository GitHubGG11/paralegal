"""Streaming turn coordination; Twilio owns ElevenLabs audio buffering and barge-in."""
import asyncio
import copy

from conversation import Conversation


class Draft:
    def __init__(self):
        self.text = ""
        self.sink = None

    def push(self, token):
        token = token[:max(0, 800 - len(self.text))]
        if not token:
            return
        self.text += token
        if self.sink:
            self.sink(token)


class RelayConversation(Conversation):
    def __init__(self, state, save, generate, update_call, emit, spawn, send_text, **kwargs):
        super().__init__(state, save, generate, update_call, emit, spawn, **kwargs)
        self.send_text = send_text
        self.streams = {}

    def new_generation(self, history):
        draft = Draft()
        if self.s["generations"] >= 100:
            task = self.spawn(self.limit_reply())
        else:
            self.s["generations"] += 1
            self.commit()
            task = self.spawn(self.generate(copy.deepcopy(history), draft.push))
        self.streams[task] = draft
        return task

    def cancel_draft(self):
        if self.draft:
            self.streams.pop(self.draft, None)
        super().cancel_draft()

    def receive(self, event):
        # A new prompt without an interrupt notification means prior playback completed.
        if (self.live() and event.get("text", "").strip() and
                event["sequence"] > self.s["sequence"] and self.s["speaking"]):
            self.remember_reply()
        super().receive(event)

    def barge_in(self, heard=""):
        self.s["revision"] += 1
        self.cancel_draft()
        if self.reply_generation and not self.reply_generation.done():
            self.reply_generation.cancel()
        if self.s["pending_text"]:
            # Twilio reports what actually played; never include the unsaid suffix.
            self.s["pending_text"] = (heard[:800] if heard else "[Reply interrupted before any confirmed words were heard.]")
            self.remember_reply()
        self.s["speaking"] = 0
        self.commit()
        self.spawn(self.emit("agent-interrupted"))

    async def finish(self, task, revision, received_at, reused):
        draft = self.streams.pop(task)
        first_token_at = None

        def send(token):
            nonlocal first_token_at
            if not self.live(revision):
                return
            if first_token_at is None:
                first_token_at = self.clock()
                self.s["speaking"] = revision
                self.s["pending_text"] = ""
                self.commit()
            self.send_text(token, False)
            self.s["pending_text"] += token

        try:
            if not self.live(revision):
                task.cancel()
                return
            # A speculative stream is silent until the final transcript matches.
            if draft.text:
                send(draft.text)
            draft.sink = send
            result = await asyncio.shield(task)
            if not self.live(revision):
                return
            if not draft.text:
                send(result["text"])
            self.send_text("", True)
            self.commit()
            await self.emit("agent-reply", text=self.s["pending_text"], sources=result.get("sources", []),
                latencyMs=max(0, (first_token_at or self.clock()) - received_at),
                generationMs=max(0, self.clock() - received_at), speculativeHit=reused, streaming=True)
        except asyncio.CancelledError:
            return
        except Exception:
            if self.live(revision):
                fallback = " Sorry, I couldn't finish that request. Please try again."
                try:
                    send(fallback)
                    self.send_text("", True)
                    self.commit()
                except Exception:
                    pass
                await self.emit("agent-error", error="Streaming reply failed or timed out.")
        finally:
            draft.sink = None
