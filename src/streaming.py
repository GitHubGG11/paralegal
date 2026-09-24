"""Bounded, incremental SSE decoding independent of network chunk boundaries."""
import codecs
import json


class SSEDecoder:
    def __init__(self):
        self.decoder = codecs.getincrementaldecoder("utf-8")()
        self.buffer = ""
        self.data = []
        self.total = 0
        self.done = False

    def feed(self, chunk, final=False):
        self.total += len(chunk)
        if self.total > 262144:
            raise ValueError("Model stream too large")
        self.buffer += self.decoder.decode(chunk, final=final)
        if final:
            self.buffer += "\n\n"
        events = []
        while "\n" in self.buffer:
            line, self.buffer = self.buffer.split("\n", 1)
            line = line.rstrip("\r")
            if not line:
                if self.data:
                    data = "\n".join(self.data)
                    self.data = []
                    if data == "[DONE]":
                        self.done = True
                    elif not self.done:
                        events.append(json.loads(data))
            elif line.startswith("data:"):
                self.data.append(line[5:].lstrip(" "))
        if len(self.buffer) > 65536:
            raise ValueError("Model event too large")
        return events


class ChatDeltas:
    def __init__(self, on_text):
        self.on_text = on_text
        self.content = ""
        self.calls = {}
        self.finished = False

    def accept(self, event):
        if event.get("error"):
            raise ValueError("Model stream error")
        for choice in event.get("choices", []):
            if choice.get("index", 0) != 0:
                continue
            delta = choice.get("delta", {})
            text = delta.get("content") or ""
            if text:
                self.content += text
                self.on_text(text)
            for piece in delta.get("tool_calls", []):
                index = piece.get("index", 0)
                if index != 0:
                    raise ValueError("Parallel tool calls are disabled")
                call = self.calls.setdefault(index, {"id": "", "type": "function", "function": {"name": "", "arguments": ""}})
                if piece.get("id"):
                    call["id"] = piece["id"]
                function = piece.get("function", {})
                for field in ("name", "arguments"):
                    call["function"][field] += function.get(field) or ""
                if len(call["function"]["arguments"]) > 4096:
                    raise ValueError("Tool arguments too large")
            if choice.get("finish_reason"):
                if choice["finish_reason"] not in ("stop", "tool_calls", "length"):
                    raise ValueError("Model did not produce a usable reply")
                self.finished = True

    def message(self):
        if not self.finished:
            raise ValueError("Model stream ended prematurely")
        result = {"role": "assistant", "content": self.content or None}
        if self.calls:
            result["tool_calls"] = list(self.calls.values())
        return result
