"""One SQLite-backed live console per workspace hostname."""
import json

from js import Object, Response, WebSocketPair
from pyodide.ffi import to_js
from workers import DurableObject

from core import now_ms

RETENTION_MS = 60 * 60 * 1000


class TranscriptRoom(DurableObject):
    """Persist recent transcript events and fan them out to authenticated viewers."""

    def __init__(self, ctx, env):
        """Create the idempotent SQLite schema when this object instance starts."""
        super().__init__(ctx, env)
        self.ctx.storage.sql.exec("""CREATE TABLE IF NOT EXISTS events (
            session TEXT NOT NULL, sequence INTEGER NOT NULL, track TEXT NOT NULL,
            received INTEGER NOT NULL, payload TEXT NOT NULL,
            PRIMARY KEY (session, sequence, track))""")

    def prune(self):
        """Delete expired events and enforce the 500-event storage ceiling."""
        self.ctx.storage.sql.exec("DELETE FROM events WHERE received <= ?", now_ms() - RETENTION_MS)
        self.ctx.storage.sql.exec("DELETE FROM events WHERE rowid NOT IN (SELECT rowid FROM events ORDER BY received DESC, rowid DESC LIMIT 500)")

    def snapshot(self):
        """Return retained events in arrival order for reconnecting consoles."""
        self.prune()
        rows = self.ctx.storage.sql.exec("SELECT payload FROM events ORDER BY received, rowid").toArray()
        return [json.loads(row.payload) for row in rows]

    async def publish(self, payload):
        """Store one callback once, schedule cleanup, and broadcast it live."""
        event = json.loads(payload)
        self.prune()
        result = self.ctx.storage.sql.exec("INSERT OR IGNORE INTO events VALUES (?, ?, ?, ?, ?)",
            event["session"], event["sequence"], event["track"], event["receivedAt"], payload)
        # The composite primary key makes Twilio webhook retries idempotent.
        if not result.rowsWritten:
            return
        self.prune()
        await self.ctx.storage.setAlarm(now_ms() + 60000)
        self.broadcast({"type": "event", "event": event})

    def broadcast(self, message):
        """Send a JSON message to active viewers and close expired sessions."""
        for ws in self.ctx.getWebSockets():
            try:
                if ws.deserializeAttachment().expires <= now_ms():
                    ws.close(1008, "Session expired")
                else:
                    ws.send(json.dumps(message))
            except Exception:
                ws.close(1011, "Reconnect")

    async def fetch(self, request):
        """Upgrade an authorized internal request to a hibernating WebSocket."""
        try:
            expires = int(request.headers.get("X-Session-Expires") or "0")
        except ValueError:
            expires = 0
        if expires <= now_ms():
            return Response.new("Unauthorized", to_js({"status": 401}, dict_converter=Object.fromEntries))
        if len(self.ctx.getWebSockets()) >= 20:
            return Response.new("Too many viewers", to_js({"status": 429}, dict_converter=Object.fromEntries))
        client, server = Object.values(WebSocketPair.new())
        self.ctx.acceptWebSocket(server)
        # Attachments survive Durable Object hibernation with the connection.
        server.serializeAttachment(to_js({"expires": expires}, dict_converter=Object.fromEntries))
        server.send(json.dumps({"type": "snapshot", "events": self.snapshot()}))
        await self.ctx.storage.setAlarm(now_ms() + 60000)
        return Response.new(None, to_js({"status": 101, "webSocket": client}, dict_converter=Object.fromEntries))

    def webSocketMessage(self, ws, message):
        """Answer heartbeats; reject all other client messages (console is read-only)."""
        if ws.deserializeAttachment().expires <= now_ms():
            ws.close(1008, "Session expired")
        elif message == "ping":
            ws.send("pong")
        else:
            ws.close(1008, "Read-only console")

    def webSocketClose(self, ws, code, reason, was_clean):
        """Complete the WebSocket close handshake."""
        ws.close(code, "")

    def webSocketError(self, ws, error):
        """Close a failed socket so the browser can reconnect cleanly."""
        ws.close(1011, "Reconnect")

    async def alarm(self):
        """Prune storage, refresh viewers, and reschedule while data/viewers remain."""
        events = self.snapshot()
        self.broadcast({"type": "snapshot", "events": events})
        if events or len(self.ctx.getWebSockets()):
            await self.ctx.storage.setAlarm(now_ms() + 60000)
