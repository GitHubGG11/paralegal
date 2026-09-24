"""Cloudflare Python Worker: voice webhook and private live console."""
import json
import re
from urllib.parse import parse_qs, urlsplit

from js import Object, Request, Response, console
from pyodide.ffi import to_js
from workers import WorkerEntrypoint

from console_assets import PAGE, SCRIPT
from core import (equal_secret, first, parse_form, parse_transcription,
                  session_cookie, session_expiry, valid_twilio_signature, voice_xml, relay_xml, listening_xml)
from room import TranscriptRoom
from agent import CallAgent
from knowledge import KnowledgeBase, validate_records


def js_options(value):
    """Convert a Python mapping to a JavaScript object for Workers runtime APIs."""
    return to_js(value, dict_converter=Object.fromEntries)


def reply(body, status=200, content_type="text/plain; charset=utf-8", headers=None):
    """Create a no-store HTTP response with consistent content headers."""
    return Response.new(body, js_options({"status": status, "headers": {
        "Content-Type": content_type, "Cache-Control": "no-store", **(headers or {})}}))


async def read_body(request):
    """Decode a UTF-8 request stream, rejecting bodies larger than 64 KiB."""
    if not request.body:
        return ""
    reader = request.body.getReader()
    data = bytearray()
    try:
        while True:
            chunk = await reader.read()
            if chunk.done:
                break
            if len(data) + chunk.value.byteLength > 65536:
                await reader.cancel()
                raise OverflowError("Body too large")
            data.extend(bytes(chunk.value.to_py()))
    finally:
        reader.releaseLock()
    return data.decode("utf-8")


class Default(WorkerEntrypoint):
    """Public Worker entrypoint for health, console, voice, and callback routes."""

    async def fetch(self, request):
        """Handle one request and fail closed without logging private call data."""
        try:
            return await self.route(request)
        except Exception:
            # Never print callback bodies, credentials or transcript contents.
            console.error(json.dumps({"event": "request_handler_error"}))
            return reply("Internal server error", 500)

    async def route(self, request):
        """Dispatch public routes and enforce route-specific authentication."""
        url = urlsplit(request.url)
        public = urlsplit(self.env.PUBLIC_VOICE_URL)
        origin = f"{public.scheme}://{public.netloc}"
        # A stable name maps every request for this hostname to the same room.
        room = lambda: self.env.TRANSCRIPTS.getByName(public.hostname)
        password = getattr(self.env, "CONSOLE_PASSWORD", None)
        headers = request.headers
        relay_match = re.fullmatch(r"/relay/(CA[a-fA-F0-9]{32})", url.path)
        if relay_match:
            if request.method != "GET" or (headers.get("Upgrade") or "").lower() != "websocket":
                return reply("Expected WebSocket", 426)
            token = getattr(self.env, "TWILIO_AUTH_TOKEN", None)
            signature = headers.get("X-Twilio-Signature") or ""
            # Validate only this configured public host/path, never a client-controlled Host.
            signed_url = origin + url.path + ("?" + url.query if url.query else "")
            if not token or not any(valid_twilio_signature(token, candidate, {}, signature) for candidate in
                                    (signed_url, signed_url.replace("https://", "wss://", 1))):
                return reply("Forbidden", 403)
            return await self.env.CALLS.getByName(relay_match[1]).fetch(Request.new(request.js_object))
        if url.path == "/health" and request.method == "GET":
            return reply("ok", headers={"X-Backend": "python"})
        if request.method == "GET" and url.path in ("/", "/console", "/console.js"):
            is_script = url.path == "/console.js"
            return reply(SCRIPT if is_script else PAGE, content_type=(
                "text/javascript; charset=utf-8" if is_script else "text/html; charset=utf-8"), headers={
                    "Content-Security-Policy": "default-src 'none'; script-src 'self'; style-src 'unsafe-inline'; connect-src 'self'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'",
                    "X-Content-Type-Options": "nosniff", "Referrer-Policy": "no-referrer"})
        if url.path == "/session":
            if request.method == "GET":
                valid = session_expiry(headers.get("Cookie"), password)
                return reply("ok" if valid else "Sign in", 200 if valid else 401)
            if request.method != "POST":
                return reply("Method not allowed", 405)
            if headers.get("Origin") != origin:
                # Origin checking prevents another site from submitting this form.
                return reply("Forbidden", 403)
            if not password:
                return reply("Console password not configured", 503)
            try:
                supplied = first(parse_form(await read_body(request)), "password")
            except (ValueError, OverflowError):
                return reply("Invalid request", 400)
            if not equal_secret(supplied, password):
                return reply("Incorrect password", 401)
            return reply("ok", headers={"Set-Cookie": session_cookie(password)})
        if url.path == "/live":
            if request.method != "GET":
                return reply("Method not allowed", 405)
            if headers.get("Origin") != origin:
                return reply("Forbidden", 403)
            expires = session_expiry(headers.get("Cookie"), password)
            if not expires:
                return reply("Sign in to the console", 401)
            if (headers.get("Upgrade") or "").lower() != "websocket":
                return reply("Expected WebSocket", 426)
            internal = Request.new(request.js_object)
            # The public Worker validates the cookie; the room receives only its expiry.
            internal.headers.set("X-Session-Expires", str(expires))
            return await room().fetch(internal)
        if url.path in ("/knowledge", "/agent/check"):
            if not session_expiry(headers.get("Cookie"), password):
                return reply("Sign in to the console", 401)
            knowledge = self.env.KNOWLEDGE.getByName(public.hostname)
            if url.path == "/knowledge" and request.method == "GET":
                return reply(await knowledge.status(), content_type="application/json")
            if request.method != "POST":
                return reply("Method not allowed", 405)
            if headers.get("Origin") != origin:
                return reply("Forbidden", 403)
            if url.path == "/agent/check":
                if not getattr(self.env, "OPENAI_API_KEY", None):
                    return reply("OpenAI key not configured", 503)
                result = await self.env.CALLS.getByName("diagnostic:" + public.hostname).check()
                return reply(result, content_type="application/json")
            try:
                body = await read_body(request)
                validate_records(body)
            except (ValueError, OverflowError):
                return reply("Expected 1–100 records with id/title/text/source, at most 64 KiB per batch", 400)
            return reply(await knowledge.ingest(body), content_type="application/json")
        if url.path not in ("/voice", "/transcription", "/agent/listen", "/relay/ended"):
            return reply("Not found", 404)
        if request.method != "POST":
            return reply("Method not allowed", 405)
        signature = headers.get("X-Twilio-Signature")
        if not signature:
            return reply("Forbidden", 403)
        token = getattr(self.env, "TWILIO_AUTH_TOKEN", None)
        if not token:
            return reply("Service not configured", 503)
        if (headers.get("Content-Type") or "").split(";")[0].strip().lower() != "application/x-www-form-urlencoded":
            return reply("Expected form data", 415)
        try:
            form = parse_form(await read_body(request))
        except OverflowError:
            return reply("Invalid request", 413)
        except ValueError:
            return reply("Invalid request", 400)
        signed_url = origin + url.path + ("?" + url.query if url.query else "")
        # Twilio includes the exact public URL, including its query, in the signature.
        if not valid_twilio_signature(token, signed_url, form, signature):
            return reply("Forbidden", 403)
        if url.path == "/transcription":
            try:
                event = parse_transcription(form)
            except (ValueError, TypeError):
                return reply("Invalid transcription event", 400)
            # JSON is an explicit language-independent RPC contract.
            await room().publish(json.dumps(event))
            if getattr(self.env, "OPENAI_API_KEY", None):
                await self.env.CALLS.getByName(event["call"]).receive(json.dumps(event))
            return reply(None, 204)
        if url.path == "/agent/listen":
            call = first(form, "CallSid")
            turn = parse_qs(url.query).get("turn", [""])[0]
            if not re.fullmatch(r"CA[a-fA-F0-9]{32}", call) or not re.fullmatch(r"\d{1,15}", turn):
                return reply("Invalid playback callback", 400)
            xml = await self.env.CALLS.getByName(call).played(int(turn))
            return reply(xml, content_type="application/xml; charset=utf-8")
        if url.path == "/relay/ended":
            call = first(form, "CallSid")
            if not re.fullmatch(r"CA[a-fA-F0-9]{32}", call):
                return reply("Invalid call", 400)
            failed = first(form, "SessionStatus") == "failed"
            if failed and await self.env.CALLS.getByName(call).fallback():
                return reply(voice_xml(origin, agent=True), content_type="application/xml; charset=utf-8")
            return reply(listening_xml(0), content_type="application/xml; charset=utf-8")
        enabled = bool(getattr(self.env, "OPENAI_API_KEY", None))
        if enabled:
            call, account = first(form, "CallSid"), first(form, "AccountSid")
            if not re.fullmatch(r"CA[a-fA-F0-9]{32}", call) or not re.fullmatch(r"AC[a-fA-F0-9]{32}", account):
                return reply("Invalid call identifiers", 400)
            transport = getattr(self.env, "VOICE_TRANSPORT", "say")
            await self.env.CALLS.getByName(call).start(call, account, transport)
            if transport == "relay":
                return reply(relay_xml(origin, call, getattr(self.env, "ELEVENLABS_VOICE", "OYTbf65OHHFELVut7v2H-flash_v2_5")),
                             content_type="application/xml; charset=utf-8")
        return reply(voice_xml(origin, agent=enabled), content_type="application/xml; charset=utf-8")
