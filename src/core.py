"""Portable Python logic: Twilio signatures, sessions, TwiML and event validation."""
import base64
import hashlib
import hmac
import json
import re
import time
from urllib.parse import parse_qs
from xml.etree.ElementTree import Element, SubElement, tostring

ANNOUNCEMENT = (
    "This call will be transcribed live. Please hang up if you do not want to be "
    "transcribed. After the pause, speak naturally. This session lasts up to ten minutes."
)


def now_ms():
    """Return Unix time in milliseconds, matching browser and Durable Object timestamps."""
    return int(time.time() * 1000)


def parse_form(body):
    """Parse a bounded URL-encoded body while preserving repeated and empty fields."""
    return parse_qs(body, keep_blank_values=True, max_num_fields=1000)


def first(form, key, default=""):
    """Return the first value from parse_qs's list-valued mapping."""
    return form.get(key, [default])[0]


def valid_twilio_signature(token, url, form, signature):
    """Verify Twilio's HMAC-SHA1 signature without timing-sensitive comparison."""
    # Twilio's documented form signature: URL + sorted names and unique values.
    data = url + "".join(key + value for key in sorted(form) for value in sorted(set(form[key])))
    expected = base64.b64encode(hmac.new(token.encode(), data.encode(), hashlib.sha1).digest())
    return hmac.compare_digest(expected, signature.encode())


def equal_secret(a, b):
    """Compare arbitrary strings in constant time after normalizing their lengths."""
    return hmac.compare_digest(hashlib.sha256(a.encode()).digest(), hashlib.sha256(b.encode()).digest())


def session_signature(expires, secret):
    """Sign a console-session expiry so clients cannot extend their own session."""
    return hmac.new(secret.encode(), f"console:{expires}".encode(), hashlib.sha256).hexdigest()


def session_cookie(secret):
    """Create an eight-hour, host-only console session cookie."""
    expires = now_ms() + 8 * 60 * 60 * 1000
    return (f"__Host-console={expires}.{session_signature(expires, secret)}; "
            "Path=/; HttpOnly; Secure; SameSite=Strict; Max-Age=28800")


def session_expiry(cookie, secret):
    """Return a valid session's expiry timestamp, or zero for missing/invalid cookies."""
    if not secret:
        return 0
    match = re.search(r"(?:^|;\s*)__Host-console=(\d+)\.([a-f0-9]{64})(?:;|$)", cookie or "")
    if not match or int(match[1]) <= now_ms():
        return 0
    return int(match[1]) if equal_secret(match[2], session_signature(match[1], secret)) else 0


def voice_xml(origin, agent=False):
    """Build the TwiML that announces, starts, and eventually stops transcription."""
    response = Element("Response")
    SubElement(response, "Say").text = (
        "How may I help you?"
        if agent else ANNOUNCEMENT)
    SubElement(SubElement(response, "Start"), "Transcription",
               statusCallbackUrl=origin + "/transcription", track="inbound_track",
               partialResults="true", languageCode="en-US",
               transcriptionEngine="google", speechModel="telephony")
    if agent:
        # SubElement(response, "Say").text = "How can I help you?"
        pass
    for _ in range(10):
        SubElement(response, "Pause", length="60")
    SubElement(SubElement(response, "Stop"), "Transcription")
    SubElement(response, "Say").text = "The transcription session has ended. Goodbye."
    SubElement(response, "Hangup")
    return '<?xml version="1.0" encoding="UTF-8"?>' + tostring(response, encoding="unicode")


def listening_xml(seconds, text="", callback=""):
    """Replace speech on barge-in, or speak then acknowledge actual playback completion."""
    response = Element("Response")
    if text:
        SubElement(response, "Say").text = text
    if callback:
        SubElement(response, "Redirect", method="POST").text = callback
    else:
        remaining = max(0, min(600, int(seconds)))
        while remaining:
            length = min(60, remaining)
            SubElement(response, "Pause", length=str(length))
            remaining -= length
        SubElement(SubElement(response, "Stop"), "Transcription")
        SubElement(response, "Hangup")
    return tostring(response, encoding="unicode")


def relay_xml(origin, call, voice):
    """ElevenLabs Flash speech receives streamed text over a signed Twilio WebSocket."""
    response = Element("Response")
    connect = SubElement(response, "Connect", action=origin + "/relay/ended", method="POST")
    SubElement(connect, "ConversationRelay", url=origin.replace("https://", "wss://") + "/relay/" + call,
        ttsProvider="ElevenLabs", voice=voice, language="en-US",
        transcriptionProvider="Deepgram", speechModel="flux", partialPrompts="true", eotThreshold="0.7",
        interruptible="speech", interruptSensitivity="high", reportInputDuringAgentSpeech="speech",
        welcomeGreeting="How may I help you?", welcomeGreetingInterruptible="speech")
    SubElement(response, "Hangup")
    return tostring(response, encoding="unicode")


def parse_transcription(form):
    """Validate a Twilio transcription callback and reduce it to stored fields."""
    call, session = first(form, "CallSid"), first(form, "TranscriptionSid")
    kind, raw_sequence = first(form, "TranscriptionEvent"), first(form, "SequenceId")
    if not (re.fullmatch(r"CA[a-f0-9]{32}", call, re.I) and
            re.fullmatch(r"GT[a-f0-9]{32}", session, re.I) and
            re.fullmatch(r"\d{1,15}", raw_sequence) and kind in (
                "transcription-started", "transcription-content", "transcription-stopped", "transcription-error")):
        raise ValueError("Invalid event")
    event = dict(call=call, session=session, type=kind, sequence=int(raw_sequence),
                 receivedAt=now_ms(), track=first(form, "Track", "inbound_track"))
    if kind == "transcription-content":
        data = json.loads(first(form, "TranscriptionData"))
        final = first(form, "Final")
        if not isinstance(data, dict) or not isinstance(data.get("transcript"), str) or len(data["transcript"]) > 16000 or final not in ("true", "false"):
            raise ValueError("Invalid content")
        event.update(text=data["transcript"], final=final == "true")
    if kind == "transcription-error":
        event.update(error=first(form, "TranscriptionError", "Transcription failed")[:1000],
                     code=first(form, "TranscriptionErrorCode")[:20])
    return event
