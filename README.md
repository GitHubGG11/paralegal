# Twilio voice lookup assistant — Python on Cloudflare

Call the existing Twilio number, ask a question, and receive a short spoken answer. The default path uses Twilio ConversationRelay with Deepgram Flux transcription, GPT-4.1 nano on OpenAI, and ElevenLabs Flash 2.5 speech. Generated text streams to Twilio while the model is still producing the answer. Cloudflare hosts the Python backend and per-call Durable Objects. Callers can interrupt replies. No caller PIN is required.

## Use it

1. Keep the Twilio number's incoming-call webhook set to **POST** `https://twilio.genehuntley.com/voice`.
2. Open https://twilio.genehuntley.com/console and sign in with the password in `.console-password`.
3. Call the number and ask a question after the short greeting.
4. Watch caller transcripts, assistant replies, source references, interruptions and response timing in the console.

The OpenAI API key is stored as the deployed `OPENAI_API_KEY` secret. Existing secrets `TWILIO_AUTH_TOKEN` and `CONSOLE_PASSWORD` are retained. The Twilio Account SID comes from the signed incoming webhook. Never put secret values in source or chat.

## Search your records

The agent has a read-only `search_records` tool. It chooses relevant keywords, queries a SQLite FTS5 index on Cloudflare, and receives at most five matching excerpts. It can refine a search once before answering. The full database is never copied into the prompt. Search uses ranked keyword matching, not vector embeddings. It handles names, subjects and identifiers well; semantic matching and direct email synchronization can be added behind the same tool later.

Import a JSON array from the private console:

```json
[
  {
    "id": "email-123",
    "title": "Orchid project meeting",
    "text": "From: Alex. Date: 2026-09-23. The Orchid meeting is Friday at 10 AM.",
    "source": "mailbox/export/email-123"
  }
]
```

Records require a stable `id` and nonempty `text`. Optional `title` and `source` give the answer attribution. Reimporting the same ID replaces the old record and its index. For emails, normalize subject into `title`, sender/date/body into `text`, and an email reference into `source`. JSON database records can be flattened into readable text, retaining their ID. Long records should be split into chunks with distinct stable IDs; each text field allows up to 40,000 characters.

For large exports, use line-delimited JSON (`.jsonl` or `.ndjson`, one record per line), which streams from disk in bounded batches:

```bash
python3 scripts/import-records.py /path/to/records.jsonl
```

The script reads `.console-password` locally and uploads batches of up to 100 records / 60 KiB. Browser imports accept JSON arrays up to 10 MB. Partial imports keep completed batches; re-run safely using the same IDs. Stored records persist until replaced or administratively removed; they do not have the transcript's one-hour expiry. There is currently no direct Gmail/Outlook connection and no source data is bundled or imported automatically. All callers can search imported records, as requested.

This is a single indexed knowledge store per hostname, not an unbounded distributed database. For datasets beyond one Durable Object's SQLite capacity or heavy concurrent searches, replace `KnowledgeBase.search` with a dedicated database/search service; the agent's tool contract stays the same.

## ElevenLabs streaming setup

`VOICE_TRANSPORT` in `wrangler.jsonc` defaults to `relay`. Twilio receives `<Connect><ConversationRelay>` and opens a signed WebSocket to `/relay/{CallSid}`. The backend validates the handshake and matches the setup message to the existing call/account before accepting prompts. Deepgram Flux provides turn detection and partial prompts; ElevenLabs Flash 2.5 voices the model's incremental text. The configured voice is `OYTbf65OHHFELVut7v2H-flash_v2_5`.

This uses ElevenLabs through Twilio; no separate ElevenLabs API key is needed. ConversationRelay is a separately billed Twilio feature. **Twilio must have the Predictive and Generative AI/ML Features Addendum enabled** under Voice → Settings → Privacy & Security (or General in the legacy console). If the relay reports a failed session, the signed Connect action switches that call to the existing Google-transcription/default-Twilio-voice path and shows a console error. An actual call is needed to verify account enablement and audible ElevenLabs output.

To deliberately use the previous path, set `VOICE_TRANSPORT` to `say` and deploy. Existing calls retain the transport they started with. To select another supported ElevenLabs voice, change `ELEVENLABS_VOICE` and deploy. Keep a Twilio-supported model suffix such as `-flash_v2_5`; `-eleven_v3_conversational` is not listed as supported by ConversationRelay.

## Latency and interruption behavior

- **Short output:** GPT-4.1 nano, 160 output tokens per request, 12 recent conversation messages.
- **Speculative replies:** after a partial transcript remains unchanged for 350 ms and contains at least four words, start one draft for that utterance. Never speak it before a final transcript. Reuse only if the final text matches after whitespace, case and trailing sentence-punctuation normalization; otherwise discard it.
- **Bounded spend:** at most one speculative draft per utterance and 100 generations per call; each generation uses at most three model requests / two read-only searches. Aborting a request does not guarantee the provider stops billing work already done.
- **Streaming and barge-in:** OpenAI SSE text deltas are forwarded to ConversationRelay as soon as available, preserving spaces. Twilio handles ElevenLabs synthesis/playback concurrently. Its interrupt event cancels the OpenAI stream, rejects late chunks, and records only the words Twilio says were heard. Speculative streams remain silent until the matching final transcript arrives. The `say` fallback retains serialized Twilio REST updates for interruption.
- **Playback history:** relay interruptions include the heard prefix, so the unsaid suffix is excluded from history. In the `say` fallback, a signed Twilio redirect acknowledges completion; interrupted replies are marked as potentially unheard.
- **Timeouts:** OpenAI requests have an eight-second timeout, search two seconds, and the entire tool loop fifteen seconds. Failures produce a short spoken fallback and a sanitized console event.
- **Call limit:** ten minutes from call initialization, enforced by a Durable Object alarm even if TwiML is replaced repeatedly.

In relay mode, the console measures final-prompt receipt to the first text token sent to speech, and records full-generation duration separately. This is **not** caller-perceived end-of-speech to first-audio latency: provider buffering, synthesis and telephone transport add delay. The `say` fallback waits for complete text and measures time to the accepted REST playback request. A real phone call is required to assess audible latency and interruption behavior.

## Code map

- `src/entry.py`: routes, console authentication, webhook signatures and request limits.
- `src/core.py`: portable signatures, sessions, transcription parsing and TwiML builders.
- `src/conversation.py`: portable per-call turn state, speculation, cancellation and playback ordering.
- `src/agent.py`: call Durable Object, signed relay WebSocket lifecycle, bounded OpenAI tool loop, Twilio REST fallback and cleanup.
- `src/relay.py`: incremental text delivery, silent speculative drafts and interruption handling.
- `src/streaming.py`: bounded UTF-8/SSE parsing and streamed tool-call reconstruction.
- `src/knowledge.py`: validated JSON imports and parameterized FTS5 retrieval.
- `src/room.py`: console event storage, deduplication and WebSocket replay.
- `web/console.html`, `web/console.js`: console and record import UI.
- `scripts/build_console.py`: packages browser assets into generated `src/console_assets.py`.
- `scripts/import-records.py`: bounded JSON / streaming JSONL importer.
- `tests/`: portable Python unit tests.
- `test/runtime-*.js`: actual packaged Python Worker and SQLite DO tests; provider calls are mocked.
- `scripts/check-live.mjs`: production health, login and WebSocket check; `--agent` adds one synthetic OpenAI request.

The former Flask prototype `twilioresponse` is not deployed.

## Develop, verify, deploy

The workspace already has Python, Node.js, pywrangler and pinned dependencies installed.

```bash
npm run check
npm run dry-run
npm run test:runtime
npm run deploy
node scripts/check-live.mjs --agent
```

For local development, put synthetic `TWILIO_AUTH_TOKEN`, `CONSOLE_PASSWORD` and optionally `OPENAI_API_KEY` in ignored `.dev.vars`, then run `npm run dev`. A real OpenAI key enables real paid API calls in local development. Runtime integration tests always intercept provider traffic and use synthetic keys.

Target: Worker `twilio-paralegal`, account `5e1935bae90ba3281d225e4d3b23dc66`, custom domain `twilio.genehuntley.com`. The existing `TranscriptRoom` migration is preserved. The additional `v2-agent` migration creates `CallAgent` and `KnowledgeBase`. Workers.dev and preview URLs remain disabled.

To change the model, edit `OPENAI_MODEL` in `wrangler.jsonc` and deploy. The current adapter uses the Chat Completions tool API and non-reasoning model parameters; changing model families may require adapter changes.

## Data handling

Twilio receives caller audio; only inbound caller audio is transcribed. This app does not record audio. Transcript text and retrieved excerpts are sent to OpenAI with `store: false`; this does not override OpenAI's provider-level data policies. Console events are bounded to 500 events / one hour. Per-call conversation state is deleted after the call cleanup alarm, no later than roughly one hour after the ten-minute session deadline. Call and knowledge objects are isolated from the console's public HTTP layer; imports require an authenticated console session and matching Origin. Twilio callbacks and relay WebSocket handshakes require valid signatures. Application logs do not print caller transcript text, numbers or secrets.

References: [OpenAI GPT-4.1 nano](https://developers.openai.com/api/docs/models/gpt-4.1-nano), [Twilio transcription](https://www.twilio.com/docs/voice/twiml/transcription), [Twilio Say](https://www.twilio.com/docs/voice/twiml/say), [Twilio call updates](https://www.twilio.com/docs/voice/api/call-resource#update-a-call-resource), [Cloudflare SQLite storage](https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/).

Streaming references: [ConversationRelay messages](https://www.twilio.com/docs/voice/conversationrelay/websocket-messages), [ElevenLabs voices](https://www.twilio.com/docs/voice/conversationrelay/voice-configuration), [Twilio onboarding](https://www.twilio.com/docs/voice/conversationrelay/onboarding), [OpenAI streaming](https://developers.openai.com/api/docs/guides/streaming-responses).
