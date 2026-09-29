# Twilio paralegal learning rebuild

This branch is a clean, non-production learning environment. The working prototype remains in
`/home/genehuntley/twilio_paralegal` and can be consulted without copying its implementation.

## First milestone

1. Learn the incoming-call flow with a Twilio-hosted TwiML Bin.
2. Add a pure `voice_xml(greeting)` function in `src/twiml.py`.
3. Write its tests in `tests/test_twiml.py` before implementing it.
4. Connect the tested function to `POST /voice` in the Worker.

Run portable tests with:

```bash
npm run check
```

Run the Worker locally with:

```bash
npm run dev
```

There is intentionally no deploy script yet. Production account IDs, custom domains, secrets,
Durable Objects, Twilio numbers, and provider integrations will be added only when their learning
milestone requires them.
