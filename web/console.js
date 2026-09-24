// Browser client for the private live-transcription console.
// It contains presentation and WebSocket state only; authentication and
// transcript persistence stay in the Python Worker.

const $ = id => document.getElementById(id);
let socket, retry, heartbeat, stopped = false, backoff = 1000;
let events = new Map();
// Twilio sequence numbers are per transcription session and audio track.
const key = e => e.session + ':' + e.track + ':' + e.sequence;
// Update the connection badge shown in the page header.
function status(text, online = false) { $('connection').textContent = text; $('connection').classList.toggle('online', online); }
// Create text-only DOM nodes so transcripts are never interpreted as HTML.
function element(tag, text, className) { const node = document.createElement(tag); node.textContent = text; if (className) node.className = className; return node; }
// Group retained events by call/session and replace the visible call cards.
function render() {
  const now = Date.now();
  for (const [id, e] of events) if (now - e.receivedAt >= 3600000) events.delete(id);
  events = new Map([...events].sort((a,b) => a[1].receivedAt - b[1].receivedAt).slice(-500));
  const calls = new Map();
  for (const e of events.values()) { if (!calls.has(e.call)) calls.set(e.call, []); calls.get(e.call).push(e); }
  const nodes = [];
  for (const [call, all] of [...calls].reverse()) {
    const card = document.createElement('article');
    const head = element('div', '', 'callhead');
    head.append(element('h2', 'Call · ' + call.slice(-8)), element('span', new Date(Math.min(...all.map(e => e.receivedAt))).toLocaleTimeString(), 'meta'));
    card.append(head, element('p', call, 'meta'));
    const sessions = new Map();
    for (const e of all) { if (!sessions.has(e.session)) sessions.set(e.session, []); sessions.get(e.session).push(e); }
    for (const list of sessions.values()) {
      list.sort((a,b) => a.sequence - b.sequence);
      if (list[0]?.track === 'agent') {
        for (const e of list) {
          if (e.type === 'agent-reply') {
            card.append(element('p', `Assistant · ${e.latencyMs} ms to ${e.streaming ? 'first text sent to speech' : 'playback request'}${e.speculativeHit ? ' · prepared draft reused' : ''}`, 'meta'));
            card.append(element('div', e.text, 'transcript'));
            if (e.sources?.length) card.append(element('p', 'Sources: ' + [...new Set(e.sources.map(s => s.title || s.source || s.id))].join(' · '), 'meta'));
          } else if (e.type === 'agent-interrupted') card.append(element('p', 'Caller interrupted playback', 'meta'));
          else if (e.type === 'agent-error') card.append(element('p', e.error, 'error'));
        }
        continue;
      }
      const ended = list.some(e => e.type === 'transcription-stopped');
      const error = list.filter(e => e.type === 'transcription-error').at(-1);
      card.append(element('p', error ? 'Transcription error · ' + error.code + ' · ' + error.error : ended ? 'Transcription ended' : 'Listening', error ? 'error' : 'meta'));
      const content = list.filter(e => e.type === 'transcription-content');
      const finals = content.filter(e => e.final);
      if (finals.length) card.append(element('div', finals.map(e => e.text).join('\n'), 'transcript'));
      const latest = content.at(-1);
      if (latest && !latest.final && !ended && !error) card.append(element('div', latest.text + ' …', 'transcript partial'));
      if (!content.length && !ended && !error) card.append(element('p', 'Waiting for speech…', 'partial'));
    }
    nodes.push(card);
  }
  if (!nodes.length) { const empty = element('section', '', 'empty'); empty.append(element('strong', 'Ready for your call'), element('p', 'Dial your Twilio number. After the announcement, start speaking.')); nodes.push(empty); }
  $('calls').replaceChildren(...nodes);
}
function connect() {
  // Open the authenticated stream and reconnect with capped exponential delay.
  clearTimeout(retry); clearInterval(heartbeat);
  status('Connecting…');
  const ws = new WebSocket(location.origin.replace(/^http/, 'ws') + '/live');
  socket = ws;
  ws.onopen = () => { backoff = 1000; $('login').hidden = true; $('viewer').hidden = false; $('error').textContent = ''; status('Connected · live', true); heartbeat = setInterval(() => { if (ws.readyState === WebSocket.OPEN) ws.send('ping'); }, 30000); };
  ws.onmessage = message => {
    if (message.data === 'pong') return;
    const data = JSON.parse(message.data);
    // A snapshot replaces local state after connect; events increment it live.
    if (data.type === 'snapshot') events = new Map(data.events.map(e => [key(e), e]));
    if (data.type === 'event') events.set(key(data.event), data.event);
    render();
  };
  ws.onclose = event => {
    if (socket !== ws) return;
    clearInterval(heartbeat);
    if (stopped) return;
    if (event.code === 1008) { stopped = true; events.clear(); render(); $('viewer').hidden = true; $('login').hidden = false; $('error').textContent = 'Session expired. Enter your password again.'; status('Signed out'); return; }
    status('Disconnected · retrying');
    $('login').hidden = false;
    retry = setTimeout(connect, backoff); backoff = Math.min(backoff * 2, 30000);
  };
}
$('login-form').addEventListener('submit', async event => {
  // Exchange the password for an HttpOnly cookie, then discard the input.
  event.preventDefault(); $('connect').disabled = true; $('error').textContent = '';
  try {
    const response = await fetch('/session', {method:'POST', body: new URLSearchParams({password:$('password').value})});
    if (!response.ok) throw new Error(await response.text());
    $('password').value = ''; stopped = false;
    if (socket) { socket.onclose = null; socket.close(); }
    connect();
  } catch (error) { $('error').textContent = error.message; }
  finally { $('connect').disabled = false; }
});
$('disconnect').addEventListener('click', () => {
  // Stop reconnect attempts and clear transcript text from the page.
  stopped = true; clearTimeout(retry); clearInterval(heartbeat); socket?.close(); events.clear(); render();
  $('viewer').hidden = true; $('login').hidden = false; status('Disconnected');
});
render();
// Reconnect automatically only when the browser already has a valid session.
fetch('/session').then(response => {
  if (response.ok) connect();
  else status('Sign in to connect');
}).catch(() => status('Connection unavailable'));

$('knowledge-file').addEventListener('change', async event => {
  const file = event.target.files[0];
  if (!file) return;
  const output = $('knowledge-status');
  try {
    if (file.size > 10 * 1024 * 1024) throw new Error('Use the import script for files larger than 10 MB.');
    const records = JSON.parse(await file.text());
    if (!Array.isArray(records)) throw new Error('Expected a JSON array of records.');
    let imported = 0, batch = [];
    const send = async () => {
      if (!batch.length) return;
      const response = await fetch('/knowledge', {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(batch)});
      if (!response.ok) throw new Error(await response.text());
      imported += (await response.json()).imported;
      output.textContent = `${imported} records imported.`;
      batch = [];
    };
    for (const record of records) {
      if (new TextEncoder().encode(JSON.stringify([record])).length > 60000) throw new Error('A record is too large; split it into smaller records.');
      if (batch.length === 100 || new TextEncoder().encode(JSON.stringify([...batch, record])).length > 60000) await send();
      batch.push(record);
    }
    await send();
    output.textContent = `${imported} records imported. Callers can now search them.`;
  } catch (error) { output.textContent = `Import stopped: ${error.message} Earlier batches remain saved.`; }
  finally { event.target.value = ''; }
});
