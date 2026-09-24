// Integration test for the packaged Python Worker in Cloudflare's local
// runtime. Twilio's Node SDK independently creates request signatures.
import {Miniflare, convertV4MiniflareOptions} from 'miniflare';
import webhooks from 'twilio/lib/webhooks/webhooks.js';
import assert from 'node:assert/strict';
import {readdirSync,statSync} from 'node:fs';
import {createHmac} from 'node:crypto';
import path from 'node:path';
const root=path.resolve('dist/python');
// Mirror Wrangler's module bundle so Miniflare executes Python and its SDK.
const files=readdirSync(root,{recursive:true}).filter(f=>!['README.md','python_modules/.synced'].includes(f) && statSync(path.join(root,f)).isFile());
const modules=files.map(f=>({path:path.join(root,f),type:f.startsWith('python_modules/') ? (f.endsWith('.js')?'ESModule':'Data') : 'PythonModule'}));
modules.sort((a,b)=>a.path===path.join(root,'entry.py')?-1:b.path===path.join(root,'entry.py')?1:0);
const origin='https://twilio.genehuntley.com', token='synthetic-test-token';
const mf=new Miniflare(convertV4MiniflareOptions({modules,modulesRoot:root,compatibilityDate:'2026-09-23',compatibilityFlags:['python_workers'],durableObjects:{TRANSCRIPTS:{className:'TranscriptRoom',useSQLite:true},CALLS:{className:'CallAgent',useSQLite:true},KNOWLEDGE:{className:'KnowledgeBase',useSQLite:true}},bindings:{PUBLIC_VOICE_URL:origin+'/voice',TWILIO_AUTH_TOKEN:token,CONSOLE_PASSWORD:'synthetic-console-password'}}));
// Build a signed form request exactly as Twilio would send it.
const signed = (path, params) => mf.dispatchFetch(origin+path,{method:'POST',headers:{'content-type':'application/x-www-form-urlencoded','X-Twilio-Signature':webhooks.getExpectedTwilioSignature(token,origin+path,params)},body:new URLSearchParams(params).toString()});
// Convert WebSocket events into an awaitable FIFO for deterministic assertions.
function messages(ws) {
  const queue=[], waiters=[];
  ws.addEventListener('message',e=>{const value=e.data==='pong'?'pong':JSON.parse(e.data); if(waiters.length)waiters.shift()(value);else queue.push(value);});
  return () => queue.length ? Promise.resolve(queue.shift()) : new Promise((resolve,reject)=>{const timer=setTimeout(()=>reject(new Error('WebSocket message timeout')),5000); waiters.push(value=>{clearTimeout(timer);resolve(value);});});
}
let ws, second;
try {
  let response=await signed('/voice',{FromCity:'Seattle'});
  assert.equal(response.status,200); assert.match(await response.text(),/partialResults="true"/);
  assert.equal((await mf.dispatchFetch(origin+'/transcription',{method:'POST'})).status,403);
  assert.equal((await mf.dispatchFetch(origin+'/live',{headers:{Origin:origin,Upgrade:'websocket'}})).status,401);
  response=await mf.dispatchFetch(origin+'/session',{method:'POST',headers:{Origin:origin},body:'password=synthetic-console-password'});
  assert.equal(response.status,200);
  const cookie=response.headers.get('set-cookie').split(';')[0];
  assert.match(response.headers.get('set-cookie'),/HttpOnly; Secure; SameSite=Strict/);
  const headers={Origin:origin,Upgrade:'websocket',Cookie:cookie};
  assert.equal((await mf.dispatchFetch(origin+'/live',{headers:{...headers,Origin:'https://evil.example'}})).status,403);
  response=await mf.dispatchFetch(origin+'/live',{headers});
  assert.equal(response.status,101,response.status===101 ? '' : await response.text()); ws=response.webSocket;
  const next=messages(ws); ws.accept(); assert.deepEqual((await next()).events,[]);
  ws.send('ping'); assert.equal(await next(),'pong');
  const expires=Date.now()+60000;
  const oldCookie='__Host-console='+expires+'.'+createHmac('sha256','synthetic-console-password').update('console:'+expires).digest('hex');
  assert.equal((await mf.dispatchFetch(origin+'/session',{headers:{Cookie:oldCookie}})).status,200);
  const repeated={FromCity:'A&B <City>',Custom:['b','a','a'],Unicode:'✓'};
  const encoded=new URLSearchParams();for(const [k,v] of Object.entries(repeated))for(const value of Array.isArray(v)?v:[v])encoded.append(k,value);
  const queryUrl=origin+'/voice?x=a%2Bb';
  assert.equal((await mf.dispatchFetch(queryUrl,{method:'POST',headers:{'content-type':'application/x-www-form-urlencoded','X-Twilio-Signature':webhooks.getExpectedTwilioSignature(token,queryUrl,repeated)},body:encoded.toString()})).status,200);
  const base={CallSid:'CA'+'a'.repeat(32),TranscriptionSid:'GT'+'b'.repeat(32),Track:'inbound_track'};
  const send=async(sequence,extra)=>{const r=await signed('/transcription',{...base,SequenceId:String(sequence),...extra});assert.equal(r.status,204);};
  await send(1,{TranscriptionEvent:'transcription-started'}); assert.equal((await next()).event.type,'transcription-started');
  await send(2,{TranscriptionEvent:'transcription-content',TranscriptionData:JSON.stringify({transcript:'hello'}),Final:'false'});
  assert.equal((await next()).event.final,false);
  const final={TranscriptionEvent:'transcription-content',TranscriptionData:JSON.stringify({transcript:'hello world <script>'}),Final:'true'};
  await send(3,final); assert.equal((await next()).event.text,'hello world <script>');
  await send(3,final); // Callback retries must not create duplicate history.
  await send(5,{TranscriptionEvent:'transcription-stopped'}); assert.equal((await next()).event.type,'transcription-stopped');
  await send(4,{...final,TranscriptionData:JSON.stringify({transcript:'late final'})}); assert.equal((await next()).event.sequence,4);
  response=await mf.dispatchFetch(origin+'/live',{headers}); second=response.webSocket;
  const replay=messages(second); second.accept(); const snapshot=await replay();
  assert.equal(snapshot.events.length,5); assert.equal(snapshot.events.filter(e=>e.sequence===3).length,1);
  const roomNamespace=await mf.getDurableObjectNamespace('TRANSCRIPTS');
  const other=roomNamespace.get(roomNamespace.idFromName('other-workspace'));
  const isolated=await other.fetch('https://internal/live',{headers:{Upgrade:'websocket','X-Session-Expires':String(Date.now()+60000)}});
  const isolatedSocket=isolated.webSocket, isolatedNext=messages(isolatedSocket); isolatedSocket.accept();
  assert.deepEqual((await isolatedNext()).events,[]);isolatedSocket.close();
  const bad=await signed('/transcription',{...base,SequenceId:'6',TranscriptionEvent:'transcription-content',TranscriptionData:'not json',Final:'true'});
  assert.equal(bad.status,400);
  console.log('Workers runtime: TwiML, authentication, live partial/final events, retry deduplication, late delivery, replay, isolation and malformed callbacks passed');
} finally {ws?.close();second?.close();await mf.dispose();}
