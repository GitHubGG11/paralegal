// Read-only production smoke test. It confirms the deployed Python marker,
// access controls, console login, and first WebSocket snapshot. It never prints
// the password or transcript event contents.
import {readFileSync} from 'node:fs';
import assert from 'node:assert/strict';
import WebSocket from 'ws';
const origin='https://twilio.genehuntley.com';
const password=readFileSync(new URL('../.console-password',import.meta.url),'utf8').trim();
const health=await fetch(origin+'/health');
assert.equal(health.status,200);assert.equal(health.headers.get('x-backend'),'python');
const page=await fetch(origin+'/console');
assert.equal(page.status,200);assert.match(await page.text(),/Voice assistant/);
assert.equal((await fetch(origin+'/live',{headers:{Origin:origin}})).status,401);
const login=await fetch(origin+'/session',{method:'POST',headers:{Origin:origin},body:new URLSearchParams({password})});
assert.equal(login.status,200);
const cookie=login.headers.get('set-cookie').split(';')[0];
// The first server message must be a complete retained-state snapshot.
await new Promise((resolve,reject)=>{
 const ws=new WebSocket(origin.replace('https:','wss:')+'/live',{headers:{Origin:origin,Cookie:cookie}});
 const timer=setTimeout(()=>{ws.terminate();reject(new Error('Live stream timeout'));},15000);
 ws.on('error',e=>{clearTimeout(timer);reject(e);});
 ws.on('message',data=>{try{const event=JSON.parse(data);assert.equal(event.type,'snapshot');assert.ok(Array.isArray(event.events));clearTimeout(timer);ws.close();resolve();}catch(e){clearTimeout(timer);ws.terminate();reject(e);}});
});
console.log('Production console, access control, login and live WebSocket verified. No transcript contents printed.');
if (process.argv.includes('--agent')) {
 const result=await fetch(origin+'/agent/check',{method:'POST',headers:{Origin:origin,Cookie:cookie}});
 assert.equal(result.status,200,`Agent connectivity check failed: ${result.status}`);
 const data=await result.json();assert.equal(data.ok,true);
 console.log(`OpenAI verified: ${data.model}, ${data.firstTokenMs} ms to first streamed text, ${data.latencyMs} ms for the full synthetic reply.`);
}
