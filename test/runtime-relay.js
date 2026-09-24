// Streaming integration: no real provider traffic or telephone calls.
import {Miniflare, convertV4MiniflareOptions} from 'miniflare';
import webhooks from 'twilio/lib/webhooks/webhooks.js';
import assert from 'node:assert/strict';
import {readdirSync,statSync} from 'node:fs';
import path from 'node:path';
const root=path.resolve('dist/python');
const modules=readdirSync(root,{recursive:true})
 .filter(f=>!['README.md','python_modules/.synced'].includes(f)&&statSync(path.join(root,f)).isFile())
 .map(f=>({path:path.join(root,f),type:f.startsWith('python_modules/')?(f.endsWith('.js')?'ESModule':'Data'):'PythonModule'}));
modules.sort((a,b)=>a.path===path.join(root,'entry.py')?-1:b.path===path.join(root,'entry.py')?1:0);
const origin='https://twilio.genehuntley.com',token='synthetic-token',requests=[];
let releaseSlow, slowCompleted=false, aborted=false;
const slowGate=new Promise(resolve=>{releaseSlow=resolve;});
const encoder=new TextEncoder();
const chunk=(delta,finish_reason=null)=>'data: '+JSON.stringify({choices:[{index:0,delta,finish_reason}]})+'\n\n';
const mf=new Miniflare(convertV4MiniflareOptions({modules,modulesRoot:root,compatibilityDate:'2026-09-23',compatibilityFlags:['python_workers'],
 durableObjects:{TRANSCRIPTS:{className:'TranscriptRoom',useSQLite:true},CALLS:{className:'CallAgent',useSQLite:true},KNOWLEDGE:{className:'KnowledgeBase',useSQLite:true}},
 bindings:{PUBLIC_VOICE_URL:origin+'/voice',TWILIO_AUTH_TOKEN:token,CONSOLE_PASSWORD:'synthetic-password',OPENAI_API_KEY:'synthetic-key',VOICE_TRANSPORT:'relay'},
 outboundService:async request=>{
  assert.equal(new URL(request.url).hostname,'api.openai.com');
  const body=await request.json();requests.push(body);assert.equal(body.stream,true);
  const last=body.messages.at(-1);
  if(last.role==='user'&&last.content==='slow reply'){
   return new Response(new ReadableStream({
    async start(controller){
     controller.enqueue(encoder.encode(chunk({content:'First words '})));
     await slowGate;slowCompleted=true;
     try{controller.enqueue(encoder.encode(chunk({content:'STALE SUFFIX'})+chunk({},'stop')+'data: [DONE]\n\n'));controller.close();}catch{aborted=true;}
    },cancel(){aborted=true;}
   }),{headers:{'content-type':'text/event-stream'}});
  }
  let stream;
  if(last.role==='user'&&last.content.includes('court')){
   stream=chunk({tool_calls:[{index:0,id:'lookup1',type:'function',function:{name:'search_records',arguments:'{"query":'}}]})+
          chunk({tool_calls:[{index:0,function:{arguments:'"Gene Huntley court"}'}}]},'tool_calls');
  }else if(last.role==='tool'){
   const result=JSON.parse(last.content);assert.equal(result.records[0].id,'gene-test');
   stream=chunk({content:'Your test court date is '})+chunk({content:'October fifteenth.'})+chunk({},'stop');
  }else{
   stream=chunk({content:'Hello '})+chunk({content:'again.'})+chunk({},'stop');
  }
  return new Response(stream+'data: [DONE]\n\n',{headers:{'content-type':'text/event-stream'}});
 }}));
const signed=(route,params)=>mf.dispatchFetch(origin+route,{method:'POST',headers:{'content-type':'application/x-www-form-urlencoded','X-Twilio-Signature':webhooks.getExpectedTwilioSignature(token,origin+route,params)},body:new URLSearchParams(params)});
const socket=call=>mf.dispatchFetch(origin+'/relay/'+call,{headers:{Upgrade:'websocket','X-Twilio-Signature':webhooks.getExpectedTwilioSignature(token,origin+'/relay/'+call,{})}});
const until=async predicate=>{const deadline=Date.now()+10000;while(!predicate()){if(Date.now()>deadline)throw new Error('Relay assertion timed out');await new Promise(r=>setTimeout(r,20));}};
let ws;
try{
 const base={CallSid:'CA'+'a'.repeat(32),AccountSid:'AC'+'b'.repeat(32)};
 assert.equal((await mf.dispatchFetch(origin+'/relay/'+base.CallSid,{headers:{Upgrade:'websocket'}})).status,403);
 assert.equal((await socket(base.CallSid)).status,409);
 let response=await signed('/voice',base);assert.equal(response.status,200);
 const xml=await response.text();assert.match(xml,/ttsProvider="ElevenLabs"/);assert.match(xml,/partialPrompts="true"/);assert.doesNotMatch(xml,/<Transcription/);
 response=await socket(base.CallSid);assert.equal(response.status,101,response.status===101?'':await response.text());
 ws=response.webSocket;const received=[];ws.addEventListener('message',event=>received.push(JSON.parse(event.data)));ws.accept();
 assert.equal((await socket(base.CallSid)).status,409);
 ws.send(JSON.stringify({type:'setup',callSid:base.CallSid,accountSid:base.AccountSid,sessionId:'VX'+'c'.repeat(32)}));
 ws.send(JSON.stringify({type:'prompt',voicePrompt:'slow reply',last:true}));
 await until(()=>received.length===1);
 assert.equal(received[0].token,'First words ');assert.equal(received[0].last,false);assert.equal(received[0].interruptible,true);
 assert.equal(slowCompleted,false,'First speech token must arrive before upstream generation finishes');
 ws.send(JSON.stringify({type:'interrupt',utteranceUntilInterrupt:'First',durationUntilInterruptMs:100}));
 // The next prompt must be served even while the old provider response is unfinished.
 ws.send(JSON.stringify({type:'prompt',voicePrompt:'hello again',last:true}));
 await until(()=>received.some(m=>m.last));
 releaseSlow();await until(()=>slowCompleted);
 assert.equal(received.some(m=>m.token.includes('STALE')),false);
 const newHistory=requests[1].messages;
 assert.equal(newHistory.filter(m=>m.role==='assistant').at(-1).content,'First');
 assert.equal(newHistory.some(m=>(m.content||'').includes('STALE')),false);
 response=await mf.dispatchFetch(origin+'/session',{method:'POST',headers:{Origin:origin},body:'password=synthetic-password'});
 const cookie=response.headers.get('set-cookie').split(';')[0];
 response=await mf.dispatchFetch(origin+'/knowledge',{method:'POST',headers:{Origin:origin,Cookie:cookie},body:JSON.stringify([
  {id:'gene-test',title:'Gene Huntley test court date',text:'Fictional court date October 15, 2026.',source:'test fixture'}])});assert.equal(response.status,200);
 const before=received.length;
 ws.send(JSON.stringify({type:'prompt',voicePrompt:'Find my court date',last:true}));
 await until(()=>received.slice(before).some(m=>m.last));
 assert.equal(received.slice(before).map(m=>m.token).join(''),'Your test court date is October fifteenth.');
 assert.equal(requests.length,4,'One normal stream and a search tool round trip expected after interruption');
 ws.close();
 response=await signed('/relay/ended',{...base,SessionStatus:'failed',ErrorCode:'64101'});assert.equal(response.status,200);
 assert.match(await response.text(),/partialResults="true"/);
 assert.equal((await socket(base.CallSid)).status,409);
 response=await signed('/relay/ended',{...base,SessionStatus:'failed'});assert.doesNotMatch(await response.text(),/<Start>/);
 const other={CallSid:'CA'+'d'.repeat(32),AccountSid:base.AccountSid};
 await signed('/voice',other);
 response=await socket(other.CallSid);const badSocket=response.webSocket;badSocket.accept();
 let closeCode;badSocket.addEventListener('close',event=>{closeCode=event.code;});
 badSocket.send(JSON.stringify({type:'setup',callSid:base.CallSid,accountSid:base.AccountSid,sessionId:'VX'+'e'.repeat(32)}));
 await until(()=>closeCode!==undefined);assert.equal(closeCode,1008);
 console.log('Relay runtime: signed WebSocket, early speech during ongoing model generation, interrupt cancellation, heard-text history, streamed retrieval, call isolation and standard-voice fallback passed.');
}finally{releaseSlow();try{ws?.close();}catch{}await mf.dispose();}
