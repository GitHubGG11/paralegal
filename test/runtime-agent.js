// Real Python Worker + SQLite DOs, with all provider traffic intercepted locally.
import {Miniflare, convertV4MiniflareOptions} from 'miniflare';
import webhooks from 'twilio/lib/webhooks/webhooks.js';
import assert from 'node:assert/strict';
import {readdirSync,statSync} from 'node:fs';
import path from 'node:path';
const root=path.resolve('dist/python');
const files=readdirSync(root,{recursive:true}).filter(f=>!['README.md','python_modules/.synced'].includes(f)&&statSync(path.join(root,f)).isFile());
const modules=files.map(f=>({path:path.join(root,f),type:f.startsWith('python_modules/')?(f.endsWith('.js')?'ESModule':'Data'):'PythonModule'}));
modules.sort((a,b)=>a.path===path.join(root,'entry.py')?-1:b.path===path.join(root,'entry.py')?1:0);
const origin='https://twilio.genehuntley.com',token='synthetic-token';
const updates=[], generations=[];
const mf=new Miniflare(convertV4MiniflareOptions({modules,modulesRoot:root,compatibilityDate:'2026-09-23',compatibilityFlags:['python_workers'],
 durableObjects:{TRANSCRIPTS:{className:'TranscriptRoom',useSQLite:true},CALLS:{className:'CallAgent',useSQLite:true},KNOWLEDGE:{className:'KnowledgeBase',useSQLite:true}},
 bindings:{PUBLIC_VOICE_URL:origin+'/voice',TWILIO_AUTH_TOKEN:token,CONSOLE_PASSWORD:'synthetic-password',OPENAI_API_KEY:'synthetic-key'},
 outboundService:async request=>{
  const url=new URL(request.url);
  if(url.hostname==='api.openai.com'){
   const body=await request.json();generations.push(body);
   assert.equal(request.headers.get('authorization'),'Bearer synthetic-key');
   assert.equal(body.model,'gpt-4.1-nano');assert.equal(body.store,false);
   const last=body.messages.at(-1);
   const message=last.role==='tool'?{role:'assistant',content:'The project email says the meeting is Friday.'}:
    {role:'assistant',content:null,tool_calls:[{id:'search_1',type:'function',function:{name:'search_records',arguments:JSON.stringify({query:'Orchid meeting'})}}]};
   return Response.json({choices:[{message}]});
  }
  if(url.hostname==='api.twilio.com'){
   assert.match(request.headers.get('authorization'),/^Basic /);
   updates.push({url:url.pathname,twiml:new URLSearchParams(await request.text()).get('Twiml')});
   return Response.json({status:'in-progress'});
  }
  throw new Error('Unexpected outbound request: '+url.hostname);
 }}));
const signed=(route,params)=>mf.dispatchFetch(origin+route,{method:'POST',headers:{'content-type':'application/x-www-form-urlencoded','X-Twilio-Signature':webhooks.getExpectedTwilioSignature(token,origin+route,params)},body:new URLSearchParams(params)});
const until=async predicate=>{const end=Date.now()+10000;while(!predicate()){if(Date.now()>end)throw new Error('Timed out waiting for provider call');await new Promise(r=>setTimeout(r,25));}};
try{
 assert.equal((await mf.dispatchFetch(origin+'/knowledge')).status,401);
 let r=await mf.dispatchFetch(origin+'/session',{method:'POST',headers:{Origin:origin},body:'password=synthetic-password'});
 assert.equal(r.status,200);
 const cookie=r.headers.get('set-cookie').split(';')[0],headers={Origin:origin,Cookie:cookie,'Content-Type':'application/json'};
 const record={id:'email-1',title:'Orchid project',text:'The Orchid project meeting is Thursday.',source:'email-archive'};
 r=await mf.dispatchFetch(origin+'/knowledge',{method:'POST',headers,body:JSON.stringify([record])});
 assert.equal(r.status,200,await r.clone().text());assert.equal((await r.json()).total,1);
 r=await mf.dispatchFetch(origin+'/knowledge',{method:'POST',headers,body:JSON.stringify([{...record,text:'The Orchid project meeting is Friday.'}])});
 assert.equal(r.status,200);assert.equal((await r.json()).total,1);
 const kn=await mf.getDurableObjectNamespace('KNOWLEDGE');
 const knowledge=kn.get(kn.idFromName('twilio.genehuntley.com'));
 // Python RPC is reached through the public Worker in the agent flow below.
 const base={CallSid:'CA'+'a'.repeat(32),AccountSid:'AC'+'b'.repeat(32)};
 r=await signed('/voice',base);assert.equal(r.status,200);assert.match(await r.text(),/How may I help you/);
 const event=(sequence,text,final)=>signed('/transcription',{...base,TranscriptionSid:'GT'+'c'.repeat(32),SequenceId:String(sequence),Track:'inbound_track',TranscriptionEvent:'transcription-content',Final:String(final),TranscriptionData:JSON.stringify({transcript:text})});
 r=await event(1,'When is the Orchid meeting?',true);assert.equal(r.status,204);
 await until(()=>updates.length===1);
 assert.equal(generations.length,2);assert.match(updates[0].twiml,/Friday/);assert.match(updates[0].twiml,/<Say>/);
 assert.match(updates[0].twiml,/\/agent\/listen\?turn=1/);
 const searchResult=JSON.parse(generations[1].messages.at(-1).content);
 assert.equal(searchResult.records.length,1);assert.match(searchResult.records[0].excerpt,/Friday/);
 assert.doesNotMatch(searchResult.records[0].excerpt,/Thursday/);
 await event(1,'When is the Orchid meeting?',true); // Retry must not speak twice.
 r=await event(2,'Wait',false);assert.equal(r.status,204);
 await until(()=>updates.length===2);
 assert.doesNotMatch(updates[1].twiml,/<Say>/);assert.match(updates[1].twiml,/<Pause/);
 assert.equal(generations.length,2);
 r=await signed('/agent/listen?turn=1',base);assert.equal(r.status,200);assert.match(await r.text(),/<Pause/);
 const other={CallSid:'CA'+'d'.repeat(32),AccountSid:base.AccountSid};
 r=await signed('/voice',other);assert.equal(r.status,200);
 r=await signed('/transcription',{...other,TranscriptionSid:'GT'+'e'.repeat(32),SequenceId:'1',Track:'inbound_track',TranscriptionEvent:'transcription-content',Final:'true',TranscriptionData:JSON.stringify({transcript:'Find Orchid'})});
 assert.equal(r.status,204);await until(()=>updates.length===3);
 assert.equal(generations[2].messages.filter(m=>m.role==='user').length,1);
 assert.match(updates[2].url,new RegExp(other.CallSid));
 r=await mf.dispatchFetch(origin+'/knowledge',{method:'POST',headers:{...headers,Origin:'https://evil.example'},body:JSON.stringify([record])});assert.equal(r.status,403);
 r=await mf.dispatchFetch(origin+'/knowledge',{method:'POST',headers,body:JSON.stringify([{id:'bad',text:12}])});assert.equal(r.status,400);
 console.log('Agent runtime: JSON import/upsert, real FTS lookup, OpenAI tool round trip, default Twilio Say, barge-in, signed playback callback, retry dedupe and call isolation passed.');
}finally{await mf.dispose();}
