// Executes shipped JavaScript against a minimal DOM adapter, not a browser.
const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');
class Element {
  constructor(tag='div') { this.tagName=tag; this.children=[]; this.style={}; this.attributes={}; this.textContent=''; this.className=''; }
  get classList() { return {contains: c=>this.className.split(' ').includes(c), toggle:(c,on)=> { const set=new Set(this.className.split(' ').filter(Boolean)); if(on) set.add(c); else set.delete(c); this.className=[...set].join(' '); }, add:c=>this.classList.toggle(c,true),remove:c=>this.classList.toggle(c,false)}; }
  set innerHTML(value) {
    this.children=[]; this._html=value;
    // Only constant queue skeletons are parsed. User strings must use textContent.
    for(const match of value.matchAll(/<(\w+)[^>]*class="([^"]+)"[^>]*>/g)) {
      const child=new Element(match[1]); child.className=match[2]; this.appendChild(child);
    }
  }
  get innerHTML() { return this._html || this.textContent.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;'); }
  appendChild(child) { child.parent=this; this.children.push(child); return child; }
  replaceChildren(...children) { this.children=[]; for(const child of children)this.appendChild(child); }
  removeChild(child) { this.children=this.children.filter(item=>item!==child); }
  remove() { if(this.parent)this.parent.removeChild(this); }
  querySelector(selector) { for(const child of this.children) { if(child.classList.contains(selector.slice(1)))return child; const nested=child.querySelector(selector); if(nested)return nested; } return null; }
  setAttribute(key,value) { this.attributes[key]=value; }
  addEventListener() {}
  focus() {}
  click() { if(this.onclick)return this.onclick(); }
  get firstChild() { return this.children[0]; }
}
function setup(options={}) {
  const timers=new Map(), sockets=[];
  let next=0;
  class Socket {
    constructor(url) { this.url=url; sockets.push(this); }
    close() { if(this.onclose)this.onclose(); }
  }
  const context=vm.createContext({URLSearchParams,document:{createElement:tag=>new Element(tag)},
    location:{protocol:'http:',host:'test'}, window:{addEventListener(){}}, WebSocket:Socket,
    setTimeout:fn=>{timers.set(++next,fn);return next;}, clearTimeout:id=>timers.delete(id),
    fetch:async()=>({ok:true,json:async()=>({ok:true})}), ...options});
  const source=fs.readFileSync(path.join(__dirname,'../webapp/static/app.js'),'utf8');
  vm.runInContext(source,context);
  const container=new Element();
  return {context,container,timers,sockets,view:opts=>context.createTransferQueueView(container,{autoRemoveDelay:null,...opts})};
}
function event(extra={}) {return {id:'one',state:'active',filename:'file',direction:'upload',offset:2,total_size:4,revision:1,...extra};}
function row(container) {return container.children.find(child=>child.className==='transfer-row');}
function buttons(container) {return row(container).querySelector('.btns').children;}

test('cancelled rows disappear on both roles and stay absent after snapshots and late events',()=>{
  for(const showControls of [true,false]) {
    const s=setup(),q=s.view({showControls});
    q.applyEvent(event());
    q.applyEvent(event({state:'cancelled',revision:2}));
    assert.equal(q.hasRow('one'),false);
    q.syncSnapshot({transfers:[event({state:'cancelled',revision:2})]});
    q.applyEvent(event({type:'TRANSFER_PROGRESS',revision:3}));
    assert.equal(q.hasRow('one'),false);
    q.applyEvent(event({id:'other',state:'paused'}));
    assert.equal(q.hasRow('other'),true);
  }
});

test('fresh views omit saved cancelled history without hiding failed or active transfers',()=>{
  const s=setup(),q=s.view();
  q.syncSnapshot({transfers:[event({state:'cancelled'}),event({id:'live'}),event({id:'failed',state:'failed'})]});
  assert.equal(q.hasRow('one'),false);
  assert.equal(q.hasRow('live'),true);
  assert.equal(q.hasRow('failed'),true);
});

test('renders progress, state and accessible value from authoritative snapshot',()=>{
  const s=setup(),q=s.view();q.applyEvent(event());
  assert.equal(row(s.container).querySelector('.pct').textContent,'50%');
  assert.equal(row(s.container).querySelector('.transfer-track').attributes['aria-valuenow'],'50');
});
test('stale progress cannot roll back a completed transfer',()=>{
  const s=setup(),q=s.view();q.applyEvent(event({state:'completed',revision:5}));q.applyEvent(event({type:'TRANSFER_PROGRESS',revision:4}));
  assert.equal(row(s.container).querySelector('.state').textContent,'Done');
});
test('late unversioned progress cannot resurrect terminal state',()=>{
  const s=setup(),q=s.view();q.applyEvent(event({state:'cancelled'}));q.applyEvent(event({type:'TRANSFER_PROGRESS',revision:undefined}));
  assert.equal(q.hasRow('one'),false);
});
test('failed action shows error and restores enabled controls',async()=>{
  const s=setup(),q=s.view({onPause:async()=>({ok:false,error:'rejected'})});q.applyEvent(event());
  await buttons(s.container)[0].onclick();
  assert.equal(row(s.container).querySelector('.action-error').textContent,'rejected');
  assert.equal(buttons(s.container)[0].disabled,false);
  assert.equal(row(s.container).querySelector('.state').textContent,'Active');
});
test('network rejection restores enabled controls',async()=>{
  const s=setup(),q=s.view({onPause:async()=>{throw new Error('offline');}});q.applyEvent(event());await buttons(s.container)[0].onclick();
  assert.equal(row(s.container).querySelector('.action-error').textContent,'offline');assert.equal(buttons(s.container)[0].disabled,false);
});
test('action response state is applied without waiting for websocket',async()=>{
  const s=setup(),q=s.view({onPause:async()=>({ok:true,transfer:event({state:'paused',revision:2})}),onResume:async()=>{}});
  q.applyEvent(event());await buttons(s.container)[0].onclick();assert.equal(buttons(s.container)[0].textContent,'Resume');
});
test('pausing does not offer premature Resume',()=>{
  const s=setup(),q=s.view({onResume:()=>{},onCancel:()=>{}});q.applyEvent(event({state:'pausing'}));assert.deepEqual(buttons(s.container).map(b=>b.textContent),['Cancel']);
});
test('resume admission intent renders without pretending ACTIVE',()=>{
  const s=setup(),q=s.view({onResume:()=>{}});q.applyEvent(event({state:'paused',resume_requested:true}));
  assert.match(row(s.container).querySelector('.state').textContent,/waiting for a slot/);assert.equal(buttons(s.container).length,0);
});
test('zero-byte completion displays 100 percent',()=>{
  const s=setup(),q=s.view();q.applyEvent(event({state:'completed',total_size:0}));assert.equal(row(s.container).querySelector('.pct').textContent,'100%');
});
test('pending resume can be withdrawn and authoritative revision restores Resume',async()=>{
  const s=setup(),q=s.view({onPause:async()=>({ok:true,transfer:event({state:'paused',revision:5,resume_requested:false})}),onResume:()=>{}});
  q.applyEvent(event({state:'paused',revision:4,resume_requested:true}));
  assert.equal(buttons(s.container)[0].textContent,'Pause');
  await buttons(s.container)[0].onclick();
  assert.equal(row(s.container).querySelector('.state').textContent,'Paused');
  assert.equal(buttons(s.container)[0].textContent,'Resume');
  q.applyEvent(event({state:'paused',revision:4,resume_requested:true}));
  assert.equal(row(s.container).querySelector('.state').textContent,'Paused');
});
test('newer paused snapshot clears pending resume without websocket delivery',()=>{
  const s=setup(),q=s.view({onPause:()=>{},onResume:()=>{}});
  q.applyEvent(event({state:'paused',revision:4,resume_requested:true}));
  q.syncSnapshot({transfers:[event({state:'paused',revision:5,resume_requested:false})]});
  assert.equal(row(s.container).querySelector('.state').textContent,'Paused');
  assert.equal(buttons(s.container)[0].textContent,'Resume');
});
test('progress is clamped and stale speed is cleared',()=>{
  const s=setup(),q=s.view();q.applyEvent(event({offset:999,bytes_per_sec:4000}));assert.equal(row(s.container).querySelector('.pct').textContent,'100%');
  q.applyEvent(event({state:'paused',revision:2}));assert.equal(row(s.container).querySelector('.speed').textContent,'');
});
test('host scopes identical IDs by peer',()=>{
  const s=setup(),q=s.view({keyForEvent:e=>`${e.peer_id}:${e.id}`});q.applyEvent(event({peer_id:'a'}));q.applyEvent(event({peer_id:'b'}));assert.equal(s.container.children.length,2);
});
test('snapshot reconciles missing rows but preserves local staging',()=>{
  const s=setup(),q=s.view();q.applyEvent(event());q.applyEvent(event({id:'local',state:'staging',local:true}));q.syncSnapshot({transfers:[]});
  assert.equal(q.hasRow('one'),false);assert.equal(q.hasRow('local'),true);
});
test('delayed snapshot cannot delete a newer live creation',()=>{
  const s=setup(),q=s.view();q.applyEvent(event({event_id:10}));q.syncSnapshot({transfers:[],event_id:9});assert.equal(q.hasRow('one'),true);
});
test('new stream resets revision history after backend restart',()=>{
  const s=setup(),q=s.view();q.syncSnapshot({stream_id:'old',transfers:[event({revision:99})]});q.syncSnapshot({stream_id:'new',transfers:[event({state:'queued',revision:1})]});
  assert.equal(row(s.container).querySelector('.state').textContent,'Queued');
});
test('switching endpoint scope does not retain a different manager revision',()=>{
  const s=setup(),q=s.view();q.syncSnapshot({scope:'a',transfers:[event({revision:99})]});q.syncSnapshot({scope:'b',transfers:[event({state:'queued',revision:1})]});
  assert.equal(row(s.container).querySelector('.state').textContent,'Queued');
});
test('terminal removal timer is not postponed by polling snapshots',()=>{
  const s=setup(),q=s.view({autoRemoveDelay:10});q.applyEvent(event({state:'completed'}));q.syncSnapshot({transfers:[event({state:'completed'})]});assert.equal(s.timers.size,1);
  for(const fn of [...s.timers.values()])fn();q.syncSnapshot({transfers:[event({state:'completed'})]});assert.equal(q.hasRow('one'),false);
});
test('retry cancels a completion timer rather than deleting the revived row',()=>{
  const s=setup(),q=s.view({autoRemoveDelay:10});q.applyEvent(event({state:'completed'}));const callback=[...s.timers.values()][0];q.applyEvent(event({state:'retrying',revision:2}));callback();assert.equal(q.hasRow('one'),true);
});
test('upload staging rename cannot orphan an already existing transfer row',()=>{
  const s=setup(),q=s.view();q.applyEvent(event({id:'local',local:true,state:'staging'}));q.applyEvent(event());q.renameRow('local','one');assert.equal(s.container.children.length,1);assert.equal(q.hasRow('local'),false);
});
test('malicious filenames and errors are text, not parsed markup',()=>{
  const s=setup(),q=s.view();const name='<img src=x onerror=alert(1)>';q.applyEvent(event({filename:name,state:'failed',error:name}));assert.equal(row(s.container).querySelector('.name').textContent,name);
});
test('offline queue retains cancellation but hides data-dependent actions',()=>{
  const s=setup(),q=s.view({onPause:()=>{},onCancel:()=>{}});q.applyEvent(event());q.setConnected(false);assert.deepEqual(buttons(s.container).map(b=>b.textContent),['Cancel']);
});
test('completed downloads stay visible with an explicit Save file action',()=>{
  const s=setup(),q=s.view({autoRemoveDelay:10,onDownload:()=>{}});q.applyEvent(event({state:'completed',direction:'download'}));assert.equal(s.timers.size,0);assert.ok(buttons(s.container).some(b=>b.textContent==='Save file'));
});
test('websocket uses wss under HTTPS and filters by side',()=>{
  const s=setup({location:{protocol:'https:',host:'test'}});s.context.connectTransferEventSocket(()=>{},{side:'host'});assert.equal(s.sockets[0].url,'wss://test/api/transfers/events?side=host');
});
test('websocket reconnects and explicit close prevents more reconnects',()=>{
  const s=setup();const handle=s.context.connectTransferEventSocket(()=>{});s.sockets[0].onclose();assert.equal(s.timers.size,1);const retry=[...s.timers.values()][0];retry();assert.equal(s.sockets.length,2);handle.close();assert.equal(s.timers.size,0);
});
test('malformed websocket data is reported without breaking the subscription',()=>{
  const s=setup();let error=false;s.context.connectTransferEventSocket(()=>{},{onError:()=>error=true});s.sockets[0].onmessage({data:'bad json'});assert.equal(error,true);
});
test('progress does not replace buttons underneath a pointer click',()=>{
  const s=setup(),q=s.view({onPause:()=>{}});q.applyEvent(event());const button=buttons(s.container)[0];q.applyEvent(event({type:'TRANSFER_PROGRESS',revision:2,offset:3}));assert.equal(buttons(s.container)[0],button);
});
test('late events from an old server stream cannot overwrite a fresh snapshot',()=>{
  const s=setup(),q=s.view();q.syncSnapshot({stream_id:'new',transfers:[event({state:'queued'})]});q.applyEvent(event({stream_id:'old',state:'failed',revision:99}));assert.equal(row(s.container).querySelector('.state').textContent,'Queued');
});
test('manager reconstruction in the same server resets revisions and retires old generations',()=>{
  const s=setup(),q=s.view();q.applyEvent(event({generation:'old-manager',state:'connection_lost',revision:99}));
  q.syncSnapshot({transfers:[event({generation:'new-manager',state:'queued',revision:1})]});
  q.applyEvent(event({generation:'old-manager',state:'failed',revision:100}));
  assert.equal(row(s.container).querySelector('.state').textContent,'Queued');
  q.removeRow('one',true);
  q.applyEvent(event({generation:'old-manager',state:'failed',revision:100}));
  assert.equal(q.hasRow('one'),false);
});

async function clientPage() {
  const s=setup({AbortController,DOMException,crypto:require('node:crypto').webcrypto,
    MutationObserver:class {observe(){}},setInterval:()=>0,confirm:()=>false});
  const nodes=new Map();
  s.context.document.getElementById=id=>{if(!nodes.has(id))nodes.set(id,new Element());return nodes.get(id);};
  s.context.window.crypto=s.context.crypto;s.context.window.location={};
  const calls=[];
  s.context.fetch=async(url,options)=>{
    calls.push({url,options});
    const data=url==='/api/client/status'?{ok:true,connected:false}:url==='/api/transfers' && options?.method==='POST'
      ?{ok:true,transfer:event({direction:'download',state:'queued'})}:{ok:true,transfers:[],files:[]};
    return {ok:true,json:async()=>data};
  };
  const html=fs.readFileSync(path.join(__dirname,'../webapp/static/client.html'),'utf8');
  for(const match of html.matchAll(/<script[^>]*>([\s\S]*?)<\/script>/g))vm.runInContext(match[1],s.context);
  await new Promise(resolve=>setImmediate(resolve));
  return {...s,calls,nodes};
}
test('browser download submits an asynchronous transfer resource instead of adapter blocking navigation',async()=>{
  const s=await clientPage();await s.context.downloadFile("quoted'name");
  const call=s.calls.find(item=>item.url==='/api/transfers' && item.options?.method==='POST');
  assert.equal(JSON.parse(call.options.body).filename,"quoted'name");
  assert.equal(JSON.parse(call.options.body).direction,'download');
  assert.equal(s.context.window.location.href,undefined);
});
test('browser staging caps simultaneous request pipelines and queued acquisition can be cancelled',async()=>{
  const s=await clientPage();const controllers=Array.from({length:4},()=>new AbortController());
  const first=await Promise.all(controllers.slice(0,3).map(c=>s.context.stagingSlot(c.signal)));
  const fourth=s.context.stagingSlot(controllers[3].signal);controllers[3].abort();
  await assert.rejects(fourth,{name:'AbortError'});
  assert.equal(vm.runInContext('activeStaging',s.context),3);
  for(const release of first)release();
  assert.equal(vm.runInContext('activeStaging',s.context),0);
});
test('browser staging releases its request slot after successful submit',async()=>{
  const s=await clientPage();let uploads=0;
  s.context.fetch=async(url,options)=>{
    if(url.startsWith('/api/uploads/')) {uploads++;return {ok:true,json:async()=>({ok:true,transfer:event({id:'real',state:'queued'})})};}
    return {ok:true,json:async()=>({ok:true,transfers:[],files:[]})};
  };
  await s.context.startUpload({name:'stage.bin',size:4,slice:()=>Buffer.from('data')},false);
  assert.equal(uploads,1);assert.equal(vm.runInContext('activeStaging',s.context),0);
  assert.equal(s.nodes.get('transferQueue').children.length,1);
});
