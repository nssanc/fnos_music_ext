(() => {
  'use strict';
  if (window.__fnmusicExtLoaded) return;
  window.__fnmusicExtLoaded = true;
  const API = '/music/api/v1/_ext';
  if (!document.querySelector('link[data-fnmusic-ext]')) {
    const link = document.createElement('link'); link.rel = 'stylesheet'; link.href = `${API}/assets/settings.css`; link.dataset.fnmusicExt = '1'; document.head.appendChild(link);
  }
  const css = `
  #fmx-open{position:fixed;right:22px;bottom:88px;z-index:9000;border:0;border-radius:22px;padding:10px 15px;background:#6d5dfc;color:white;font-weight:650;box-shadow:0 8px 30px #0007;cursor:pointer}
  #fmx-player-source{position:fixed;right:22px;bottom:38px;z-index:9001;border:1px solid #ffffff28;border-radius:18px;padding:7px 12px;background:#242731;color:#fff;font-size:12px;box-shadow:0 5px 20px #0006;cursor:pointer}#fmx-source-menu{position:fixed;right:22px;bottom:76px;z-index:9002;min-width:170px;padding:7px;background:#20232a;color:#fff;border:1px solid #ffffff24;border-radius:11px;box-shadow:0 12px 38px #000a}#fmx-source-menu button{display:block;width:100%;border:0;border-radius:7px;padding:9px 10px;text-align:left;background:transparent;color:#fff;cursor:pointer}#fmx-source-menu button:hover{background:#ffffff14}#fmx-source-menu button:disabled{opacity:.45;cursor:wait}
  #fmx-overlay{position:fixed;inset:0;z-index:99999;background:#0009;display:flex;align-items:center;justify-content:center;padding:20px}
  .fmx-panel{width:min(840px,96vw);max-height:88vh;overflow:auto;background:#191b20;color:#eee;border:1px solid #ffffff18;border-radius:16px;box-shadow:0 24px 80px #000b;padding:22px;box-sizing:border-box;-webkit-overflow-scrolling:touch}.fmx-head{display:flex;align-items:center;justify-content:space-between}.fmx-head h2{margin:0;font-size:21px}.fmx-x,.fmx-btn{border:0;border-radius:8px;padding:8px 12px;color:#fff;background:#343842;cursor:pointer}.fmx-primary{background:#6d5dfc}.fmx-danger{background:#a53b48}.fmx-muted{color:#a8adb8;font-size:13px}.fmx-card{border:1px solid #ffffff18;background:#22252c;border-radius:12px;padding:14px;margin-top:12px}.fmx-row{display:flex;gap:10px;align-items:center;flex-wrap:wrap}.fmx-grow{flex:1;min-width:0}.fmx-name{font-weight:650;overflow-wrap:anywhere}.fmx-status{font-size:12px;padding:3px 8px;border-radius:20px;background:#353944}.fmx-ok{background:#174f36;color:#8ff0b9}.fmx-warn{background:#584517;color:#f7d478}.fmx-input,.fmx-textarea{width:100%;box-sizing:border-box;background:#121419;color:#eee;border:1px solid #ffffff28;border-radius:8px;padding:10px;margin-top:8px}.fmx-textarea{min-height:110px;resize:vertical}.fmx-section{margin-top:22px}.fmx-section h3{font-size:16px;margin:0 0 8px}.fmx-qr{width:210px;height:210px;max-width:calc(100vw - 64px);object-fit:contain;background:#fff;border-radius:8px;padding:8px}.fmx-msg{margin-top:10px;color:#efc76e;white-space:pre-wrap;overflow-wrap:anywhere}.fmx-switch{accent-color:#6d5dfc;width:18px;height:18px;flex:0 0 auto}.fmx-hidden{display:none!important}
  @media(max-width:720px){
    html,body,#root{width:100%;max-width:100%;overflow-x:hidden}.music-player-root{min-width:0!important}.fmx-mobile .min-w-\\[1120px\\]{min-width:0!important;width:100vw!important}.fmx-mobile .min-w-\\[1120px\\]>div.relative.z-10.flex.min-h-0.flex-1>aside{display:none!important}.fmx-mobile .min-w-\\[1120px\\]>div.relative.z-10.flex.min-h-0.flex-1>aside+div{width:100%!important;min-width:0!important}.fmx-mobile :has(>.min-h-0.box-border.w-full.pr-10.pt-20){margin-left:0!important;width:100%!important}.fmx-mobile .min-h-0.box-border.w-full.pr-10.pt-20{padding-left:16px!important;padding-right:16px!important;padding-top:68px!important}.fmx-mobile section.grid.grid-cols-4{grid-template-columns:repeat(2,minmax(0,1fr))!important;gap:10px!important}.fmx-mobile section.grid.grid-cols-4>div{height:auto!important;aspect-ratio:268/240}.semi-table-container,.semi-table-body{max-width:100vw;overflow-x:auto!important;-webkit-overflow-scrolling:touch}.semi-modal,.semi-modal-content{max-width:calc(100vw - 24px)!important}.semi-modal{margin:12px!important}.semi-dropdown{max-width:calc(100vw - 16px)!important}
    #fmx-overlay{align-items:flex-end;padding:0;padding-top:max(16px,env(safe-area-inset-top));overscroll-behavior:contain}.fmx-panel{width:100vw;max-height:calc(100dvh - env(safe-area-inset-top));border-radius:18px 18px 0 0;border-left:0;border-right:0;border-bottom:0;padding:16px max(14px,env(safe-area-inset-right)) calc(18px + env(safe-area-inset-bottom)) max(14px,env(safe-area-inset-left))}.fmx-head{position:sticky;top:-16px;z-index:2;margin:-16px -14px 0;padding:16px 14px 12px;background:#191b20eF;backdrop-filter:blur(16px)}.fmx-section{margin-top:18px}.fmx-card{padding:12px}.fmx-row{align-items:flex-start}.fmx-btn,.fmx-x{min-height:44px;padding:10px 14px}.fmx-input,.fmx-textarea{min-height:44px;font-size:16px}.fmx-switch{width:22px;height:22px;margin-top:8px}
    #fmx-open{right:max(10px,env(safe-area-inset-right));top:64px;bottom:auto;width:42px;height:42px;padding:0;border-radius:50%;font-size:0}#fmx-open:after{content:'音源';font-size:11px}
    #fmx-player-source{right:max(10px,env(safe-area-inset-right));bottom:calc(142px + env(safe-area-inset-bottom));max-width:calc(100vw - 20px);min-height:38px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}#fmx-source-menu{left:10px;right:10px;bottom:calc(72px + env(safe-area-inset-bottom));min-width:0;border-radius:14px;padding:8px}#fmx-source-menu button{min-height:46px;font-size:15px}
    #fmx-mobile-nav{position:fixed;left:0;right:0;bottom:0;z-index:8999;height:calc(58px + env(safe-area-inset-bottom));padding:4px max(6px,env(safe-area-inset-right)) env(safe-area-inset-bottom) max(6px,env(safe-area-inset-left));display:grid;grid-template-columns:repeat(5,minmax(0,1fr));align-items:center;background:#17151fef;border-top:1px solid #ffffff16;backdrop-filter:blur(22px);box-sizing:border-box}#fmx-mobile-nav a,#fmx-mobile-nav button{height:48px;min-width:0;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:2px;border:0;background:transparent;color:#aaa6b5;font-size:10px;text-decoration:none;border-radius:10px}#fmx-mobile-nav .fmx-nav-icon{font-size:20px;line-height:22px}#fmx-mobile-nav .fmx-active{color:#fff;background:#ffffff12}#fmx-mobile-more{position:fixed;left:10px;right:10px;bottom:calc(64px + env(safe-area-inset-bottom));z-index:9003;display:grid;grid-template-columns:repeat(3,1fr);gap:6px;padding:12px;background:#211f2aee;border:1px solid #ffffff1d;border-radius:16px;box-shadow:0 14px 42px #000a;backdrop-filter:blur(24px)}#fmx-mobile-more a{min-height:46px;display:flex;align-items:center;justify-content:center;color:#fff;text-decoration:none;background:#ffffff0d;border-radius:10px;font-size:13px}.fmx-mobile .min-w-\\[1120px\\]>.pointer-events-none.absolute.inset-x-0.bottom-0{bottom:calc(58px + env(safe-area-inset-bottom))!important;padding-left:8px!important;padding-right:8px!important;padding-bottom:8px!important}
    .fmx-now-playing-open #fmx-mobile-nav,.fmx-now-playing-open #fmx-mobile-more{display:none!important}.fmx-mobile [role="dialog"][aria-modal="true"]>div.absolute.inset-0.flex.min-h-0.flex-col>div.flex.min-h-0.flex-1.overflow-hidden{padding:0 12px 8px!important}.fmx-mobile [role="dialog"][aria-modal="true"] [style*="--music-player-now-playing-player-gap"]{--music-player-now-playing-player-gap:8px!important;--music-player-now-playing-player-cover-size:min(40vw,168px)!important;--music-player-now-playing-player-left-width:100%!important;--music-player-now-playing-player-right-width:100%!important;--music-player-now-playing-player-lyrics-height:100%!important;flex-direction:column!important;justify-content:flex-start!important;gap:8px!important;max-width:100%!important;overflow:hidden}.fmx-mobile [role="dialog"][aria-modal="true"] [style*="--music-player-now-playing-player-gap"]>div:first-child{width:100%!important;flex:none!important;align-self:auto!important;gap:10px!important;justify-content:flex-start!important}.fmx-mobile [role="dialog"][aria-modal="true"] [style*="--music-player-now-playing-player-gap"]>div:last-child{width:100%!important;min-height:180px!important;flex:1 1 0!important;align-self:auto!important;padding:0 0 6px!important}.fmx-mobile [role="dialog"][aria-modal="true"] .music--now-playing-title-marquee{font-size:20px!important;line-height:26px!important;max-height:52px!important;overflow:hidden}.fmx-mobile [role="dialog"][aria-modal="true"] [data-lyric-index]{min-height:42px!important;padding-left:4px!important;padding-right:8px!important;font-size:18px!important;line-height:27px!important}.fmx-now-playing-open #fmx-player-source{top:72px!important;bottom:auto!important}.fmx-now-playing-open #fmx-open{display:none!important}
  }
  @media(max-width:720px) and (orientation:landscape){#fmx-open{top:12px}#fmx-player-source{bottom:calc(70px + env(safe-area-inset-bottom));right:62px}.fmx-panel{max-height:94dvh}}
  `;
  const style = document.createElement('style'); style.textContent = css; document.head.appendChild(style);

  async function api(path, options = {}) {
    const headers = { 'X-FnMusic-Ext': '1', ...(options.headers || {}) };
    if (options.body && !(options.body instanceof FormData)) headers['Content-Type'] = 'application/json';
    const response = await fetch(API + path, { credentials: 'same-origin', ...options, headers });
    const data = await response.json().catch(() => ({}));
    if (!response.ok || data.ok === false) throw new Error(data.error || data.message || `HTTP ${response.status}`);
    return data;
  }
  const esc = (value) => String(value ?? '').replace(/[&<>"']/g, ch => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[ch]));
  const selected = (actual, value) => actual === value ? 'selected' : '';
  const sourceNames = { musicdl: '酷我 / 咪咕', netease: '网易云音乐', qqmusic: 'QQ 音乐', lx: '洛雪自定义源' };
  let qrTimer = null;
  let switchedTrack = null;

  function shell() {
    const root = document.createElement('div'); root.id = 'fmx-overlay';
    root.innerHTML = `<section class="fmx-panel"><div class="fmx-head"><div><h2>在线音源</h2><div class="fmx-muted">管理搜索、播放兜底和账号授权</div></div><button class="fmx-x" data-close>关闭</button></div><div id="fmx-body"><div class="fmx-card">正在加载…</div></div></section>`;
    root.addEventListener('click', e => { if (e.target === root || e.target.closest('[data-close]')) close(root); });
    document.body.appendChild(root); load(root);
    return root;
  }
  function close(root) { if (qrTimer) clearInterval(qrTimer); qrTimer = null; root.remove(); }

  async function load(root) {
    const body = root.querySelector('#fmx-body');
    try {
      const data = await api('/sources');
      const prefs = data.preferences || {};
      body.innerHTML = `<div class="fmx-section"><h3>歌曲与歌词来源</h3><div class="fmx-card"><label class="fmx-name">优先歌曲源</label><select class="fmx-input" data-preference="audioSource"><option value="auto" ${selected(prefs.audioSource, 'auto')}>自动选择（推荐）</option><option value="qqmusic" ${selected(prefs.audioSource, 'qqmusic')}>QQ 音乐优先</option><option value="netease" ${selected(prefs.audioSource, 'netease')}>网易云优先</option><option value="musicdl" ${selected(prefs.audioSource, 'musicdl')}>酷我 / 咪咕优先</option></select><div class="fmx-muted" style="margin-top:7px">同名歌曲会保留不同来源，标题后的〔来源〕可直接区分和切换。</div><label class="fmx-name" style="display:block;margin-top:14px">歌词源</label><select class="fmx-input" data-preference="lyricSource"><option value="auto" ${selected(prefs.lyricSource, 'auto')}>自动选择（推荐）</option><option value="same" ${selected(prefs.lyricSource, 'same')}>跟随歌曲源</option><option value="qqmusic" ${selected(prefs.lyricSource, 'qqmusic')}>QQ 音乐歌词</option><option value="netease" ${selected(prefs.lyricSource, 'netease')}>网易云歌词</option><option value="musicdl" ${selected(prefs.lyricSource, 'musicdl')}>酷我 / 咪咕歌词</option><option value="lx" ${selected(prefs.lyricSource, 'lx')}>洛雪歌词源</option></select></div></div><div class="fmx-section"><h3>内置音源</h3>${data.builtins.map(s => `
        <div class="fmx-card fmx-row"><div class="fmx-grow"><div class="fmx-name">${esc(sourceNames[s.id] || s.id)}</div><div class="fmx-muted">${esc(s.description)}</div></div><span class="fmx-status ${s.status === 'ok' ? 'fmx-ok' : 'fmx-warn'}">${esc(s.status)}</span><input class="fmx-switch" type="checkbox" data-toggle="${esc(s.id)}" ${s.enabled ? 'checked' : ''}></div>`).join('')}</div>
        <div class="fmx-section"><h3>QQ 音乐账号</h3><div class="fmx-card"><div class="fmx-row"><div class="fmx-grow"><div class="fmx-name" id="fmx-qq-state">${data.qq.loggedIn ? `已登录${data.qq.nickname ? `：${esc(data.qq.nickname)}` : ''}` : '未登录'}</div><div class="fmx-muted">扫码后按账号实际会员权益请求无损/高品质音频，不保存账号密码。</div></div><button class="fmx-btn fmx-primary" data-qq-login>${data.qq.loggedIn ? '重新登录' : '扫码登录'}</button>${data.qq.loggedIn ? '<button class="fmx-btn" data-qq-logout>退出</button>' : ''}</div><div id="fmx-qr-wrap" class="fmx-hidden" style="margin-top:14px"><img class="fmx-qr" id="fmx-qr"><div class="fmx-msg" id="fmx-qr-msg">请使用手机 QQ 扫码</div></div></div></div>
        <div class="fmx-section"><h3>洛雪音乐源</h3><div class="fmx-muted">兼容洛雪桌面版自定义源脚本，可从 URL、文件或文本导入。只导入你信任的来源。</div><div id="fmx-lx-list">${renderLx(data.lxSources)}</div><div class="fmx-card"><input class="fmx-input" id="fmx-lx-url" placeholder="https://example.com/source.js"><input class="fmx-input" id="fmx-lx-file" type="file" accept=".js,text/javascript"><textarea class="fmx-textarea" id="fmx-lx-script" placeholder="也可以直接粘贴洛雪自定义源脚本"></textarea><div class="fmx-row" style="margin-top:10px"><button class="fmx-btn fmx-primary" data-lx-import>导入音乐源</button></div><div class="fmx-msg" id="fmx-msg"></div></div></div>`;
      bind(root);
    } catch (error) { body.innerHTML = `<div class="fmx-card fmx-msg">加载失败：${esc(error.message)}</div>`; }
  }
  function renderLx(items) {
    if (!items?.length) return '<div class="fmx-card fmx-muted">尚未导入洛雪源</div>';
    return items.map(s => `<div class="fmx-card fmx-row"><div class="fmx-grow"><div class="fmx-name">${esc(s.name)}</div><div class="fmx-muted">${esc([s.version, s.author, s.description].filter(Boolean).join(' · '))}</div></div><input class="fmx-switch" type="checkbox" data-lx-toggle="${esc(s.id)}" ${s.enabled ? 'checked' : ''}><button class="fmx-btn fmx-danger" data-lx-delete="${esc(s.id)}">删除</button></div>`).join('');
  }
  function bind(root) {
    root.querySelectorAll('[data-preference]').forEach(el => el.addEventListener('change', async () => { try { await api('/preferences', { method:'PATCH', body:JSON.stringify({[el.dataset.preference]:el.value}) }); } catch(e) { alert(e.message); await load(root); } }));
    root.querySelectorAll('[data-toggle]').forEach(el => el.addEventListener('change', async () => { try { await api(`/sources/${el.dataset.toggle}`, { method:'PATCH', body:JSON.stringify({enabled:el.checked}) }); } catch(e) { el.checked=!el.checked; alert(e.message); } }));
    root.querySelectorAll('[data-lx-toggle]').forEach(el => el.addEventListener('change', async () => { try { await api(`/lx-sources/${el.dataset.lxToggle}`, { method:'PATCH', body:JSON.stringify({enabled:el.checked}) }); } catch(e) { el.checked=!el.checked; alert(e.message); } }));
    root.querySelectorAll('[data-lx-delete]').forEach(el => el.addEventListener('click', async () => { if (!confirm('删除这个洛雪音乐源？')) return; try { await api(`/lx-sources/${el.dataset.lxDelete}`, {method:'DELETE'}); await load(root); } catch(e) { alert(e.message); } }));
    root.querySelector('[data-lx-import]').addEventListener('click', async () => {
      const msg=root.querySelector('#fmx-msg'), file=root.querySelector('#fmx-lx-file').files[0]; msg.textContent='正在验证并导入…';
      try { const script=file ? await file.text() : root.querySelector('#fmx-lx-script').value; const url=root.querySelector('#fmx-lx-url').value.trim(); await api('/lx-sources',{method:'POST',body:JSON.stringify(script?{script}:{url})}); await load(root); } catch(e) { msg.textContent=`导入失败：${e.message}`; }
    });
    root.querySelector('[data-qq-login]').addEventListener('click', () => startQr(root));
    root.querySelector('[data-qq-logout]')?.addEventListener('click', async () => { await api('/qq/logout',{method:'POST'}); await load(root); });
  }
  async function startQr(root) {
    const wrap=root.querySelector('#fmx-qr-wrap'), img=root.querySelector('#fmx-qr'), msg=root.querySelector('#fmx-qr-msg'); wrap.classList.remove('fmx-hidden'); msg.textContent='正在获取二维码…';
    try { const result=await api('/qq/qrcode',{method:'POST'}), qr=result.data; img.src=`data:${qr.mime};base64,${qr.data}`; msg.textContent='请使用手机 QQ 扫码'; if(qrTimer)clearInterval(qrTimer); qrTimer=setInterval(async()=>{ try { const check=await api('/qq/qrcode/check',{method:'POST',body:JSON.stringify({identifier:qr.identifier,type:'qq'})}), event=Number(check.data.event); const labels={0:'登录成功',1:'已扫码，请在手机确认',2:'已确认，正在授权',3:'已拒绝',4:'二维码已过期','-1':'状态异常'}; msg.textContent=labels[event]||'等待扫码'; if(event===0){clearInterval(qrTimer);qrTimer=null;await load(root);} if([3,4,-1].includes(event)){clearInterval(qrTimer);qrTimer=null;} }catch(e){msg.textContent=e.message;} },2000); } catch(e) { msg.textContent=`二维码获取失败：${e.message}`; }
  }
  function addButton() {
    if (document.querySelector('#fmx-open')) return;
    const button=document.createElement('button');button.id='fmx-open';button.textContent='在线音源';button.title='在线音源设置';button.setAttribute('aria-label','在线音源设置');button.addEventListener('click',shell);document.body.appendChild(button);
  }
  function activeAudio() {
    const items=[...document.querySelectorAll('audio')];
    return items.find(el => /\/music\/api\/v1\/track\/(?:stream|hls)/.test(el.currentSrc || el.src || '')) || items[0] || null;
  }
  function guidFromUrl(value) {
    try {
      const url=new URL(value, location.href), query=url.searchParams.get('guid') || url.searchParams.get('trackGUID');
      if(query?.startsWith('online:')) return query;
      const match=decodeURIComponent(url.pathname).match(/\/track\/hls\/(online:[^/]+)/);
      return match?.[1] || '';
    } catch (_) { return ''; }
  }
  function currentTrack() {
    const audio=activeAudio();
    let guid=guidFromUrl(audio?.currentSrc || audio?.src || '');
    if(!guid) {
      const resources=performance.getEntriesByType('resource');
      for(let i=resources.length-1;i>=0 && !guid;i--) guid=guidFromUrl(resources[i].name);
    }
    const meta=navigator.mediaSession?.metadata;
    return {audio,guid,title:meta?.title || switchedTrack?.title || '',artist:meta?.artist || switchedTrack?.artist || ''};
  }
  function closeSourceMenu() { document.querySelector('#fmx-source-menu')?.remove(); }
  async function openSourceMenu() {
    closeSourceMenu();
    const track=currentTrack();
    if(!track.audio || !track.guid) return;
    const menu=document.createElement('div'); menu.id='fmx-source-menu'; menu.innerHTML='<div style="padding:7px 10px;color:#a8adb8;font-size:12px">选择当前歌曲的播放源</div>';
    document.body.appendChild(menu);
    try {
      const listing=await api('/sources');
      const labels={qqmusic:'QQ 音乐',netease:'网易云音乐',musicdl:'酷我 / 咪咕'};
      (listing.builtins || []).filter(s => labels[s.id]).forEach(source => {
        const button=document.createElement('button'); button.textContent=`${labels[source.id]}${source.enabled ? '' : '（未启用）'}`; button.disabled=!source.enabled;
        button.addEventListener('click',()=>switchPlayingSource(source.id,button)); menu.appendChild(button);
      });
    } catch(error) { menu.innerHTML=`<div style="padding:10px">加载失败：${esc(error.message)}</div>`; }
  }
  async function switchPlayingSource(source, button) {
    const current=currentTrack(), audio=current.audio;
    if(!audio) return;
    button.disabled=true; button.textContent='正在匹配并切换…';
    try {
      const result=await api('/resolve-track-source',{method:'POST',body:JSON.stringify({guid:current.guid,title:current.title,artist:current.artist,source})});
      const paused=audio.paused, position=Number.isFinite(audio.currentTime) ? audio.currentTime : 0;
      switchedTrack={guid:result.track.guid,title:result.track.title,artist:result.track.artist,sourceName:result.sourceName};
      audio.src=result.streamUrl; audio.load();
      audio.addEventListener('loadedmetadata',()=>{ try { audio.currentTime=Math.min(position,Math.max(0,(audio.duration || position)-.5)); } catch(_){} if(!paused) audio.play().catch(()=>{}); },{once:true});
      closeSourceMenu(); updatePlayerSource();
    } catch(error) { button.disabled=false; button.textContent=`切换失败：${error.message}`; }
  }
  function updatePlayerSource() {
    const track=currentTrack(), existing=document.querySelector('#fmx-player-source');
    if(!track.guid) { existing?.remove(); closeSourceMenu(); switchedTrack=null; return; }
    let label=switchedTrack?.guid===track.guid ? switchedTrack.sourceName : '';
    if(!label) { const src=track.guid.split(':')[1]; label={qq:'QQ 音乐',netease:'网易云',kuwo:'酷我',migu:'咪咕',kugou:'酷狗'}[src] || '在线音源'; }
    const button=existing || document.createElement('button'); button.id='fmx-player-source'; button.textContent=`来源：${label} ▾`; button.title='点击切换当前歌曲的播放源';
    if(!existing) { button.addEventListener('click',openSourceMenu); document.body.appendChild(button); }
  }
  const mobileNavItems=[['首页','⌂','/music/'],['收藏','♡','/music/favorites'],['最近','◷','/music/recent'],['搜索','⌕','/music/search']];
  function closeMobileMore(){ document.querySelector('#fmx-mobile-more')?.remove(); }
  function toggleMobileMore(){
    const current=document.querySelector('#fmx-mobile-more'); if(current){current.remove();return;}
    const menu=document.createElement('div');menu.id='fmx-mobile-more';
    menu.innerHTML=[['专辑','/music/albums'],['歌手','/music/artists'],['流派','/music/genres'],['音乐库','/music/library'],['歌单','/music/playlists'],['设置','/music/settings']].map(([label,href])=>`<a href="${href}">${label}</a>`).join('');
    document.body.appendChild(menu);
  }
  function updateMobileNav(){
    const mobile=window.matchMedia('(max-width:720px)').matches;
    document.documentElement.classList.toggle('fmx-mobile',mobile);
    document.documentElement.classList.toggle('fmx-now-playing-open',mobile && !!document.querySelector('[role="dialog"][aria-modal="true"]'));
    if(!mobile){document.querySelector('#fmx-mobile-nav')?.remove();closeMobileMore();return;}
    let nav=document.querySelector('#fmx-mobile-nav');
    if(!nav){
      nav=document.createElement('nav');nav.id='fmx-mobile-nav';nav.setAttribute('aria-label','手机导航');
      nav.innerHTML=mobileNavItems.map(([label,icon,href])=>`<a href="${href}" data-fmx-path="${href}"><span class="fmx-nav-icon">${icon}</span><span>${label}</span></a>`).join('')+'<button type="button" data-fmx-more><span class="fmx-nav-icon">☰</span><span>更多</span></button>';
      nav.querySelector('[data-fmx-more]').addEventListener('click',toggleMobileMore);document.body.appendChild(nav);
    }
    const path=location.pathname.replace(/\/+$/,'') || '/music';
    nav.querySelectorAll('[data-fmx-path]').forEach(link=>{const target=link.dataset.fmxPath.replace(/\/+$/,'');link.classList.toggle('fmx-active',target==='/music'?path==='/music':path.startsWith(target));});
  }
  document.addEventListener('click',event=>{ if(!event.target.closest('#fmx-player-source,#fmx-source-menu')) closeSourceMenu();if(!event.target.closest('#fmx-mobile-more,[data-fmx-more]')) closeMobileMore(); });
  if (window.__FNMUSIC_EXT_STANDALONE__) { const mount=document.querySelector('#fnmusic-ext-standalone'); const root=shell(); root.style.position='relative'; root.style.minHeight='100vh'; root.style.background='transparent'; mount.replaceWith(root); }
  else { addButton(); updatePlayerSource(); updateMobileNav(); setInterval(()=>{updatePlayerSource();updateMobileNav();},1000);window.addEventListener('resize',updateMobileNav,{passive:true});new MutationObserver(()=>{addButton();updateMobileNav();}).observe(document.documentElement,{childList:true,subtree:true}); }
})();
