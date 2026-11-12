'use strict';
/* AI 坦克大战 · 联机服务器
 * 用法：node server.js   （需要 Node.js 18 或更新版本，不用安装任何依赖）
 * 作用：1) 把同目录下的 index.html（游戏本体）发给浏览器；
 *       2) 在 /ws 上提供“房间”通道：同一个房间里的人互相看得到对方公开的一小块状态。
 * 游戏逻辑在各玩家的浏览器里算，服务器只负责转发，所以很省资源。
 * 端口：环境变量 PORT，默认 8080。
 */
const http = require('http'), fs = require('fs'), path = require('path'), crypto = require('crypto');
const PORT = +process.env.PORT || 8080;
const MAX_STATE = 4096, MAX_ROOMS = 16, MAX_PER_ROOM = 64, MAX_CONN = 2000, MAX_FRAME = 65536;
const HTML = fs.readFileSync(path.join(__dirname, 'index.html'));
const rooms = new Map();          // 房间名 -> Map(成员编号 -> 连接)
const conns = new Set(); let nextId = 1;

const server = http.createServer((req, res) => {
  const u = req.url.split('?')[0];
  if (u === '/' || u === '/index.html') { res.writeHead(200, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-cache' }); res.end(HTML); }
  else if (u === '/healthz') { res.writeHead(200, { 'content-type': 'text/plain' }); res.end('ok ' + conns.size); }
  else { res.writeHead(404, { 'content-type': 'text/plain' }); res.end('not found'); }
});

server.on('upgrade', (req, sock) => {
  const key = req.headers['sec-websocket-key'];
  if (req.url.split('?')[0] !== '/ws' || !key || conns.size >= MAX_CONN) { sock.destroy(); return; }
  const acc = crypto.createHash('sha1').update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
  sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + acc + '\r\n\r\n');
  sock.setNoDelay(true); accept(sock);
});

function frame(op, buf) {
  const n = buf.length; let h;
  if (n < 126) { h = Buffer.alloc(2); h[1] = n; } else if (n < 65536) { h = Buffer.alloc(4); h[1] = 126; h.writeUInt16BE(n, 2); } else { h = Buffer.alloc(10); h[1] = 127; h.writeUInt32BE(0, 2); h.writeUInt32BE(n, 6); }
  h[0] = 0x80 | op; return Buffer.concat([h, buf]);
}
function send(c, obj) { if (c.dead) return; if (c.sock.writableLength > 1 << 20) return drop(c); c.sock.write(frame(1, Buffer.from(JSON.stringify(obj)))); }
function members(name) { let m = rooms.get(name); if (!m) { m = new Map(); rooms.set(name, m); } return m; }
function leave(c, name) { const m = rooms.get(name); if (!m || !m.delete(c.peer)) return; c.rooms.delete(name); c.state.delete(name); for (const o of m.values()) send(o, { t: 'bye', room: name, peer: c.peer }); if (!m.size) { rooms.delete(name); stopSeq(name); } }
function drop(c) { if (c.dead) return; for (const r of [...c.rooms]) leave(c, r); c.dead = true; conns.delete(c); c.sock.destroy(); }

function accept(sock) {
  const c = { sock, peer: 'p' + (nextId++).toString(36) + crypto.randomBytes(3).toString('hex'), rooms: new Set(), state: new Map(), dead: false, alive: true, tokens: 150, last: Date.now(), buf: Buffer.alloc(0), parts: [] };
  conns.add(c);
  members('').set(c.peer, c); c.rooms.add('');           // 大厅：所有人都在，快速匹配用
  send(c, { t: 'hi', peer: c.peer });
  for (const o of members('').values()) if (o !== c && o.state.has('')) send(c, { t: 'p', room: '', peer: o.peer, presence: o.state.get('') });
  sock.on('data', d => { c.buf = c.buf.length ? Buffer.concat([c.buf, d]) : d; try { parse(c); } catch (e) { drop(c); } });
  sock.on('close', () => drop(c)); sock.on('error', () => drop(c));
}
function parse(c) {
  for (; ;) {
    const b = c.buf; if (b.length < 2) return;
    const fin = b[0] & 0x80, op = b[0] & 15, masked = b[1] & 0x80; let len = b[1] & 127, off = 2;
    if (len === 126) { if (b.length < 4) return; len = b.readUInt16BE(2); off = 4; } else if (len === 127) { if (b.length < 10) return; if (b.readUInt32BE(2)) return drop(c); len = b.readUInt32BE(6); off = 10; }
    if (!masked || len > MAX_FRAME) return drop(c);
    if (b.length < off + 4 + len) return;
    const mask = b.subarray(off, off + 4), data = Buffer.from(b.subarray(off + 4, off + 4 + len)); for (let i = 0; i < len; i++) data[i] ^= mask[i & 3];
    c.buf = b.subarray(off + 4 + len);
    if (op === 8) return drop(c);
    if (op === 9) { c.sock.write(frame(10, data)); continue; }
    if (op === 10) { c.alive = true; continue; }
    if (op === 1 || op === 0) { c.parts.push(data); if (c.parts.reduce((n, p) => n + p.length, 0) > MAX_FRAME) return drop(c); if (fin) { const text = Buffer.concat(c.parts).toString('utf8'); c.parts = []; handle(c, text); if (c.dead) return; } }
  }
}
function handle(c, text) {
  const now = Date.now(); c.tokens = Math.min(150, c.tokens + (now - c.last) * 0.1); c.last = now; if (c.tokens < 1) return; c.tokens--;      // 每人每秒最多约 100 条
  let m; try { m = JSON.parse(text); } catch (e) { return; }
  if (!m || typeof m !== 'object' || typeof m.room !== 'string') return; const name = m.room;
  if (m.t === 'join') {
    if (!/^[a-z0-9][a-z0-9_.-]{0,47}$/.test(name) || c.rooms.size > MAX_ROOMS) return send(c, { t: 'no', room: name });
    const mem = members(name); if (!mem.has(c.peer) && mem.size >= MAX_PER_ROOM) return send(c, { t: 'no', room: name });
    mem.set(c.peer, c); c.rooms.add(name); send(c, { t: 'ok', room: name });
    for (const o of mem.values()) if (o !== c && o.state.has(name)) send(c, { t: 'p', room: name, peer: o.peer, presence: o.state.get(name) });
  } else if (m.t === 'leave') { if (name) leave(c, name); }
  else if (m.t === 'seq') startSeq(c, name, m);
  else if (m.t === 'seqend') { const S = seqs.get(name); if (S && S.id === m.id && c.rooms.has(name) && !S.ended) { S.ended = true; S.endAt = Date.now(); } }
  else if (m.t === 'p') {
    if (!c.rooms.has(name) || !m.presence || typeof m.presence !== 'object' || Array.isArray(m.presence)) return;
    if (Buffer.byteLength(JSON.stringify(m.presence)) > MAX_STATE || (typeof m.presence.c === 'string' && m.presence.c[0] === '~')) return;
    c.state.set(name, m.presence); for (const o of rooms.get(name).values()) if (o !== c) send(o, { t: 'p', room: name, peer: c.peer, presence: m.presence });
  }
}
/* ---------- 排指令 ----------
 * 对局开始后，服务器按每秒 60 帧的节拍，决定每一帧每位玩家的指令（取各人最新公开的按键状态），
 * 把“最近 4 秒的指令变化”发给房间里所有人；各人的浏览器照着同一份指令各自推进游戏。
 * 服务器不运行游戏逻辑，只当裁判表。这样每个人的操作延迟都只是“自己到服务器往返一趟”。
 * 指令字节：低 3 位方向、8 开火、16 道具、32 技能、64 由 AI 代打（掉线或落后太多）。
 */
const seqs = new Map(), TICK = 1000 / 60, WIN = 240;
function stopSeq(name) { const S = seqs.get(name); if (S) { clearInterval(S.timer); seqs.delete(name); } }
function startSeq(c, name, m) {
  if (!name || !c.rooms.has(name) || typeof m.id !== 'string' || m.id.length > 40 || !Array.isArray(m.cids) || m.cids.length < 1 || m.cids.length > 8 || !m.cids.every(x => typeof x === 'string' && x.length <= 16)) return;
  stopSeq(name); const nh = m.cids.length;
  const S = { room: name, id: m.id, cids: m.cids, nh, T: 0, t0: Date.now() + 1500, log: [], bj: 0, cur: new Array(nh).fill(0), ic: [], ac: [], fc: [], dc: [], away: [], served: new Map(), ended: false, endAt: 0, lastSend: 0, timer: null };
  S.timer = setInterval(() => seqTick(S), 8); seqs.set(name, S);
}
function seqTick(S) {
  const mem = rooms.get(S.room), now = Date.now(); if (!mem || !mem.size) return stopSeq(S.room);
  if (S.ended) { if (now - S.endAt > 20000) return stopSeq(S.room); if (now - S.lastSend >= 250) sendFrames(S, mem, now); return; }      // 结束后再发 20 秒，等慢的人追上
  let due = Math.floor((now - S.t0) / TICK); if (due <= S.T) return; if (due > 216000) { S.ended = true; S.endAt = now; return; }     // 一局最长 1 小时
  if (due - S.T > 30) due = S.T + 30;
  const pres = new Map(); for (const o of mem.values()) { const q = o.state.get(S.room); if (q && typeof q.c === 'string') pres.set(q.c, q); }
  while (S.T < due) {
    const T = ++S.T;
    for (let s = 0; s < S.nh; s++) {
      const q = pres.get(S.cids[s]), i = q && Array.isArray(q.i) ? q.i : null, lag = q ? T - (q.a | 0) : 1e9; let b;
      if (!i || lag > (S.away[s] ? 90 : 300)) { b = 64; S.away[s] = 1; }
      else {
        S.away[s] = 0; b = i[0] & 15; if ((b & 7) > 4) b &= 8;
        if (i[1] !== S.ic[s]) { if (S.ic[s] !== undefined) b |= 16; S.ic[s] = i[1]; }
        if (i[2] !== S.ac[s]) { if (S.ac[s] !== undefined) b |= 32; S.ac[s] = i[2]; }
        if (i[3] !== S.fc[s]) { if (S.fc[s] !== undefined) b |= 8; S.fc[s] = i[3]; }
        if (i[4] !== S.dc[s]) { if (S.dc[s] !== undefined && (b & 7) === 0 && i[5] >= 1 && i[5] <= 4) b |= i[5]; S.dc[s] = i[4]; }
      }
      if (b !== S.cur[s]) { S.cur[s] = b; S.log.push(T * 1024 + s * 128 + b); }
    }
  }
  sendFrames(S, mem, now);
}
function delta(L, i, from) { const d = []; let prev = from; for (; i < L.length; i++) { const tk = Math.floor(L[i] / 1024); d.push((tk - prev) * 1024 + L[i] % 1024); prev = tk; } return d; }
function sendFrames(S, mem, now) {
  S.lastSend = now; const L = S.log, b = Math.max(0, S.T - WIN); while (S.bj < L.length && Math.floor(L[S.bj] / 1024) <= b) S.bj++;
  const fr = { c: '~seq', h: 1, id: S.id, t: S.T, b, d: delta(L, S.bj, b), ds: [], lg: null };
  const plain = frame(1, Buffer.from(JSON.stringify({ t: 'p', room: S.room, peer: '~seq', presence: fr })));
  for (const o of mem.values()) {
    const q = o.state.get(S.room), rq = q && Array.isArray(q.rq) ? q.rq : null;
    if (rq && S.served.get(q.c) !== rq[0]) {                    // 这个人缺了一段（刚重连或刷新）：把缺口之后的全部指令一次补给他
      if (Array.isArray(q.la) && q.la[0] === rq[0]) S.served.set(q.c, rq[0]);
      else if (!(now - (o.lgAt || 0) < 300)) {
        o.lgAt = now; const from = Math.max(0, rq[1] | 0); let i = 0; while (i < L.length && Math.floor(L[i] / 1024) <= from) i++;
        send(o, { t: 'p', room: S.room, peer: '~seq', presence: Object.assign({}, fr, { lg: { to: q.c, n: rq[0], i: 0, m: 1, f: from, d: delta(L, i, from), e: S.T } }) }); continue;
      }
    }
    if (!o.dead) { if (o.sock.writableLength > 1 << 20) drop(o); else o.sock.write(plain); }
  }
}

// 心跳：25 秒问一次，没回应的连接清掉
setInterval(() => { for (const c of [...conns]) { if (!c.alive) { drop(c); continue; } c.alive = false; c.sock.write(frame(9, Buffer.alloc(0))); } }, 25000);
server.listen(PORT, () => console.log('AI 坦克大战已启动：http://localhost:' + PORT));
