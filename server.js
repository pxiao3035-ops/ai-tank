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
function leave(c, name) { const m = rooms.get(name); if (!m || !m.delete(c.peer)) return; c.rooms.delete(name); c.state.delete(name); for (const o of m.values()) send(o, { t: 'bye', room: name, peer: c.peer }); if (!m.size) rooms.delete(name); }
function drop(c) { if (c.dead) return; for (const r of [...c.rooms]) leave(c, r); c.dead = true; conns.delete(c); c.sock.destroy(); }

function accept(sock) {
  const c = { sock, peer: 'p' + (nextId++).toString(36) + crypto.randomBytes(3).toString('hex'), rooms: new Set(), state: new Map(), dead: false, alive: true, tokens: 80, last: Date.now(), buf: Buffer.alloc(0), parts: [] };
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
  const now = Date.now(); c.tokens = Math.min(80, c.tokens + (now - c.last) * 0.05); c.last = now; if (c.tokens < 1) return; c.tokens--;      // 每人每秒最多约 50 条
  let m; try { m = JSON.parse(text); } catch (e) { return; }
  if (!m || typeof m !== 'object' || typeof m.room !== 'string') return; const name = m.room;
  if (m.t === 'join') {
    if (!/^[a-z0-9][a-z0-9_.-]{0,47}$/.test(name) || c.rooms.size > MAX_ROOMS) return send(c, { t: 'no', room: name });
    const mem = members(name); if (!mem.has(c.peer) && mem.size >= MAX_PER_ROOM) return send(c, { t: 'no', room: name });
    mem.set(c.peer, c); c.rooms.add(name); send(c, { t: 'ok', room: name });
    for (const o of mem.values()) if (o !== c && o.state.has(name)) send(c, { t: 'p', room: name, peer: o.peer, presence: o.state.get(name) });
  } else if (m.t === 'leave') { if (name) leave(c, name); }
  else if (m.t === 'p') {
    if (!c.rooms.has(name) || !m.presence || typeof m.presence !== 'object' || Array.isArray(m.presence)) return;
    if (Buffer.byteLength(JSON.stringify(m.presence)) > MAX_STATE) return;
    c.state.set(name, m.presence); for (const o of rooms.get(name).values()) if (o !== c) send(o, { t: 'p', room: name, peer: c.peer, presence: m.presence });
  }
}
// 心跳：25 秒问一次，没回应的连接清掉
setInterval(() => { for (const c of [...conns]) { if (!c.alive) { drop(c); continue; } c.alive = false; c.sock.write(frame(9, Buffer.alloc(0))); } }, 25000);
server.listen(PORT, () => console.log('AI 坦克大战已启动：http://localhost:' + PORT));
