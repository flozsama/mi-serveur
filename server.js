'use strict';
/**
 * L'héritage de la MI — serveur de salons (Node.js + Socket.io)
 *
 * Rôle : relais « bête » entre 2 joueurs. Aucune logique de jeu ici : le jeu garde son
 * architecture actuelle (J1 = hôte autoritaire qui simule tout, J2 = invité qui envoie ses
 * touches et affiche les snapshots). Le serveur se contente de :
 *   - créer un salon (code à 4 caractères) pour 2 joueurs maximum,
 *   - apparier l'invité avec l'hôte,
 *   - relayer les messages de l'un vers l'autre (fiable ou « volatile »),
 *   - prévenir l'autre joueur quand quelqu'un part, et détruire le salon.
 *
 * Protocole (client -> serveur) :
 *   'create' (ack)            -> ack({ok:true, code}) | ack({ok:false, err})
 *   'join'   (code, ack)      -> ack({ok:true, code}) | ack({ok:false, err:'not_found'|'full'|'self'})
 *   'm'      (string)         -> relais fiable   (actions, choix, boutique…)
 *   'v'      (string)         -> relais volatile (snapshots 30 Hz, touches 60 Hz : un paquet en retard est jeté)
 *   'p'      (ack)            -> ping
 *   'leave'                   -> quitter le salon
 * Serveur -> client : 'paired' {role:'host'|'guest'}, 'm' (string), 'peer-left', 'room-expired'.
 */
const http = require('http');
const crypto = require('crypto');
const { Server } = require('socket.io');

const PORT = +process.env.PORT || 3000;
/* Sites autorisés à se connecter (séparés par des virgules). Surchargeable : ALLOWED_ORIGINS=https://a.io,https://b.io */
const ORIGINS = (process.env.ALLOWED_ORIGINS || 'https://flozsama.github.io').split(',').map(s => s.trim()).filter(Boolean);
const LOCAL_RE = /^https?:\/\/(localhost|127\.0\.0\.1)(:\d+)?$/;
const DEV = process.env.NODE_ENV !== 'production';
const originOk = o => !o || ORIGINS.includes(o) || (DEV && LOCAL_RE.test(o));   // pas d'Origin = outil non-navigateur (tests)

const MAX_MSG = 256 * 1024;            // taille max d'un message relayé (caractères)
const RATE_PER_SEC = 200, BURST = 400; // limite par socket (jeu : ~60 msg/s côté invité, ~30 côté hôte)
const WAIT_TTL = 10 * 60 * 1000;       // un salon sans J2 expire après 10 min
const MAX_ROOMS = 2000;
const ALPHABET = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';   // sans I, O, 0, 1 (évite les confusions)

/** code -> { code, host: Socket, guest: Socket|null, born: ms } */
const rooms = new Map();

const server = http.createServer((req, res) => {
  if (req.url === '/' || req.url === '/health') {     // utile pour l'hébergeur (health check) et pour « réveiller » le serveur
    res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
    res.end(JSON.stringify({ ok: true, rooms: rooms.size }));
    return;
  }
  res.writeHead(404); res.end();
});

const io = new Server(server, {
  cors: { origin: (o, cb) => cb(null, originOk(o)) },
  allowRequest: (req, cb) => cb(null, originOk(req.headers.origin)),   // CORS ne protège pas le WebSocket : on vérifie l'Origin ici aussi
  maxHttpBufferSize: 1e6,
  pingInterval: 10000,
  pingTimeout: 8000,
  perMessageDeflate: { threshold: 1024, zlibDeflateOptions: { level: 1 } }   // les snapshots JSON se compressent très bien
});

const noop = () => {};
const fn = f => (typeof f === 'function' ? f : noop);

function newCode() {
  if (rooms.size >= MAX_ROOMS) return null;
  for (let n = 0; n < 50; n++) {
    let c = '';
    for (let i = 0; i < 4; i++) c += ALPHABET[crypto.randomInt(ALPHABET.length)];
    if (!rooms.has(c)) return c;
  }
  return null;
}

function allow(s) {                                   // seau à jetons par socket
  const now = Date.now(), d = s.data.rl || (s.data.rl = { t: now, n: BURST });
  d.n = Math.min(BURST, d.n + (now - d.t) * RATE_PER_SEC / 1000); d.t = now;
  if (d.n < 1) return false;
  d.n--; return true;
}

/** Retire `socket` de son salon. Le salon est détruit et l'autre joueur est prévenu. */
function leave(socket) {
  const code = socket.data.room;
  if (!code) return;
  socket.data.room = null;
  const r = rooms.get(code);
  if (!r) return;
  rooms.delete(code);
  const other = r.host === socket ? r.guest : r.host;
  if (other) { other.data.room = null; other.emit('peer-left'); }
  console.log(`[room ${code}] fermé (${r.guest ? '2 joueurs' : 'sans invité'}), restants : ${rooms.size}`);
}

function relay(socket, data, volatile) {
  if (typeof data !== 'string' || data.length > MAX_MSG || !allow(socket)) return;
  const r = rooms.get(socket.data.room);
  if (!r || !r.guest) return;                          // pas encore à deux : on jette
  const other = r.host === socket ? r.guest : r.host;
  (volatile ? other.volatile : other).emit('m', data);
}

io.on('connection', socket => {
  socket.on('create', ack => {
    ack = fn(ack);
    if (!allow(socket)) return ack({ ok: false, err: 'busy' });
    leave(socket);
    const code = newCode();
    if (!code) return ack({ ok: false, err: 'busy' });
    rooms.set(code, { code, host: socket, guest: null, born: Date.now() });
    socket.data.room = code;
    console.log(`[room ${code}] créé (${rooms.size} salon(s))`);
    ack({ ok: true, code });
  });

  socket.on('join', (code, ack) => {
    ack = fn(ack);
    if (!allow(socket)) return ack({ ok: false, err: 'busy' });
    code = typeof code === 'string' ? code.trim().toUpperCase() : '';
    const r = rooms.get(code);
    if (!r) {
      if ((socket.data.bad = (socket.data.bad || 0) + 1) > 20) socket.disconnect(true);   // anti-devinette de codes
      return ack({ ok: false, err: 'not_found' });
    }
    if (r.host === socket) return ack({ ok: false, err: 'self' });
    if (r.guest) return ack({ ok: false, err: 'full' });
    leave(socket);
    r.guest = socket; socket.data.room = code;
    console.log(`[room ${code}] J2 a rejoint`);
    ack({ ok: true, code });
    r.host.emit('paired', { role: 'host' });
    socket.emit('paired', { role: 'guest' });
  });

  socket.on('m', d => relay(socket, d, false));
  socket.on('v', d => relay(socket, d, true));
  socket.on('p', ack => { if (allow(socket)) fn(ack)(); });
  socket.on('leave', () => leave(socket));
  socket.on('disconnect', () => leave(socket));
});

/* Salons d'attente abandonnés */
const sweeper = setInterval(() => {
  const now = Date.now();
  for (const [code, r] of rooms) {
    if (!r.guest && now - r.born > WAIT_TTL) {
      rooms.delete(code); r.host.data.room = null; r.host.emit('room-expired');
      console.log(`[room ${code}] expiré`);
    }
  }
}, 30000);
sweeper.unref();

if (require.main === module) {
  server.listen(PORT, () => console.log(`Serveur MI prêt sur :${PORT} — origines autorisées : ${ORIGINS.join(', ')}${DEV ? ' (+ localhost)' : ''}`));
  const stop = () => { io.close(() => process.exit(0)); setTimeout(() => process.exit(0), 3000).unref(); };
  process.on('SIGTERM', stop); process.on('SIGINT', stop);
}

module.exports = { server, io, rooms };
