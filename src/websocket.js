const { WebSocketServer } = require('ws');
const Redis = require('ioredis');

const PORT = 5005;
const wss = new WebSocketServer({ port: PORT });

// Suscriptor de Redis en canal Pub/Sub
const redisSub = new Redis({ host: '127.0.0.1', port: 6380 });

redisSub.subscribe('analytics:stream', (err, count) => {
  if (err) console.error('❌ Error suscripción Redis Pub/Sub:', err);
  else console.log(`📡 [WebSocket Server] Suscrito a ${count} canal(es) de Redis Pub/Sub`);
});

wss.on('connection', (ws) => {
  console.log('⚡ Nuevo cliente WebSocket conectado');
  ws.send(JSON.stringify({ message: 'Conectado al feed de analíticas en tiempo real' }));
});

redisSub.on('message', (channel, message) => {
  // Reenviar eventos de Redis a todos los clientes WebSocket conectados
  wss.clients.forEach((client) => {
    if (client.readyState === 1) { // OPEN
      client.send(message);
    }
  });
});

console.log(`🚀 WebSocket Engine corriendo en ws://localhost:${PORT}`);
