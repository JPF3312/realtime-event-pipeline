const { kafka } = require('./kafka');
const Redis = require('ioredis');

const redis = new Redis({ host: '127.0.0.1', port: 6380 });
const redisPub = new Redis({ host: '127.0.0.1', port: 6380 });

const consumer = kafka.consumer({ groupId: 'analytics-worker-group' });

const runConsumer = async () => {
  await consumer.connect();
  console.log('✅ Consumer connected to Kafka Broker');

  await consumer.subscribe({ topic: 'telemetry-events', fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const event = JSON.parse(message.value.toString());
      const { eventType, userId } = event;

      console.log(`📥 [Worker Processed] Partition: ${partition} | Type: ${eventType} | User: ${userId}`);

      await redis.hincrby('analytics:events_count', eventType, 1);
      await redis.lpush(`user:${userId}:activity`, JSON.stringify(event));
      await redis.ltrim(`user:${userId}:activity`, 0, 9);
      const totalEvents = await redis.incr('analytics:total_events');

      // Publicar notificación Pub/Sub para WebSockets
      const broadcastData = {
        eventType,
        userId,
        totalEvents,
        timestamp: new Date().toISOString()
      };

      await redisPub.publish('analytics:stream', JSON.stringify(broadcastData));
    }
  });
};

runConsumer().catch(err => {
  console.error('❌ Error in Consumer Worker:', err);
});
