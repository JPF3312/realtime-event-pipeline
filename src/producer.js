const express = require('express');
const { producer, connectProducer } = require('./kafka');

const app = express();
const PORT = process.env.PORT || 4000;

app.use(express.json());

app.post('/events', async (req, res) => {
  const { eventType, userId, payload } = req.body;

  if (!eventType || !userId) {
    return res.status(400).json({ error: 'eventType y userId son requeridos' });
  }

  const eventMessage = {
    eventType,
    userId,
    payload: payload || {},
    timestamp: new Date().toISOString()
  };

  try {
    // Publicamos el evento en la partición correspondiente
    await producer.send({
      topic: 'telemetry-events',
      messages: [
        {
          key: userId, // La clave garantiza el orden de eventos por usuario
          value: JSON.stringify(eventMessage)
        }
      ]
    });

    console.log(`[Event Produced] Topic: telemetry-events | Type: ${eventType} | User: ${userId}`);
    res.status(202).json({ status: 'Accepted', event: eventMessage });
  } catch (error) {
    console.error('❌ Error publishing event to Kafka:', error);
    res.status(500).json({ error: 'Failed to publish event' });
  }
});

app.get('/health', (req, res) => {
  res.json({ status: 'Producer API is healthy' });
});

app.listen(PORT, async () => {
  await connectProducer();
  console.log(`🚀 Ingestion Producer API running on port ${PORT}`);
});
