const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'analytics-producer-service',
  brokers: ['localhost:19092']
});

const producer = kafka.producer();

const connectProducer = async () => {
  try {
    await producer.connect();
    console.log('✅ Connected to Kafka/Redpanda Broker');
  } catch (error) {
    console.error('❌ Error connecting to Kafka:', error);
    process.exit(1);
  }
};

module.exports = { kafka, producer, connectProducer };
