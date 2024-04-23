const express = require('express');
const kafka = require('kafka-node');

const app = express();

const kafkaHost = 'localhost:9092'; 

const client = new kafka.KafkaClient({ kafkaHost });
const producer = new kafka.Producer(client);

producer.on('ready', () => {
  console.log('Producer is ready');
});

producer.on('error', (err) => {
  console.error('Producer error:', err);
});

app.get('/', (req, res) => {
  res.send('Hello Kafka!');
});

app.post('/send', (req, res) => {
  const payload = [{ topic: 'test-topic', messages: 'Hello Kafka!' }];
  producer.send(payload, (err, data) => {
    if (err) {
      console.error('Error sending message:', err);
      res.status(500).send('Error sending message');
    } else {
      console.log('Message sent:', data);
      res.send('Message sent successfully');
    }
  });
});

const server = app.listen(3000, () => {
  console.log('Server is running on port 3000');
});

process.on('SIGINT', () => {
  producer.close(() => {
    server.close();
    process.exit();
  });
});
