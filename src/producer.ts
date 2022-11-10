import reader from "readline-sync";
import { Kafka } from "kafkajs";

const kafka = new Kafka({
  clientId: "test-app",
  brokers: ["localhost:29092"],
});

const producer = kafka.producer({
  maxInFlightRequests: 1,
  idempotent: true,
});

const topic = 'test-topic';

const sendMessage = async (input: string) => {
  return producer
    .send({
      topic,
      messages: [{
          key: 'test-key',
          value: Buffer.from(JSON.stringify(input))
        }],
    });
}

const run = async () => {
  await producer.connect();
  while (true) {
    let input: string = await reader.question("Data: ");
    if (input === "exit") {
      process.exit(0);
    }
    try {
      await sendMessage(input);
    } catch (e) {
      console.error(e);
    }
  }
}

run().catch(console.log);