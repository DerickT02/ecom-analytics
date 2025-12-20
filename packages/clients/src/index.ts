import { Kafka } from "kafkajs";

const kafka = new Kafka({
    clientId: 'event-analytics',
    brokers: ["localhost:9092"]
})

export function createProducer() {
    return kafka.producer();
}