import { Kafka } from "kafkajs";

const kafka = new Kafka({
    clientId: 'event-analytics',
    brokers: ["localhost:9092"]
})

export function createProducer() {
    return kafka.producer();
}

export function createConsumer(consumer_name: string) {
    return kafka.consumer({ groupId: consumer_name });
}