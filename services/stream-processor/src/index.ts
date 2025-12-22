import { createConsumer } from "@analytics/clients"

const consumer = createConsumer("stream-processor")
consumer.subscribe({topic: "events_raw", fromBeginning: false})

await consumer.run({
    eachMessage: async ({ message}) => {
        console.log(`Received message: ${message.value?.toString()}`)
    }
})

process.on("SIGINT", async () => {
    await consumer.stop()
    process.exit(0)
});

