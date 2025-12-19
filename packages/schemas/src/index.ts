import * as z from "zod";

export const BaseEvent = z.object({
    eventType: z.string(),
    timestamp: z.date(),
    eventId: z.string(),
    source: z.string(),

    properties: z.object({
        key: z.string(),
        value: z.any()
    }).nullable()
})

export const EventPackage = z.object({
    events: z.array(BaseEvent),
    writeKey: z.string(),
    timestamp: z.date(),
})


