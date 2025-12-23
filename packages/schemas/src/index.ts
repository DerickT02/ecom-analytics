import * as z from "zod";

export const EventSchema = z.object({
  eventId: z.number().int().positive(),

  name: z.string().min(1),

  time: z.string().datetime(), // ISO 8601 timestamp

  source: z.enum(["web", "mobile", "backend"]),

  anonymousId: z.string().min(1),

  sessionId: z.string().min(1),

  properties: z.record(z.string(), z.any()),

  schemaVersion: z.number().int().positive()
});

export const EventPackage = z.object({
    packageId: z.number().int(),
    events: z.array(EventSchema),
    
})


