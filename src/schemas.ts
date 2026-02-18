import { z } from "zod";

export const signatureSchema = z.object({
  alg: z.literal("EdDSA").default("EdDSA"),
  ts: z.string(),
  nonce: z.string().min(8),
  sig: z.string().min(16),
  kid: z.string().optional(),
});

export const syncRequestSchema = z.object({
  siteId: z.string().uuid(),
  sitePubKey: z.string().min(16),
  siteKeyAlg: z.literal("ed25519").default("ed25519"),
  siteUrl: z.string().optional().nullable(),
  appVersion: z.string().max(64).optional().nullable(),
  buildId: z.string().max(128).optional().nullable(),
  commit: z.string().max(128).optional().nullable(),
  builtAt: z.string().optional().nullable(),
  idempotencyKey: z.string().max(255).optional().nullable(),
  signature: signatureSchema,
});

export const deregisterRequestSchema = z.object({
  siteId: z.string().uuid(),
  reason: z.string().max(255).optional().nullable(),
  requestedAt: z.string().optional().nullable(),
  signature: signatureSchema,
});

export const statusRequestSchema = z.object({
  siteId: z.string().uuid(),
  requestedAt: z.string().optional().nullable(),
  signature: signatureSchema,
});

export const dispatchMessageSchema = z.object({
  deliveryId: z.string().min(1),
  instanceId: z.string().min(1),
  siteId: z.string().uuid(),
  siteUrl: z.string().url(),
  scheduledFor: z.string(),
  enqueuedAt: z.string(),
});

export type SignaturePayload = z.infer<typeof signatureSchema>;
export type SyncRequestPayload = z.infer<typeof syncRequestSchema>;
export type DeregisterRequestPayload = z.infer<typeof deregisterRequestSchema>;
export type StatusRequestPayload = z.infer<typeof statusRequestSchema>;
