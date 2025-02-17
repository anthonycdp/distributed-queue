import { describe, it, expect, beforeEach, vi } from 'vitest';
import { IdempotencyManager } from '../src/core/idempotency.js';
import { createLogger } from '../src/observability/logger.js';
import type { Message, ProcessingResult } from '../src/types/index.js';

describe('IdempotencyManager', () => {
  let manager: IdempotencyManager;
  const logger = createLogger('error');

  // Mock Redis store to simulate Redis behavior
  const mockStore = new Map<string, { value: string; ttl?: number }>();

  // Create mock Redis client
  const createMockRedis = () => ({
    get: vi.fn(async (key: string) => {
      const entry = mockStore.get(key);
      return entry?.value ?? null;
    }),
    setex: vi.fn(async (key: string, ttl: number, value: string) => {
      mockStore.set(key, { value, ttl });
      return 'OK';
    }),
    set: vi.fn(async (key: string, ...args: (string | number)[]) => {
      // Handle SET key value EX ttl NX format
      // args[0] = value, args[1] = 'EX', args[2] = ttl, args[3] = 'NX'
      const hasNX = args.includes('NX');
      if (hasNX) {
        if (mockStore.has(key)) {
          return null;
        }
        const exIndex = args.indexOf('EX');
        const ttl = exIndex !== -1 && args[exIndex + 1] ? Number(args[exIndex + 1]) : undefined;
        mockStore.set(key, { value: String(args[0]), ttl });
        return 'OK';
      }
      mockStore.set(key, { value: String(args[0]) });
      return 'OK';
    }),
    del: vi.fn(async (...keys: string[]) => {
      let deleted = 0;
      for (const key of keys) {
        if (mockStore.delete(key)) {
          deleted++;
        }
      }
      return deleted;
    }),
    ttl: vi.fn(async (key: string) => {
      const entry = mockStore.get(key);
      if (!entry) {
        return -2; // Key doesn't exist
      }
      if (entry.ttl === undefined) {
        return -1; // No expiration
      }
      return entry.ttl;
    }),
    keys: vi.fn(async (pattern: string) => {
      // Simple pattern matching for test purposes
      const prefix = pattern.replace('*', '');
      return Array.from(mockStore.keys()).filter(k => k.startsWith(prefix));
    }),
    quit: vi.fn(async () => 'OK'),
  });

  let mockRedis: ReturnType<typeof createMockRedis>;

  beforeEach(() => {
    mockStore.clear();
    mockRedis = createMockRedis();
    manager = new IdempotencyManager(mockRedis as unknown as ReturnType<typeof import('ioredis').default>, logger, 60, 'test:idempotency:');
  });

  const createTestMessage = (id: string, idempotencyKey?: string): Message => ({
    id,
    payload: { data: 'test' },
    topic: 'test-topic',
    headers: {},
    correlationId: 'corr-123',
    createdAt: Date.now(),
    idempotencyKey,
  });

  it('should generate correct key for message with idempotency key', () => {
    const message = createTestMessage('msg-1', 'unique-key-123');
    const key = manager.generateKey(message);

    expect(key).toContain('test:idempotency:test-topic:unique-key-123');
  });

  it('should generate correct key for message without idempotency key', () => {
    const message = createTestMessage('msg-2');
    const key = manager.generateKey(message);

    expect(key).toContain('test:idempotency:test-topic:msg-2');
  });

  it('should return isProcessed: false for new message', async () => {
    const message = createTestMessage('msg-3', `key-${Date.now()}`);
    const result = await manager.checkProcessed(message);

    expect(result.isProcessed).toBe(false);
    expect(result.result).toBeUndefined();
  });

  it('should mark message as processed and retrieve result', async () => {
    const message = createTestMessage('msg-4', `key-${Date.now()}`);
    const processingResult: ProcessingResult<{ value: string }> = {
      success: true,
      data: { value: 'processed' },
      duration: 100,
    };

    // Mark as processed
    await manager.markProcessed(message, processingResult);

    // Check it was recorded
    const result = await manager.checkProcessed<{ value: string }>(message);

    expect(result.isProcessed).toBe(true);
    expect(result.result?.success).toBe(true);
    expect(result.result?.data?.value).toBe('processed');
    expect(result.result?.processedAt).toBeDefined();
  });

  it('should store error in result', async () => {
    const message = createTestMessage('msg-5', `key-${Date.now()}`);
    const processingResult: ProcessingResult = {
      success: false,
      error: new Error('Processing failed'),
      duration: 50,
    };

    await manager.markProcessed(message, processingResult);

    const result = await manager.checkProcessed(message);

    expect(result.isProcessed).toBe(true);
    expect(result.result?.success).toBe(false);
    expect(result.result?.error).toBe('Processing failed');
  });

  it('should acquire and release processing lock', async () => {
    const message = createTestMessage('msg-6', `lock-key-${Date.now()}`);

    // First acquire should succeed
    const acquired1 = await manager.tryAcquire(message);
    expect(acquired1).toBe(true);

    // Second acquire should fail (already locked)
    const acquired2 = await manager.tryAcquire(message);
    expect(acquired2).toBe(false);

    // Release the lock
    await manager.release(message);

    // Now acquire should succeed again
    const acquired3 = await manager.tryAcquire(message);
    expect(acquired3).toBe(true);

    // Cleanup
    await manager.release(message);
  });

  it('should clear idempotency record', async () => {
    const message = createTestMessage('msg-7', `clear-key-${Date.now()}`);

    // Mark as processed
    await manager.markProcessed(message, { success: true, duration: 10 });

    // Verify it exists
    const result1 = await manager.checkProcessed(message);
    expect(result1.isProcessed).toBe(true);

    // Clear the record
    await manager.clear(message);

    // Verify it's gone
    const result2 = await manager.checkProcessed(message);
    expect(result2.isProcessed).toBe(false);
  });

  it('should return TTL for idempotency key', async () => {
    const message = createTestMessage('msg-8', `ttl-key-${Date.now()}`);

    // Initially no TTL
    const ttl1 = await manager.getTTL(message);
    expect(ttl1).toBe(-2); // Key doesn't exist

    // Mark as processed
    await manager.markProcessed(message, { success: true, duration: 10 });

    // Should have TTL
    const ttl2 = await manager.getTTL(message);
    expect(ttl2).toBeGreaterThan(0);
    expect(ttl2).toBeLessThanOrEqual(60);
  });

  it('should generate unique idempotency keys', () => {
    const key1 = IdempotencyManager.generateIdempotencyKey();
    const key2 = IdempotencyManager.generateIdempotencyKey();

    expect(key1).toBeDefined();
    expect(key2).toBeDefined();
    expect(key1).not.toBe(key2);
  });
});
