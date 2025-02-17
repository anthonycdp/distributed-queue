import { describe, it, expect, beforeEach, vi } from 'vitest';
import { DeadLetterQueue } from '../src/core/dead-letter-queue.js';
import { MetricsCollector } from '../src/observability/metrics.js';
import { createLogger } from '../src/observability/logger.js';
import type { Message, DLQEntry } from '../src/types/index.js';

// Mock BullMQ
vi.mock('bullmq', () => {
  const mockJobs: Map<string, { id: string; data: DLQEntry; state: string }> = new Map();

  return {
    Queue: vi.fn().mockImplementation((name: string) => {
      return {
        add: vi.fn(async (name: string, data: DLQEntry, options?: { jobId?: string }) => {
          const id = options?.jobId || `job-${Date.now()}-${Math.random().toString(36).slice(2)}`;
          mockJobs.set(id, { id, data, state: 'waiting' });
          return { id };
        }),
        getJob: vi.fn(async (id: string) => {
          const job = mockJobs.get(id);
          if (!job) return null;
          return { id: job.id, data: job.data, remove: vi.fn(async () => mockJobs.delete(id)) };
        }),
        getJobs: vi.fn(async (states: string[], start = 0, end = -1) => {
          const jobs = Array.from(mockJobs.values())
            .filter(j => states.includes(j.state))
            .slice(start, end === -1 ? undefined : end + 1);
          return jobs.map(j => ({
            id: j.id,
            data: j.data,
            remove: vi.fn(async () => mockJobs.delete(j.id)),
          }));
        }),
        getJobCounts: vi.fn(async (...states: string[]) => {
          const counts: Record<string, number> = {};
          for (const state of states) {
            counts[state] = Array.from(mockJobs.values()).filter(j => j.state === state).length;
          }
          return counts;
        }),
        drain: vi.fn(async () => {
          mockJobs.clear();
        }),
        close: vi.fn(async () => {}),
      };
    }),
  };
});

describe('DeadLetterQueue', () => {
  let dlq: DeadLetterQueue;
  let metrics: MetricsCollector;
  const logger = createLogger('error');
  const topicName = 'test-dlq-topic';

  // Mock Redis store
  const mockRedisStore = new Map<string, string>();

  const mockRedis = {
    setex: vi.fn(async (key: string, ttl: number, value: string) => {
      mockRedisStore.set(key, value);
      return 'OK';
    }),
    del: vi.fn(async (...keys: string[]) => {
      let deleted = 0;
      for (const key of keys) {
        // Support wildcard deletion for test purposes
        if (key.includes('*')) {
          const prefix = key.replace('*', '');
          for (const k of mockRedisStore.keys()) {
            if (k.startsWith(prefix)) {
              mockRedisStore.delete(k);
              deleted++;
            }
          }
        } else if (mockRedisStore.delete(key)) {
          deleted++;
        }
      }
      return deleted;
    }),
    quit: vi.fn(async () => 'OK'),
  };

  beforeEach(() => {
    mockRedisStore.clear();
    metrics = new MetricsCollector(logger);

    dlq = new DeadLetterQueue(topicName, mockRedis as unknown as ReturnType<typeof import('ioredis').default>, logger, metrics, {
      maxAgeDays: 1,
      maxSize: 100,
      autoCleanup: false,
    });
  });

  const createTestMessage = (id: string): Message => ({
    id,
    payload: { data: 'test' },
    topic: topicName,
    headers: {},
    correlationId: 'corr-123',
    createdAt: Date.now(),
  });

  it('should add a message to DLQ', async () => {
    const message = createTestMessage('msg-1');
    const error = new Error('Test error');

    const jobId = await dlq.add(message, error, 3);

    expect(jobId).toBeDefined();
    expect(typeof jobId).toBe('string');
  });

  it('should retrieve a DLQ entry', async () => {
    const message = createTestMessage('msg-2');
    const error = new Error('Retrieval test error');

    const jobId = await dlq.add(message, error, 2);
    const entry = await dlq.get(jobId);

    expect(entry).toBeDefined();
    expect(entry?.message.id).toBe('msg-2');
    expect(entry?.error.message).toBe('Retrieval test error');
    expect(entry?.attemptsMade).toBe(2);
    expect(entry?.topic).toBe(topicName);
  });

  it('should return null for non-existent entry', async () => {
    const entry = await dlq.get('non-existent-id');
    expect(entry).toBeNull();
  });

  it('should get all DLQ entries', async () => {
    const message = createTestMessage('msg-3');
    await dlq.add(message, new Error('Error 3'), 1);

    const entries = await dlq.getAll();

    expect(entries.length).toBeGreaterThan(0);
    expect(entries.some((e) => e.message.id === 'msg-3')).toBe(true);
  });

  it('should get DLQ size', async () => {
    const initialSize = await dlq.size();

    const message = createTestMessage('msg-4');
    await dlq.add(message, new Error('Size test'), 1);

    const newSize = await dlq.size();
    expect(newSize).toBe(initialSize + 1);
  });

  it('should remove an entry from DLQ', async () => {
    const message = createTestMessage('msg-5');
    const jobId = await dlq.add(message, new Error('Remove test'), 1);

    const removed = await dlq.remove(jobId);
    expect(removed).toBe(true);

    const entry = await dlq.get(jobId);
    expect(entry).toBeNull();
  });

  it('should retry a message to main queue', async () => {
    const message = createTestMessage('msg-6');
    const jobId = await dlq.add(message, new Error('Retry test'), 1);

    // Create a mock main queue
    const mockMainQueue = {
      add: vi.fn(async () => ({ id: 'new-job-id' })),
    };

    const result = await dlq.retry(jobId, mockMainQueue as unknown as ReturnType<typeof import('bullmq').Queue>);

    expect(result.success).toBe(true);

    // Entry should be removed from DLQ
    const entry = await dlq.get(jobId);
    expect(entry).toBeNull();
  });

  it('should return error for non-existent retry', async () => {
    const mockMainQueue = {
      add: vi.fn(async () => ({ id: 'new-job-id' })),
    };

    const result = await dlq.retry('non-existent-id', mockMainQueue as unknown as ReturnType<typeof import('bullmq').Queue>);

    expect(result.success).toBe(false);
    expect(result.error).toBe('Entry not found');
  });

  it('should get DLQ statistics', async () => {
    const message = createTestMessage('msg-7');
    await dlq.add(message, new Error('Stats test'), 1);

    const stats = await dlq.getStats();

    expect(stats.size).toBeGreaterThanOrEqual(0);
    expect(stats.errorTypes).toBeInstanceOf(Map);
  });

  it('should clear all entries from DLQ', async () => {
    const message = createTestMessage('msg-8');
    await dlq.add(message, new Error('Clear test'), 1);

    await dlq.clear();

    const size = await dlq.size();
    expect(size).toBe(0);
  });
});
