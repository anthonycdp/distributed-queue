import { Queue } from 'bullmq';
import type { ConnectionOptions } from 'bullmq';
import Redis from 'ioredis';
import { v4 as uuidv4 } from 'uuid';
import type {
  Logger,
  DLQEntry,
  Message,
  DLQConfig,
} from '../types/index.js';
import type { MetricsCollector } from '../observability/metrics.js';

/**
 * Dead Letter Queue manager
 * Handles failed messages that have exceeded their retry limit
 */
export class DeadLetterQueue {
  private readonly queue: Queue;
  private readonly redis: Redis;
  private readonly logger: Logger;
  private readonly metrics: MetricsCollector;
  private readonly config: Required<DLQConfig>;
  private readonly topicName: string;
  private readonly prefix: string;
  private cleanupInterval?: NodeJS.Timeout;

  constructor(
    topicName: string,
    redis: Redis,
    logger: Logger,
    metrics: MetricsCollector,
    config: DLQConfig = {},
    prefix: string = 'dmq'
  ) {
    this.topicName = topicName;
    this.redis = redis;
    this.logger = logger;
    this.metrics = metrics;
    this.prefix = prefix;
    this.config = {
      maxAgeDays: config.maxAgeDays ?? 7,
      maxSize: config.maxSize ?? 10000,
      autoCleanup: config.autoCleanup ?? true,
      cleanupInterval: config.cleanupInterval ?? 3600000, // 1 hour
    };

    // Create DLQ queue with BullMQ using consistent prefix
    this.queue = new Queue(`${this.prefix}:${topicName}:dlq`, {
      connection: redis as unknown as ConnectionOptions,
      defaultJobOptions: {
        removeOnComplete: false,
        removeOnFail: false,
      },
    });

    // Start automatic cleanup if enabled
    if (this.config.autoCleanup) {
      this.startCleanup();
    }
  }

  /**
   * Add a failed message to the dead letter queue
   */
  async add(message: Message, error: Error, attemptsMade: number): Promise<string> {
    const entry: DLQEntry = {
      message,
      error: {
        message: error.message,
        stack: error.stack,
        name: error.name,
      },
      failedAt: Date.now(),
      topic: this.topicName,
      attemptsMade,
      retriable: true,
    };

    try {
      // Check if DLQ size limit is reached
      const currentSize = await this.size();
      if (currentSize >= this.config.maxSize) {
        this.logger.warn('DLQ size limit reached, removing oldest entry', {
          topic: this.topicName,
          currentSize,
          maxSize: this.config.maxSize,
        });
        await this.removeOldest();
      }

      // Add to BullMQ queue
      const job = await this.queue.add('dlq-entry', entry, {
        jobId: uuidv4(),
        removeOnComplete: false,
        removeOnFail: false,
      });

      // Also store in Redis for quick access
      await this.storeEntry(job.id ?? uuidv4(), entry);

      this.metrics.recordDeadLettered(this.topicName);
      this.metrics.updateDLQSize(this.topicName, currentSize + 1);

      this.logger.info('Message added to DLQ', {
        messageId: message.id,
        jobId: job.id,
        topic: this.topicName,
        attemptsMade,
        error: error.message,
      });

      return job.id ?? '';
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      this.logger.error('Failed to add message to DLQ', {
        messageId: message.id,
        error: errorMessage,
      });
      throw error;
    }
  }

  /**
   * Get a DLQ entry by ID
   */
  async get(id: string): Promise<DLQEntry | null> {
    try {
      const job = await this.queue.getJob(id);
      if (!job) {
        return null;
      }
      return job.data as DLQEntry;
    } catch (error) {
      this.logger.debug('Failed to get DLQ entry', { id, error: error instanceof Error ? error.message : String(error) });
      return null;
    }
  }

  /**
   * Get all DLQ entries
   */
  async getAll(start: number = 0, end: number = -1): Promise<DLQEntry[]> {
    try {
      const jobs = await this.queue.getJobs(['waiting', 'completed'], start, end);
      return jobs.map((job) => job.data as DLQEntry);
    } catch (error) {
      this.logger.debug('Failed to get all DLQ entries', { error: error instanceof Error ? error.message : String(error) });
      return [];
    }
  }

  /**
   * Get the current size of the DLQ
   */
  async size(): Promise<number> {
    try {
      const counts = await this.queue.getJobCounts('waiting', 'completed');
      return (counts.waiting ?? 0) + (counts.completed ?? 0);
    } catch (error) {
      this.logger.debug('Failed to get DLQ size', { error: error instanceof Error ? error.message : String(error) });
      return 0;
    }
  }

  /**
   * Retry a message from the DLQ
   */
  async retry(
    id: string,
    mainQueue: Queue
  ): Promise<{ success: boolean; error?: string }> {
    try {
      const entry = await this.get(id);
      if (!entry) {
        return { success: false, error: 'Entry not found' };
      }

      // Reset attempts and add back to main queue
      await mainQueue.add(entry.message.id, entry.message, {
        jobId: entry.message.id,
      });

      // Remove from DLQ
      await this.remove(id);

      this.logger.info('Retried message from DLQ', {
        messageId: entry.message.id,
        topic: this.topicName,
      });

      return { success: true };
    } catch (error) {
      return {
        success: false,
        error: error instanceof Error ? error.message : String(error),
      };
    }
  }

  /**
   * Remove an entry from the DLQ
   */
  async remove(id: string): Promise<boolean> {
    try {
      const job = await this.queue.getJob(id);
      if (job) {
        await job.remove();
        await this.deleteEntry(id);
        return true;
      }
      return false;
    } catch (error) {
      this.logger.debug('Failed to remove DLQ entry', { id, error: error instanceof Error ? error.message : String(error) });
      return false;
    }
  }

  /**
   * Remove oldest entry from DLQ
   */
  private async removeOldest(): Promise<void> {
    try {
      const jobs = await this.queue.getJobs(['completed'], 0, 0);
      if (jobs.length > 0 && jobs[0]) {
        await jobs[0].remove();
        await this.deleteEntry(jobs[0].id ?? '');
      }
    } catch (error) {
      this.logger.error('Failed to remove oldest DLQ entry', {
        error: error instanceof Error ? error.message : String(error),
      });
    }
  }

  /**
   * Clear all entries from the DLQ
   */
  async clear(): Promise<number> {
    try {
      await this.queue.drain();
      await this.redis.del(this.getRedisKey('*'));
      this.metrics.updateDLQSize(this.topicName, 0);
      this.logger.info('DLQ cleared', { topic: this.topicName });
      return 0;
    } catch (error) {
      this.logger.error('Failed to clear DLQ', { topic: this.topicName, error: error instanceof Error ? error.message : String(error) });
      return -1;
    }
  }

  /**
   * Start automatic cleanup of old entries
   */
  private startCleanup(): void {
    this.cleanupInterval = setInterval(async () => {
      try {
        await this.cleanup();
      } catch (error) {
        this.logger.error('DLQ cleanup failed', {
          error: error instanceof Error ? error.message : String(error),
        });
      }
    }, this.config.cleanupInterval);
  }

  /**
   * Cleanup entries older than maxAgeDays
   */
  private async cleanup(): Promise<number> {
    const cutoffTime = Date.now() - this.config.maxAgeDays * 24 * 60 * 60 * 1000;
    let removed = 0;

    try {
      const jobs = await this.queue.getJobs(['completed']);
      for (const job of jobs) {
        const entry = job.data as DLQEntry;
        if (entry.failedAt < cutoffTime) {
          await job.remove();
          await this.deleteEntry(job.id ?? '');
          removed++;
        }
      }

      if (removed > 0) {
        this.logger.info('DLQ cleanup completed', {
          topic: this.topicName,
          removed,
          maxAgeDays: this.config.maxAgeDays,
        });
      }
    } catch (error) {
      this.logger.error('DLQ cleanup error', {
        error: error instanceof Error ? error.message : String(error),
      });
    }

    return removed;
  }

  /**
   * Get DLQ statistics
   */
  async getStats(): Promise<{
    size: number;
    oldestEntry?: number;
    newestEntry?: number;
    errorTypes: Map<string, number>;
  }> {
    const size = await this.size();
    const errorTypes = new Map<string, number>();
    let oldestEntry: number | undefined;
    let newestEntry: number | undefined;

    try {
      const entries = await this.getAll(0, 100);
      for (const entry of entries) {
        errorTypes.set(
          entry.error.name,
          (errorTypes.get(entry.error.name) ?? 0) + 1
        );

        if (!oldestEntry || entry.failedAt < oldestEntry) {
          oldestEntry = entry.failedAt;
        }
        if (!newestEntry || entry.failedAt > newestEntry) {
          newestEntry = entry.failedAt;
        }
      }
    } catch (error) {
      this.logger.debug('Failed to collect DLQ stats', { topic: this.topicName, error: error instanceof Error ? error.message : String(error) });
    }

    return { size, oldestEntry, newestEntry, errorTypes };
  }

  /**
   * Store entry in Redis for quick access
   */
  private async storeEntry(id: string, entry: DLQEntry): Promise<void> {
    await this.redis.setex(
      this.getRedisKey(id),
      this.config.maxAgeDays * 24 * 60 * 60,
      JSON.stringify(entry)
    );
  }

  /**
   * Delete entry from Redis
   */
  private async deleteEntry(id: string): Promise<void> {
    await this.redis.del(this.getRedisKey(id));
  }

  /**
   * Get Redis key for DLQ entry
   */
  private getRedisKey(id: string): string {
    return `${this.prefix}:dlq:${this.topicName}:${id}`;
  }

  /**
   * Close the DLQ and cleanup resources
   */
  async close(): Promise<void> {
    if (this.cleanupInterval) {
      clearInterval(this.cleanupInterval);
    }
    await this.queue.close();
  }
}
