import Redis from 'ioredis';
import { v4 as uuidv4 } from 'uuid';
import type { Logger, Message, ProcessingResult } from '../types/index.js';

/**
 * Stored result of a previously processed message
 */
interface StoredResult<T = unknown> {
  success: boolean;
  data?: T;
  error?: string;
  processedAt: number;
}

/**
 * Idempotency manager to prevent duplicate processing
 */
export class IdempotencyManager {
  private readonly redis: Redis;
  private readonly logger: Logger;
  private readonly ttlSeconds: number;
  private readonly keyPrefix: string;

  constructor(
    redis: Redis,
    logger: Logger,
    ttlSeconds: number = 86400, // 24 hours default
    keyPrefix: string = 'dmq:idempotency:'
  ) {
    this.redis = redis;
    this.logger = logger;
    this.ttlSeconds = ttlSeconds;
    this.keyPrefix = keyPrefix;
  }

  /**
   * Generate an idempotency key from message
   */
  generateKey(message: Message): string {
    if (message.idempotencyKey) {
      return `${this.keyPrefix}${message.topic}:${message.idempotencyKey}`;
    }
    // Use message ID as fallback
    return `${this.keyPrefix}${message.topic}:${message.id}`;
  }

  /**
   * Check if a message has already been processed
   * Returns the previous result if exists, null otherwise
   */
  async checkProcessed<T = unknown>(
    message: Message
  ): Promise<{ isProcessed: boolean; result?: StoredResult<T> }> {
    const key = this.generateKey(message);

    try {
      const stored = await this.redis.get(key);

      if (stored) {
        const result = JSON.parse(stored) as StoredResult<T>;
        this.logger.info('Idempotency hit - message already processed', {
          messageId: message.id,
          topic: message.topic,
          key,
        });
        return { isProcessed: true, result };
      }

      return { isProcessed: false };
    } catch (error) {
      this.logger.error('Failed to check idempotency', {
        messageId: message.id,
        error: error instanceof Error ? error.message : String(error),
      });
      // On error, allow processing to continue
      return { isProcessed: false };
    }
  }

  /**
   * Mark a message as processed and store its result
   */
  async markProcessed<T = unknown>(
    message: Message,
    result: ProcessingResult<T>
  ): Promise<boolean> {
    const key = this.generateKey(message);

    try {
      const storedResult: StoredResult<T> = {
        success: result.success,
        data: result.data,
        error: result.error?.message,
        processedAt: Date.now(),
      };

      await this.redis.setex(
        key,
        this.ttlSeconds,
        JSON.stringify(storedResult)
      );

      this.logger.debug('Marked message as processed', {
        messageId: message.id,
        topic: message.topic,
        key,
        ttl: this.ttlSeconds,
      });

      return true;
    } catch (error) {
      this.logger.error('Failed to mark message as processed', {
        messageId: message.id,
        error: error instanceof Error ? error.message : String(error),
      });
      return false;
    }
  }

  /**
   * Check and mark as processing atomically using SETNX
   * Returns true if this is the first attempt, false if already processed
   */
  async tryAcquire(message: Message): Promise<boolean> {
    const key = this.generateKey(message);
    const processingKey = `${key}:processing`;

    try {
      // Try to set the processing key atomically
      const result = await this.redis.set(
        processingKey,
        Date.now().toString(),
        'EX',
        300, // 5 minute processing timeout
        'NX'
      );

      return result === 'OK';
    } catch (error) {
      this.logger.error('Failed to acquire processing lock', {
        messageId: message.id,
        error: error instanceof Error ? error.message : String(error),
      });
      // Allow processing on error
      return true;
    }
  }

  /**
   * Release the processing lock
   */
  async release(message: Message): Promise<void> {
    const key = this.generateKey(message);
    const processingKey = `${key}:processing`;

    try {
      await this.redis.del(processingKey);
    } catch (error) {
      this.logger.error('Failed to release processing lock', {
        messageId: message.id,
        error: error instanceof Error ? error.message : String(error),
      });
    }
  }

  /**
   * Clear idempotency record for a message
   */
  async clear(message: Message): Promise<void> {
    const key = this.generateKey(message);

    try {
      await this.redis.del(key);
      await this.redis.del(`${key}:processing`);
    } catch (error) {
      this.logger.error('Failed to clear idempotency record', {
        messageId: message.id,
        error: error instanceof Error ? error.message : String(error),
      });
    }
  }

  /**
   * Get the remaining TTL for an idempotency key
   */
  async getTTL(message: Message): Promise<number> {
    const key = this.generateKey(message);

    try {
      return await this.redis.ttl(key);
    } catch (error) {
      this.logger.debug('Failed to get TTL for idempotency key', {
        messageId: message.id,
        error: error instanceof Error ? error.message : String(error),
      });
      return -1;
    }
  }

  /**
   * Generate a unique idempotency key
   */
  static generateIdempotencyKey(): string {
    return uuidv4();
  }
}
