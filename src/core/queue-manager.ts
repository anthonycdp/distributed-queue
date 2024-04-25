import { Queue, Worker, Job, QueueEvents } from 'bullmq';
import Redis from 'ioredis';
import { v4 as uuidv4 } from 'uuid';
import type { ConnectionOptions } from 'bullmq';
import type {
  Logger,
  Message,
  ProduceOptions,
  MessageHandler,
  ProcessingContext,
  ProcessingResult,
  TopicConfig,
  DistributedQueueConfig,
  QueueStats,
  Span,
} from '../types/index.js';
import type { MetricsCollector } from '../observability/metrics.js';
import { createLogger } from '../observability/logger.js';
import { MetricsCollector as MetricsCollectorClass } from '../observability/metrics.js';
import { TracingService } from '../observability/tracing.js';
import { IdempotencyManager } from './idempotency.js';
import { DeadLetterQueue } from './dead-letter-queue.js';

//region Constants - Clean Code: Avoid Magic Numbers
const REDIS_MAX_RETRIES = 10;
const REDIS_BACKOFF_BASE_MS = 100;
const REDIS_BACKOFF_MAX_MS = 3000;
const DEFAULT_REMOVE_ON_FAIL = 100;
const DEFAULT_CONCURRENCY = 5;
const DEFAULT_MAX_RETRIES = 3;
const DEFAULT_BACKOFF_DELAY_MS = 1000;
const IDEMPOTENCY_DEFAULT_TTL_SECONDS = 86400;
//endregion

//region Custom Errors - Clean Code: Use Exceptions
class TopicNotRegisteredError extends Error {
  constructor(topic: string) {
    super(`Topic "${topic}" is not registered`);
    this.name = 'TopicNotRegisteredError';
  }
}

class TopicAlreadyRegisteredError extends Error {
  constructor(topic: string) {
    super(`Topic "${topic}" is already registered`);
    this.name = 'TopicAlreadyRegisteredError';
  }
}
//endregion

interface TopicManager {
  queue: Queue;
  worker?: Worker;
  queueEvents?: QueueEvents;
  dlq: DeadLetterQueue;
  config: TopicConfig;
}

export class DistributedQueueManager {
  private readonly redis: Redis;
  private readonly logger: Logger;
  private readonly metrics: MetricsCollector;
  private readonly tracing: TracingService;
  private readonly idempotency: IdempotencyManager;
  private readonly topics: Map<string, TopicManager> = new Map();
  private readonly config: DistributedQueueConfig;
  private readonly prefix: string;
  private isShuttingDown = false;

  constructor(config: DistributedQueueConfig) {
    this.config = config;
    this.prefix = config.prefix ?? 'dmq';
    this.redis = this.createRedisConnection();
    this.logger = this.createLoggerInstance();
    this.metrics = new MetricsCollectorClass(this.logger);
    this.tracing = this.createTracingInstance();
    this.idempotency = this.createIdempotencyInstance();
    this.setupRedisEventHandlers();

    this.logger.info('Distributed Queue Manager initialized', { prefix: this.prefix });
  }

  //region Private Factory Methods - Clean Code: Small Functions
  private createRedisConnection(): Redis {
    return new Redis({
      host: this.config.redis.host,
      port: this.config.redis.port,
      password: this.config.redis.password,
      db: this.config.redis.db ?? 0,
      maxRetriesPerRequest: this.config.redis.maxRetries ?? DEFAULT_MAX_RETRIES,
      enableReadyCheck: this.config.redis.enableReadyCheck ?? true,
      retryStrategy: (attemptNumber: number) => {
        if (attemptNumber > REDIS_MAX_RETRIES) {
          return null;
        }
        return Math.min(attemptNumber * REDIS_BACKOFF_BASE_MS, REDIS_BACKOFF_MAX_MS);
      },
    });
  }

  private createLoggerInstance(): Logger {
    return createLogger(
      this.config.observability?.logLevel ?? 'info',
      'distributed-queue'
    );
  }

  private createTracingInstance(): TracingService {
    return new TracingService(
      this.logger,
      this.config.observability?.tracing ?? false,
      this.config.observability?.tracingEndpoint
    );
  }

  private createIdempotencyInstance(): IdempotencyManager {
    const ttlSeconds = parseInt(
      process.env['IDEMPOTENCY_TTL_SECONDS'] ?? String(IDEMPOTENCY_DEFAULT_TTL_SECONDS),
      10
    );
    return new IdempotencyManager(this.redis, this.logger, ttlSeconds, `${this.prefix}:idempotency:`);
  }
  //endregion

  //region Redis Event Handlers - Clean Code: Extract to Small Functions
  private setupRedisEventHandlers(): void {
    this.redis.on('connect', () => this.logger.info('Redis connection established'));
    this.redis.on('ready', () => this.logger.info('Redis client ready to receive commands'));
    this.redis.on('error', (error) => this.logRedisError(error));
    this.redis.on('close', () => this.logger.warn('Redis connection closed'));
    this.redis.on('reconnecting', (delay: number) => this.logger.info('Redis reconnecting', { delayMs: delay }));
    this.redis.on('end', () => this.logger.error('Redis connection ended, no more reconnections'));
  }

  private logRedisError(error: Error): void {
    this.logger.error('Redis connection error', {
      error: error.message,
      code: (error as NodeJS.ErrnoException).code,
    });
  }
  //endregion

  //region Private Helpers - Clean Code: DRY
  private get redisConnection(): ConnectionOptions {
    return this.redis as unknown as ConnectionOptions;
  }

  private requireTopic(topic: string): TopicManager {
    const topicManager = this.topics.get(topic);
    if (!topicManager) {
      throw new TopicNotRegisteredError(topic);
    }
    return topicManager;
  }

  private resolveBackoffConfig(config: TopicConfig): { type: string; delay: number } {
    const retryConfig = config.retry ?? this.config.defaultRetry;
    if (retryConfig?.backoff) {
      return {
        type: retryConfig.backoff.type === 'exponential' ? 'exponential' : 'fixed',
        delay: retryConfig.backoff.delay,
      };
    }
    return { type: 'exponential', delay: DEFAULT_BACKOFF_DELAY_MS };
  }

  private normalizeTopicConfig(config: TopicConfig): TopicConfig {
    return {
      ...config,
      concurrency: config.concurrency ?? DEFAULT_CONCURRENCY,
      maxRetries: config.maxRetries ?? DEFAULT_MAX_RETRIES,
      enableDLQ: config.enableDLQ ?? true,
    };
  }
  //endregion

  //region Topic Management
  async registerTopic(config: TopicConfig): Promise<void> {
    if (this.topics.has(config.name)) {
      throw new TopicAlreadyRegisteredError(config.name);
    }

    const queueName = `${this.prefix}:${config.name}`;
    const queue = this.createQueue(queueName, config);
    const dlq = new DeadLetterQueue(config.name, this.redis, this.logger, this.metrics, this.config.dlq, this.prefix);

    this.topics.set(config.name, {
      queue,
      dlq,
      config: this.normalizeTopicConfig(config),
    });

    this.logger.info('Topic registered', {
      topic: config.name,
      queueName,
      concurrency: config.concurrency,
    });
  }

  private createQueue(queueName: string, config: TopicConfig): Queue {
    return new Queue(queueName, {
      connection: this.redisConnection,
      defaultJobOptions: {
        attempts: config.maxRetries ?? this.config.defaultRetry?.attempts ?? DEFAULT_MAX_RETRIES,
        backoff: this.resolveBackoffConfig(config),
        removeOnComplete: true,
        removeOnFail: DEFAULT_REMOVE_ON_FAIL,
      },
    });
  }
  //endregion

  //region Message Production
  async produce<T = unknown>(topic: string, payload: T, options: ProduceOptions = {}): Promise<Message<T>> {
    const topicManager = this.requireTopic(topic);

    const messageId = options.jobId ?? uuidv4();
    const correlationId = options.correlationId ?? uuidv4();
    const message = this.createMessage(topic, payload, messageId, correlationId, options);

    const span = this.startProducerSpan(topic, messageId, correlationId);

    try {
      if (await this.isDuplicateMessage(topic, message, span)) {
        return this.markAsDuplicate(message);
      }

      await this.enqueueMessage(topicManager, message, options);
      this.recordSuccessfulProduction(topic, messageId, correlationId, options.delay, span);

      return message;
    } catch (error) {
      this.handleProductionError(topic, messageId, error, span);
      throw error;
    } finally {
      span.end();
    }
  }

  private createMessage<T>(topic: string, payload: T, messageId: string, correlationId: string, options: ProduceOptions): Message<T> {
    return {
      id: messageId,
      payload,
      topic,
      headers: options.headers ?? {},
      correlationId,
      createdAt: Date.now(),
      scheduledAt: options.delay ? Date.now() + options.delay : undefined,
      attempts: 0,
      idempotencyKey: options.idempotencyKey,
    };
  }

  private startProducerSpan(topic: string, messageId: string, correlationId: string): Span {
    const span = this.tracing.startProducerSpan(topic, messageId);
    span.setAttribute('message.id', messageId);
    span.setAttribute('message.topic', topic);
    span.setAttribute('message.correlationId', correlationId);
    return span;
  }

  private async isDuplicateMessage<T>(topic: string, message: Message<T>, span: Span): Promise<boolean> {
    if (!message.idempotencyKey) {
      return false;
    }

    const { isProcessed } = await this.idempotency.checkProcessed<T>(message);
    if (isProcessed) {
      this.metrics.recordIdempotencyHit(topic);
      span.addEvent('idempotency_hit');
      span.setStatus('ok');
      return true;
    }
    return false;
  }

  private markAsDuplicate<T>(message: Message<T>): Message<T> {
    this.logger.info('Idempotency hit - message already processed, skipping', {
      messageId: message.id,
      topic: message.topic,
      idempotencyKey: message.idempotencyKey,
    });

    return {
      ...message,
      headers: { ...message.headers, 'x-idempotency-hit': true },
    };
  }

  private async enqueueMessage<T>(topicManager: TopicManager, message: Message<T>, options: ProduceOptions): Promise<void> {
    await topicManager.queue.add(message.id, message, {
      jobId: message.id,
      delay: options.delay,
      priority: options.priority,
      removeOnComplete: options.removeOnComplete ?? true,
      removeOnFail: options.removeOnFail ?? DEFAULT_REMOVE_ON_FAIL,
    });
    this.metrics.recordProduced(message.topic);
  }

  private recordSuccessfulProduction(topic: string, messageId: string, correlationId: string, delay: number | undefined, span: Span): void {
    span.addEvent('message_queued');
    span.setStatus('ok');
    this.logger.info('Message produced', { messageId, topic, correlationId, delay });
  }

  private handleProductionError(topic: string, messageId: string, error: unknown, span: Span): void {
    const errorMessage = error instanceof Error ? error.message : String(error);
    span.setStatus('error', errorMessage);
    this.logger.error('Failed to produce message', { messageId, topic, error: errorMessage });
  }
  //endregion

  //region Message Consumption
  async consume<T = unknown, R = unknown>(topic: string, handler: MessageHandler<T, R>): Promise<void> {
    const topicManager = this.requireTopic(topic);

    if (topicManager.worker) {
      throw new Error(`Consumer already running for topic "${topic}"`);
    }

    const worker = this.createWorker(topic, topicManager, handler);
    const queueEvents = this.createQueueEvents(topic);

    this.setupWorkerEventHandlers(worker, topic);
    topicManager.worker = worker;
    topicManager.queueEvents = queueEvents;

    this.logger.info('Consumer started', { topic, concurrency: topicManager.config.concurrency });
  }

  private createWorker<T, R>(topic: string, topicManager: TopicManager, handler: MessageHandler<T, R>): Worker {
    return new Worker(
      `${this.prefix}:${topic}`,
      async (job: Job<Message<T>>) => this.processMessage(job, topicManager, handler),
      {
        connection: this.redisConnection,
        concurrency: topicManager.config.concurrency ?? DEFAULT_CONCURRENCY,
        limiter: topicManager.config.rateLimit
          ? { max: topicManager.config.rateLimit.max, duration: topicManager.config.rateLimit.duration }
          : undefined,
      }
    );
  }

  private createQueueEvents(topic: string): QueueEvents {
    return new QueueEvents(`${this.prefix}:${topic}`, {
      connection: this.redisConnection,
    });
  }

  private setupWorkerEventHandlers(worker: Worker, topic: string): void {
    worker.on('completed', (job: Job) => {
      this.logger.debug('Job completed', { jobId: job.id, topic });
    });

    worker.on('failed', (job: Job | undefined, error: Error) => {
      this.logger.error('Job failed', { jobId: job?.id, topic, error: error.message });
    });

    worker.on('error', (error: Error) => {
      this.logger.error('Worker error', { topic, error: error.message });
    });
  }
  //endregion

  //region Message Processing
  private async processMessage<T, R>(job: Job<Message<T>>, topicManager: TopicManager, handler: MessageHandler<T, R>): Promise<R> {
    const message = job.data;
    const topic = message.topic;
    const startTime = Date.now();

    this.recordQueueLatency(topic, message, startTime);

    const span = this.startConsumerSpan(topic, message, job.attemptsMade);
    const context = this.createProcessingContext(job, span);

    try {
      const cachedResult = await this.checkCachedResult(topic, message, span);
      if (cachedResult.found) {
        return cachedResult.data as R;
      }

      const result = await handler(message, context);
      const duration = Date.now() - startTime;
      this.metrics.recordProcessingDuration(topic, duration);

      if (result.success) {
        return this.handleSuccessfulProcessing(topic, message, result, span);
      }

      await this.handleFailedProcessing(job, topicManager, result, span);
      throw result.error ?? new Error('Processing failed');
    } catch (error) {
      const wrappedError = error instanceof Error ? error : new Error(String(error));
      await this.handleFailedProcessing(
        job,
        topicManager,
        { success: false, error: wrappedError, duration: Date.now() - startTime },
        span
      );
      throw wrappedError;
    } finally {
      span.end();
    }
  }

  private recordQueueLatency(topic: string, message: Message, startTime: number): void {
    const latency = startTime - message.createdAt;
    this.metrics.recordQueueLatency(topic, latency);
  }

  private startConsumerSpan(topic: string, message: Message, attemptsMade: number): Span {
    const span = this.tracing.startConsumerSpan(topic, message.id);
    span.setAttribute('message.id', message.id);
    span.setAttribute('message.correlationId', message.correlationId ?? '');
    span.setAttribute('job.attemptsMade', attemptsMade);
    return span;
  }

  private createProcessingContext(job: Job, span: Span): ProcessingContext {
    return {
      updateProgress: async (progress: number) => {
        await job.updateProgress(progress);
        span.addEvent('progress', { value: progress });
      },
      getJob: () => job,
      logger: this.logger,
      span,
    };
  }

  private async checkCachedResult<R>(topic: string, message: Message, span: Span): Promise<{ found: boolean; data?: R }> {
    if (!message.idempotencyKey) {
      return { found: false };
    }

    const { isProcessed, result } = await this.idempotency.checkProcessed<R>(message);
    if (isProcessed && result) {
      this.metrics.recordIdempotencyHit(topic);
      span.addEvent('idempotency_hit');
      span.setStatus('ok');
      return { found: true, data: result.data };
    }
    return { found: false };
  }

  private async handleSuccessfulProcessing<T, R>(topic: string, message: Message<T>, result: ProcessingResult<R>, span: Span): Promise<R> {
    if (message.idempotencyKey) {
      await this.idempotency.markProcessed(message, result);
    }

    this.metrics.recordConsumed(topic, 'success');
    span.setStatus('ok');
    this.logger.info('Message processed successfully', { messageId: message.id, topic });

    return result.data as R;
  }
  //endregion

  //region Failure Handling
  private async handleFailedProcessing<T, R>(job: Job<Message<T>>, topicManager: TopicManager, result: ProcessingResult<R>, span: Span): Promise<void> {
    const message = job.data;
    const topic = message.topic;
    const error = result.error ?? new Error('Unknown error');

    this.metrics.recordFailure(topic, error.name);
    this.metrics.recordConsumed(topic, 'failure');

    span.setAttribute('error.type', error.name);
    span.setAttribute('error.message', error.message);
    span.setStatus('error', error.message);

    const maxAttempts = topicManager.config.maxRetries ?? DEFAULT_MAX_RETRIES;
    if (job.attemptsMade < maxAttempts) {
      await this.scheduleRetry(topic, message, job.attemptsMade, maxAttempts, error, span);
    } else {
      await this.moveToDeadLetterQueue(topicManager, message, job.attemptsMade, error, span);
    }
  }

  private scheduleRetry(topic: string, message: Message, attempt: number, maxAttempts: number, error: Error, span: Span): void {
    this.metrics.recordRetry(topic, attempt);
    span.addEvent('retry_scheduled', { attempt });
    this.logger.warn('Message failed, will retry', {
      messageId: message.id,
      topic,
      attempt,
      maxAttempts,
      error: error.message,
    });
  }

  private async moveToDeadLetterQueue<T>(topicManager: TopicManager, message: Message<T>, attempts: number, error: Error, span: Span): Promise<void> {
    if (!topicManager.config.enableDLQ) {
      return;
    }

    await topicManager.dlq.add(message, error, attempts);
    span.addEvent('moved_to_dlq');
    this.logger.error('Message moved to DLQ after max retries', {
      messageId: message.id,
      topic: message.topic,
      attempts,
      error: error.message,
    });
  }
  //endregion

  //region Statistics & Control
  async getStats(topic: string): Promise<QueueStats> {
    const topicManager = this.requireTopic(topic);
    const counts = await topicManager.queue.getJobCounts('waiting', 'active', 'completed', 'failed');
    const dlqSize = await topicManager.dlq.size();

    return {
      topic,
      waiting: counts.waiting ?? 0,
      active: counts.active ?? 0,
      completed: counts.completed ?? 0,
      failed: counts.failed ?? 0,
      deadLettered: dlqSize,
      totalProcessed: (counts.completed ?? 0) + (counts.failed ?? 0),
      avgProcessingTime: 0,
    };
  }

  async getAllStats(): Promise<QueueStats[]> {
    const topicNames = Array.from(this.topics.keys());
    return Promise.all(topicNames.map((topic) => this.getStats(topic)));
  }

  getDLQ(topic: string): DeadLetterQueue {
    return this.requireTopic(topic).dlq;
  }

  async pause(topic: string): Promise<void> {
    const topicManager = this.requireTopic(topic);
    await topicManager.queue.pause();
    this.logger.info('Topic paused', { topic });
  }

  async resume(topic: string): Promise<void> {
    const topicManager = this.requireTopic(topic);
    await topicManager.queue.resume();
    this.logger.info('Topic resumed', { topic });
  }

  async getMetrics(): Promise<string> {
    return this.metrics.getMetrics();
  }

  async isHealthy(): Promise<boolean> {
    try {
      if (this.redis.status !== 'ready') {
        return false;
      }
      const pong = await this.redis.ping();
      return pong === 'PONG';
    } catch {
      return false;
    }
  }

  getConnectionStatus(): { redis: string; topics: number; activeWorkers: number } {
    const activeWorkers = Array.from(this.topics.values()).filter((manager) => manager.worker).length;
    return {
      redis: this.redis.status,
      topics: this.topics.size,
      activeWorkers,
    };
  }
  //endregion

  //region Shutdown
  async shutdown(): Promise<void> {
    if (this.isShuttingDown) {
      return;
    }

    this.isShuttingDown = true;
    this.logger.info('Shutting down distributed queue manager...');

    await this.closeAllTopics();
    await this.redis.quit();
    this.logger.info('Distributed queue manager shut down complete');
  }

  private async closeAllTopics(): Promise<void> {
    const closePromises = Array.from(this.topics.entries()).map(async ([topic, manager]) => {
      try {
        await manager.worker?.close();
        await manager.queueEvents?.close();
        await manager.queue.close();
        await manager.dlq.close();
      } catch (error) {
        this.logger.error('Error closing topic', {
          topic,
          error: error instanceof Error ? error.message : String(error),
        });
      }
    });

    await Promise.all(closePromises);
  }
  //endregion
}
