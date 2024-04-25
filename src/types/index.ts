import type { Job, JobsOptions } from 'bullmq';

/**
 * Topic configuration for organizing message queues
 */
export interface TopicConfig {
  /** Unique topic identifier */
  name: string;
  /** Number of concurrent workers for this topic */
  concurrency?: number;
  /** Rate limiting configuration */
  rateLimit?: RateLimitConfig;
  /** Default retry configuration */
  retry?: RetryConfig;
  /** Enable dead letter queue for failed messages */
  enableDLQ?: boolean;
  /** Maximum retries before moving to DLQ */
  maxRetries?: number;
}

/**
 * Rate limiting configuration
 */
export interface RateLimitConfig {
  /** Maximum number of messages per duration */
  max: number;
  /** Duration in milliseconds */
  duration: number;
}

/**
 * Retry configuration with backoff support
 */
export interface RetryConfig {
  /** Maximum retry attempts */
  attempts: number;
  /** Backoff strategy */
  backoff: BackoffConfig;
}

/**
 * Backoff strategy configuration
 */
export interface BackoffConfig {
  /** Backoff type: exponential, fixed, or custom */
  type: 'exponential' | 'fixed' | 'custom';
  /** Initial delay in milliseconds */
  delay: number;
  /** Maximum delay cap for exponential backoff */
  maxDelay?: number;
  /** Custom backoff function (type: 'custom' only) */
  customFunction?: (attemptsMade: number) => number;
}

/**
 * Message wrapper with metadata
 */
export interface Message<T = unknown> {
  /** Unique message identifier */
  id: string;
  /** Message payload */
  payload: T;
  /** Topic name */
  topic: string;
  /** Message headers for metadata */
  headers: MessageHeaders;
  /** Correlation ID for distributed tracing */
  correlationId?: string;
  /** Timestamp when message was created */
  createdAt: number;
  /** Timestamp when message should be processed (delayed) */
  scheduledAt?: number;
  /** Number of processing attempts */
  attempts?: number;
  /** Idempotency key for deduplication */
  idempotencyKey?: string;
}

/**
 * Message headers
 */
export interface MessageHeaders {
  [key: string]: string | number | boolean;
}

/**
 * Options when producing a message
 */
export interface ProduceOptions {
  /** Delay in milliseconds before processing */
  delay?: number;
  /** Priority (lower = higher priority) */
  priority?: number;
  /** Custom job ID */
  jobId?: string;
  /** Idempotency key */
  idempotencyKey?: string;
  /** Correlation ID for tracing */
  correlationId?: string;
  /** Custom retry configuration override */
  retry?: Partial<RetryConfig>;
  /** Additional headers */
  headers?: MessageHeaders;
  /** Time-to-live in milliseconds */
  ttl?: number;
  /** Remove job on completion */
  removeOnComplete?: boolean | number;
  /** Remove job on failure */
  removeOnFail?: boolean | number;
}

/**
 * Result of message processing
 */
export interface ProcessingResult<T = unknown> {
  /** Whether processing was successful */
  success: boolean;
  /** Result data from processing */
  data?: T;
  /** Error if processing failed */
  error?: Error;
  /** Processing duration in milliseconds */
  duration: number;
  /** Whether to retry (if failed) */
  shouldRetry?: boolean;
}

/**
 * Message handler function type
 */
export type MessageHandler<T = unknown, R = unknown> = (
  message: Message<T>,
  context: ProcessingContext
) => Promise<ProcessingResult<R>>;

/**
 * Processing context with utilities
 */
export interface ProcessingContext {
  /** Signal to abort processing */
  signal?: AbortSignal;
  /** Update progress (0-100) */
  updateProgress: (progress: number) => Promise<void>;
  /** Get job instance */
  getJob: () => Job;
  /** Logger instance */
  logger: Logger;
  /** Tracing span */
  span?: Span;
}

/**
 * Simplified logger interface
 */
export interface Logger {
  debug(message: string, meta?: Record<string, unknown>): void;
  info(message: string, meta?: Record<string, unknown>): void;
  warn(message: string, meta?: Record<string, unknown>): void;
  error(message: string, meta?: Record<string, unknown>): void;
}

/**
 * Simplified span interface for tracing
 */
export interface Span {
  spanId: string;
  traceId: string;
  setAttribute(key: string, value: string | number | boolean): void;
  addEvent(name: string, attributes?: Record<string, unknown>): void;
  setStatus(status: 'ok' | 'error', description?: string): void;
  end(): void;
}

/**
 * Dead letter queue entry
 */
export interface DLQEntry {
  /** Original message */
  message: Message;
  /** Error that caused failure */
  error: {
    message: string;
    stack?: string;
    name: string;
  };
  /** Timestamp when moved to DLQ */
  failedAt: number;
  /** Original topic */
  topic: string;
  /** Number of attempts before failure */
  attemptsMade: number;
  /** Whether the message can be retried */
  retriable: boolean;
}

/**
 * Queue statistics
 */
export interface QueueStats {
  /** Topic name */
  topic: string;
  /** Jobs waiting to be processed */
  waiting: number;
  /** Jobs currently being processed */
  active: number;
  /** Jobs completed successfully */
  completed: number;
  /** Jobs that failed */
  failed: number;
  /** Jobs in DLQ */
  deadLettered: number;
  /** Total jobs processed */
  totalProcessed: number;
  /** Average processing time in ms */
  avgProcessingTime: number;
  /** Last activity timestamp */
  lastActivity?: number;
}

/**
 * Configuration for the distributed queue manager
 */
export interface DistributedQueueConfig {
  /** Redis connection options */
  redis: RedisConfig;
  /** Prefix for all queue names */
  prefix?: string;
  /** Default topic configurations */
  topics?: TopicConfig[];
  /** Default retry configuration */
  defaultRetry?: RetryConfig;
  /** Default rate limit configuration */
  defaultRateLimit?: RateLimitConfig;
  /** Enable observability */
  observability?: ObservabilityConfig;
  /** Dead letter queue configuration */
  dlq?: DLQConfig;
}

/**
 * Redis connection configuration
 */
export interface RedisConfig {
  host: string;
  port: number;
  password?: string;
  db?: number;
  /** Maximum retries for connection */
  maxRetries?: number;
  /** Enable ready check */
  enableReadyCheck?: boolean;
}

/**
 * Observability configuration
 */
export interface ObservabilityConfig {
  /** Enable metrics collection */
  metrics?: boolean;
  /** Enable distributed tracing */
  tracing?: boolean;
  /** Logging level */
  logLevel?: 'debug' | 'info' | 'warn' | 'error';
  /** Metrics port */
  metricsPort?: number;
  /** Tracing endpoint (OTLP) */
  tracingEndpoint?: string;
}

/**
 * Dead letter queue configuration
 */
export interface DLQConfig {
  /** Maximum age of DLQ entries in days */
  maxAgeDays?: number;
  /** Maximum number of entries in DLQ */
  maxSize?: number;
  /** Enable automatic cleanup */
  autoCleanup?: boolean;
  /** Cleanup interval in milliseconds */
  cleanupInterval?: number;
}

/**
 * Metrics data structure
 */
export interface Metrics {
  /** Total messages produced */
  messagesProduced: number;
  /** Total messages consumed */
  messagesConsumed: number;
  /** Total messages failed */
  messagesFailed: number;
  /** Total messages in DLQ */
  messagesDeadLettered: number;
  /** Processing latency histogram */
  processingLatency: number[];
  /** Retry count histogram */
  retryCount: Map<string, number>;
  /** Queue depth by topic */
  queueDepth: Map<string, number>;
}

/**
 * Convert Message to BullMQ JobsOptions
 */
export function toJobOptions(options: ProduceOptions, defaultRetry?: RetryConfig): JobsOptions {
  const jobOptions: JobsOptions = {
    jobId: options.jobId,
    delay: options.delay,
    priority: options.priority,
    removeOnComplete: options.removeOnComplete ?? true,
    removeOnFail: options.removeOnFail ?? 100,
  };

  // Handle retry configuration
  if (options.retry || defaultRetry) {
    const retry = { ...defaultRetry, ...options.retry };
    if (retry.attempts) {
      jobOptions.attempts = retry.attempts;
    }
    if (retry.backoff) {
      if (retry.backoff.type === 'custom' && retry.backoff.customFunction) {
        jobOptions.backoff = {
          type: 'custom',
        } as unknown as JobsOptions['backoff'];
      } else {
        jobOptions.backoff = {
          type: retry.backoff.type,
          delay: retry.backoff.delay,
        };
      }
    }
  }

  return jobOptions;
}
