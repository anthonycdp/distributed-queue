// Main exports
export { DistributedQueueManager } from './core/queue-manager.js';
export {
  IdempotencyManager,
  DeadLetterQueue,
  calculateBackoff,
  calculateExponentialBackoff,
  createBullMQBackoff,
  BackoffStrategies,
  DefaultRetryPolicies,
  type RetryPolicy,
} from './core/index.js';

// Observability exports
export {
  createLogger,
  logger,
  MetricsCollector,
  TracingService,
} from './observability/index.js';

// Type exports
export type {
  TopicConfig,
  RateLimitConfig,
  RetryConfig,
  BackoffConfig,
  Message,
  MessageHeaders,
  ProduceOptions,
  ProcessingResult,
  MessageHandler,
  ProcessingContext,
  Logger,
  Span,
  DLQEntry,
  QueueStats,
  DistributedQueueConfig,
  RedisConfig,
  ObservabilityConfig,
  DLQConfig,
  Metrics,
} from './types/index.js';

// Re-export useful BullMQ types
export { Queue, Worker, Job, QueueEvents } from 'bullmq';
