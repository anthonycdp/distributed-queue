# Distributed Message Queue

A production-ready distributed message queue system built with Node.js, TypeScript, and BullMQ. Features include topic-based routing, retry with exponential backoff, idempotent processing, dead letter queues, and comprehensive observability.

## Features

- **Topic-Based Queues**: Organize messages by topic with independent configurations
- **Retry with Backoff**: Configurable retry strategies (exponential, fixed, linear, custom)
- **Idempotent Processing**: Prevent duplicate message processing with idempotency keys
- **Dead Letter Queue (DLQ)**: Automatic handling of failed messages with retry capability
- **Observability**: Built-in metrics (Prometheus), distributed tracing (OpenTelemetry), and structured logging
- **Rate Limiting**: Control message processing rate per topic
- **Priority Queues**: Support for message prioritization
- **Delayed Messages**: Schedule messages for future processing
- **Type-Safe**: Full TypeScript support with comprehensive type definitions

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Distributed Queue Manager                     │
├─────────────────────────────────────────────────────────────────┤
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────────────┐ │
│  │ Producer │  │ Consumer │  │   DLQ    │  │  Idempotency     │ │
│  │  Client  │  │  Worker  │  │ Manager  │  │  Manager         │ │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────────┬─────────┘ │
│       │             │             │                  │           │
│       └─────────────┴─────────────┴──────────────────┘           │
│                           │                                      │
│                    ┌──────┴──────┐                               │
│                    │    Redis    │                               │
│                    │  (BullMQ)   │                               │
│                    └─────────────┘                               │
└─────────────────────────────────────────────────────────────────┘
                           │
              ┌────────────┼────────────┐
              │            │            │
        ┌─────┴─────┐ ┌────┴────┐ ┌─────┴─────┐
        │ Prometheus│ │ Jaeger  │ │  Grafana  │
        │ (Metrics) │ │(Tracing)│ │(Dashboard)│
        └───────────┘ └─────────┘ └───────────┘
```

## Quick Start

### Prerequisites

- Node.js >= 18.0.0
- Redis >= 6.0
- Docker (optional, for observability stack)

### Installation

```bash
# Install dependencies
npm install

# Copy environment file
cp .env.example .env

# Start Redis and observability stack
docker-compose up -d
```

### Basic Usage

#### 1. Initialize the Queue Manager

```typescript
import { DistributedQueueManager } from 'distributed-message-queue';

const queueManager = new DistributedQueueManager({
  redis: {
    host: 'localhost',
    port: 6379,
  },
  prefix: 'my-app',
  defaultRetry: {
    attempts: 3,
    backoff: {
      type: 'exponential',
      delay: 1000,
    },
  },
  observability: {
    metrics: true,
    tracing: true,
    logLevel: 'info',
  },
});
```

#### 2. Register Topics

```typescript
// Email queue with 5 concurrent workers
await queueManager.registerTopic({
  name: 'email',
  concurrency: 5,
  maxRetries: 3,
  enableDLQ: true,
  retry: {
    attempts: 3,
    backoff: {
      type: 'exponential',
      delay: 1000,
    },
  },
});

// High-priority notifications
await queueManager.registerTopic({
  name: 'notifications',
  concurrency: 10,
  maxRetries: 5,
  rateLimit: {
    max: 100,
    duration: 1000, // 100 messages per second
  },
});
```

#### 3. Produce Messages

```typescript
// Simple message
const message = await queueManager.produce('email', {
  to: 'user@example.com',
  subject: 'Welcome!',
  template: 'welcome',
});

// With options
await queueManager.produce('notifications', {
  userId: 'user-123',
  type: 'push',
  title: 'Update Available',
}, {
  priority: 1,              // Higher priority
  delay: 5000,              // Process after 5 seconds
  idempotencyKey: 'notif-123', // Prevent duplicates
  correlationId: 'request-abc', // For distributed tracing
});
```

#### 4. Consume Messages

```typescript
await queueManager.consume('email', async (message, context) => {
  const { to, subject, template } = message.payload;

  // Access logger from context
  context.logger.info('Processing email', { to, subject });

  // Report progress (optional)
  await context.updateProgress(50);

  // Process the message
  await sendEmail(to, subject, template);

  await context.updateProgress(100);

  // Return result with duration tracking
  return {
    success: true,
    data: { sent: true },
    duration: 150, // Processing time in ms
  };
});
```

### Running Examples

```bash
# Start consumer (in one terminal)
npm run consumer

# Start producer (in another terminal)
npm run producer
```

## Advanced Features

### Retry with Backoff

Multiple backoff strategies are available:

```typescript
import { BackoffStrategies, DefaultRetryPolicies } from 'distributed-message-queue';

// Fixed delay
const fixed = BackoffStrategies.fixed(1000); // Always 1 second

// Exponential backoff
const exponential = BackoffStrategies.exponential(1000, 30000); // Start 1s, max 30s

// Linear backoff
const linear = BackoffStrategies.linear(500); // 500ms, 1000ms, 1500ms...

// Fibonacci backoff
const fibonacci = BackoffStrategies.fibonacci(100); // 100ms, 100ms, 200ms, 300ms...

// Use in topic config
await queueManager.registerTopic({
  name: 'api-calls',
  retry: {
    attempts: 5,
    backoff: exponential,
  },
});
```

### Idempotent Processing

Prevent duplicate message processing:

```typescript
// Producing with idempotency key
await queueManager.produce('orders', orderData, {
  idempotencyKey: `order-${orderId}`, // Unique key
});

// The message will only be processed once, even if produced multiple times
// Results are cached for 24 hours (configurable)
```

### Dead Letter Queue

Handle failed messages:

```typescript
// Get DLQ for a topic
const dlq = queueManager.getDLQ('email');

// Get all failed messages
const entries = await dlq.getAll();

// Get DLQ statistics
const stats = await dlq.getStats();
console.log(`DLQ size: ${stats.size}`);
console.log(`Error types:`, stats.errorTypes);

// Retry a failed message
await dlq.retry('message-id', mainQueue);

// Clear the DLQ
await dlq.clear();
```

### Observability

#### Metrics (Prometheus)

```typescript
// Get metrics in Prometheus format
const metrics = await queueManager.getMetrics();

// Expose metrics endpoint (example with Express)
app.get('/metrics', async (req, res) => {
  res.set('Content-Type', 'text/plain');
  res.send(await queueManager.getMetrics());
});
```

Available metrics:
- `dmq_messages_produced_total` - Total messages produced
- `dmq_messages_consumed_total` - Total messages consumed
- `dmq_messages_failed_total` - Total failed messages
- `dmq_messages_dead_lettered_total` - Messages moved to DLQ
- `dmq_retries_total` - Total retry attempts
- `dmq_idempotency_hits_total` - Duplicate messages detected
- `dmq_processing_duration_seconds` - Processing time histogram
- `dmq_queue_latency_seconds` - Queue latency histogram
- `dmq_queue_depth` - Current queue depth
- `dmq_dlq_size` - Dead letter queue size
- `dmq_active_workers` - Active worker count

#### Distributed Tracing (OpenTelemetry)

```typescript
// Tracing is automatically integrated
// View traces in Jaeger at http://localhost:16686

// Custom spans in handlers
await queueManager.consume('orders', async (message, context) => {
  context.span?.setAttribute('order.id', message.payload.orderId);
  context.span?.addEvent('payment_processing_started');

  // ... processing

  context.span?.addEvent('payment_completed');
});
```

#### Structured Logging

```typescript
// Logs are JSON-formatted with context
// Log levels: debug, info, warn, error

// In handlers, use the provided logger
await queueManager.consume('email', async (message, context) => {
  context.logger.info('Processing email', {
    messageId: message.id,
    to: message.payload.to,
  });
});
```

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `REDIS_HOST` | Redis host | `localhost` |
| `REDIS_PORT` | Redis port | `6379` |
| `REDIS_PASSWORD` | Redis password | (none) |
| `QUEUE_PREFIX` | Queue name prefix | `dmq` |
| `MAX_CONCURRENCY` | Default max concurrency | `5` |
| `DEFAULT_RETRY_ATTEMPTS` | Default retry attempts | `3` |
| `DEFAULT_BACKOFF_DELAY` | Default backoff delay (ms) | `1000` |
| `DLQ_MAX_AGE_DAYS` | DLQ entry retention (days) | `7` |
| `DLQ_MAX_SIZE` | Maximum DLQ size | `10000` |
| `LOG_LEVEL` | Logging level | `info` |
| `METRICS_PORT` | Prometheus metrics port | `9091` |
| `TRACING_ENABLED` | Enable distributed tracing | `true` |
| `TRACING_ENDPOINT` | OTLP tracing endpoint | `localhost:4317` |
| `IDEMPOTENCY_TTL_SECONDS` | Idempotency cache TTL | `86400` |

## API Reference

### DistributedQueueManager

#### Constructor

```typescript
new DistributedQueueManager(config: DistributedQueueConfig)
```

#### Methods

| Method | Description |
|--------|-------------|
| `registerTopic(config)` | Register a new topic |
| `produce(topic, payload, options?)` | Produce a message |
| `consume(topic, handler)` | Start consuming messages |
| `getStats(topic)` | Get queue statistics |
| `getAllStats()` | Get all topic statistics |
| `getDLQ(topic)` | Get dead letter queue |
| `pause(topic)` | Pause a topic |
| `resume(topic)` | Resume a topic |
| `getMetrics()` | Get Prometheus metrics |
| `isHealthy()` | Check if queue manager is healthy |
| `getConnectionStatus()` | Get connection status (redis, topics, activeWorkers) |
| `shutdown()` | Graceful shutdown |

### TopicConfig

```typescript
interface TopicConfig {
  name: string;           // Topic identifier
  concurrency?: number;   // Concurrent workers (default: 5)
  maxRetries?: number;    // Max retry attempts (default: 3)
  enableDLQ?: boolean;    // Enable dead letter queue (default: true)
  rateLimit?: {           // Rate limiting
    max: number;
    duration: number;
  };
  retry?: RetryConfig;    // Retry configuration
}
```

### ProduceOptions

```typescript
interface ProduceOptions {
  delay?: number;           // Delay before processing (ms)
  priority?: number;        // Priority (lower = higher)
  jobId?: string;           // Custom job ID
  idempotencyKey?: string;  // Idempotency key
  correlationId?: string;   // Tracing correlation ID
  headers?: MessageHeaders; // Custom headers
  ttl?: number;             // Time-to-live (ms)
  removeOnComplete?: boolean | number;
  removeOnFail?: boolean | number;
}
```

## Testing

```bash
# Run all tests
npm test

# Run tests in watch mode
npm run test:watch

# Run tests with coverage
npm run test:coverage
```

### Test Structure

- `tests/backoff.test.ts` - Backoff strategy tests
- `tests/idempotency.test.ts` - Idempotency manager tests
- `tests/metrics.test.ts` - Metrics collector tests
- `tests/dlq.test.ts` - Dead letter queue tests
- `tests/integration.test.ts` - End-to-end integration tests

## Observability Stack

The included Docker Compose sets up:

- **Redis**: Message broker
- **Redis Commander**: Redis GUI at http://localhost:8081
- **Prometheus**: Metrics at http://localhost:9090
- **Grafana**: Dashboards at http://localhost:3000 (admin/admin)
- **Jaeger**: Distributed tracing at http://localhost:16686

### Grafana Dashboard

Import the pre-configured dashboard from `observability/grafana-dashboard.json` for:
- Message throughput
- Processing latency (p95, p99)
- Queue depth
- Failure rates
- DLQ size
- Retry patterns

## Production Considerations

### High Availability

1. **Redis Clustering**: Use Redis Cluster for HA
2. **Multiple Workers**: Run consumers on multiple instances
3. **Health Checks**: Monitor queue depth and processing times

### Scaling

```typescript
// Scale horizontally by running multiple consumer instances
// BullMQ handles distributed locking and job assignment

// Per-instance concurrency
await queueManager.registerTopic({
  name: 'high-volume',
  concurrency: 10, // 10 per instance
});

// With 5 instances = 50 concurrent workers
```

### Monitoring Alerts

Recommended Prometheus alerts:

```yaml
# High queue depth
- alert: HighQueueDepth
  expr: dmq_queue_depth > 1000
  for: 5m

# High failure rate
- alert: HighFailureRate
  expr: rate(dmq_messages_failed_total[5m]) > 10
  for: 5m

# DLQ growing
- alert: DLQGrowing
  expr: rate(dmq_messages_dead_lettered_total[5m]) > 1
  for: 10m
```

## License

MIT

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request
