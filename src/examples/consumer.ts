import 'dotenv/config';
import { DistributedQueueManager, type Message, type ProcessingContext, type ProcessingResult } from '../index.js';

/**
 * Email message handler
 */
async function handleEmail(
  message: Message<{ to: string; subject: string; template: string }>,
  context: ProcessingContext
): Promise<ProcessingResult<{ sent: boolean }>> {
  const startTime = Date.now();

  context.logger.info('Processing email', {
    messageId: message.id,
    to: message.payload.to,
  });

  try {
    // Simulate sending email
    await context.updateProgress(25);
    await new Promise((resolve) => setTimeout(resolve, 100));

    await context.updateProgress(50);
    // Simulate email API call
    await new Promise((resolve) => setTimeout(resolve, 200));

    await context.updateProgress(75);
    // Simulate confirmation
    await new Promise((resolve) => setTimeout(resolve, 50));

    await context.updateProgress(100);

    return {
      success: true,
      data: { sent: true },
      duration: Date.now() - startTime,
    };
  } catch (error) {
    return {
      success: false,
      error: error instanceof Error ? error : new Error(String(error)),
      duration: Date.now() - startTime,
    };
  }
}

/**
 * Notification message handler
 * Demonstrates error handling and retry behavior
 */
async function handleNotification(
  message: Message<{ userId: string; type: string; title: string; body: string }>,
  context: ProcessingContext
): Promise<ProcessingResult> {
  const startTime = Date.now();

  context.logger.info('Processing notification', {
    messageId: message.id,
    userId: message.payload.userId,
    type: message.payload.type,
  });

  // Simulate occasional failures for demonstration
  const shouldFail = Math.random() < 0.3; // 30% failure rate

  if (shouldFail) {
    context.logger.warn('Simulated notification failure', {
      messageId: message.id,
    });

    return {
      success: false,
      error: new Error('Simulated failure - will trigger retry'),
      duration: Date.now() - startTime,
      shouldRetry: true,
    };
  }

  // Simulate processing
  await context.updateProgress(50);
  await new Promise((resolve) => setTimeout(resolve, 150));
  await context.updateProgress(100);

  return {
    success: true,
    duration: Date.now() - startTime,
  };
}

/**
 * Order message handler
 */
async function handleOrder(
  message: Message<{
    orderId: string;
    customerId: string;
    items: Array<{ productId: string; quantity: number; price: number }>;
    total: number;
  }>,
  context: ProcessingContext
): Promise<ProcessingResult<{ orderId: string; status: string }>> {
  const startTime = Date.now();

  context.logger.info('Processing order', {
    messageId: message.id,
    orderId: message.payload.orderId,
    total: message.payload.total,
  });

  try {
    // Simulate order processing steps
    await context.updateProgress(20);
    await new Promise((resolve) => setTimeout(resolve, 100)); // Validate order

    await context.updateProgress(40);
    await new Promise((resolve) => setTimeout(resolve, 100)); // Check inventory

    await context.updateProgress(60);
    await new Promise((resolve) => setTimeout(resolve, 100)); // Process payment

    await context.updateProgress(80);
    await new Promise((resolve) => setTimeout(resolve, 100)); // Create shipment

    await context.updateProgress(100);

    return {
      success: true,
      data: {
        orderId: message.payload.orderId,
        status: 'confirmed',
      },
      duration: Date.now() - startTime,
    };
  } catch (error) {
    return {
      success: false,
      error: error instanceof Error ? error : new Error(String(error)),
      duration: Date.now() - startTime,
    };
  }
}

/**
 * Example consumer application
 */
async function main(): Promise<void> {
  // Initialize the queue manager
  const queueManager = new DistributedQueueManager({
    redis: {
      host: process.env['REDIS_HOST'] || 'localhost',
      port: parseInt(process.env['REDIS_PORT'] || '6379', 10),
    },
    prefix: 'dmq',
    defaultRetry: {
      attempts: 3,
      backoff: {
        type: 'exponential',
        delay: 1000,
        maxDelay: 30000,
      },
    },
    dlq: {
      maxAgeDays: 7,
      maxSize: 10000,
      autoCleanup: true,
    },
    observability: {
      metrics: true,
      tracing: true,
      logLevel: 'info',
    },
  });

  // Register topics
  await queueManager.registerTopic({
    name: 'email',
    concurrency: 5,
    maxRetries: 3,
    enableDLQ: true,
  });

  await queueManager.registerTopic({
    name: 'notifications',
    concurrency: 10,
    maxRetries: 5,
    enableDLQ: true,
  });

  await queueManager.registerTopic({
    name: 'orders',
    concurrency: 3,
    maxRetries: 5,
    enableDLQ: true,
  });

  // Start consumers
  console.log('Starting consumers...\n');

  await queueManager.consume('email', handleEmail);
  console.log('Email consumer started');

  await queueManager.consume('notifications', handleNotification);
  console.log('Notification consumer started');

  await queueManager.consume('orders', handleOrder);
  console.log('Order consumer started');

  console.log('\nConsumers running. Press Ctrl+C to stop.\n');

  // Display periodic stats
  const statsInterval = setInterval(async () => {
    const stats = await queueManager.getAllStats();
    console.log('\n--- Queue Statistics ---');
    for (const stat of stats) {
      console.log(
        `${stat.topic}: waiting=${stat.waiting}, active=${stat.active}, ` +
        `completed=${stat.completed}, failed=${stat.failed}, dlq=${stat.deadLettered}`
      );
    }
  }, 10000);

  // Graceful shutdown handler
  const shutdown = async () => {
    console.log('\nShutting down consumers...');
    clearInterval(statsInterval);
    await queueManager.shutdown();
    console.log('Shutdown complete');
    process.exit(0);
  };

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

main().catch((error) => {
  console.error('Consumer error:', error);
  process.exit(1);
});
