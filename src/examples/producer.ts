import 'dotenv/config';
import { DistributedQueueManager } from '../index.js';

/**
 * Example producer application
 * Demonstrates producing messages to multiple topics
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
    observability: {
      metrics: true,
      tracing: true,
      logLevel: 'debug',
    },
  });

  // Register topics
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
    rateLimit: {
      max: 100,
      duration: 1000, // 100 orders per second
    },
  });

  console.log('Producer started. Sending messages...\n');

  // Produce some email messages
  for (let i = 1; i <= 5; i++) {
    const message = await queueManager.produce('email', {
      to: `user${i}@example.com`,
      subject: `Welcome Email ${i}`,
      template: 'welcome',
    }, {
      idempotencyKey: `email-${i}`, // Ensure idempotent processing
      correlationId: `batch-1`,
    });
    console.log(`Produced email message: ${message.id}`);
  }

  // Produce notification messages with priority
  await queueManager.produce('notifications', {
    userId: 'user-123',
    type: 'push',
    title: 'Important Update',
    body: 'Your account has been updated',
  }, {
    priority: 1, // High priority
    correlationId: 'notification-urgent',
  });
  console.log('Produced high-priority notification');

  // Produce delayed order message
  await queueManager.produce('orders', {
    orderId: 'order-456',
    customerId: 'customer-789',
    items: [
      { productId: 'prod-1', quantity: 2, price: 29.99 },
      { productId: 'prod-2', quantity: 1, price: 49.99 },
    ],
    total: 109.97,
  }, {
    delay: 5000, // Process after 5 seconds
    correlationId: 'order-delayed',
  });
  console.log('Produced delayed order message (will process in 5 seconds)');

  // Get and display stats
  console.log('\n--- Queue Statistics ---');
  const stats = await queueManager.getAllStats();
  for (const stat of stats) {
    console.log(`${stat.topic}: waiting=${stat.waiting}, active=${stat.active}`);
  }

  // Graceful shutdown after a short delay
  console.log('\nProducer completed. Shutting down in 2 seconds...');
  setTimeout(async () => {
    await queueManager.shutdown();
    console.log('Producer shut down complete');
    process.exit(0);
  }, 2000);
}

main().catch((error) => {
  console.error('Producer error:', error);
  process.exit(1);
});
