import { KinesisClient, PutRecordCommand } from '@aws-sdk/client-kinesis';

const kinesis = new KinesisClient({});
const STREAM_NAME = process.env.STREAM_NAME || '';

const STOCK_BASES: Record<string, { price: number; volBase: number }> = {
  AAPL: { price: 245.12, volBase: 12000 },
  TSLA: { price: 178.45, volBase: 8500 },
  GOOG: { price: 172.90, volBase: 9400 },
  AMZN: { price: 185.50, volBase: 11000 },
  NVDA: { price: 920.80, volBase: 15000 },
};

export const handler = async (event: any): Promise<any> => {
  console.log('Received event:', JSON.stringify(event, null, 2));

  // 1. HOT SHARD TEST MODE (Manual Trigger)
  if (event.mode === 'HOT_SHARD') {
    const symbol = event.symbol || 'AAPL';
    const count = event.count || 500;
    console.log(`[PRODUCER] Starting HOT SHARD mode for symbol: ${symbol}, count: ${count}`);

    const base = STOCK_BASES[symbol] || { price: 100.0, volBase: 5000 };
    let currentPrice = base.price;

    for (let i = 0; i < count; i++) {
      // Fast random walk to simulate fluctuating stock ticks
      currentPrice += (Math.random() - 0.5) * 0.5;
      const volume = Math.floor(base.volBase * (0.8 + Math.random() * 0.4));

      const payload = {
        symbol,
        price: parseFloat(currentPrice.toFixed(2)),
        volume,
        timestamp: new Date().toISOString(),
        isHotShardTest: true,
        sequenceNum: i + 1,
      };

      await kinesis.send(
        new PutRecordCommand({
          StreamName: STREAM_NAME,
          PartitionKey: symbol,
          Data: Buffer.from(JSON.stringify(payload)),
        })
      );
    }

    console.log(`[PRODUCER] HOT SHARD test completed. Pushed ${count} records.`);
    return {
      statusCode: 200,
      body: JSON.stringify({ message: `Successfully flooded stream with ${count} records for ${symbol}` }),
    };
  }

  // 2. NORMAL SCHEDULED MODE (Cron Trigger)
  console.log('[PRODUCER] Running in scheduled mode');
  const results = [];
  const minutesSinceEpoch = Math.floor(Date.now() / 60000);

  for (const [symbol, base] of Object.entries(STOCK_BASES)) {
    // Generate a moving trend (sinusoidal) + random noise so the stock price drifts over time
    const trend = Math.sin(minutesSinceEpoch / 10) * (base.price * 0.02); // 2% fluctuation
    const noise = (Math.random() - 0.5) * (base.price * 0.005); // 0.5% noise
    const price = parseFloat((base.price + trend + noise).toFixed(2));
    const volume = Math.floor(base.volBase * (0.9 + Math.random() * 0.2));

    const payload = {
      symbol,
      price,
      volume,
      timestamp: new Date().toISOString(),
    };

    console.log(`[PRODUCER] Publishing payload for ${symbol}:`, JSON.stringify(payload));

    const response = await kinesis.send(
      new PutRecordCommand({
        StreamName: STREAM_NAME,
        PartitionKey: symbol,
        Data: Buffer.from(JSON.stringify(payload)),
      })
    );

    results.push({
      symbol,
      sequenceNumber: response.SequenceNumber,
      shardId: response.ShardId,
    });
  }

  return {
    statusCode: 200,
    results,
  };
};
