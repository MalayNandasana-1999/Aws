import { KinesisStreamEvent } from 'aws-lambda';

export const handler = async (event: KinesisStreamEvent): Promise<void> => {
  console.log(`[MONITOR] Triggered with ${event.Records.length} records.`);

  let totalBytes = 0;
  const shardSummary: Record<string, { count: number; symbols: Set<string> }> = {};

  for (const record of event.Records) {
    try {
      // 1. Extract routing details
      const partitionKey = record.kinesis.partitionKey;
      const eventID = record.eventID;
      
      // In AWS Lambda Kinesis event sources, eventID is formatted as "shardId-00000000000X:sequenceNumber"
      const shardId = eventID.split(':')[0] || 'unknown-shard';

      // 2. Decode payload to measure size
      const dataStr = Buffer.from(record.kinesis.data, 'base64').toString('utf-8');
      const dataBytes = Buffer.byteLength(dataStr, 'utf8');
      totalBytes += dataBytes;

      const payload = JSON.parse(dataStr);

      // Track distribution per shard
      if (!shardSummary[shardId]) {
        shardSummary[shardId] = { count: 0, symbols: new Set() };
      }
      shardSummary[shardId].count += 1;
      shardSummary[shardId].symbols.add(partitionKey);

      // Log detailed record flow info
      console.log(
        `[RECORD] Shard: ${shardId} | Key (Symbol): ${partitionKey} | Price: $${payload.price} | Volume: ${payload.volume} | Size: ${dataBytes} bytes`
      );
    } catch (err) {
      console.error('[MONITOR] Error parsing record for monitoring:', err);
    }
  }

  // 3. Log aggregate throughput and distribution stats for the batch
  console.log(`[SUMMARY] Total Records: ${event.Records.length} | Total Payload Size: ${totalBytes} bytes`);
  for (const [shardId, summary] of Object.entries(shardSummary)) {
    console.log(
      `[SHARD_FLOW] Shard ${shardId} processed ${summary.count} records containing symbols: ${Array.from(
        summary.symbols
      ).join(', ')}`
    );
  }
};
