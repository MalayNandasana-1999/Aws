import { KinesisStreamEvent } from 'aws-lambda';
import { DynamoDBClient, GetItemCommand, PutItemCommand } from '@aws-sdk/client-dynamodb';
import { marshall, unmarshall } from '@aws-sdk/util-dynamodb';

const ddb = new DynamoDBClient({});
const TABLE_NAME = process.env.TABLE_NAME || '';

export const handler = async (event: KinesisStreamEvent): Promise<void> => {
  console.log(`[ANALYTICS] Received ${event.Records.length} records from Kinesis stream.`);

  // 1. Group records by stock symbol to process updates in batches
  const groups: Record<string, any[]> = {};

  for (const record of event.Records) {
    try {
      // Kinesis data is base64 encoded
      const dataStr = Buffer.from(record.kinesis.data, 'base64').toString('utf-8');
      const payload = JSON.parse(dataStr);
      
      if (!payload.symbol || payload.price === undefined) {
        console.warn('[ANALYTICS] Invalid record format:', dataStr);
        continue;
      }

      if (!groups[payload.symbol]) {
        groups[payload.symbol] = [];
      }
      groups[payload.symbol].push(payload);
    } catch (err) {
      console.error('[ANALYTICS] Error parsing record:', err);
    }
  }

  // 2. Aggregate stats for each symbol group and update DynamoDB
  for (const [symbol, records] of Object.entries(groups)) {
    try {
      // Sort records by timestamp to ensure chronological calculation
      records.sort((a, b) => new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime());

      let batchHighest = -Infinity;
      let batchLowest = Infinity;
      let batchSum = 0;
      const batchCount = records.length;

      for (const r of records) {
        if (r.price > batchHighest) batchHighest = r.price;
        if (r.price < batchLowest) batchLowest = r.price;
        batchSum += r.price;
      }

      const lastRecord = records[records.length - 1];
      const batchLatestPrice = lastRecord.price;
      const batchLatestTimestamp = lastRecord.timestamp;

      console.log(`[ANALYTICS] Aggregating ${batchCount} records for ${symbol}: latest=${batchLatestPrice}, highest=${batchHighest}, lowest=${batchLowest}`);

      // Read current statistics from DynamoDB
      const getResponse = await ddb.send(
        new GetItemCommand({
          TableName: TABLE_NAME,
          Key: marshall({ symbol }),
        })
      );

      let finalItem: any;

      if (!getResponse.Item) {
        // First record for this stock
        finalItem = {
          symbol,
          latestPrice: batchLatestPrice,
          highestPrice: batchHighest,
          lowestPrice: batchLowest,
          averagePrice: parseFloat((batchSum / batchCount).toFixed(2)),
          dataPointsCount: batchCount,
          updatedAt: batchLatestTimestamp,
        };
      } else {
        // Update existing statistics
        const existing = unmarshall(getResponse.Item);
        const existingCount = existing.dataPointsCount || 0;
        const existingAvg = existing.averagePrice || 0;
        const existingHighest = existing.highestPrice || 0;
        const existingLowest = existing.lowestPrice || 0;

        const totalCount = existingCount + batchCount;
        const combinedAvg = ((existingAvg * existingCount) + batchSum) / totalCount;

        finalItem = {
          symbol,
          latestPrice: batchLatestPrice,
          highestPrice: Math.max(existingHighest, batchHighest),
          lowestPrice: Math.min(existingLowest, batchLowest),
          averagePrice: parseFloat(combinedAvg.toFixed(2)),
          dataPointsCount: totalCount,
          updatedAt: batchLatestTimestamp,
        };
      }

      console.log(`[ANALYTICS] Writing stats for ${symbol} to DynamoDB:`, JSON.stringify(finalItem));

      // Save consolidated state back to DynamoDB
      await ddb.send(
        new PutItemCommand({
          TableName: TABLE_NAME,
          Item: marshall(finalItem),
        })
      );
    } catch (err) {
      console.error(`[ANALYTICS] Error processing stock ${symbol}:`, err);
    }
  }
};
