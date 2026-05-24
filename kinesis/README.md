# Kinesis Shard Scaling & Testing Guide

This guide provides instructions and reference commands for testing shard routing, executing manual resharding (splitting and merging shards), and configuring automated scaling.

---

## 1. Triggering the "Hot Shard" Test
Flood a single shard with high-frequency traffic using a single partition key (`AAPL`) to observe throttling and queue batching behavior.

Run this command in your terminal:
```bash
aws lambda invoke \
  --function-name stock-producer-lambda \
  --payload '{"mode": "HOT_SHARD", "symbol": "AAPL", "count": 500}' \
  --cli-binary-format raw-in-base64-out \
  --region us-east-1 \
  response.json
```

---

## 2. Inspecting Stream Shards
Check the current list of shards, their operational status (`ACTIVE` / `CLOSED`), and their hash key ranges:

```bash
aws kinesis describe-stream --stream-name stock-data-stream --region us-east-1
```

---

## 3. Manual Resharding

### A. Split Shard (Scale Up)
To split an active shard (e.g., `shardId-000000000000`) into two new child shards, divide its hash range in half by targeting the midpoint hash key:

```bash
aws kinesis split-shard \
  --stream-name stock-data-stream \
  --shard-to-split shardId-000000000000 \
  --new-starting-hash-key 85070591730234615865843651857942052864 \
  --region us-east-1
```
*Note: The split target shard will be marked `CLOSED` for writes once the split completes, and two new `ACTIVE` child shards will handle the new hash ranges.*

### B. Merge Shards (Scale Down / Shrink)
To reduce stream capacity and costs, merge two adjacent shards back into a single shard:

```bash
aws kinesis merge-shards \
  --stream-name stock-data-stream \
  --shard-to-merge shardId-000000000002 \
  --adjacent-shard-to-merge shardId-000000000003 \
  --region us-east-1
```
*Note: This closes both target shards and activates a single consolidated child shard.*

---

## 4. Auto-Scaling Options

### Option 1: Native Kinesis On-Demand Mode (Recommended)
Let AWS automatically scale shards in response to traffic volume without manual intervention or custom infrastructure.

Modify the stream properties in [lib/kinesis-stack.ts](file:///Users/malaynandasana/Aws/kinesis/lib/kinesis-stack.ts):
```typescript
const stockStream = new kinesis.Stream(this, 'StockDataStream', {
  streamName: 'stock-data-stream',
  streamMode: kinesis.StreamMode.ON_DEMAND, // Set mode to ON_DEMAND
  removalPolicy: cdk.RemovalPolicy.DESTROY,
});
```

### Option 2: Event-Driven Autoscaling (Provisioned Mode)
Automate scaling for provisioned streams using CloudWatch metrics:
1. **CloudWatch Alarm**: Create alarms monitoring `IncomingBytes` or `WriteProvisionedThroughputExceeded`.
2. **Lambda Trigger**: Configure the alarm to invoke a scaling Lambda helper.
3. **API Execution**: The Lambda uses the AWS SDK to dynamically call `split-shard` (scale up) or `merge-shards` (scale down) based on traffic thresholds.