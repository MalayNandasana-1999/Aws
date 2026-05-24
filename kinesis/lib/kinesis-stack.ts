import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as kinesis from 'aws-cdk-lib/aws-kinesis';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import { NodejsFunction } from 'aws-cdk-lib/aws-lambda-nodejs';
import { KinesisEventSource } from 'aws-cdk-lib/aws-lambda-event-sources';
import * as events from 'aws-cdk-lib/aws-events';
import * as targets from 'aws-cdk-lib/aws-events-targets';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as firehose from 'aws-cdk-lib/aws-kinesisfirehose';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as path from 'path';

// ==========================================
// CONFIGURATION PARAMETERS (Easy Modification)
// ==========================================
const SHARD_COUNT = 2;
const RETENTION_PERIOD_HOURS = 24;

export class KinesisStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // ==========================================
    // 1. KINESIS DATA STREAM DEFINITION
    // ==========================================
    const stockStream = new kinesis.Stream(this, 'StockDataStream', {
      streamName: 'stock-data-stream',
      shardCount: SHARD_COUNT,
      streamMode: kinesis.StreamMode.PROVISIONED, // Provisioned mode is best for demonstrating specific shards
      retentionPeriod: cdk.Duration.hours(RETENTION_PERIOD_HOURS),
      removalPolicy: cdk.RemovalPolicy.DESTROY, // Deletes the stream when stack is destroyed
    });

    // ==========================================
    // 2. DYNAMODB TABLE DEFINITION
    // ==========================================
    const stocksTable = new dynamodb.Table(this, 'StocksTable', {
      tableName: 'stocks',
      partitionKey: { name: 'symbol', type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST, // On-Demand billing (fully free if no requests)
      removalPolicy: cdk.RemovalPolicy.DESTROY, // Table will be deleted when stack is destroyed
    });

    // ==========================================
    // 3. LAMBDA FUNCTIONS DEFINITION & LOG GROUPS
    // ==========================================

    // Explicit Log Groups with static names so CloudWatch logs are destroyed on cdk destroy
    const producerLogGroup = new logs.LogGroup(this, 'ProducerLogGroup', {
      logGroupName: '/aws/lambda/stock-producer-lambda',
      retention: logs.RetentionDays.ONE_DAY,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    const analyticsLogGroup = new logs.LogGroup(this, 'AnalyticsLogGroup', {
      logGroupName: '/aws/lambda/stock-analytics-lambda',
      retention: logs.RetentionDays.ONE_DAY,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    const monitoringLogGroup = new logs.LogGroup(this, 'MonitoringLogGroup', {
      logGroupName: '/aws/lambda/stock-monitoring-lambda',
      retention: logs.RetentionDays.ONE_DAY,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    // A. PRODUCER LAMBDA (Fetches/Simulates data and publishes to Kinesis)
    const producerLambda = new NodejsFunction(this, 'ProducerLambda', {
      functionName: 'stock-producer-lambda',
      entry: path.join(__dirname, '../lambdas/producer/index.ts'),
      handler: 'handler',
      runtime: lambda.Runtime.NODEJS_20_X,
      timeout: cdk.Duration.seconds(30),
      logGroup: producerLogGroup,
      environment: {
        STREAM_NAME: stockStream.streamName,
      },
      bundling: {
        minify: true,
        sourceMap: true,
      },
    });

    // B. ANALYTICS CONSUMER LAMBDA (Processes stream, aggregates stats, saves to DynamoDB)
    const analyticsLambda = new NodejsFunction(this, 'AnalyticsLambda', {
      functionName: 'stock-analytics-lambda',
      entry: path.join(__dirname, '../lambdas/analytics/index.ts'),
      handler: 'handler',
      runtime: lambda.Runtime.NODEJS_20_X,
      timeout: cdk.Duration.seconds(30),
      logGroup: analyticsLogGroup,
      environment: {
        TABLE_NAME: stocksTable.tableName,
      },
      bundling: {
        minify: true,
        sourceMap: true,
      },
    });

    // C. MONITORING CONSUMER LAMBDA (Processes stream in parallel, logs shard routing behavior)
    const monitoringLambda = new NodejsFunction(this, 'MonitoringLambda', {
      functionName: 'stock-monitoring-lambda',
      entry: path.join(__dirname, '../lambdas/monitoring/index.ts'),
      handler: 'handler',
      runtime: lambda.Runtime.NODEJS_20_X,
      timeout: cdk.Duration.seconds(30),
      logGroup: monitoringLogGroup,
      bundling: {
        minify: true,
        sourceMap: true,
      },
    });

    // ==========================================
    // 4. EVENTBRIDGE SCHEDULER / CRON TARGET
    // ==========================================

    // Trigger the Producer Lambda every 1 minutes to ingest stock updates
    const schedulerRule = new events.Rule(this, 'ProducerScheduleRule', {
      schedule: events.Schedule.rate(cdk.Duration.minutes(1)),
      description: 'Trigger Producer Lambda every 1 minutes to fetch/simulate stock prices.',
    });
    schedulerRule.addTarget(new targets.LambdaFunction(producerLambda));

    // ==========================================
    // 5. IAM PERMISSIONS & EVENT SOURCE MAPPINGS
    // ==========================================

    // Grant Producer write permissions to Kinesis Stream
    stockStream.grantWrite(producerLambda);

    // Grant Analytics Lambda write permissions to DynamoDB
    stocksTable.grantReadWriteData(analyticsLambda);

    // Bind Analytics Lambda to Kinesis Stream (Shared/Standard throughput consumer)
    analyticsLambda.addEventSource(
      new KinesisEventSource(stockStream, {
        startingPosition: lambda.StartingPosition.LATEST,
        batchSize: 100, // Read up to 100 records at a time
        maxBatchingWindow: cdk.Duration.seconds(10), // Buffer for up to 10 seconds before triggering
      })
    );

    // Bind Monitoring Lambda to Kinesis Stream (Parallel Shared/Standard consumer)
    monitoringLambda.addEventSource(
      new KinesisEventSource(stockStream, {
        startingPosition: lambda.StartingPosition.LATEST,
        batchSize: 100,
        maxBatchingWindow: cdk.Duration.seconds(10),
      })
    );

    // ==========================================
    // 6. FIREHOSE ARCHIVING (TO S3)
    // ==========================================

    // A. Destination S3 Bucket
    const archiveBucket = new s3.Bucket(this, 'KinesisArchiveBucket', {
      bucketName: `stock-data-archive-bucket-${cdk.Aws.ACCOUNT_ID}`,
      encryption: s3.BucketEncryption.S3_MANAGED,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true, // Automatically empty and delete S3 bucket contents on cdk destroy
    });

    // B. IAM Role for Amazon Data Firehose
    const firehoseRole = new iam.Role(this, 'FirehoseDeliveryRole', {
      assumedBy: new iam.ServicePrincipal('firehose.amazonaws.com'),
      description: 'IAM Role for Firehose to read Kinesis Stream and write to S3 Bucket.',
    });

    // Grant Firehose permission to read from the Kinesis Stream
    stockStream.grantRead(firehoseRole);

    // Explicit Kinesis describe permissions (Firehose requires this to check stream health)
    firehoseRole.addToPolicy(
      new iam.PolicyStatement({
        actions: ['kinesis:DescribeStream', 'kinesis:DescribeStreamSummary', 'kinesis:ListShards'],
        resources: [stockStream.streamArn],
      })
    );

    // Grant Firehose permission to write files to the S3 Archive Bucket
    archiveBucket.grantWrite(firehoseRole);

    // C. Firehose Delivery Stream (L1 Construct to avoid unstable Alpha modules)
    const firehoseDeliveryStream = new firehose.CfnDeliveryStream(this, 'KinesisS3Firehose', {
      deliveryStreamName: 'stock-data-firehose-delivery-stream',
      deliveryStreamType: 'KinesisStreamAsSource',
      kinesisStreamSourceConfiguration: {
        kinesisStreamArn: stockStream.streamArn,
        roleArn: firehoseRole.roleArn,
      },
      extendedS3DestinationConfiguration: {
        bucketArn: archiveBucket.bucketArn,
        roleArn: firehoseRole.roleArn,
        bufferingHints: {
          intervalInSeconds: 60, // Dump buffer every 60 seconds (optimized for fast demo cycles)
          sizeInMBs: 1,          // Or when buffer reaches 1 MB
        },
        compressionFormat: 'GZIP', // Compress JSON strings using Gzip
        // Partition S3 keys by year/month/day/hour of record ingestion
        prefix: 'raw/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}/',
        errorOutputPrefix: 'error/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}/!{firehose:error-output-type}/',
      },
    });

    // Ensure Kinesis Stream and IAM Role exist before creating Firehose Stream
    firehoseDeliveryStream.node.addDependency(stockStream);
    firehoseDeliveryStream.node.addDependency(firehoseRole);

    // ==========================================
    // 7. STACK OUTPUTS
    // ==========================================
    new cdk.CfnOutput(this, 'KinesisStreamName', {
      value: stockStream.streamName,
      description: 'The name of the Kinesis Data Stream.',
    });

    new cdk.CfnOutput(this, 'KinesisStreamArn', {
      value: stockStream.streamArn,
      description: 'The ARN of the Kinesis Data Stream.',
    });

    new cdk.CfnOutput(this, 'DynamoDbTableName', {
      value: stocksTable.tableName,
      description: 'The name of the stocks DynamoDB tracking table.',
    });

    new cdk.CfnOutput(this, 'ProducerLambdaName', {
      value: producerLambda.functionName,
      description: 'The name of the Producer Lambda function (use for manual invocations).',
    });

    new cdk.CfnOutput(this, 'S3ArchiveBucketName', {
      value: archiveBucket.bucketName,
      description: 'The name of the S3 Bucket storing the compressed Firehose logs.',
    });

    new cdk.CfnOutput(this, 'FirehoseStreamName', {
      value: firehoseDeliveryStream.deliveryStreamName || '',
      description: 'The name of the Firehose Delivery Stream.',
    });
  }
}
