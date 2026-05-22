import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as sqs from 'aws-cdk-lib/aws-sqs';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import { NodejsFunction } from 'aws-cdk-lib/aws-lambda-nodejs';
import { SqsEventSource } from 'aws-cdk-lib/aws-lambda-event-sources';
import * as apigateway from 'aws-cdk-lib/aws-apigateway';
import * as path from 'path';

export class SqsStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // ==========================================
    // 1. S3 BUCKETS DEFINITION
    // ==========================================

    // The Source Bucket stores the original images uploaded by the users.
    const sourceBucket = new s3.Bucket(this, 'SourceImageBucket', {
      versioned: true,
      encryption: s3.BucketEncryption.S3_MANAGED,
      // For testing and learning, we enable DESTROY and autoDeleteObjects.
      // This ensures that when you destroy the stack, the S3 buckets and their contents are deleted.
      // In production, you would typically use RETAIN to prevent accidental data loss.
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true, 
    });

    // The Processed Bucket stores the resized and format-converted images.
    const processedBucket = new s3.Bucket(this, 'ProcessedImageBucket', {
      encryption: s3.BucketEncryption.S3_MANAGED,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
    });


    // ==========================================
    // 2. SQS QUEUES DEFINITION (QUEUE & DLQ)
    // ==========================================

    // A Dead Letter Queue (DLQ) is a standard SQS queue where messages that fail 
    // to process successfully after multiple attempts are sent. 
    // This prevents bad messages from clogging the queue (poison pill messages).
    const deadLetterQueue = new sqs.Queue(this, 'ImageProcessingDLQ', {
      queueName: 'image-processing-dlq',
      retentionPeriod: cdk.Duration.days(14), // Store failed messages for 14 days for debugging
    });

    // This is the main SQS Queue where upload metadata is placed.
    // The Worker Lambda polls this queue.
    const imageProcessingQueue = new sqs.Queue(this, 'ImageProcessingQueue', {
      queueName: 'image-processing-queue',
      // The visibility timeout must be greater than or equal to the Worker Lambda's timeout.
      // Visibility timeout is the time a message is hidden from other consumers while being processed.
      // If the Lambda fails or times out, the message becomes visible again after this period.
      visibilityTimeout: cdk.Duration.seconds(180), // 3 minutes
      deadLetterQueue: {
        queue: deadLetterQueue,
        // maxReceiveCount is the number of times a message can be read from the queue before 
        // being sent to the DLQ. We set this to 2, meaning it will retry once on failure,
        // and on the 3rd attempt, if it still fails, it goes to the DLQ.
        maxReceiveCount: 2,
      },
    });


    // ==========================================
    // 3. LAMBDA FUNCTIONS DEFINITION
    // ==========================================

    // The Upload Lambda processes incoming HTTP requests from API Gateway, 
    // decodes the image, saves it in S3, and notifies SQS.
    // NodejsFunction automatically uses esbuild to bundle TS/JS code into a deployment package.
    const uploadLambda = new NodejsFunction(this, 'UploadImageLambda', {
      entry: path.join(__dirname, '../lambdas/upload/index.ts'),
      handler: 'handler',
      runtime: lambda.Runtime.NODEJS_20_X,
      timeout: cdk.Duration.seconds(15), // Gives plenty of time for S3/SQS network calls
      environment: {
        SOURCE_BUCKET_NAME: sourceBucket.bucketName,
        QUEUE_URL: imageProcessingQueue.queueUrl,
      },
      bundling: {
        minify: true,
        sourceMap: true,
      },
    });

    // The Worker Lambda is triggered asynchronously by SQS.
    // It downloads the image, resizes it, converts it to JPEG, and uploads it.
    const workerLambda = new NodejsFunction(this, 'WorkerImageLambda', {
      entry: path.join(__dirname, '../lambdas/worker/index.ts'),
      handler: 'handler',
      runtime: lambda.Runtime.NODEJS_20_X,
      // Resizing images is CPU and memory intensive, so we assign more memory (512MB)
      // and set a 30-second timeout (well below the SQS visibility timeout of 180s).
      memorySize: 512,
      timeout: cdk.Duration.seconds(30),
      environment: {
        PROCESSED_BUCKET_NAME: processedBucket.bucketName,
      },
      bundling: {
        minify: true,
        sourceMap: true,
        // Since Jimp is a pure JavaScript library, it will be bundled inside the Lambda cleanly by esbuild.
      },
    });


    // ==========================================
    // 4. IAM PERMISSIONS & CONNECTIONS
    // ==========================================

    // Give Upload Lambda permission to write (PutObject) to the Source Bucket.
    sourceBucket.grantWrite(uploadLambda);

    // Give Upload Lambda permission to send messages to the SQS queue.
    imageProcessingQueue.grantSendMessages(uploadLambda);

    // Give Worker Lambda permission to read (GetObject) from the Source Bucket.
    sourceBucket.grantRead(workerLambda);

    // Give Worker Lambda permission to write (PutObject) to the Processed Bucket.
    processedBucket.grantWrite(workerLambda);

    // Connect SQS to the Worker Lambda.
    // This tells AWS to automatically invoke the Worker Lambda when messages arrive in SQS.
    // batchSize: 5 means the Lambda receives up to 5 messages at a time to process.
    workerLambda.addEventSource(new SqsEventSource(imageProcessingQueue, {
      batchSize: 5,
    }));


    // ==========================================
    // 5. API GATEWAY DEFINITION
    // ==========================================

    // Create a REST API. This acts as the entry point for the user.
    const api = new apigateway.RestApi(this, 'ImageUploadApi', {
      restApiName: 'Image Upload Service',
      description: 'API Gateway that accepts image uploads and triggers the processing pipeline.',
      // Tell API Gateway to treat multipart/form-data as binary, encoding it as base64 for Lambda
      binaryMediaTypes: ['multipart/form-data'],
      // Configure default CORS behavior to make it accessible if tested from a web client
      defaultCorsPreflightOptions: {
        allowOrigins: apigateway.Cors.ALL_ORIGINS,
        allowMethods: apigateway.Cors.ALL_METHODS,
        allowHeaders: ['Content-Type'],
      },
    });

    // Create an endpoint: POST /upload
    const uploadResource = api.root.addResource('upload');
    const lambdaIntegration = new apigateway.LambdaIntegration(uploadLambda);
    uploadResource.addMethod('POST', lambdaIntegration);


    // ==========================================
    // 6. STACK OUTPUTS
    // ==========================================

    // Print out the endpoints and bucket names to make testing easy
    new cdk.CfnOutput(this, 'ApiGatewayUrl', {
      value: `${api.url}upload`,
      description: 'The API endpoint to POST images for uploading and processing.',
    });

    new cdk.CfnOutput(this, 'SourceS3BucketName', {
      value: sourceBucket.bucketName,
      description: 'The name of the S3 Bucket storing the original uploaded images.',
    });

    new cdk.CfnOutput(this, 'ProcessedS3BucketName', {
      value: processedBucket.bucketName,
      description: 'The name of the S3 Bucket storing the processed (resized) images.',
    });

    new cdk.CfnOutput(this, 'SqsQueueUrl', {
      value: imageProcessingQueue.queueUrl,
      description: 'The URL of the SQS queue that coordinates processing messages.',
    });

    new cdk.CfnOutput(this, 'DlqUrl', {
      value: deadLetterQueue.queueUrl,
      description: 'The URL of the Dead Letter Queue (DLQ) for failed messages.',
    });
  }
}
