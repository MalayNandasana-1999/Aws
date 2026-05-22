import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as sqs from 'aws-cdk-lib/aws-sqs';
import * as sns from 'aws-cdk-lib/aws-sns';
import * as subs from 'aws-cdk-lib/aws-sns-subscriptions';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import { NodejsFunction } from 'aws-cdk-lib/aws-lambda-nodejs';
import { SqsEventSource } from 'aws-cdk-lib/aws-lambda-event-sources';
import * as apigateway from 'aws-cdk-lib/aws-apigateway';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as path from 'path';

export class SnsStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // ==========================================
    // 0. PARAMETERS
    // ==========================================
    // This allows the deployer to specify their verified SES sender email at deploy time.
    // E.g., npx cdk deploy --parameters SenderEmail=verified-sender@yourdomain.com
    const senderEmailParam = new cdk.CfnParameter(this, 'SenderEmail', {
      type: 'String',
      description: 'The verified Amazon SES sender email address (required to send emails in sandbox mode).',
      default: 'malaynandasana@gmail.com',
    });


    // ==========================================
    // 1. SNS TOPIC DEFINITION
    // ==========================================
    // The central event bus where OTP generation events are published.
    const otpTopic = new sns.Topic(this, 'OtpTopic', {
      topicName: 'otp-generation-topic',
      displayName: 'OTP Generation Notification Topic',
    });

    // ==========================================
    // 2. SQS QUEUES DEFINITION (QUEUES & DLQs)
    // ==========================================

    // --- EMAIL QUEUES ---
    // Dead Letter Queue for failed email notifications.
    const emailDlq = new sqs.Queue(this, 'EmailDlq', {
      queueName: 'email-otp-dlq',
      retentionPeriod: cdk.Duration.days(14),
    });

    // Main Email queue. The Email Worker Lambda will poll this queue.
    const emailQueue = new sqs.Queue(this, 'EmailQueue', {
      queueName: 'email-otp-queue',
      // Visibility timeout should be at least 3x the Lambda function timeout.
      // Since Email Worker Lambda has a 15s timeout, we set this to 45s.
      visibilityTimeout: cdk.Duration.seconds(45),
      deadLetterQueue: {
        queue: emailDlq,
        maxReceiveCount: 3, // Retry up to 3 times before sending to DLQ
      },
    });

    // --- SMS QUEUES ---
    // Dead Letter Queue for failed SMS notifications.
    const smsDlq = new sqs.Queue(this, 'SmsDlq', {
      queueName: 'sms-otp-dlq',
      retentionPeriod: cdk.Duration.days(14),
    });

    // Main SMS queue. The SMS Worker Lambda will poll this queue.
    const smsQueue = new sqs.Queue(this, 'SmsQueue', {
      queueName: 'sms-otp-queue',
      // Visibility timeout should be at least 3x the Lambda function timeout.
      visibilityTimeout: cdk.Duration.seconds(45),
      deadLetterQueue: {
        queue: smsDlq,
        maxReceiveCount: 3, // Retry up to 3 times before sending to DLQ
      },
    });

    // ==========================================
    // 3. SNS SUBSCRIPTIONS WITH FILTER POLICIES
    // ==========================================

    // Subscribe Email Queue to SNS Topic.
    // We filter based on the 'sendEmail' message attribute, routing only when it equals 'true'.
    // rawMessageDelivery: true sends the published JSON directly to the SQS queue, 
    // eliminating the SNS wrapper metadata format for simpler parsing.
    otpTopic.addSubscription(new subs.SqsSubscription(emailQueue, {
      rawMessageDelivery: true,
      filterPolicy: {
        sendEmail: sns.SubscriptionFilter.stringFilter({
          allowlist: ['true'],
        }),
      },
    }));

    // Subscribe SMS Queue to SNS Topic.
    // We filter based on the 'sendSMS' message attribute, routing only when it equals 'true'.
    otpTopic.addSubscription(new subs.SqsSubscription(smsQueue, {
      rawMessageDelivery: true,
      filterPolicy: {
        sendSMS: sns.SubscriptionFilter.stringFilter({
          allowlist: ['true'],
        }),
      },
    }));

    // ==========================================
    // 4. LAMBDA FUNCTIONS DEFINITION
    // ==========================================

    // Log Groups with 1-day retention for Lambdas
    const otpGeneratorLogGroup = new logs.LogGroup(this, 'OtpGeneratorLogGroup', {
      retention: logs.RetentionDays.ONE_DAY,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    const emailWorkerLogGroup = new logs.LogGroup(this, 'EmailWorkerLogGroup', {
      retention: logs.RetentionDays.ONE_DAY,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    const smsWorkerLogGroup = new logs.LogGroup(this, 'SmsWorkerLogGroup', {
      retention: logs.RetentionDays.ONE_DAY,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    // --- OTP GENERATOR LAMBDA ---
    // Handles API Gateway requests, generates the OTP, and publishes it to the SNS topic.
    const otpGeneratorLambda = new NodejsFunction(this, 'OtpGeneratorLambda', {
      entry: path.join(__dirname, '../lambdas/otp-generator/index.ts'),
      handler: 'handler',
      runtime: lambda.Runtime.NODEJS_20_X,
      timeout: cdk.Duration.seconds(10),
      logGroup: otpGeneratorLogGroup,
      environment: {
        SNS_TOPIC_ARN: otpTopic.topicArn,
      },
      bundling: {
        minify: true,
        sourceMap: true,
      },
    });

    // --- EMAIL WORKER LAMBDA ---
    // Polls Email SQS queue and sends emails via Amazon SES.
    const emailWorkerLambda = new NodejsFunction(this, 'EmailWorkerLambda', {
      entry: path.join(__dirname, '../lambdas/email-worker/index.ts'),
      handler: 'handler',
      runtime: lambda.Runtime.NODEJS_20_X,
      timeout: cdk.Duration.seconds(15),
      logGroup: emailWorkerLogGroup,
      environment: {
        SENDER_EMAIL: senderEmailParam.valueAsString,
      },
      bundling: {
        minify: true,
        sourceMap: true,
      },
    });

    // --- SMS WORKER LAMBDA ---
    // Polls SMS SQS queue and sends SMS texts via Amazon SNS SMS capability.
    const smsWorkerLambda = new NodejsFunction(this, 'SmsWorkerLambda', {
      entry: path.join(__dirname, '../lambdas/sms-worker/index.ts'),
      handler: 'handler',
      runtime: lambda.Runtime.NODEJS_20_X,
      timeout: cdk.Duration.seconds(15),
      logGroup: smsWorkerLogGroup,
      bundling: {
        minify: true,
        sourceMap: true,
      },
    });

    // ==========================================
    // 5. IAM PERMISSIONS & EVENT SOURCE MAPS
    // ==========================================

    // Grant OTP Generator Lambda permission to publish to the SNS Topic
    otpTopic.grantPublish(otpGeneratorLambda);

    // Bind SQS Queues as Lambda Event Sources
    emailWorkerLambda.addEventSource(new SqsEventSource(emailQueue, {
      batchSize: 5, // Process up to 5 messages at a time
    }));

    smsWorkerLambda.addEventSource(new SqsEventSource(smsQueue, {
      batchSize: 5, // Process up to 5 messages at a time
    }));

    // Grant Email Worker Lambda permission to send emails via SES.
    // Since AWS SES resources are referenced by verified email identities, 
    // it's common to scope permissions to "*" or specific verified identity ARNs.
    emailWorkerLambda.addToRolePolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: ['ses:SendEmail', 'ses:SendRawEmail'],
      resources: ['*'],
    }));

    // Grant SMS Worker Lambda permission to publish SMS messages via SNS.
    // SNS SMS publishing requires action `sns:Publish` on target resource `*` 
    // because the phone numbers are dynamic and do not correspond to specific AWS resource ARNs.
    smsWorkerLambda.addToRolePolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: ['sns:Publish'],
      resources: ['*'],
    }));

    // ==========================================
    // 6. API GATEWAY DEFINITION
    // ==========================================
    const api = new apigateway.RestApi(this, 'OtpApi', {
      restApiName: 'OTP Request Service',
      description: 'API Gateway that accepts OTP requests and triggers the fan-out flow.',
      defaultCorsPreflightOptions: {
        allowOrigins: apigateway.Cors.ALL_ORIGINS,
        allowMethods: apigateway.Cors.ALL_METHODS,
        allowHeaders: ['Content-Type'],
      },
    });

    // POST /otp
    const otpResource = api.root.addResource('otp');
    otpResource.addMethod('POST', new apigateway.LambdaIntegration(otpGeneratorLambda)); //tells API Gateway to trigger the generator Lambda when a client calls POST /otp.

    // ==========================================
    // 7. STACK OUTPUTS
    // ==========================================
    new cdk.CfnOutput(this, 'ApiGatewayUrl', {
      value: `${api.url}otp`,
      description: 'The API endpoint to request OTPs via POST requests.',
    });

    new cdk.CfnOutput(this, 'SnsTopicArn', {
      value: otpTopic.topicArn,
      description: 'The ARN of the central SNS topic.',
    });

    new cdk.CfnOutput(this, 'EmailQueueUrl', {
      value: emailQueue.queueUrl,
      description: 'The SQS queue URL for processing email notifications.',
    });

    new cdk.CfnOutput(this, 'SmsQueueUrl', {
      value: smsQueue.queueUrl,
      description: 'The SQS queue URL for processing SMS notifications.',
    });

    new cdk.CfnOutput(this, 'EmailDlqUrl', {
      value: emailDlq.queueUrl,
      description: 'The SQS DLQ URL for failed email notifications.',
    });

    new cdk.CfnOutput(this, 'SmsDlqUrl', {
      value: smsDlq.queueUrl,
      description: 'The SQS DLQ URL for failed SMS notifications.',
    });
  }
}
