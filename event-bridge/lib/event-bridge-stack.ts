import * as path from 'path';
import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as sqs from 'aws-cdk-lib/aws-sqs';
import * as events from 'aws-cdk-lib/aws-events';
import * as targets from 'aws-cdk-lib/aws-events-targets';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as lambdaNodejs from 'aws-cdk-lib/aws-lambda-nodejs';
import * as apigateway from 'aws-cdk-lib/aws-apigateway';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as logs from 'aws-cdk-lib/aws-logs';
import { SqsEventSource } from 'aws-cdk-lib/aws-lambda-event-sources';

export class EventBridgeStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // Retrieve verified contact info from environment or context
    const verifiedEmail = process.env.VERIFIED_EMAIL || this.node.tryGetContext('verifiedEmail') || '';
    const verifiedPhone = process.env.VERIFIED_PHONE || this.node.tryGetContext('verifiedPhone') || '';

    // ==========================================
    // 1. DynamoDB Tables
    // ==========================================

    // Balance Table (PK: userId)
    const balanceTable = new dynamodb.Table(this, 'BalanceTable', {
      partitionKey: { name: 'userId', type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      removalPolicy: cdk.RemovalPolicy.DESTROY, // For demo/sandbox purposes
    });

    // Transactions Table (PK: transactionId)
    const transactionsTable = new dynamodb.Table(this, 'TransactionsTable', {
      partitionKey: { name: 'transactionId', type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      removalPolicy: cdk.RemovalPolicy.DESTROY, // For demo/sandbox purposes
    });

    // ==========================================
    // 2. SQS Queues & Dead Letter Queues (DLQs)
    // ==========================================

    // High Amount SQS FIFO DLQ
    const highAmountDlq = new sqs.Queue(this, 'HighAmountDlq.fifo', {
      queueName: 'HighAmountDlq.fifo',
      fifo: true,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    // High Amount SQS FIFO Queue (deduplication enabled)
    const highAmountQueue = new sqs.Queue(this, 'HighAmountQueue.fifo', {
      queueName: 'HighAmountQueue.fifo',
      fifo: true,
      contentBasedDeduplication: true,
      visibilityTimeout: cdk.Duration.seconds(30),
      deadLetterQueue: {
        queue: highAmountDlq,
        maxReceiveCount: 3,
      },
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    // Audit SQS DLQ (Standard)
    const auditDlq = new sqs.Queue(this, 'AuditDlq', {
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    // Audit Queue (Standard Queue)
    const auditQueue = new sqs.Queue(this, 'AuditQueue', {
      visibilityTimeout: cdk.Duration.seconds(30),
      deadLetterQueue: {
        queue: auditDlq,
        maxReceiveCount: 3,
      },
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    // ==========================================
    // 3. EventBridge Custom Event Bus
    // ==========================================
    const bankingEventBus = new events.EventBus(this, 'BankingEventBus', {
      eventBusName: 'BankingEventBus',
    });

    // ==========================================
    // 4. Lambda Functions
    // ==========================================

    // Helper to create CloudWatch Log Groups that delete automatically on stack destruction
    const createLogGroup = (id: string, functionName: string) => {
      return new logs.LogGroup(this, id, {
        logGroupName: `/aws/lambda/${this.stackName}-${functionName}`,
        retention: logs.RetentionDays.ONE_DAY,
        removalPolicy: cdk.RemovalPolicy.DESTROY,
      });
    };

    // Common bundler options for TypeScript lambdas using esbuild
    const lambdaProps = {
      runtime: lambda.Runtime.NODEJS_18_X,
      bundling: {
        minify: true,
        sourceMap: true,
        externalModules: ['@aws-sdk/*'], // Use the pre-installed SDK in Node 18 runtime
      },
    };

    // 4a. API Lambda (Ingestion)
    const apiLambda = new lambdaNodejs.NodejsFunction(this, 'ApiLambda', {
      ...lambdaProps,
      logGroup: createLogGroup('ApiLambdaLogGroup', 'ApiLambda'),
      entry: path.join(__dirname, '../lambda/api.ts'),
      environment: {
        EVENT_BUS_NAME: bankingEventBus.eventBusName,
      },
    });
    // Grant API Lambda permission to put events on the custom bus
    bankingEventBus.grantPutEventsTo(apiLambda);

    // 4b. Credit Lambda
    const creditLambda = new lambdaNodejs.NodejsFunction(this, 'CreditLambda', {
      ...lambdaProps,
      logGroup: createLogGroup('CreditLambdaLogGroup', 'CreditLambda'),
      entry: path.join(__dirname, '../lambda/credit.ts'),
      environment: {
        BALANCE_TABLE: balanceTable.tableName,
        TRANSACTIONS_TABLE: transactionsTable.tableName,
        VERIFIED_EMAIL: verifiedEmail,
        EVENT_BUS_NAME: bankingEventBus.eventBusName,
      },
    });
    balanceTable.grantReadWriteData(creditLambda);
    transactionsTable.grantReadWriteData(creditLambda);
    creditLambda.addToRolePolicy(new iam.PolicyStatement({
      actions: ['ses:SendEmail', 'ses:SendRawEmail'],
      resources: ['*'],
    }));
    bankingEventBus.grantPutEventsTo(creditLambda);

    // 4c. Debit Lambda
    const debitLambda = new lambdaNodejs.NodejsFunction(this, 'DebitLambda', {
      ...lambdaProps,
      logGroup: createLogGroup('DebitLambdaLogGroup', 'DebitLambda'),
      entry: path.join(__dirname, '../lambda/debit.ts'),
      environment: {
        BALANCE_TABLE: balanceTable.tableName,
        TRANSACTIONS_TABLE: transactionsTable.tableName,
        VERIFIED_EMAIL: verifiedEmail,
        EVENT_BUS_NAME: bankingEventBus.eventBusName,
      },
    });
    balanceTable.grantReadWriteData(debitLambda);
    transactionsTable.grantReadWriteData(debitLambda);
    debitLambda.addToRolePolicy(new iam.PolicyStatement({
      actions: ['ses:SendEmail', 'ses:SendRawEmail'],
      resources: ['*'],
    }));
    bankingEventBus.grantPutEventsTo(debitLambda);

    // 4d. SMS Alert Lambda
    const smsAlertLambda = new lambdaNodejs.NodejsFunction(this, 'SmsAlertLambda', {
      ...lambdaProps,
      logGroup: createLogGroup('SmsAlertLambdaLogGroup', 'SmsAlertLambda'),
      entry: path.join(__dirname, '../lambda/sms-alert.ts'),
      environment: {
        VERIFIED_PHONE: verifiedPhone,
      },
    });
    // Trigger SMS Alert lambda from SQS FIFO Queue
    smsAlertLambda.addEventSource(new SqsEventSource(highAmountQueue, {
      batchSize: 5,
    }));
    smsAlertLambda.addToRolePolicy(new iam.PolicyStatement({
      actions: ['sns:Publish'],
      resources: ['*'],
    }));

    // 4e. Audit Lambda
    const auditLambda = new lambdaNodejs.NodejsFunction(this, 'AuditLambda', {
      ...lambdaProps,
      logGroup: createLogGroup('AuditLambdaLogGroup', 'AuditLambda'),
      entry: path.join(__dirname, '../lambda/audit.ts'),
      environment: {
        TRANSACTIONS_TABLE: transactionsTable.tableName,
      },
    });
    // Trigger Audit Lambda from SQS Audit Queue
    auditLambda.addEventSource(new SqsEventSource(auditQueue, {
      batchSize: 10,
    }));
    transactionsTable.grantReadWriteData(auditLambda);

    // ==========================================
    // 5. API Gateway REST API
    // ==========================================
    const api = new apigateway.RestApi(this, 'BankingTransactionApi', {
      restApiName: 'Banking Transaction Service',
      description: 'Ingests transaction requests and forwards them to EventBridge',
      defaultCorsPreflightOptions: {
        allowOrigins: apigateway.Cors.ALL_ORIGINS,
        allowMethods: apigateway.Cors.ALL_METHODS,
      },
    });

    const transactionsResource = api.root.addResource('transactions');
    transactionsResource.addMethod('POST', new apigateway.LambdaIntegration(apiLambda));

    // ==========================================
    // 6. EventBridge Rules
    // ==========================================

    // Credit Transaction Rule -> Credit Lambda
    new events.Rule(this, 'CreditTransactionRule', {
      eventBus: bankingEventBus,
      eventPattern: {
        source: ['custom.banking.transaction'],
        detailType: ['TransactionRequest'],
        detail: {
          transactionType: ['CREDIT'],
        },
      },
      targets: [new targets.LambdaFunction(creditLambda)],
    });

    // Debit Transaction Rule -> Debit Lambda
    new events.Rule(this, 'DebitTransactionRule', {
      eventBus: bankingEventBus,
      eventPattern: {
        source: ['custom.banking.transaction'],
        detailType: ['TransactionRequest'],
        detail: {
          transactionType: ['DEBIT'],
        },
      },
      targets: [new targets.LambdaFunction(debitLambda)],
    });

    // High Amount Transaction Rule -> High Amount SQS FIFO Queue (only if COMPLETED)
    new events.Rule(this, 'HighAmountRule', {
      eventBus: bankingEventBus,
      eventPattern: {
        source: ['custom.banking.transaction'],
        detailType: ['TransactionProcessed'],
        detail: {
          status: ['COMPLETED'],
          amount: [{ numeric: ['>', 50000] }],
        },
      },
      targets: [new targets.SqsQueue(highAmountQueue, {
        messageGroupId: 'HighAmountGroup',
      })],
    });

    // Audit Rule (All transaction requests) -> Audit Queue
    new events.Rule(this, 'AuditRule', {
      eventBus: bankingEventBus,
      eventPattern: {
        source: ['custom.banking.transaction'],
        detailType: ['TransactionRequest'],
      },
      targets: [new targets.SqsQueue(auditQueue)],
    });

    // ==========================================
    // 7. Stack Outputs
    // ==========================================
    new cdk.CfnOutput(this, 'ApiEndpoint', {
      value: `${api.url}transactions`,
      description: 'The endpoint for transactions API Gateway POST requests',
    });
    new cdk.CfnOutput(this, 'BalanceTableName', {
      value: balanceTable.tableName,
    });
    new cdk.CfnOutput(this, 'TransactionsTableName', {
      value: transactionsTable.tableName,
    });
  }
}
