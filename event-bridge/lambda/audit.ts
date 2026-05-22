import { SQSEvent } from 'aws-lambda';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, UpdateCommand } from '@aws-sdk/lib-dynamodb';

const dbClient = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(dbClient);

const TRANSACTIONS_TABLE = process.env.TRANSACTIONS_TABLE || '';

interface TransactionDetail {
  transactionId: string;
  userId: string;
  transactionType: 'CREDIT' | 'DEBIT';
  amount: number;
  username: string;
  timestamp: string;
}

export const handler = async (event: SQSEvent): Promise<void> => {
  console.log('Received SQS audit event:', JSON.stringify(event));

  for (const record of event.Records) {
    try {
      const eventBridgeEvent = JSON.parse(record.body);
      const detail: TransactionDetail = eventBridgeEvent.detail;

      console.log('Auditing transaction:', JSON.stringify(detail));
      const { transactionId, userId, transactionType, amount, timestamp } = detail;

      // Upsert transaction in the transactions table with an audited flag
      // This ensures we have a record of every transaction routed through the event bus
      console.log(`Writing audit log to ${TRANSACTIONS_TABLE} for TX: ${transactionId}`);
      await docClient.send(
        new UpdateCommand({
          TableName: TRANSACTIONS_TABLE,
          Key: { transactionId },
          UpdateExpression: 'SET userId = :userId, transactionType = :type, amount = :amount, audited = :audited, auditTimestamp = :auditTs, eventTimestamp = :eventTs',
          ExpressionAttributeValues: {
            ':userId': userId,
            ':type': transactionType,
            ':amount': amount,
            ':audited': true,
            ':auditTs': new Date().toISOString(),
            ':eventTs': timestamp || new Date().toISOString(),
          },
        })
      );
      console.log(`Successfully audited transaction ${transactionId}`);
    } catch (recordError) {
      console.error('Error auditing SQS record:', recordError);
    }
  }
};
