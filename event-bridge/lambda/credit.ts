import { EventBridgeEvent } from 'aws-lambda';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, UpdateCommand, PutCommand } from '@aws-sdk/lib-dynamodb';
import { SESClient, SendEmailCommand } from '@aws-sdk/client-ses';
import { EventBridgeClient, PutEventsCommand } from '@aws-sdk/client-eventbridge';

interface TransactionDetail {
  transactionId: string;
  userId: string;
  transactionType: 'CREDIT' | 'DEBIT';
  amount: number;
  username: string;
  timestamp: string;
}

const dbClient = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(dbClient);
const ses = new SESClient({});
const eventBridge = new EventBridgeClient({});

const BALANCE_TABLE = process.env.BALANCE_TABLE || '';
const TRANSACTIONS_TABLE = process.env.TRANSACTIONS_TABLE || '';
const SENDER_EMAIL = process.env.VERIFIED_EMAIL || '';
const EVENT_BUS_NAME = process.env.EVENT_BUS_NAME || '';

export const handler = async (event: EventBridgeEvent<'TransactionRequest', TransactionDetail>): Promise<void> => {
  console.log('Received credit event:', JSON.stringify(event));
  const { transactionId, userId, amount, username, transactionType, timestamp } = event.detail;

  try {
    // 1. Update Balance Table (Upsert user balance)
    console.log(`Updating balance in ${BALANCE_TABLE} for user ${userId}`);
    await docClient.send(
      new UpdateCommand({
        TableName: BALANCE_TABLE,
        Key: { userId },
        UpdateExpression: 'SET balance = if_not_exists(balance, :zero) + :amount, username = :username',
        ExpressionAttributeValues: {
          ':zero': 0,
          ':amount': amount,
          ':username': username,
        },
      })
    );
    console.log(`Successfully credited balance for user ${userId}`);

    // 2. Write Transaction Log as COMPLETED
    console.log(`Writing completed transaction to ${TRANSACTIONS_TABLE}`);
    await docClient.send(
      new UpdateCommand({
        TableName: TRANSACTIONS_TABLE,
        Key: { transactionId },
        UpdateExpression: 'SET userId = :userId, transactionType = :type, amount = :amount, #status = :status, #ts = :ts',
        ExpressionAttributeNames: {
          '#status': 'status',
          '#ts': 'timestamp',
        },
        ExpressionAttributeValues: {
          ':userId': userId,
          ':type': transactionType,
          ':amount': amount,
          ':status': 'COMPLETED',
          ':ts': timestamp || new Date().toISOString(),
        },
      })
    );
    console.log(`Successfully logged transaction ${transactionId}`);

    // 2.5 Publish TransactionProcessed event to EventBridge
    if (EVENT_BUS_NAME) {
      try {
        console.log(`Publishing TransactionProcessed event for ${transactionId}`);
        await eventBridge.send(
          new PutEventsCommand({
            Entries: [
              {
                Source: 'custom.banking.transaction',
                DetailType: 'TransactionProcessed',
                Detail: JSON.stringify({
                  transactionId,
                  userId,
                  transactionType,
                  amount,
                  username,
                  status: 'COMPLETED',
                  timestamp: timestamp || new Date().toISOString(),
                }),
                EventBusName: EVENT_BUS_NAME,
              },
            ],
          })
        );
        console.log('Successfully published TransactionProcessed event');
      } catch (ebError: any) {
        console.error('Failed to publish TransactionProcessed event:', ebError.message);
      }
    }

    // 3. Send SES email (gracefully catch errors if SES Sandbox is not fully set up)
    if (SENDER_EMAIL) {
      try {
        console.log(`Attempting to send email to ${SENDER_EMAIL}`);
        const emailParams = {
          Source: SENDER_EMAIL,
          Destination: {
            ToAddresses: [SENDER_EMAIL],
          },
          Message: {
            Subject: {
              Data: `Banking Alert: Credit Successful - TX ID: ${transactionId}`,
            },
            Body: {
              Text: {
                Data: `Hello ${username},\n\nYour account has been credited with $${amount}.\nTransaction ID: ${transactionId}\nTime: ${timestamp}\n\nThank you for banking with us!`,
              },
            },
          },
        };
        await ses.send(new SendEmailCommand(emailParams));
        console.log('SES Email sent successfully.');
      } catch (emailError: any) {
        console.warn('SES Email sending skipped or failed (likely due to sandbox limitations):', emailError.message);
      }
    } else {
      console.log('No SENDER_EMAIL configured, skipping SES email.');
    }
  } catch (error) {
    console.error('Error in Credit Lambda:', error);
    // Write a FAILED transaction record if possible
    try {
      await docClient.send(
        new UpdateCommand({
          TableName: TRANSACTIONS_TABLE,
          Key: { transactionId },
          UpdateExpression: 'SET userId = :userId, transactionType = :type, amount = :amount, #status = :status, #err = :err, #ts = :ts',
          ExpressionAttributeNames: {
            '#status': 'status',
            '#ts': 'timestamp',
            '#err': 'error',
          },
          ExpressionAttributeValues: {
            ':userId': userId,
            ':type': transactionType,
            ':amount': amount,
            ':status': 'FAILED',
            ':err': String(error),
            ':ts': timestamp || new Date().toISOString(),
          },
        })
      );
    } catch (dbErr) {
      console.error('Failed to log failed transaction to DynamoDB:', dbErr);
    }
    throw error;
  }
};
