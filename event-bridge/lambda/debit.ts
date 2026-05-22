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
  console.log('Received debit event:', JSON.stringify(event));
  const { transactionId, userId, amount, username, transactionType, timestamp } = event.detail;

  try {
    // 1. Perform conditional update to decrement balance
    console.log(`Deducting $${amount} from balance in ${BALANCE_TABLE} for user ${userId}`);
    
    try {
      await docClient.send(
        new UpdateCommand({
          TableName: BALANCE_TABLE,
          Key: { userId },
          UpdateExpression: 'SET balance = balance - :amount',
          ConditionExpression: 'attribute_exists(balance) AND balance >= :amount',
          ExpressionAttributeValues: {
            ':amount': amount,
          },
        })
      );
      console.log(`Successfully debited balance for user ${userId}`);

      // 2. Log COMPLETED transaction
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

      // 3. Send email for successful debit
      await sendEmail(
        `Banking Alert: Debit Successful - TX ID: ${transactionId}`,
        `Hello ${username || 'Valued Customer'},\n\nYour account has been debited with $${amount}.\nTransaction ID: ${transactionId}\nTime: ${timestamp}\n\nThank you for banking with us!`
      );

    } catch (dbError: any) {
      if (dbError.name === 'ConditionalCheckFailedException') {
        console.warn(`Debit failed for user ${userId}: Insufficient balance or user does not exist.`);

        // Log FAILED_INSUFFICIENT_FUNDS transaction
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
              ':status': 'FAILED_INSUFFICIENT_FUNDS',
              ':ts': timestamp || new Date().toISOString(),
            },
          })
        );

        // Send email for failed debit
        await sendEmail(
          `Banking Alert: Debit Failed (Insufficient Funds) - TX ID: ${transactionId}`,
          `Hello ${username || 'Valued Customer'},\n\nWe were unable to process your debit of $${amount} due to insufficient funds or because your account has not been initialized with a credit.\nTransaction ID: ${transactionId}\nTime: ${timestamp}`
        );
      } else {
        throw dbError; // Rethrow other DynamoDB errors
      }
    }
  } catch (error) {
    console.error('Error in Debit Lambda:', error);
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
      console.error('Failed to log failed transaction:', dbErr);
    }
    throw error;
  }
};

async function sendEmail(subject: string, bodyText: string) {
  if (!SENDER_EMAIL) {
    console.log('No SENDER_EMAIL configured, skipping SES email.');
    return;
  }
  try {
    console.log(`Attempting to send email to ${SENDER_EMAIL}`);
    const emailParams = {
      Source: SENDER_EMAIL,
      Destination: {
        ToAddresses: [SENDER_EMAIL],
      },
      Message: {
        Subject: { Data: subject },
        Body: { Text: { Data: bodyText } },
      },
    };
    await ses.send(new SendEmailCommand(emailParams));
    console.log('SES Email sent successfully.');
  } catch (emailError: any) {
    console.warn('SES Email sending skipped or failed (likely due to sandbox limitations):', emailError.message);
  }
}
