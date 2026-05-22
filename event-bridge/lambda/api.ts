import { APIGatewayProxyHandler } from 'aws-lambda';
import { EventBridgeClient, PutEventsCommand } from '@aws-sdk/client-eventbridge';

import * as crypto from 'crypto';

const eventBridge = new EventBridgeClient({});
const EVENT_BUS_NAME = process.env.EVENT_BUS_NAME || '';

export const handler: APIGatewayProxyHandler = async (event) => {
  console.log('Received transaction event:', JSON.stringify(event));

  try {
    if (!event.body) {
      return {
        statusCode: 400,
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ error: 'Missing request body' }),
      };
    }

    const payload = JSON.parse(event.body);
    const { userId, transactionType, amount, username } = payload;

    // Validate request parameters (transactionId is now auto-generated)
    if (!userId || !transactionType || typeof amount !== 'number') {
      return {
        statusCode: 400,
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          error: 'Missing or invalid parameters. Requires userId, transactionType (CREDIT/DEBIT), and amount (number).',
        }),
      };
    }

    if (amount <= 0) {
      return {
        statusCode: 400,
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          error: 'Invalid amount. Amount must be strictly greater than 0.',
        }),
      };
    }

    const transactionId = `TX-${crypto.randomUUID()}`;

    if (transactionType !== 'CREDIT' && transactionType !== 'DEBIT') {
      return {
        statusCode: 400,
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          error: 'Invalid transactionType. Must be CREDIT or DEBIT.',
        }),
      };
    }

    // Publish custom event to EventBridge Custom Bus
    const putEventsParams = {
      Entries: [
        {
          Source: 'custom.banking.transaction',
          DetailType: 'TransactionRequest',
          Detail: JSON.stringify({
            transactionId,
            userId,
            transactionType,
            amount,
            username: username || 'Anonymous User',
            timestamp: new Date().toISOString(),
          }),
          EventBusName: EVENT_BUS_NAME,
        },
      ],
    };

    console.log('Publishing event to EventBridge:', JSON.stringify(putEventsParams));
    const result = await eventBridge.send(new PutEventsCommand(putEventsParams));
    console.log('EventBridge result:', JSON.stringify(result));

    return {
      statusCode: 202,
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        message: 'Transaction received and processing initiated.',
        transactionId,
        eventEntryId: result.Entries?.[0]?.EventId,
      }),
    };
  } catch (error: any) {
    console.error('Error processing transaction request:', error);
    return {
      statusCode: 500,
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        error: 'Internal Server Error',
        details: error.message,
      }),
    };
  }
};
