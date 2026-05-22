import { SQSEvent } from 'aws-lambda';
import { SNSClient, PublishCommand } from '@aws-sdk/client-sns';

const sns = new SNSClient({});
const VERIFIED_PHONE = process.env.VERIFIED_PHONE || '';

interface TransactionDetail {
  transactionId: string;
  userId: string;
  transactionType: 'CREDIT' | 'DEBIT';
  amount: number;
  username: string;
  timestamp: string;
}

export const handler = async (event: SQSEvent): Promise<void> => {
  console.log('Received SQS high amount alert event:', JSON.stringify(event));

  for (const record of event.Records) {
    try {
      // SQS body contains the EventBridge event wrapper
      const eventBridgeEvent = JSON.parse(record.body);
      const detail: TransactionDetail = eventBridgeEvent.detail;

      console.log('Processing high amount transaction:', JSON.stringify(detail));
      
      const { transactionId, userId, amount, transactionType } = detail;
      const message = `URGENT SECURITY ALERT: A high amount transaction of $${amount} (${transactionType}) was detected on account ${userId}. Transaction ID: ${transactionId}.`;

      if (VERIFIED_PHONE) {
        try {
          console.log(`Sending SMS to ${VERIFIED_PHONE}`);
          await sns.send(
            new PublishCommand({
              Message: message,
              PhoneNumber: VERIFIED_PHONE,
            })
          );
          console.log(`SMS Alert sent successfully to ${VERIFIED_PHONE}`);
        } catch (snsError: any) {
          console.warn('SNS SMS sending failed/skipped (likely sandbox issue):', snsError.message);
        }
      } else {
        console.log('No VERIFIED_PHONE configured, SMS alert logged only:', message);
      }
    } catch (recordError) {
      console.error('Error processing SQS record:', recordError);
      // We don't want to throw here to avoid poisoning the whole batch if it's a parsing issue,
      // but in production we might configure DLQ or handle partial batch failures.
    }
  }
};
