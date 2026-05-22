import { SQSEvent, SQSRecord } from 'aws-lambda';
import { SNSClient, PublishCommand } from '@aws-sdk/client-sns';

const snsClient = new SNSClient({});

export const handler = async (event: SQSEvent): Promise<void> => {
  console.log('Received SQS SMS Worker event:', JSON.stringify(event));

  for (const record of event.Records) {
    try {
      await processRecord(record);
    } catch (error) {
      console.error(`Failed to process SMS record: ${record.messageId}`, error);
      // Re-throw to make the Lambda function fail so SQS retries the message
      throw error;
    }
  }
};

async function processRecord(record: SQSRecord): Promise<void> {
  console.log(`Processing SQS record ${record.messageId}`);

  // Parse the body. Since we set rawMessageDelivery to true on the subscription,
  // the SQS message body is the direct JSON payload sent to the SNS topic.
  const payload = JSON.parse(record.body);
  const { otp, phoneNumber } = payload;

  if (!phoneNumber) {
    console.warn(`No phone number found in message payload: ${record.body}. Skipping.`);
    return;
  }

  if (!otp) {
    throw new Error('No OTP found in message payload');
  }

  console.log(`Sending OTP SMS to: ${phoneNumber}`);

  const smsParams = {
    PhoneNumber: phoneNumber,
    Message: `Your verification code (OTP) is: ${otp}. Valid for 10 minutes.`,
    MessageAttributes: {
      'AWS.SNS.SMS.SMSType': {
        DataType: 'String',
        StringValue: 'Transactional', // Transactional is prioritized for critical messages like OTPs
      },
    },
  };

  try {
    const snsResponse = await snsClient.send(new PublishCommand(smsParams));
    console.log(`SMS sent successfully to ${phoneNumber}. Message ID: ${snsResponse.MessageId}`);
  } catch (error: any) {
    console.error(`Error occurred while sending SMS via SNS to ${phoneNumber}:`, error);
    throw error;
  }
}
