import { SQSEvent, SQSRecord } from 'aws-lambda';
import { SESClient, SendEmailCommand } from '@aws-sdk/client-ses';

const sesClient = new SESClient({});
const SENDER_EMAIL = process.env.SENDER_EMAIL;

export const handler = async (event: SQSEvent): Promise<void> => {
  console.log('Received SQS Email Worker event:', JSON.stringify(event));

  if (!SENDER_EMAIL) {
    throw new Error('SENDER_EMAIL environment variable is not defined');
  }

  for (const record of event.Records) {
    try {
      await processRecord(record);
    } catch (error) {
      console.error(`Failed to process email record: ${record.messageId}`, error);
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
  const { otp, email } = payload;

  if (!email) {
    console.warn(`No email address found in message payload: ${record.body}. Skipping.`);
    return;
  }

  if (!otp) {
    throw new Error('No OTP found in message payload');
  }

  console.log(`Sending OTP email to: ${email}`);

  const emailParams = {
    Source: SENDER_EMAIL,
    Destination: {
      ToAddresses: [email],
    },
    Message: {
      Subject: {
        Data: 'Your Verification Code (OTP)',
        Charset: 'UTF-8',
      },
      Body: {
        Html: {
          Data: `
            <div style="font-family: Arial, sans-serif; padding: 20px; border: 1px solid #eee; border-radius: 5px; max-width: 500px;">
              <h2 style="color: #333;">Security Verification</h2>
              <p>You requested a verification code. Please use the following One-Time Password (OTP) to complete your transaction:</p>
              <div style="background-color: #f7f9fa; padding: 15px; border-radius: 4px; text-align: center; margin: 20px 0;">
                <span style="font-size: 32px; font-weight: bold; letter-spacing: 5px; color: #007bff;">${otp}</span>
              </div>
              <p style="color: #666; font-size: 12px;">This code is valid for 10 minutes. If you did not request this, please ignore this email.</p>
            </div>
          `,
          Charset: 'UTF-8',
        },
        Text: {
          Data: `Your verification code (OTP) is: ${otp}. This code is valid for 10 minutes.`,
          Charset: 'UTF-8',
        },
      },
    },
  };

  try {
    const sesResponse = await sesClient.send(new SendEmailCommand(emailParams));
    console.log(`Email sent successfully to ${email}. Message ID: ${sesResponse.MessageId}`);
  } catch (error: any) {
    console.error(`Error occurred while sending email via SES to ${email}:`, error);
    throw error;
  }
}
