import { APIGatewayProxyEvent, APIGatewayProxyResult } from 'aws-lambda';
import { SNSClient, PublishCommand } from '@aws-sdk/client-sns';

const snsClient = new SNSClient({});
const SNS_TOPIC_ARN = process.env.SNS_TOPIC_ARN;

export const handler = async (event: APIGatewayProxyEvent): Promise<APIGatewayProxyResult> => {
  console.log('Received OTP request event:', JSON.stringify(event));

  try {
    // 1. Parse body
    if (!event.body) {
      return {
        statusCode: 400,
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ error: 'Missing request body' }),
      };
    }

    const { email, phoneNumber } = JSON.parse(event.body);

    // 2. Validate input
    if (!email && !phoneNumber) {
      return {
        statusCode: 400,
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          error: 'At least one of "email" or "phoneNumber" must be provided',
        }),
      };
    }

    // Basic email validation if email is provided
    if (email && !email.includes('@')) {
      return {
        statusCode: 400,
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ error: 'Invalid email address' }),
      };
    }

    // Basic phone number validation if phone number is provided (should be E.164 format starting with +)
    if (phoneNumber && !/^\+[1-9]\d{1,14}$/.test(phoneNumber)) {
      return {
        statusCode: 400,
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          error: 'Invalid phone number format. Must be E.164 format (e.g. +1234567890)',
        }),
      };
    }

    // 3. Generate 6-digit OTP
    const otp = Math.floor(100000 + Math.random() * 900000).toString();
    console.log(`[DEMO LOG] Generated OTP: ${otp} for Email: ${email || 'N/A'}, SMS: ${phoneNumber || 'N/A'}`);

    if (!SNS_TOPIC_ARN) {
      throw new Error('SNS_TOPIC_ARN environment variable is not defined');
    }

    // 4. Publish OTP event to SNS Topic
    // We add message attributes so that SQS queue subscription filter policies can determine 
    // whether to route this message to the Email queue, SMS queue, or both.
    const messageBody = {
      otp,
      email: email || null,
      phoneNumber: phoneNumber || null,
      timestamp: new Date().toISOString(),
    };

    const snsParams = {
      TopicArn: SNS_TOPIC_ARN,
      Message: JSON.stringify(messageBody),
      MessageAttributes: {
        sendEmail: {
          DataType: 'String',
          StringValue: email ? 'true' : 'false',
        },
        sendSMS: {
          DataType: 'String',
          StringValue: phoneNumber ? 'true' : 'false',
        },
      },
    };

    console.log('Publishing message to SNS Topic with params:', JSON.stringify(snsParams));
    const publishResponse = await snsClient.send(new PublishCommand(snsParams));
    console.log('SNS Publish response:', JSON.stringify(publishResponse));

    return {
      statusCode: 200,
      headers: {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*',
      },
      body: JSON.stringify({
        message: 'OTP request received and is being processed.',
        messageId: publishResponse.MessageId,
        channels: {
          email: email ? 'Initiated' : 'Skipped',
          sms: phoneNumber ? 'Initiated' : 'Skipped',
        },
      }),
    };

  } catch (error: any) {
    console.error('Error generating or publishing OTP:', error);
    return {
      statusCode: 500,
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        error: 'Internal Server Error',
        details: error.message || error,
      }),
    };
  }
};
