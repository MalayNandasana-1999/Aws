import { APIGatewayProxyEvent, APIGatewayProxyResult } from 'aws-lambda';
import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';
import { SQSClient, SendMessageCommand } from '@aws-sdk/client-sqs';
import * as multipartParser from 'lambda-multipart-parser';
import * as path from 'path';

const s3 = new S3Client({});
const sqs = new SQSClient({});

const SOURCE_BUCKET = process.env.SOURCE_BUCKET_NAME || '';
const QUEUE_URL = process.env.QUEUE_URL || '';

export const handler = async (event: APIGatewayProxyEvent): Promise<APIGatewayProxyResult> => {
  console.log('Received upload request event:', JSON.stringify(event, null, 2));

  try {
    const parsedRequest = await multipartParser.parse(event);
    const file = parsedRequest.files && parsedRequest.files[0];

    if (!file) {
      return {
        statusCode: 400,
        headers: {
          'Content-Type': 'application/json',
          'Access-Control-Allow-Origin': '*',
        },
        body: JSON.stringify({ error: 'No file found. Please upload an image using a form-data field.' }),
      };
    }

    const { filename, content, contentType } = file;

    // Clean and generate a unique key in the source bucket
    const cleanFileName = path.basename(filename);
    const uniqueId = `${Date.now()}-${Math.random().toString(36).substring(2, 8)}`;
    const sourceKey = `original/${uniqueId}-${cleanFileName}`;

    console.log(`Uploading ${cleanFileName} to S3 bucket ${SOURCE_BUCKET} at key ${sourceKey}...`);

    // Upload the raw image file to the Source S3 Bucket
    await s3.send(
      new PutObjectCommand({
        Bucket: SOURCE_BUCKET,
        Key: sourceKey,
        Body: content,
        ContentType: contentType || 'application/octet-stream',
      })
    );

    console.log('Image uploaded to S3 successfully.');

    // Construct the SQS message payload
    const sqsPayload = {
      sourceBucket: SOURCE_BUCKET,
      sourceKey: sourceKey,
      fileName: cleanFileName,
    };

    console.log(`Sending metadata message to SQS queue:`, JSON.stringify(sqsPayload));

    // Send message to SQS
    const sqsResponse = await sqs.send(
      new SendMessageCommand({
        QueueUrl: QUEUE_URL,
        MessageBody: JSON.stringify(sqsPayload),
      })
    );

    console.log(`SQS message sent successfully. Message ID: ${sqsResponse.MessageId}`);

    // Return response indicating the upload is accepted
    return {
      statusCode: 202,
      headers: {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Allow-Methods': 'OPTIONS,POST',
      },
      body: JSON.stringify({
        message: 'Image upload accepted and processing started.',
        sourceBucket: SOURCE_BUCKET,
        sourceKey: sourceKey,
        messageId: sqsResponse.MessageId,
      }),
    };

  } catch (error: any) {
    console.error('Error handling upload request:', error);
    return {
      statusCode: 500,
      headers: {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*',
      },
      body: JSON.stringify({
        error: 'Internal Server Error',
        details: error.message || error,
      }),
    };
  }
};
