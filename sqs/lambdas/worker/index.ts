import { SQSEvent, SQSHandler } from 'aws-lambda';
import { S3Client, GetObjectCommand, PutObjectCommand } from '@aws-sdk/client-s3';
import Jimp from 'jimp';
import * as path from 'path';

const s3 = new S3Client({});

const PROCESSED_BUCKET = process.env.PROCESSED_BUCKET_NAME || '';

interface SqsMessagePayload {
  sourceBucket: string;
  sourceKey: string;
  fileName: string;
}

export const handler: SQSHandler = async (event: SQSEvent): Promise<void> => {
  console.log(`Processing SQS Event:`, JSON.stringify(event, null, 2));

  // Loop through all SQS messages in the batch.
  // Note: By default, Lambda processes messages in batches (default batch size is 10).
  for (const record of event.Records) {
    const messageId = record.messageId;
    console.log(`Starting processing for SQS message ID: ${messageId}`);

    try {
      // Parse SQS Message Body
      const payload: SqsMessagePayload = JSON.parse(record.body);
      const { sourceBucket, sourceKey, fileName } = payload;

      if (!sourceBucket || !sourceKey || !fileName) {
        throw new Error(`Invalid message payload structure for record ${messageId}`);
      }

      console.log(`Retrieving original image from S3 bucket: ${sourceBucket}, key: ${sourceKey}`);

      // 1. Download original image from the Source S3 Bucket
      const s3Response = await s3.send(
        new GetObjectCommand({
          Bucket: sourceBucket,
          Key: sourceKey,
        })
      );

      if (!s3Response.Body) {
        throw new Error(`S3 Object Body is empty or missing for key ${sourceKey}`);
      }

      // Read the readable stream from S3 into a buffer
      const byteArray = await s3Response.Body.transformToByteArray();
      const imageBuffer = Buffer.from(byteArray);

      console.log(`Downloaded image. Size: ${imageBuffer.length} bytes. Processing with Jimp...`);

      // 2. Load the image into Jimp
      const image = await Jimp.read(imageBuffer);

      // 3. Resize the image to 300x300 (predefined dimension)
      console.log(`Resizing image to 300x300...`);
      image.resize(300, 300);

      // 4. Convert the image to JPEG (predefined format)
      console.log(`Converting image to JPEG...`);
      const processedBuffer = await image.getBufferAsync(Jimp.MIME_JPEG);

      // 5. Build clean processed S3 Key: processed/300x300-<originalNameWithoutExt>.jpg
      const originalExtension = path.extname(fileName);
      const baseName = path.basename(fileName, originalExtension);
      const processedKey = `processed/300x300-${baseName}.jpg`;

      console.log(`Uploading processed image to S3 bucket ${PROCESSED_BUCKET} at key ${processedKey}...`);

      // 6. Upload processed image to the Processed S3 Bucket
      await s3.send(
        new PutObjectCommand({
          Bucket: PROCESSED_BUCKET,
          Key: processedKey,
          Body: processedBuffer,
          ContentType: 'image/jpeg',
        })
      );

      console.log(`Successfully processed image and uploaded to ${PROCESSED_BUCKET}/${processedKey}`);

    } catch (error) {
      console.error(`Failed to process SQS message ${messageId}:`, error);
      // Re-throw the error so that AWS SQS knows this message failed.
      // SQS will then retry the message according to the redrive policy.
      // If it keeps failing, it will be moved to the Dead Letter Queue (DLQ).
      throw error;
    }
  }
};
