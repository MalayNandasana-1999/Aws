import * as cdk from 'aws-cdk-lib';
import { Template } from 'aws-cdk-lib/assertions';
import * as Sns from '../lib/sns-stack';

test('OTP Architecture Resources Created', () => {
  const app = new cdk.App();
  
  // WHEN
  const stack = new Sns.SnsStack(app, 'MyTestStack');
  
  // THEN
  const template = Template.fromStack(stack);

  // 1. Verify SNS Topic exists
  template.resourceCountIs('AWS::SNS::Topic', 1);

  // 2. Verify SQS Queues exist (2 main queues + 2 DLQs = 4 total)
  template.resourceCountIs('AWS::SQS::Queue', 4);

  // Verify specific queue configurations exist (visibility timeout, etc.)
  template.hasResourceProperties('AWS::SQS::Queue', {
    QueueName: 'email-otp-queue',
    VisibilityTimeout: 45,
  });

  template.hasResourceProperties('AWS::SQS::Queue', {
    QueueName: 'sms-otp-queue',
    VisibilityTimeout: 45,
  });

  // 3. Verify Lambda Functions exist (3 Node.js Lambda functions)
  template.resourceCountIs('AWS::Lambda::Function', 3);

  // 4. Verify API Gateway RestApi exists
  template.resourceCountIs('AWS::ApiGateway::RestApi', 1);
  template.hasResourceProperties('AWS::ApiGateway::Method', {
    HttpMethod: 'POST',
  });
});
