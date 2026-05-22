#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib/core';
import { SqsStack } from '../lib/sqs-stack';

const app = new cdk.App();
new SqsStack(app, 'SqsStack', {
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: 'us-east-1'
  },
});
