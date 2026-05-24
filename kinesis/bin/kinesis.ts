#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import { KinesisStack } from '../lib/kinesis-stack';

const app = new cdk.App();

new KinesisStack(app, 'KinesisStack', {
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: 'us-east-1',
  },
});
