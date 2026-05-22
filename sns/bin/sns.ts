#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib/core';
import { SnsStack } from '../lib/sns-stack';

const app = new cdk.App();
new SnsStack(app, 'SnsStack', {
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: 'us-east-1'
  },
});

