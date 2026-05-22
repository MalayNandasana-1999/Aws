#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib/core';
import { EventBridgeStack } from '../lib/event-bridge-stack';

const app = new cdk.App();
new EventBridgeStack(app, 'EventBridgeStack', {
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: 'us-east-1'
  },
});
