import * as cdk from "aws-cdk-lib"
import { Construct } from "constructs"

export interface EventHubProps extends cdk.StackProps {
  serviceName: string;
  stage: string;
}

export class EventHub extends cdk.Stack {
  constructor(scope: Construct, id: string, props: EventHubProps) {
    super(scope, id, props)
    // Add your infra here...
  }
}
