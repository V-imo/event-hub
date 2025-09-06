import * as cdk from "aws-cdk-lib"
import * as events from "aws-cdk-lib/aws-events"
import * as glue from "aws-cdk-lib/aws-glue"
import * as iam from "aws-cdk-lib/aws-iam"
import * as firehose from "aws-cdk-lib/aws-kinesisfirehose"
import * as s3 from "aws-cdk-lib/aws-s3"
import * as ssm from "aws-cdk-lib/aws-ssm"

import { Construct } from "constructs"

export interface EventHubProps extends cdk.StackProps {
  serviceName: string
  stage: string
}

export class EventHub extends cdk.Stack {
  constructor(scope: Construct, id: string, props: EventHubProps) {
    super(scope, id, props)

    // --- Event Bus ---
    const eventBus = new events.EventBus(this, "Bus", {})

    new ssm.StringParameter(this, "BusArn", {
      parameterName: `/vimo/${props.stage}/event-bus-arn`,
      stringValue: eventBus.eventBusArn,
      tier: ssm.ParameterTier.ADVANCED,
    })

    eventBus.archive("ArchiveEventLake", {
      eventPattern: {
        source: ["custom"],
        detail: { "detail-type": [{ exists: true }] },
      },
    })

    // --- Event Lake S3 Bucket ---
    const eventLake = new s3.Bucket(this, "EventLake", {
      removalPolicy: cdk.RemovalPolicy.RETAIN,
    })

    new ssm.StringParameter(this, "EventLakeName", {
      parameterName: `/vimo/${props.stage}/event-lake-name`,
      stringValue: eventLake.bucketName,
    })

    // --- Firehose Delivery Stream ---
    new firehose.DeliveryStream(this, "DeliveryStream", {
      destination: new firehose.S3Bucket(eventLake, {
        dataOutputPrefix: `${cdk.Stack.of(this).region}/`,
      }),
    })

    // --- Glue Integration ---
    // Glue Database
    const glueDatabase = new glue.CfnDatabase(this, "EventLakeDatabase", {
      catalogId: this.account,
      databaseInput: {
        name: `event_lake_${props.stage}`, // unique per stage
      },
    })

    // Glue Role
    const glueRole = new iam.Role(this, "GlueCrawlerRole", {
      assumedBy: new iam.ServicePrincipal("glue.amazonaws.com"),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName(
          "service-role/AWSGlueServiceRole",
        ),
      ],
    })

    eventLake.grantRead(glueRole)

    // Glue Crawler
    const glueCrawler = new glue.CfnCrawler(this, "EventLakeCrawler", {
      role: glueRole.roleArn,
      databaseName: glueDatabase.ref,
      targets: {
        s3Targets: [
          {
            path: `s3://${eventLake.bucketName}/`,
          },
        ],
      },
      schemaChangePolicy: {
        updateBehavior: "UPDATE_IN_DATABASE",
        deleteBehavior: "DEPRECATE_IN_DATABASE",
      },
    })

    new ssm.StringParameter(this, "GlueDatabaseName", {
      parameterName: `/vimo/${props.stage}/glue-database-name`,
      stringValue: glueDatabase.ref,
    })

    new ssm.StringParameter(this, "GlueCrawlerName", {
      parameterName: `/vimo/${props.stage}/glue-crawler-name`,
      stringValue: glueCrawler.ref,
    })

    // Glue Table with Partition Projection for date-based sorting
    const eventsTable = new glue.CfnTable(this, "EventsTable", {
      catalogId: this.account,
      databaseName: glueDatabase.ref,
      tableInput: {
        name: "events",
        tableType: "EXTERNAL_TABLE",
        parameters: {
          "projection.enabled": "true",
          "projection.datehour.type": "date",
          "projection.datehour.format": "yyyy/MM/dd/HH",
          "projection.datehour.range": "2024/01/01/00,NOW",
          "projection.datehour.interval": "1",
          "projection.datehour.interval.unit": "HOURS",
          "storage.location.template": `s3://${eventLake.bucketName}/${this.region}/\${datehour}/`,
          "case.insensitive": "FALSE",
        },
        storageDescriptor: {
          location: `s3://${eventLake.bucketName}/${this.region}/`,
          inputFormat: "org.apache.hadoop.mapred.TextInputFormat",
          outputFormat:
            "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
          serdeInfo: {
            serializationLibrary: "org.openx.data.jsonserde.JsonSerDe",
          },
          columns: [
            {
              name: "timestamp",
              type: "bigint",
            },
            {
              name: "type",
              type: "string",
            },
            {
              name: "data",
              type: "string",
            },
            {
              name: "from",
              type: "string",
            },
          ],
        },
        partitionKeys: [
          {
            name: "datehour",
            type: "string",
          },
        ],
      },
    })

    eventsTable.addDependency(glueDatabase)

    new ssm.StringParameter(this, "GlueTableName", {
      parameterName: `/vimo/${props.stage}/glue-table-name`,
      stringValue: eventsTable.ref,
    })
  }
}
