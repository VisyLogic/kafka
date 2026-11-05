# Amazon MSK hands-on sample

Open the offline course's **Amazon MSK hands-on** section for the ordered lessons M01–M05. This is a separate sample application, not a deployment of the full finance pipeline.

```text
Java produce → learning.msk.loan-events
                    ├─ Java consume → terminal
                    └─ Java enrich (mock credit score = 720)
                          → learning.msk.enriched-loans
                               → optional Java bridge → SQS → manual test reader
```

These Kafka topics are the streaming destinations sometimes called “Kafka queues.” SQS is a separate optional service; it is not required between these two topics. Only sample data and enrichment are mocked. MSK, Kafka acknowledgements, consumer groups and SQS calls are real when deployed to AWS.

## Build

Install Java 21 and Maven. From this folder run `mvn -B package`. The executable artifact is `target/msk-hands-on.jar`. No dependency on EventFlow's configuration, databases or LocalStack is required. Credentials use the default AWS provider chain (the EC2 role in the AWS lab); do not put keys in source.

## Run inside the VPC

Use the course's SSM-managed EC2 client. CloudShell's ordinary public environment and an unconnected workstation cannot reach these private brokers. Upload/extract this source folder on that client and build, or copy the built JAR using the documented S3 handoff. Bash:

```bash
export AWS_REGION=us-east-1
export MSK_BOOTSTRAP='PASTE_BootstrapBrokerStringSaslIam_HERE'
java -jar target/msk-hands-on.jar init
java -jar target/msk-hands-on.jar produce 10
java -jar target/msk-hands-on.jar consume 10 msk-learning-reader-01
java -jar target/msk-hands-on.jar enrich 10 msk-learning-enricher-01
java -jar target/msk-hands-on.jar consume-enriched 10 msk-learning-output-reader-01
export MSK_SQS_URL='PASTE_LAB_QUEUE_URL_HERE'
java -jar target/msk-hands-on.jar bridge 10 msk-learning-bridge-01
```

Use new consumer group suffixes to read retained records again. Repeating produce intentionally adds more events. Replaying an enricher/bridge may append duplicate outputs: this sample has no DynamoDB financial idempotency store. Kafka producer idempotence does not make Kafka plus SQS atomic. These are bounded lab processes (default 10 records, two-minute consumption window), not production daemons.

`init` creates three-partition topics with replication factor two and min.insync.replicas two. On a two-broker lab this favors durability and blocks writes if either replica is unavailable; it is not a production availability design. For an isolated local broker only, set `MSK_IAM=false` and `MSK_REPLICAS=1`. Do not use those settings for AWS.

## Infrastructure

`msk-lab.yaml` uses an existing VPC and two private broker subnets in distinct AZs. The client subnet must have outbound NAT connectivity for Session Manager, package installation, Maven and the optional S3 handoff. Alternative private endpoints require the appropriate SSM/S3/SQS and download connectivity. It creates two billable Standard brokers, a small EC2 client, IAM role, security groups and SQS/DLQ. The main course network template can create a disposable VPC/NAT if needed. Review current AWS availability, quotas, costs and the change set before execution. No AWS resources are created by opening HTML or compiling this project.

## Evidence boundary and cleanup

Java compilation/package and local CLI usage validation can be checked without AWS. Actual MSK authentication, network connectivity and SQS delivery must be tested in your AWS sandbox. Preserve lab results, then delete the lab stack and any separately created download bucket/objects; delete a dedicated network stack only after dependents are gone. Existing EventFlow infrastructure is not part of this cleanup.
