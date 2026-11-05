package com.eventflow.shared;

import java.net.URI;
import java.time.Duration;
import software.amazon.awssdk.auth.credentials.*;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sfn.SfnClient;

public final class Aws {
    private static final Region REGION = Region.of(Json.env("AWS_REGION","us-east-1"));
    private static final boolean LOCAL = !Json.env("AWS_ENDPOINT_URL", "").isBlank();
    private static final URI ENDPOINT = URI.create(Json.env("AWS_ENDPOINT_URL","https://localhost"));
    private static final AwsCredentialsProvider CREDS = LOCAL
        ? StaticCredentialsProvider.create(AwsBasicCredentials.create("test","test"))
        : DefaultCredentialsProvider.create();
    private static final ClientOverrideConfiguration TIMEOUT = ClientOverrideConfiguration.builder()
        .apiCallTimeout(Duration.ofSeconds(85)).apiCallAttemptTimeout(Duration.ofSeconds(75)).build();
    public static final DynamoDbClient DB = db();
    public static final S3Client S3 = s3();
    public static final SqsClient SQS = sqs();
    public static final SfnClient SFN = sfn();
    public static final String TABLE=Json.env("TABLE_NAME","eventflow-data");
    public static final String BUCKET=Json.env("BUCKET_NAME","eventflow-artifacts-local");
    public static final String PREFIX="arn:aws:states:"+REGION+":"+Json.env("AWS_ACCOUNT_ID","000000000000")+":";
    private static DynamoDbClient db(){var b=DynamoDbClient.builder().region(REGION).credentialsProvider(CREDS).overrideConfiguration(TIMEOUT).httpClientBuilder(UrlConnectionHttpClient.builder());if(LOCAL)b.endpointOverride(ENDPOINT);return b.build();}
    private static S3Client s3(){var b=S3Client.builder().region(REGION).credentialsProvider(CREDS).overrideConfiguration(TIMEOUT).forcePathStyle(LOCAL).httpClientBuilder(UrlConnectionHttpClient.builder());if(LOCAL)b.endpointOverride(ENDPOINT);return b.build();}
    private static SqsClient sqs(){var b=SqsClient.builder().region(REGION).credentialsProvider(CREDS).overrideConfiguration(TIMEOUT).httpClientBuilder(UrlConnectionHttpClient.builder());if(LOCAL)b.endpointOverride(ENDPOINT);return b.build();}
    private static SfnClient sfn(){var b=SfnClient.builder().region(REGION).credentialsProvider(CREDS).overrideConfiguration(TIMEOUT).httpClientBuilder(UrlConnectionHttpClient.builder().socketTimeout(Duration.ofSeconds(70)));if(LOCAL)b.endpointOverride(ENDPOINT);return b.build();}
    public static String queue(){return SQS.getQueueUrl(b->b.queueName("eventflow-ingress")).queueUrl();}
}
