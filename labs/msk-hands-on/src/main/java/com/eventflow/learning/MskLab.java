package com.eventflow.learning;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

/** Standalone teaching client. No EventFlow databases or accounting topics are used. */
public final class MskLab {
    static final String INPUT="learning.msk.loan-events", OUTPUT="learning.msk.enriched-loans";
    static final ObjectMapper JSON=new ObjectMapper();
    static String env(String name,String fallback){return System.getenv().getOrDefault(name,fallback);}
    static Properties properties(){
        String brokers=env("MSK_BOOTSTRAP","");
        if(brokers.isBlank())throw new IllegalArgumentException("Set MSK_BOOTSTRAP to IAM bootstrap brokers first");
        var p=new Properties();p.put("bootstrap.servers",brokers);p.put("client.id","msk-learning");
        p.put("request.timeout.ms","30000");p.put("default.api.timeout.ms","60000");
        if(Boolean.parseBoolean(env("MSK_IAM","true"))){
            p.put("security.protocol","SASL_SSL");p.put("sasl.mechanism","AWS_MSK_IAM");
            p.put("sasl.jaas.config","software.amazon.msk.auth.iam.IAMLoginModule required;");
            p.put("sasl.client.callback.handler.class","software.amazon.msk.auth.iam.IAMClientCallbackHandler");
        }else p.put("security.protocol","PLAINTEXT"); // Only an isolated local broker.
        return p;
    }
    static KafkaProducer<String,String> producer(){
        var p=properties();p.put("key.serializer",StringSerializer.class.getName());p.put("value.serializer",StringSerializer.class.getName());
        p.put("acks","all");p.put("enable.idempotence","true");p.put("delivery.timeout.ms","60000");
        return new KafkaProducer<>(p);
    }
    static KafkaConsumer<String,String> consumer(String group,String topic){
        if(!group.startsWith("msk-learning-"))throw new IllegalArgumentException("Group must begin msk-learning-");
        var p=properties();p.put("group.id",group);p.put("enable.auto.commit","false");p.put("auto.offset.reset","earliest");
        p.put("allow.auto.create.topics","false");p.put("max.poll.records","1");
        p.put("key.deserializer",StringDeserializer.class.getName());p.put("value.deserializer",StringDeserializer.class.getName());
        var c=new KafkaConsumer<String,String>(p);c.subscribe(List.of(topic));return c;
    }
    static void init()throws Exception{
        try(var admin=Admin.create(properties())){
            var existing=admin.listTopics().names().get(60,TimeUnit.SECONDS);
            short replicas=Short.parseShort(env("MSK_REPLICAS","2"));
            for(String topic:List.of(INPUT,OUTPUT))if(!existing.contains(topic))
                admin.createTopics(List.of(new NewTopic(topic,3,replicas).configs(Map.of("min.insync.replicas",replicas>1?"2":"1"))))
                     .all().get(60,TimeUnit.SECONDS);
            var descriptions=admin.describeTopics(List.of(INPUT,OUTPUT)).allTopicNames().get(60,TimeUnit.SECONDS);
            descriptions.forEach((name,description)->System.out.println(name+" "+description));
        }
    }
    static void produce(int count)throws Exception{
        String run=UUID.randomUUID().toString();
        try(var producer=producer()){
            for(int i=1;i<=count;i++){
                String key="LN-"+String.format("%04d",i);
                String value=JSON.writeValueAsString(Map.of("eventId",run+"-"+i,"loanId",key,"eventType","settled","principal",100000+i,"mock",true));
                var ack=producer.send(new ProducerRecord<>(INPUT,key,value)).get(65,TimeUnit.SECONDS);
                System.out.printf("ACK loan=%s partition=%d offset=%d%n",key,ack.partition(),ack.offset());
            }
        }System.out.println("PASS produced="+count+" run="+run);
    }
    static void consume(String mode,int count,String group)throws Exception{
        String topic=(mode.equals("bridge")||mode.equals("consume-enriched"))?OUTPUT:INPUT;
        String queue=env("MSK_SQS_URL","");
        if(mode.equals("bridge")&&queue.isBlank())throw new IllegalArgumentException("Set MSK_SQS_URL for bridge");
        try(var consumer=consumer(group,topic);
            var publisher=mode.equals("enrich")?producer():null;
            var sqs=mode.equals("bridge")?SqsClient.create():null){
            int received=0;long deadline=System.nanoTime()+Duration.ofMinutes(2).toNanos();
            while(received<count && System.nanoTime()<deadline){
                for(var record:consumer.poll(Duration.ofSeconds(1))){
                    if(mode.equals("enrich")){
                        var loan=JSON.readTree(record.value());
                        if(!loan.isObject())throw new IllegalArgumentException("Expected loan object");
                        ((com.fasterxml.jackson.databind.node.ObjectNode)loan).put("mockCreditScore",720);
                        publisher.send(new ProducerRecord<>(OUTPUT,record.key(),JSON.writeValueAsString(loan))).get(65,TimeUnit.SECONDS);
                    }else if(mode.equals("bridge")){
                        sqs.sendMessage(r->r.queueUrl(queue).messageBody(record.value())); // Wait for SQS acceptance.
                    }
                    System.out.printf("%s key=%s partition=%d offset=%d value=%s%n",mode,record.key(),record.partition(),record.offset(),record.value());
                    consumer.commitSync(); // max.poll.records=1: commit only after this record's effect.
                    received++;
                }
            }
            if(received!=count)throw new IllegalStateException("Timed out: expected="+count+" received="+received+"; check group history and topic");
            System.out.println("PASS "+mode+" count="+received+" group="+group);
        }
    }
    public static void main(String[] args)throws Exception{
        if(args.length==0){System.out.println("Usage: init | produce [10] | consume|consume-enriched|enrich|bridge [10] [msk-learning-GROUP]");return;}
        String mode=args[0];
        if(!List.of("init","produce","consume","consume-enriched","enrich","bridge").contains(mode))throw new IllegalArgumentException("Unknown command "+mode);
        int count=args.length>1?Integer.parseInt(args[1]):10;
        if(count<1||count>10000)throw new IllegalArgumentException("Lab count must be 1..10000");
        switch(mode){
            case "init"->init();case "produce"->produce(count);
            default->consume(mode,count,args.length>2?args[2]:"msk-learning-"+mode);
        }
    }
}
