package com.eventflow.shared;

import java.util.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.*;

public final class Kafka {
    public static final String EVENTS="eventflow.business-events", LOANS="eventflow.enriched-loans";
    public static Properties properties(){
        var p=new Properties();
        var brokers=Json.env("KAFKA_BOOTSTRAP_SERVERS","");
        if(brokers.isBlank())brokers=Json.text(Store.config(),"bootstrapServers");
        p.put("security.protocol",Json.env("KAFKA_SECURITY_PROTOCOL","PLAINTEXT"));
        if(Json.env("KAFKA_SASL_MECHANISM","").equals("AWS_MSK_IAM")){
            p.put("sasl.mechanism","AWS_MSK_IAM");
            p.put("sasl.jaas.config","software.amazon.msk.auth.iam.IAMLoginModule required;");
            p.put("sasl.client.callback.handler.class","software.amazon.msk.auth.iam.IAMClientCallbackHandler");
        }
        p.put("bootstrap.servers",brokers);p.put("client.id",Json.env("HOSTNAME","eventflow"));return p;
    }
    public static KafkaProducer<String,String> producer(){
        var p=properties();p.put("key.serializer",StringSerializer.class.getName());p.put("value.serializer",StringSerializer.class.getName());
        p.put("acks","all");p.put("enable.idempotence","true");p.put("compression.type","gzip");p.put("max.request.size",1048576);
        p.put("max.block.ms",30000);p.put("delivery.timeout.ms",60000);return new KafkaProducer<>(p);
    }
    public static KafkaConsumer<String,String> consumer(String group,String topic){
        var p=properties();p.put("key.deserializer",StringDeserializer.class.getName());p.put("value.deserializer",StringDeserializer.class.getName());
        p.put("allow.auto.create.topics","false");p.put("group.id",group);p.put("enable.auto.commit","false");p.put("auto.offset.reset","earliest");p.put("max.poll.records","1");
        p.put("max.poll.interval.ms","900000");var c=new KafkaConsumer<String,String>(p);c.subscribe(List.of(topic));return c;
    }
    public static void topics(int partitions)throws Exception{
        try(var admin=Admin.create(properties())){
            var existing=admin.listTopics().names().get();
            for(String name:List.of(EVENTS,LOANS))if(!existing.contains(name))admin.createTopics(List.of(new NewTopic(name,partitions,Short.parseShort(Json.env("KAFKA_REPLICATION_FACTOR","1"))))).all().get();
        }
    }
    public static Map<String,Object> metrics(){
        try(var admin=Admin.create(properties())){
            var info=new HashMap<String,Object>();
            for(String topic:List.of(EVENTS,LOANS)){
                var desc=admin.describeTopics(List.of(topic)).allTopicNames().get().get(topic);
                var requests=new HashMap<org.apache.kafka.common.TopicPartition,OffsetSpec>();
                desc.partitions().forEach(p->requests.put(new org.apache.kafka.common.TopicPartition(topic,p.partition()),OffsetSpec.latest()));
                var ends=admin.listOffsets(requests).all().get();
                var group=topic.equals(EVENTS)?"eventflow-adapter":"eventflow-activity";
                var commits=admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get();
                long lag=0,total=0;for(var e:ends.entrySet()){long end=e.getValue().offset();total+=end;lag+=Math.max(0,end-(commits.containsKey(e.getKey())?commits.get(e.getKey()).offset():0));}
                info.put(topic,Map.of("partitions",desc.partitions().size(),"messages",total,"lag",lag,"unit",topic.equals(LOANS)?"loan envelopes":"business events"));
            }return info;
        }catch(Exception e){return Map.of("error",e.getMessage());}
    }
}
