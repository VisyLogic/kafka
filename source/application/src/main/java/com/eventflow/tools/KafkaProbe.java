package com.eventflow.tools;

import com.eventflow.shared.*;
import java.time.Duration;
import java.util.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.ProducerRecord;

/** A separate lab topic tests the data plane without injecting fake accounting events. */
public final class KafkaProbe {
    public static void main(String[] args)throws Exception{
        String topic="eventflow.lab",marker="probe-"+UUID.randomUUID();
        try(var admin=Admin.create(Kafka.properties())){
            if(!admin.listTopics().names().get().contains(topic))
                admin.createTopics(List.of(new NewTopic(topic,1,Short.parseShort(Json.env("KAFKA_REPLICATION_FACTOR","1"))))).all().get();
        }
        try(var producer=Kafka.producer();var consumer=Kafka.consumer("eventflow-lab-check-"+UUID.randomUUID(),topic)){
            producer.send(new ProducerRecord<>(topic,marker,marker)).get();
            long deadline=System.nanoTime()+Duration.ofSeconds(45).toNanos();
            while(System.nanoTime()<deadline){
                for(var record:consumer.poll(Duration.ofSeconds(1)))if(record.value().equals(marker)){
                    consumer.commitSync();System.out.println("PASS: produced, consumed and committed lab marker on "+topic);return;
                }
            }
        }throw new IllegalStateException("Kafka probe timed out; check listeners, DNS, routes, authentication and permissions");
    }
}
