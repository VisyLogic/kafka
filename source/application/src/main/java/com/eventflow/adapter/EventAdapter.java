package com.eventflow.adapter;
import com.eventflow.shared.*;
import java.time.Duration;

/** Kafka offset moves only after SQS accepts the event. Redelivery is safe downstream. */
public final class EventAdapter {
    public static void run(){
        try(var consumer=Kafka.consumer("eventflow-adapter",Kafka.EVENTS)){
            String queue=Aws.queue();
            while(true){
                var records=consumer.poll(Duration.ofSeconds(1));
                for(var record:records){
                    // Do not skip a failed record and commit later offsets in its partition.
                    while(true){try{Aws.SQS.sendMessage(b->b.queueUrl(queue).messageBody(record.value()));break;}
                        catch(Exception e){Json.log("adapter",e.toString());sleep(2000);}}
                }if(!records.isEmpty())consumer.commitSync();
            }
        }
    }
    private static void sleep(long ms){try{Thread.sleep(ms);}catch(InterruptedException e){Thread.currentThread().interrupt();throw new RuntimeException(e);}}
}
