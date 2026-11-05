package com.eventflow.sourcing;

import com.eventflow.shared.*;
import com.google.protobuf.StringValue;
import io.grpc.*;
import io.grpc.stub.ClientCalls;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.producer.*;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.core.sync.RequestBody;

public final class SourcingService {
    public static void run(){
        var threads=Executors.newFixedThreadPool(2);
        for(String level:List.of("pool","loan"))
            threads.submit(()->WorkflowWorker.poll("eventflow-source-"+level,SourcingService::source));
    }
    @SuppressWarnings("unchecked")
    private static Map<String,Object> source(Map<String,Object> input,AtomicBoolean alive)throws Exception{
        var events=Json.rows(Store.load(Json.text(input,"manifestKey")).get("events"));
        int concurrency=Json.number((Map<String,Object>)events.getFirst().get("config"),"sourceConcurrency",1);
        var executor=Executors.newFixedThreadPool(concurrency);var futures=new ArrayList<Future<?>>();
        try{
            for(var event:events)futures.add(executor.submit(()->{
                try{stream(event,alive);}catch(Exception e){throw new CompletionException(e);}
            }));
            for(var future:futures)future.get();
        }catch(Exception e){for(var future:futures)future.cancel(true);throw e;}
        finally{executor.shutdownNow();executor.awaitTermination(10,TimeUnit.SECONDS);}
        return input;
    }
    @SuppressWarnings("unchecked")
    private static void stream(Map<String,Object> event,AtomicBoolean alive)throws Exception{
        String runId=Json.text(event,"runId"),eventId=Json.text(event,"eventId");
        String pk="RUN#"+runId,sk="EVENT#"+eventId,path="checkpoints/"+runId+"/"+eventId+".json";
        var config=(Map<String,Object>)event.get("config");String etag=null;int cursor=0;
        try{
            var object=Aws.S3.getObjectAsBytes(b->b.bucket(Aws.BUCKET).key(path));
            etag=object.response().eTag();cursor=Json.number(Json.read(object.asUtf8String()),"cursor",0);
        }catch(NoSuchKeyException e){/* No prior source progress. */}
        int count=Json.number(event,"loanCount",1);String snapshot=eventId+"-snapshot-v1";
        if(cursor>=count)return;
        Store.patch(pk,sk,Map.of("status","SOURCING","snapshotId",snapshot));
        var channel=ManagedChannelBuilder.forTarget(Json.env("GRPC_TARGET","mock-source:9095")).usePlaintext().build();
        try(var producer=Kafka.producer()){
            var request=new HashMap<String,Object>();
            request.putAll(Map.of("eventId",eventId,"poolId",event.get("poolId"),"count",count,
                "afterCursor",cursor,"snapshotId",snapshot,"seed",Json.number(config,"seed",42),
                "streamRate",Json.number(config,"streamRate",1000),"disconnectAt",Json.number(config,"disconnectAt",0)));
            var iterator=ClientCalls.blockingServerStreamingCall(channel,GetDataContract.STREAM_LOANS,
                CallOptions.DEFAULT.withDeadlineAfter(1,TimeUnit.HOURS),StringValue.of(Json.write(request)));
            var loans=new ArrayList<Map<String,Object>>();int size=Json.number(config,"envelopeSize",100);long opened=System.nanoTime();
            while(iterator.hasNext()){
                guard(runId,alive);
                var loan=Json.read(iterator.next().getValue());loans.add(loan);
                if(loans.size()>=size||System.nanoTime()-opened>=2_000_000_000L){
                    etag=publish(producer,event,loans,path,etag,snapshot,alive);
                    cursor=Json.number(loan,"cursor",0);loans.clear();opened=System.nanoTime();
                }
            }
            if(!loans.isEmpty()){
                cursor=Json.number(loans.getLast(),"cursor",0);
                etag=publish(producer,event,loans,path,etag,snapshot,alive);
            }
            if(cursor!=count)throw new IllegalStateException("Incomplete source stream");
            Store.patch(pk,sk,Map.of("sourcingComplete",true,"published",count,"checkpointKey",path));
        }finally{channel.shutdownNow();}
    }
    private static void guard(String runId,AtomicBoolean alive)throws Exception{
        if(!alive.get()||Thread.currentThread().isInterrupted())throw new IllegalStateException("Source task no longer owned");
        // Metadata control is checked at envelope boundaries in publish(), not per loan.
    }
    private static String publish(KafkaProducer<String,String> producer,Map<String,Object> event,
            List<Map<String,Object>> loans,String path,String etag,String snapshot,AtomicBoolean alive)throws Exception{
        guard(Json.text(event,"runId"),alive);
        var control=Store.get("RUN#"+event.get("runId"),"META");
        while(Boolean.TRUE.equals(control.get("paused"))&&!Boolean.TRUE.equals(control.get("stopRequested"))){
            guard(Json.text(event,"runId"),alive);Thread.sleep(500);
            control=Store.get("RUN#"+event.get("runId"),"META");
        }
        if(Boolean.TRUE.equals(control.get("stopRequested")))throw new IllegalStateException("Source stopped by user; published messages continue draining");
        int cursor=Json.number(loans.getLast(),"cursor",0);
        var envelope=Map.of("event",event,"snapshotId",snapshot,"endingCursor",cursor,"loans",List.copyOf(loans));
        String body=Json.write(envelope);
        if(body.getBytes(java.nio.charset.StandardCharsets.UTF_8).length>900_000)
            throw new IllegalArgumentException("Loan envelope exceeds 900 KB; reduce envelope size");
        producer.send(new ProducerRecord<>(Kafka.LOANS,Json.text(event,"eventId"),body)).get(65,TimeUnit.SECONDS);
        guard(Json.text(event,"runId"),alive);
        var request=PutObjectRequest.builder().bucket(Aws.BUCKET).key(path).contentType("application/json");
        if(etag==null)request.ifNoneMatch("*");else request.ifMatch(etag);
        String updated=Aws.S3.putObject(request.build(),RequestBody.fromString(Json.write(
            Map.of("snapshotId",snapshot,"cursor",cursor,"updatedAt",java.time.Instant.now().toString())))).eTag();
        Store.patch("RUN#"+event.get("runId"),"EVENT#"+event.get("eventId"),Map.of("published",cursor,"checkpointKey",path));
        return updated;
    }
}
