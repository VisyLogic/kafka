package com.eventflow.sourcing;

import com.eventflow.shared.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import software.amazon.awssdk.services.sfn.model.*;

/** Step Functions Activities let a plain Java process work without an inbound public API. */
public final class WorkflowWorker {
    @FunctionalInterface public interface Work { Map<String,Object> apply(Map<String,Object> input,AtomicBoolean alive)throws Exception; }
    public static void poll(String name,Work work){
        while(true){try{
            var task=Aws.SFN.getActivityTask(b->b.activityArn(Aws.PREFIX+"activity:"+name).workerName(Json.env("HOSTNAME","local-worker")));
            if(task.taskToken()==null||task.taskToken().isBlank())continue;
            var alive=new AtomicBoolean(true);var timer=Executors.newSingleThreadScheduledExecutor();
            timer.scheduleAtFixedRate(()->{try{Aws.SFN.sendTaskHeartbeat(b->b.taskToken(task.taskToken()));}catch(Exception e){alive.set(false);Json.log(name,"Lost task ownership: "+e.getClass().getSimpleName());}},10,10,TimeUnit.SECONDS);
            try{var result=work.apply(Json.read(task.input()),alive);if(alive.get())Aws.SFN.sendTaskSuccess(b->b.taskToken(task.taskToken()).output(Json.write(result)));}
            catch(Exception e){Json.log(name,e.toString());if(alive.get())try{Aws.SFN.sendTaskFailure(b->b.taskToken(task.taskToken()).error("EventFlow.WorkerFailure").cause(Objects.toString(e.getMessage(),"Unknown error").substring(0,Math.min(1000,Objects.toString(e.getMessage(),"Unknown error").length()))));}catch(Exception ignored){}}
            finally{alive.set(false);timer.shutdownNow();}
        }catch(Exception e){Json.log(name,e.toString());try{Thread.sleep(2000);}catch(InterruptedException stop){Thread.currentThread().interrupt();return;}}}
    }
    public static Map<String,Object> awaitAccounting(Map<String,Object> input,AtomicBoolean alive)throws Exception{
        var manifest=Store.load(Json.text(input,"manifestKey"));String batch=Json.text(input,"batchId");
        while(alive.get()){
            boolean complete=true;long errors=0;
            for(var event:Json.rows(manifest.get("events"))){
                var row=Store.get("RUN#"+event.get("runId"),"EVENT#"+event.get("eventId"));
                int apps=Json.rows(((Map<?,?>)event.get("config")).get("applications")).size();
                long expected=(long)Json.number(event,"loanCount",0)*apps;
                if(Json.number(row,"processed",0)<expected){complete=false;continue;}
                int failed=Json.number(row,"failed",0);errors+=failed;
                Store.status("RUN#"+event.get("runId"),"EVENT#"+event.get("eventId"),failed==0?"COMMITTED":"REVIEW_REQUIRED");
            }
            if(complete){Store.patch("BATCH#"+batch,"META",Map.of("accountingOutcome",errors==0?"COMMITTED":"REVIEW_REQUIRED"));return Map.of("batchId",batch,"status",errors==0?"COMMITTED":"REVIEW_REQUIRED","failedLoanApplications",errors);}
            Thread.sleep(1500);
        }throw new IllegalStateException("Workflow task no longer active");
    }
}
