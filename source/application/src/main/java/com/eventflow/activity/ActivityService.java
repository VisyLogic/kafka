package com.eventflow.activity;

import com.eventflow.shared.*;
import java.time.Duration;
import java.util.*;
import software.amazon.awssdk.services.dynamodb.model.*;

public final class ActivityService {
    public static void run(){
        try(var consumer=Kafka.consumer("eventflow-activity",Kafka.LOANS)){
            while(true){var records=consumer.poll(Duration.ofSeconds(1));
                for(var record:records){
                    // Keep the failed envelope here until durable outcomes exist; no offset skipping.
                    while(true){try{process(Json.read(record.value()));break;}catch(Exception e){Json.log("activity",e.toString());sleep(2000);}}
                }if(!records.isEmpty())consumer.commitSync();
            }
        }
    }
    @SuppressWarnings("unchecked") public static void process(Map<String,Object> envelope){
        var event=(Map<String,Object>)envelope.get("event");var config=(Map<String,Object>)event.get("config");
        for(var loan:Json.rows(envelope.get("loans")))for(var app:Json.rows(config.get("applications")))post(loan,event,app);
    }
    private static void post(Map<String,Object> loan,Map<String,Object> event,Map<String,Object> app){
        String pk="RUN#"+event.get("runId"),sk="RESULT#"+loan.get("loanId")+"#"+app.get("id");
        // Existing output is checked, not overwritten. Source snapshot/config are frozen for a run.
        var old=Store.get(pk,sk);if(!old.isEmpty()){
            if(!Json.write(old.get("loan")).equals(Json.write(loan)))throw new IllegalStateException("Conflicting duplicate loan "+loan.get("loanId"));return;
        }
        var row=new LinkedHashMap<String,Object>();row.put("eventId",event.get("eventId"));row.put("loanId",loan.get("loanId"));row.put("application",app.get("id"));row.put("loan",loan);
        long count=0;boolean failed=false;
        try{var records=Accounting.calculate(loan,event,app);row.put("records",records);count=records.size();row.put("status","COMMITTED");}
        catch(IllegalArgumentException e){failed=true;row.put("status","FAILED");row.put("error",e.getMessage());row.put("records",List.of());}
        row.put("configVersion",((Map<?,?>)event.get("config")).get("version"));
        var put=Put.builder().tableName(Aws.TABLE).item(Store.item(pk,sk,row)).conditionExpression("attribute_not_exists(pk)").build();
        var values=Map.of(":one",Store.n(1),":count",Store.n(count),":failed",Store.n(failed?1:0));
        var update=Update.builder().tableName(Aws.TABLE).key(Store.key(pk,"EVENT#"+event.get("eventId")))
            .updateExpression("ADD #processed :one, #be :count, #ar :count, #failed :failed")
            .expressionAttributeNames(Map.of("#processed","processed","#be","beCount","#ar","arCount","#failed","failed"))
            .expressionAttributeValues(values).build();
        try{Aws.DB.transactWriteItems(b->b.transactItems(TransactWriteItem.builder().put(put).build(),TransactWriteItem.builder().update(update).build()));}
        catch(TransactionCanceledException e){if(Store.get(pk,sk).isEmpty())throw e;}
    }
    private static void sleep(long ms){try{Thread.sleep(ms);}catch(InterruptedException e){Thread.currentThread().interrupt();throw new RuntimeException(e);}}
}
