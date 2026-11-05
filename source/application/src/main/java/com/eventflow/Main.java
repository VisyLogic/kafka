package com.eventflow;
import com.eventflow.shared.*;
import com.eventflow.mock.MockGetData;
import com.eventflow.adapter.EventAdapter;
import com.eventflow.coordinator.BatchCoordinator;
import com.eventflow.sourcing.*;
import com.eventflow.activity.ActivityService;
import com.eventflow.web.ConsoleServer;

/** One image, explicit service roles. Each role runs in its own container/pod. */
public final class Main {
    public static void main(String[] args)throws Exception{
        String role=Json.env("SERVICE_ROLE","console");Json.log("startup","Starting "+role);
        if(role.equals("mock-source")){MockGetData.run();return;}
        // Startup ordering: resources must exist before any consumer runs.
        for(int attempt=0;;attempt++){try{Store.config();break;}catch(Exception e){if(attempt>=90)throw e;Thread.sleep(2000);}}
        switch(role){
            case "console" -> {
                Kafka.topics(6);
                new Thread(BatchCoordinator::run,"batch-coordinator").start();
                new Thread(()->WorkflowWorker.poll("eventflow-await-accounting",WorkflowWorker::awaitAccounting),"workflow-completion").start();
                ConsoleServer.run();
            }
            case "adapter" -> EventAdapter.run();
            case "sourcing" -> SourcingService.run();
            case "activity" -> ActivityService.run();
            default -> throw new IllegalArgumentException("Unknown service role: "+role);
        }
    }
}
