package com.eventflow.web;

import com.eventflow.shared.Json;
import java.time.Instant;
import java.util.*;
import java.util.function.BiFunction;

/** Explicit upload-and-start trigger. Uploading in the S3 console alone does not run accounting. */
public final class S3EventTrigger {
    public interface Artifacts {
        void save(String key,Map<String,Object> request);
        Map<String,Object> load(String key);
        default Map<String,Object> metadata(){return Map.of();}
    }
    private final Artifacts artifacts;
    private final BiFunction<Map<String,Object>,Map<String,Object>,Map<String,Object>> start;
    private final String bucket;
    private final int maxLoans;
    public S3EventTrigger(Artifacts artifacts,BiFunction<Map<String,Object>,Map<String,Object>,Map<String,Object>> start,String bucket,int maxLoans){
        this.artifacts=artifacts;this.start=start;this.bucket=bucket;this.maxLoans=maxLoans;
    }
    @SuppressWarnings("unchecked") public Map<String,Object> upload(Map<String,Object> body){
        String name=Json.text(body,"fileName");
        if(!name.matches("(?i)[a-z0-9_. -]{1,120}\\.json"))throw new IllegalArgumentException("Choose a JSON filename without path separators");
        if(!(body.get("request") instanceof Map<?,?>))throw new IllegalArgumentException("request must be a JSON object");
        var request=(Map<String,Object>)body.get("request");validate(request);
        String key="event-requests/"+UUID.randomUUID()+"/"+name;
        artifacts.save(key,request);
        return startStored(key,name);
    }
    public Map<String,Object> importObject(String key){
        if(!key.startsWith("event-requests/")||key.contains("..")||!key.endsWith(".json")||key.length()>400)
            throw new IllegalArgumentException("Use a JSON object key under event-requests/ in the configured bucket");
        return startStored(key,key.substring(key.lastIndexOf('/')+1));
    }
    private Map<String,Object> startStored(String key,String name){
        var stored=artifacts.load(key);validate(stored);
        var origin=new LinkedHashMap<String,Object>(Map.of("type","S3_FILE","bucket",bucket,"objectKey",key,"fileName",name,"startedAt",Instant.now().toString()));
        for(String field:List.of("objectVersionId","objectETag"))if(artifacts.metadata().containsKey(field))origin.put(field,artifacts.metadata().get(field));
        var result=new LinkedHashMap<>(start.apply(stored,origin));result.put("trigger",origin);return result;
    }
    private void validate(Map<String,Object> request){
        var allowed=Set.of("totalLoans","poolSize","level","eventType","accountingDate");
        if(!allowed.containsAll(request.keySet()))throw new IllegalArgumentException("Request files accept test settings only; remove unknown fields");
        for(String field:allowed)
            if(!request.containsKey(field))throw new IllegalArgumentException("Request file is missing "+field);
        RunRequest.parse(request,maxLoans);
    }
}
