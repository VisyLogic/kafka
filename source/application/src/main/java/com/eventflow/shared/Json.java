package com.eventflow.shared;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.*;

public final class Json {
    public static final ObjectMapper MAPPER = new ObjectMapper();
    public static String write(Object value) {
        try { return MAPPER.writeValueAsString(value); }
        catch (Exception e) { throw new IllegalArgumentException("Cannot encode JSON", e); }
    }
    public static Map<String,Object> read(String text) {
        try { return MAPPER.readValue(text, new TypeReference<>() {}); }
        catch (Exception e) { throw new IllegalArgumentException("Invalid JSON", e); }
    }
    public static int number(Map<String,Object> m, String key, int fallback) {
        return m.containsKey(key) ? Integer.parseInt(m.get(key).toString()) : fallback;
    }
    public static String text(Map<String,Object> m, String key) { return Objects.toString(m.get(key), ""); }
    @SuppressWarnings("unchecked") public static List<Map<String,Object>> rows(Object value) {
        return (List<Map<String,Object>>) value;
    }
    public static String env(String name, String fallback) { return System.getenv().getOrDefault(name,fallback); }
    public static void log(String component, String message) {
        System.out.println(write(Map.of("time",java.time.Instant.now().toString(),"component",component,"message",message)));
    }
}
