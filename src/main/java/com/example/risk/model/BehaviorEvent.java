package com.example.risk.model;

import java.time.Instant;

public class BehaviorEvent {

    // ===== JSON fields (keep aligned with JSON keys) =====
    private String event_id;
    private String user_id;
    private String event_type;
    private String page;
    private String event_time; // ISO-8601 like "2024-01-01T12:00:01Z"
    private String ip;

    // ===== Required by Flink/Jackson: no-arg constructor =====
    public BehaviorEvent() {}

    // ===== Getters/Setters: required for POJO + Jackson =====
    public String getEvent_id() { return event_id; }
    public void setEvent_id(String event_id) { this.event_id = event_id; }

    public String getUser_id() { return user_id; }
    public void setUser_id(String user_id) { this.user_id = user_id; }

    public String getEvent_type() { return event_type; }
    public void setEvent_type(String event_type) { this.event_type = event_type; }

    public String getPage() { return page; }
    public void setPage(String page) { this.page = page; }

    public String getEvent_time() { return event_time; }
    public void setEvent_time(String event_time) { this.event_time = event_time; }

    public String getIp() { return ip; }
    public void setIp(String ip) { this.ip = ip; }

    // ===== Helper methods for Flink event time =====
    public long getEventTimeMillis() {
        // Why: WatermarkStrategy expects a timestamp (long millis).
        // If event_time is invalid, we return 0 to avoid throwing and breaking the job;
        // bad records should be filtered/side-output earlier, but this is a safe guard.
        try {
            return Instant.parse(event_time).toEpochMilli();
        } catch (Exception e) {
            return 0L;
        }
    }

    // Convenience methods (optional but makes job code cleaner)
    public String userId() { return user_id; }
    public String eventType() { return event_type; }
}