package com.example.risk.model;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public class RiskResult {

    // basic info
    private String rule_id;
    private String user_id;
    private String event_id;
    private String event_type;

    // strategy result
    private boolean hit;
    private String action;
    private String severity;

    private String hit_time; // ISO format

    // additional info
    private Map<String, String> detail = new HashMap<>();

    public RiskResult() {}

    public static RiskResult build(RiskRule rule, BehaviorEvent event) {
        RiskResult result = new RiskResult();
        result.rule_id = rule.getRule_id();
        result.user_id = event.getUser_id();
        result.event_id = event.getEvent_id();
        result.event_type = event.getEvent_type();
        result.hit = true;
        result.action = rule.getAction();
        result.severity = rule.getSeverity();
        result.hit_time = Instant.now().toString(); // processing time
        return result;
    }

    // ===== getters & setters =====

    public String getRule_id() { return rule_id; }
    public void setRule_id(String rule_id) { this.rule_id = rule_id; }

    public String getUser_id() { return user_id; }
    public void setUser_id(String user_id) { this.user_id = user_id; }

    public String getEvent_id() { return event_id; }
    public void setEvent_id(String event_id) { this.event_id = event_id; }

    public String getEvent_type() { return event_type; }
    public void setEvent_type(String event_type) { this.event_type = event_type; }

    public boolean isHit() { return hit; }
    public void setHit(boolean hit) { this.hit = hit; }

    public String getAction() { return action; }
    public void setAction(String action) { this.action = action; }

    public String getSeverity() { return severity; }
    public void setSeverity(String severity) { this.severity = severity; }

    public String getHit_time() { return hit_time; }
    public void setHit_time(String hit_time) { this.hit_time = hit_time; }

    public Map<String, String> getDetail() { return detail; }
    public void setDetail(Map<String, String> detail) { this.detail = detail; }
}