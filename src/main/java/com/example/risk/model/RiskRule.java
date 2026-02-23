package com.example.risk.model;

public class RiskRule {

    private String rule_id;
    private boolean enabled = true;

    // v1: count_in_window
    private String rule_type;
    private String event_type;
    private int threshold;
    private int window_minutes;

    private String action;     // alert / block / tag ...
    private String severity;   // low / mid / high
    private String updated_at; // ISO time

    public RiskRule() {}

    public String getRule_id() { return rule_id; }
    public void setRule_id(String rule_id) { this.rule_id = rule_id; }

    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }

    public String getRule_type() { return rule_type; }
    public void setRule_type(String rule_type) { this.rule_type = rule_type; }

    public String getEvent_type() { return event_type; }
    public void setEvent_type(String event_type) { this.event_type = event_type; }

    public int getThreshold() { return threshold; }
    public void setThreshold(int threshold) { this.threshold = threshold; }

    public int getWindow_minutes() { return window_minutes; }
    public void setWindow_minutes(int window_minutes) { this.window_minutes = window_minutes; }

    public String getAction() { return action; }
    public void setAction(String action) { this.action = action; }

    public String getSeverity() { return severity; }
    public void setSeverity(String severity) { this.severity = severity; }

    public String getUpdated_at() { return updated_at; }
    public void setUpdated_at(String updated_at) { this.updated_at = updated_at; }
}