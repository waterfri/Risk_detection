package main.java.com.example.risk.model;

public class RuleEnvelope{
    private String op; // "UPSERT" or "DELETE"
    private RiskRule rule;

    public RuleEnvelope() {}

    public String getOp() { return op; }
    public void setOp(String op) { this.op = op; }

    public RiskRule getRule() { return rule; }
    public void setRule(RiskRule rule) { this.rule = rule; }
    
}