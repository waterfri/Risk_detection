package com.example.risk;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import com.example.risk.model.BehaviorEvent;
import com.example.risk.model.CountWindowState;
import com.example.risk.model.RiskResult;
import com.example.risk.model.RuleEnvelope;
import com.example.risk.model.RiskRule;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;

import java.time.Duration;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

public class RiskJob {

    public static final MapStateDescriptor<String, RiskRule> RULES_BROADCAST_DESC = 
        new MapStateDescriptor<>(
            "rules-broadcast",
            Types.STRING,
            Types.POJO(RiskRule.class)
        );
    public static void main(String[] args) throws Exception {

        // ===== create env =====
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // checkpoint
        env.enableCheckpointing(30_000, CheckpointingMode.EXACTLY_ONCE);

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setMinPauseBetweenCheckpoints(10_000);
        checkpointConfig.setCheckpointTimeout(120_000);

        // restart strategy
        env.setRestartStrategy(
            RestartStrategies.fixedDelayRestart(3, 5000)
        );


        // ===== Kafka Config =====
        final String bootstrapServers = "kafka:9092";
        final String behaviorTopic = "behavior-events";

        final String rulesTopic = "risk-rules";


        // ===== Kafka behavior Source =====
        KafkaSource<String> behaviorSource = KafkaSource.<String>builder()
            .setBootstrapServers(bootstrapServers)
            .setTopics(behaviorTopic)
            .setGroupId("risk-job-behavior")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        DataStream<String> behaviorRaw = env.fromSource(
            behaviorSource,
            WatermarkStrategy.noWatermarks(),
            "behavior-source"
        );

        behaviorRaw.print("RAW");

        // ===== Kafka Rule Source =====
        KafkaSource<String> ruleSource = KafkaSource.<String>builder()
            .setBootstrapServers(bootstrapServers)
            .setTopics(rulesTopic)
            .setGroupId("risk-job-rules")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        DataStream<String> rulesRaw = env.fromSource(
            ruleSource,
            WatermarkStrategy.noWatermarks(),
            "rules-source"
        );

        rulesRaw.print("RULE_RAW");
        
        // ===== JSON parse =====
        ObjectMapper mapper = new ObjectMapper();
        
        DataStream<BehaviorEvent> behaviorParsed = behaviorRaw
            .map(json -> mapper.readValue(json, BehaviorEvent.class))
            .returns(BehaviorEvent.class);

        DataStream<RuleEnvelope> rulesParsed = rulesRaw
            .map((String json) -> mapper.readValue(json, RuleEnvelope.class))
            .returns(RuleEnvelope.class);
        
        // ===== watermark + event_time =====
        DataStream<BehaviorEvent> behaviorWithWm = behaviorParsed
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<BehaviorEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                    .withTimestampAssigner(
                        (event, timestamp) -> event.getEventTimeMillis()
                    )
            );

        // ===== Broadcast Rules =====
        BroadcastStream<RuleEnvelope> rulesBroadcast = 
            rulesParsed.broadcast(RULES_BROADCAST_DESC);

        DataStream<String> results = behaviorWithWm
            .keyBy(BehaviorEvent::getUser_id)
            .connect(rulesBroadcast)
            .process(new RiskMatchFunction())
            .map(r -> mapper.writeValueAsString(r))
            .returns(String.class);

        results.print("Risk");

        env.execute("Rule Driven Risk Detection Job");
    }
    
    public static class RiskMatchFunction extends KeyedBroadcastProcessFunction<String, BehaviorEvent, RuleEnvelope, RiskResult> {
        
        private transient MapState<String, CountWindowState> perUserRuleState;

        // initialize Keyed state descriptor and get state
        @Override
        public void open(Configuration parameters) {
            MapStateDescriptor<String, CountWindowState> desc =
                new MapStateDescriptor<>(
                    "per-user-rule-state",
                    Types.STRING,
                    Types.POJO(CountWindowState.class)
                );

            perUserRuleState = getRuntimeContext().getMapState(desc);
        }

        @Override
        public void processBroadcastElement(
            RuleEnvelope value,
            Context ctx,
            Collector<RiskResult> out) throws Exception {

            BroadcastState<String, RiskRule> rulesState = ctx.getBroadcastState(RULES_BROADCAST_DESC);

            // todo
            String op = value.getOp();
            RiskRule rule = value.getRule();

            if(op == null || rule == null || rule.getRule_id() == null) {
                return;
            }

            if("DELETE".equalsIgnoreCase(op)) {
                rulesState.remove(rule.getRule_id());
            }
            else{
                rulesState.put(rule.getRule_id(), rule); // UPSERT
            }

        }


        @Override
        public void processElement(
            BehaviorEvent evt,
            ReadOnlyContext ctx,
            Collector<RiskResult> out) throws Exception {

            ReadOnlyBroadcastState<String, RiskRule> rulesState = ctx.getBroadcastState(RULES_BROADCAST_DESC);
        
            // todo
            for(var entry : rulesState.immutableEntries()){
                RiskRule rule = entry.getValue();
                if(rule == null) continue;

                // enabled
                if(!rule.isEnabled()) continue;

                // match event_type
                if(evt.getEvent_type() == null || rule.getEvent_type() == null) continue;
                if(!evt.getEvent_type().equals(rule.getEvent_type())) continue;

                // only implement count_in_window in v1
                if(rule.getRule_type() == null || !"count_in_window".equals(rule.getRule_type())) continue;

                // evaluate count window
                boolean hit = evaluateCountInWindow(evt, rule);

                // output only when hit
                if(hit){
                    RiskResult rr = RiskResult.build(rule, evt);
                    rr.getDetail().put("threshold", String.valueOf(rule.getThreshold()));
                    rr.getDetail().put("window_minutes", String.valueOf(rule.getWindow_minutes()));
                    out.collect(rr);
                }
            }
        }

        private boolean evaluateCountInWindow(BehaviorEvent evt, RiskRule rule) throws Exception {
            String ruleId = rule.getRule_id();

            long windowMs = rule.getWindow_minutes() * 60_000L;
            long eventTs = evt.getEventTimeMillis();

            CountWindowState st = perUserRuleState.get(ruleId); // get current user's state according to the specifc rule

            // first time for this (user, rule)
            if(st == null){
                st = new CountWindowState();
                st.setWindow_start_ms(eventTs);
                st.setCount(1);
                st.setLast_event_ms(eventTs);
                perUserRuleState.put(ruleId, st);
                return false;
            }

            // window expired -> reset
            if(eventTs - st.getWindow_start_ms() >= windowMs){
                st.setWindow_start_ms(eventTs);
                st.setCount(1);
                st.setLast_event_ms(eventTs);
                perUserRuleState.put(ruleId, st);
                return false;
            }

            // within window -> increment
            st.setCount(st.getCount() + 1);
            st.setLast_event_ms(eventTs);
            perUserRuleState.put(ruleId, st);

            return st.getCount() == rule.getThreshold() + 1; // only alert once
        }
    }

}