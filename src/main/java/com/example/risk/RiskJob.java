import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;

public class RiskJob {
    public static void main(String[] args) throws Exception {

        // create env
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // checkpoint
        env.enableCheckpointing(30_000, CheckpointingMode.EXACTLY_ONCE);

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setMinPauseBetweenCheckpointConfig(10_000);
        checkpointConfig.setCheckpointTimeout(120_000);

        // restart strategy
        env.setRestartStrategy(
            RestartStrategies.fixedDelayRestart(3, 5000)
        );

        System.out.println("Risk Detection Job Started.");

        env.execute("Rule Driven Risk Detection Job");
    }   
}