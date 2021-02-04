package com.zlh.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @package com.zlh.util
 * @company: dacheng
 * @author: zlh
 * @createDate: 2020/12/7
 */
@Slf4j
public class FlinkEnvUtil {
    public static StreamExecutionEnvironment getFlinkEnv(ParameterTool params){
        //获取Flink的运行环境
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        /**
         * 执行相应操作的机器的系统时间
         */
        executionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        /**
         * make parameters available in the web interface
         */
        executionEnvironment.getConfig().setGlobalJobParameters(params);
        //设置此可以屏蔽掉日记打印情况
        executionEnvironment.getConfig().disableSysoutLogging();
        //每隔60s启动一个检查点
        executionEnvironment.enableCheckpointing(60000);
        //有且仅有一次(默认)
        //当不开启Checkpoint时，节点发生故障时可能会导致数据丢失，这就是At-Most-Once
        //当开启Checkpoint但不进行Barrier对齐时，对于有多个输入流的节点如果发生故障，会导致有一部分数据可能会被处理多次，这就是At-Least-Once
        //当开启Checkpoint并进行Barrier对齐时，可以保证每条数据在故障恢复时只会被重放一次，这就是Exactly-Once
        executionEnvironment.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //checkpoint最小间隔 如果两次Checkpoint的时间很短，会导致整个系统大部分资源都用于执行Checkpoint，影响正常作业的执行
        executionEnvironment.getCheckpointConfig().setMinPauseBetweenCheckpoints(60000);
        //checkpoint超时时间一分钟，超出丢弃
        executionEnvironment.getCheckpointConfig().setCheckpointTimeout(60000);
        //同一时间只允许一个检查点
        executionEnvironment.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        //flink程序被cancel后，RETAIN_ON_CANCELLATION 会保留checkpoint数据 DELETE_ON_CANCELLATION 自动删除
        executionEnvironment.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
        //用于指定在checkpoint发生异常的时候，是否应该fail该task，默认为true，如果设置为false，则task会拒绝checkpoint然后继续运行
        //executionEnvironment.getCheckpointConfig().setFailOnCheckpointingErrors(false);

        return executionEnvironment ;
    }
}
