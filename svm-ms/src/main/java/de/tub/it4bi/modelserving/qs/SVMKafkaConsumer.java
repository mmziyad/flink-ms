package de.tub.it4bi.modelserving.qs;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Consume the SVM model from the Kafka topic and load to the specified state backend as queryable state
 */
public class SVMKafkaConsumer {

    public static void main(String[] args) {
        // parse input arguments
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        if (parameterTool.getNumberOfParameters() < 6) {
            System.out.println("Missing parameters!\nUsage: Kafka --topic <topic> " +
                    "--bootstrap.servers <kafka brokers> " +
                    "--zookeeper.connect <zk quorum> " +
                    "--group.id <some id>" +
                    "--checkpointDataUri <hdfs/local url> " +
                    "--stateBackend <rocksdb/memory/fs> ");
            return;
        }
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableSysoutLogging();
        env.getConfig().setGlobalJobParameters(parameterTool);

        // default checkpoint interval is set to 1 minute
        env.enableCheckpointing(parameterTool.getInt("checkPointInterval", 60000));
        // allow only one checkpoint to be in progress at the same time
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // fixed delay restart
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3, // number of restart attempts
                Time.of(10, TimeUnit.SECONDS) // delay
        ));

        // set state backend
        try {
            if (parameterTool.getRequired("stateBackend").equals("rocksdb")) {
                env.setStateBackend(new RocksDBStateBackend(parameterTool.getRequired("checkpointDataUri")));
            } else if (parameterTool.getRequired("stateBackend").equals("fs")) {
                env.setStateBackend(new FsStateBackend(parameterTool.getRequired("checkpointDataUri")));
            } else if (parameterTool.getRequired("stateBackend").equals("memory")) {
                env.setStateBackend(new MemoryStateBackend());
            }
        } catch (IOException e) {
            System.err.printf("Unable to create state backend: %s", parameterTool.getRequired("stateBackend"));
            e.printStackTrace();
        }

        DataStream<String> messageStream = env.addSource(new FlinkKafkaConsumer010<String>(
                parameterTool.getRequired("topic"),
                new SimpleStringSchema(),
                parameterTool.getProperties()));

        // From the kafka stream, store the feature ID and its value
        DataStream<Tuple2<String, String>> modelFactors = messageStream
                .rebalance() // evenly distribute the load to next operator
                .map(new MapFunction<String, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> map(String value) throws Exception {
                        String tokens[] = value.split(",");
                        return new Tuple2<String, String>(tokens[0], tokens[1]);
                    }
                });

        // store the values in the state
        ValueStateDescriptor<Tuple2<String, String>> modelState = new ValueStateDescriptor<>(
                "SVM_MODEL",
                TypeInformation.of(new TypeHint<Tuple2<String, String>>() {
                }));

        // make the state queryable
        modelFactors.keyBy(0)
                .asQueryableState("SVM_MODEL", modelState);

        try {
            env.execute("[SVM] model-serving with queryable-state");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
