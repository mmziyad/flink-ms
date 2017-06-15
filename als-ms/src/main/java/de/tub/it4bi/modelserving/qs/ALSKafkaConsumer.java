package de.tub.it4bi.modelserving.qs;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueStateDescriptor;
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

/**
 * Created by zis on 06/05/17.
 */
public class ALSKafkaConsumer {

    public static void main(String[] args) {
        // parse input arguments
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        if (parameterTool.getNumberOfParameters() < 4) {
            System.out.println("Missing parameters!\nUsage: Kafka --topic <topic> " +
                    "--bootstrap.servers <kafka brokers> --zookeeper.connect <zk quorum> --group.id <some id>" +
                    "--checkpointDataUri <hdfs/local url> --stateBackend <rocksdb/memory/fs> ");
            return;
        }
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().disableSysoutLogging();
        // env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
        // env.enableCheckpointing(5000); // create a checkpoint every 5 seconds
        env.getConfig().setGlobalJobParameters(parameterTool); // make parameters available in the web interface

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

        // From the kafka stream, derive the ID, type (user/item), and feature values
        DataStream<Tuple2<String, String>> modelFactors = messageStream.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                String tokens[] = value.split(",");
                String id = tokens[0] + "-" + tokens[1];
                return new Tuple2<String, String>(id, tokens[2]);
            }
        });

        // store the values in the state
        ValueStateDescriptor<Tuple2<String, String>> modelState = new ValueStateDescriptor<>(
                "ALS_MODEL",
                TypeInformation.of(new TypeHint<Tuple2<String, String>>() {
                }));

        // make the state queryable
        modelFactors.keyBy(0)
                .asQueryableState("ALS_MODEL", modelState);

        try {
            env.execute("ALS Model Serving with Queryable State");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
