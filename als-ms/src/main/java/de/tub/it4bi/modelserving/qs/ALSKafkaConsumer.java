package de.tub.it4bi.modelserving.qs;

import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.util.Arrays;

;

/**
 * Created by zis on 06/05/17.
 */
public class ALSKafkaConsumer {

    public static void main(String[] args) throws Exception {
        // parse input arguments
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        if (parameterTool.getNumberOfParameters() < 4) {
            System.out.println("Missing parameters!\nUsage: Kafka --topic <topic> " +
                    "--bootstrap.servers <kafka brokers> --zookeeper.connect <zk quorum> --group.id <some id>");
            return;
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // env.getConfig().disableSysoutLogging();
        // env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
        // env.enableCheckpointing(5000); // create a checkpoint every 5 seconds
        env.getConfig().setGlobalJobParameters(parameterTool); // make parameters available in the web interface

        DataStream<String> messageStream = env.addSource(new FlinkKafkaConsumer010<String>(
                parameterTool.getRequired("topic"),
                new SimpleStringSchema(),
                parameterTool.getProperties()));

        DataStream<Tuple2<String, ArrayRealVector>> modelFactors = messageStream.map(new MapFunction<String, Tuple2<String, ArrayRealVector>>() {
            public Tuple2<String, ArrayRealVector> map(String value) throws Exception {
                String tokens[] = value.split(",");
                String id = tokens[0] + "-" + tokens[1];
                double features[] = Arrays.stream(tokens[2].split(";"))
                        .mapToDouble(Double::parseDouble)
                        .toArray();
                return new Tuple2<>(id, new ArrayRealVector(features));
            }
        });

        // store the values in the state
        ValueStateDescriptor<Tuple2<String, ArrayRealVector>> modelState = new
                ValueStateDescriptor<Tuple2<String, ArrayRealVector>>(
                "ALS_MODEL",
                TypeInformation.of(new TypeHint<Tuple2<String, ArrayRealVector>>() {}));

        modelFactors
                .keyBy(0)
                .asQueryableState("ALS_MODEL", modelState);

        // Get the Job ID for querying client
        JobGraph jobGraph = env.getStreamGraph().getJobGraph();
        System.out.println("[info] Job ID: " + jobGraph.getJobID());
        System.out.println();

        env.execute("ALS Model Serving with Queryable State");
    }
}
