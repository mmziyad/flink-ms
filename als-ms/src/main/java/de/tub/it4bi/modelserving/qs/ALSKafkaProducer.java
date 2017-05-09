package de.tub.it4bi.modelserving.qs;

import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010.FlinkKafkaProducer010Configuration;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

/**
 * Created by zis on 06/05/17.
 */
public class ALSKafkaProducer {
    public static void main(String[] args) throws Exception {
        // create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // parse user parameters
        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        // add the stream source
        TextInputFormat io = new TextInputFormat(new Path(parameterTool.getRequired("input")));
        io.setNestedFileEnumeration(true);
        DataStream<String> messageStream = env.readFile(io, parameterTool.getRequired("input"));

        // write stream to Kafka
        FlinkKafkaProducer010Configuration myProducerConfig = FlinkKafkaProducer010.writeToKafkaWithTimestamps(
                messageStream,                           // input stream
                parameterTool.getRequired("topic"), // target topic
                new SimpleStringSchema(),                // serialization schema
                parameterTool.getProperties());          // custom configuration for KafkaProducer (including broker list)

        // the following is necessary for at-least-once delivery guarantee
        myProducerConfig.setLogFailuresOnly(false);   // "false" by default
        myProducerConfig.setFlushOnCheckpoint(true);  // "false" by default

        env.execute();
    }
}
