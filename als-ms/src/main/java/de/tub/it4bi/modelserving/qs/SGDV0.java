package de.tub.it4bi.modelserving.qs;

import de.tub.it4bi.modelserving.utils.QueryClientHelper;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.ml.common.WeightVector;
import org.apache.flink.ml.math.DenseVector;
import org.apache.flink.ml.math.Vector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010.FlinkKafkaProducer010Configuration;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.Optional;

/**
 * Perform online updates to the Recommender model using SGDV0
 */
public class SGDV0 {
    public static void main(String[] args) {
        // create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // parse user parameters
        ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);

        // add the stream source
        TextInputFormat io = new TextInputFormat(new Path(params.getRequired("input")));
        io.setNestedFileEnumeration(true);

        // set the process as a continuous stream job or process once job
        DataStream<String> messageStream = null;
        if (params.getRequired("mode").equals("continuous")) {
            messageStream = env.readFile(
                    io,
                    params.getRequired("input"),
                    FileProcessingMode.PROCESS_CONTINUOUSLY,
                    params.getLong("interval", 60000));
        } else if (params.getRequired("mode").equals("once")) {
            messageStream = env.readFile(
                    io,
                    params.getRequired("input"),
                    FileProcessingMode.PROCESS_ONCE,
                    params.getLong("interval", 60000));
        } else {
            System.err.println("Invalid mode. Specify --mode [continuous|once] ");
            System.exit(-1);
        }

        // Apply SGDV0 and update the model
        DataStream<String> modelUpdates = messageStream
                .map(new InputParser(params))// parse input rating data
                .flatMap(new SGDStep(params));

        if (params.getRequired("outputMode").equals("kafka")) {
            // write updated model to Kafka topic
            FlinkKafkaProducer010Configuration myProducerConfig = FlinkKafkaProducer010.writeToKafkaWithTimestamps(
                    modelUpdates,                     // input stream
                    params.getRequired("topic"), // target topic
                    new SimpleStringSchema(),         // serialization schema
                    params.getProperties());          // custom configuration for KafkaProducer (including broker list)

            // the following is necessary for at-least-once delivery guarantee
            myProducerConfig.setLogFailuresOnly(false);   // "false" by default
            myProducerConfig.setFlushOnCheckpoint(true);  // "false" by default
        } else if (params.getRequired("outputMode").equals("hdfs") && params.has("outputPath")) {
            modelUpdates.writeAsText(params.get("outputPath"), FileSystem.WriteMode.OVERWRITE);
        }
        try {
            env.execute("[ALS] online-updates using SGDV0");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Parse the rating data, to derive userID, itemID and rating
     */
    private static class InputParser implements MapFunction<String, Tuple3<Integer, Integer, Double>> {
        ParameterTool params;

        public InputParser(ParameterTool params) {
            this.params = params;
        }

        @Override
        public Tuple3<Integer, Integer, Double> map(String value) throws Exception {
            // values are expected in the order: userID, itemID, rating
            String tokens[] = value.split(params.get("fieldDelimiter", "\t"));
            return new Tuple3<>(Integer.parseInt(tokens[0]), Integer.parseInt(tokens[1]), Double.parseDouble(tokens[2]));
        }
    }

    /**
     * Apply SGDV0 and produce updated vectors to be sent to Kafka
     */

    private static class SGDStep extends RichFlatMapFunction<Tuple3<Integer, Integer, Double>, String> {
        ParameterTool params;
        QueryClientHelper<String, Tuple2<String, String>> client;
        String userMeanFactors;
        String itemMeanFactors;
        StringBuilder sb = new StringBuilder();

        public SGDStep(ParameterTool params) {
            this.params = params;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            final JobID jobId = JobID.fromHexString(params.getRequired("jobId"));
            final String jobManagerHost = params.get("jobManagerHost", "localhost");
            final int jobManagerPort = params.getInt("jobManagerPort", 6123);
            final Time queryTimeout = Time.seconds(params.getInt("queryTimeout", 5));

            final StringSerializer keySerializer = StringSerializer.INSTANCE;
            final TypeSerializer<Tuple2<String, String>> valueSerializer =
                    TypeInformation.of(new TypeHint<Tuple2<String, String>>() {
                    }).createSerializer(new ExecutionConfig());

            client = new QueryClientHelper<>(jobManagerHost, jobManagerPort, jobId,
                    keySerializer, valueSerializer, queryTimeout);

            // load the mean vectors from the model and if not found, from user input.
            // Note: In the current version, mean vectors will be queried only at the start of this job.
            userMeanFactors = client.queryState("ALS_MODEL", "MEAN-U")
                    .map(x -> x.f1).orElse(params.get("userMean"));
            itemMeanFactors = client.queryState("ALS_MODEL", "MEAN-I")
                    .map(x -> x.f1).orElse(params.get("itemMean"));

            if (userMeanFactors == null || itemMeanFactors == null) {
                throw new IllegalStateException("Unable to load User mean or item mean factors.");
            }
        }

        @Override
        public void close() throws Exception {
            client.close();
        }

        @Override
        public void flatMap(Tuple3<Integer, Integer, Double> value, Collector<String> out) throws Exception {

            // hyper parameters for SGDV0 online learning
            final double learningRate = params.getDouble("learningRate", 0.1);
            final double userReg = params.getDouble("userRegularization", 0.0);
            final double itemReg = params.getDouble("itemRegularization", 0.0);

            int userID = value.f0;
            int itemID = value.f1;
            double rating = value.f2;

            WeightVector uwv = extractWeightVector(userID, "-U", userMeanFactors, 0.0);
            WeightVector iwv = extractWeightVector(itemID, "-I", itemMeanFactors, 0.0);

            double userBias = uwv.intercept();
            double itemBias = iwv.intercept();
            Vector userVector = uwv.weights();
            Vector itemVector = iwv.weights();

            double prediction = userVector.dot(itemVector);
            double error = rating - prediction;

            // update bias
            userBias += learningRate * (error - userReg * userBias);
            itemBias += learningRate * (error - itemReg * itemBias);

            // update latent factors
            for (int i = 0; i < userVector.size(); i++) {
                double updatedLatentFactor = userVector.apply(i) + (learningRate *
                        (error * itemVector.apply(i) - userReg * userVector.apply(i)));
                userVector.update(i, updatedLatentFactor);
            }
            for (int i = 0; i < itemVector.size(); i++) {
                double updatedLatentFactor = itemVector.apply(i) + (learningRate *
                        (error * userVector.apply(i) - itemReg * itemVector.apply(i)));
                itemVector.update(i, updatedLatentFactor);
            }

            // prepare user record
            sb.setLength(0);
            for (int i = 0; i < userVector.size(); i++) {
                sb.append(userVector.apply(i));
                if (i != userVector.size() - 1) sb.append(";");
            }
            // TODO: Add updated bias to the user record in the model
            String userRecord = userID + ",U," + sb.toString();
            if (userRecord.contains("NaN")) {
                System.out.println("NaN in userRecord" + userRecord);
            } else {
                out.collect(userRecord);
            }

            // prepare item record
            sb.setLength(0);
            for (int i = 0; i < itemVector.size(); i++) {
                sb.append(itemVector.apply(i));
                if (i != itemVector.size() - 1) sb.append(";");
            }
            // TODO: Add updated bias to the item record in the model
            String itemRecord = itemID + ",I," + sb.toString();

            if (itemRecord.contains("NaN")) {
                System.out.println("NaN in itemRecord" + itemRecord);
            } else {
                out.collect(itemRecord);
            }
        }

        private WeightVector extractWeightVector(int id, String suffix, String meanFactors, double bias) {
            String queryKey = id + suffix;
            Optional<Tuple2<String, String>> output = null;

            try {
                output = client.queryState("ALS_MODEL", queryKey);
            } catch (Exception e) {
                e.printStackTrace();
            }
            String result;
            // String result = output.map(x -> x.f1).orElse(meanFactors);

            if (output.isPresent()) {
                result = output.get().f1;
            } else {
                result = meanFactors;
            }
            double[] latentFactors = Arrays
                    .stream(result.split(";"))
                    .mapToDouble(Double::parseDouble)
                    .toArray();

            for (int i = 0; i < latentFactors.length; i++) {
                if (((Double) latentFactors[i]).isNaN()) {
                    System.out.println("NaN detected for: " + id + suffix);
                }
            }

            //TODO: If the model contains bias, use that bias instead of user provided bias
            return new WeightVector(new DenseVector(latentFactors), bias);
        }
    }
}