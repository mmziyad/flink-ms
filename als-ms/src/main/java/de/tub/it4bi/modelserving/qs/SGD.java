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
 * Perform online updates to the Recommender model using SGD
 */
public class SGD {
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

        // Apply SGD and update the model
        DataStream<String> modelUpdates = messageStream
                .map(new InputParser(params))
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
            env.execute("[ALS] online-updates using SGD");
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
            return new Tuple3<>(Integer.parseInt(tokens[0]), Integer.parseInt(tokens[1]),
                    Double.parseDouble(tokens[2]));
        }
    }

    /**
     * Apply SGD and produce updated vectors to be sent to Kafka
     */

    private static class SGDStep extends RichFlatMapFunction<Tuple3<Integer, Integer, Double>, String> {
        ParameterTool params;
        QueryClientHelper<String, Tuple2<String, String>> client;
        String userMeanFactors;
        String itemMeanFactors;

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
            final TypeSerializer<Tuple2<String, String>> valueSerializer = TypeInformation.of(new TypeHint<Tuple2<String, String>>() {
            }).createSerializer(new ExecutionConfig());

            client = new QueryClientHelper<>(jobManagerHost, jobManagerPort, jobId,
                    keySerializer, valueSerializer, queryTimeout);

            // load the mean vectors from the model and if not found, from user input.
            // Note: In the current version, mean vectors will be queried only at the start of this job.
            userMeanFactors = client
                    .queryState("ALS_MODEL", "MEAN-U")
                    .map(x -> x.f1).orElse(params.get("userMean"));
            itemMeanFactors = client
                    .queryState("ALS_MODEL", "MEAN-I")
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

            // hyper parameters for SGD online learning
            final double learningRate = params.getDouble("learningRate", 0.1);
            final double userReg = params.getDouble("userRegularization", 0.0);
            final double itemReg = params.getDouble("itemRegularization", 0.0);

            int userID = value.f0;
            int itemID = value.f1;
            double rating = value.f2;


            Tuple2<String, Double> userParams = getModelParameters(userID, "-U", userMeanFactors, 0.0);
            Tuple2<String, Double> itemParams = getModelParameters(itemID, "-I", itemMeanFactors, 0.0);

            double[] userFactors = Arrays.stream(userParams.f0.split(";"))
                    .mapToDouble(Double::parseDouble).toArray();

            double[] itemFactors = Arrays.stream(itemParams.f0.split(";"))
                    .mapToDouble(Double::parseDouble).toArray();

            // calculate the error
            double prediction = 0;
            for (int i = 0; i < userFactors.length; i++) {
                prediction += userFactors[i] * itemFactors[i];
            }
            double error = rating - prediction;

            double userBias = userParams.f1;
            double itemBias = itemParams.f1;

            // update bias
            userBias += learningRate * (error - userReg * userBias);
            itemBias += learningRate * (error - itemReg * itemBias);

            // update latent factors
            String[] outputUserFactors = new String[userFactors.length];
            String[] outputItemFactors = new String[itemFactors.length];

            for (int i = 0; i < userFactors.length; i++) {
                outputUserFactors[i] = String.valueOf(userFactors[i] + learningRate *
                        (error * itemFactors[i] - userReg * userFactors[i]));
            }

            for (int i = 0; i < itemFactors.length; i++) {
                outputItemFactors[i] = String.valueOf(itemFactors[i] + learningRate *
                        (error * userFactors[i] - itemReg * itemFactors[i]));
            }

            // TODO: Add updated bias to the user record in the model
            // prepare user record
            String userRecord = userID + ",U," + String.join(";", outputUserFactors);
            out.collect(userRecord);

            // prepare item record
            String itemRecord = itemID + ",I," + String.join(";", outputItemFactors);
            out.collect(itemRecord);
        }

        private Tuple2<String, Double> getModelParameters(int id, String suffix, String meanFactors, double initialBias) {
            String queryKey = id + suffix;
            Optional<Tuple2<String, String>> output = null;
            try {
                output = client.queryState("ALS_MODEL", queryKey);
            } catch (Exception e) {
                e.printStackTrace();
            }
            String latentFactors = output.map(x -> x.f1).orElse(meanFactors);
            // double bias = output.map(x -> x.f2).orElse(initialBias);

            if (latentFactors.contains("NaN")) System.out.println("NaN detected for: " + id + suffix);

            //TODO: If the model contains bias, use that bias instead of user provided bias
            return new Tuple2<>(latentFactors, initialBias);
        }
    }
}