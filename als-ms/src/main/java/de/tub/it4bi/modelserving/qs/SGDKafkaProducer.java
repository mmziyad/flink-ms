package de.tub.it4bi.modelserving.qs;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
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
 * Perform online updates to the Recommender model using SGD
 */
public class SGDKafkaProducer {
    public static void main(String[] args) {
        // create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // parse user parameters
        ParameterTool params = ParameterTool.fromArgs(args);
        // add the stream source
        TextInputFormat io = new TextInputFormat(new Path(params.getRequired("input")));
        io.setNestedFileEnumeration(true);
        DataStream<String> messageStream = env.readFile(io, params.getRequired("input"),
                FileProcessingMode.PROCESS_CONTINUOUSLY, params.getLong("interval", 60000));

        // Apply SGD and update the model
        DataStream<String> modelUpdates = messageStream
                .map(new InputParser(params))          // parse input rating data
                .map(new CurrentLatentFactors(params)) // retrieve current model parameters
                .map(new SGDRecommender(params))       // apply SGD
                .flatMap(new ModelUpdater());          // update model with the new parameters

        // write updated model to Kafka topic
        FlinkKafkaProducer010Configuration myProducerConfig = FlinkKafkaProducer010.writeToKafkaWithTimestamps(
                modelUpdates,                     // input stream
                params.getRequired("topic"), // target topic
                new SimpleStringSchema(),         // serialization schema
                params.getProperties());          // custom configuration for KafkaProducer (including broker list)

        // the following is necessary for at-least-once delivery guarantee
        myProducerConfig.setLogFailuresOnly(false);   // "false" by default
        myProducerConfig.setFlushOnCheckpoint(true);  // "false" by default

        try {
            env.execute("[ALS] online-updates using SGD");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Retrieves the current latent factors from the ALS model served using queryable state
     */
    private static class CurrentLatentFactors extends
            RichMapFunction<Tuple3<Integer, Integer, Double>,
                    Tuple3<Tuple2<Integer, WeightVector>, Tuple2<Integer, WeightVector>, Double>> {
        QueryClientHelper<String, Tuple2<String, String>> client;
        ParameterTool params;
        String userMeanLatentFactors;
        String itemMeanLatentFactors;

        public CurrentLatentFactors(ParameterTool params) {
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

            client = new QueryClientHelper<>(
                    jobManagerHost,
                    jobManagerPort,
                    jobId,
                    keySerializer,
                    valueSerializer,
                    queryTimeout);

            // TODO: Load the mean user and item latent factors
            // Option 1: store the mean vectors in the model itself
            // Option 2: pass as a parameter string (if 100s of factors, this could be troublesome.
            // Option 3: As broadcast variable
        }

        @Override
        public Tuple3<Tuple2<Integer, WeightVector>, Tuple2<Integer, WeightVector>, Double> map
                (Tuple3<Integer, Integer, Double> value) throws Exception {

            int userID = value.f0;
            int itemID = value.f1;
            double rating = value.f2;

            // suffix the IDs with relevant type
            String userQuery = userID + "-U";
            String itemQuery = itemID + "-I";

            Optional<Tuple2<String, String>> userTuple = client.queryState("ALS_MODEL", userQuery);
            Optional<Tuple2<String, String>> itemTuple = client.queryState("ALS_MODEL", itemQuery);

            // if no value is found in the model, use the mean values obtained after training
            String userLatentFactors = userTuple.map(x -> x.f1).orElse(userMeanLatentFactors);
            String itemLatentFactors = itemTuple.map(x -> x.f1).orElse(itemMeanLatentFactors);

            double[] userFactors = Arrays.stream(userLatentFactors.split(";"))
                    .mapToDouble(Double::parseDouble).toArray();

            double[] itemFactors = Arrays.stream(itemLatentFactors.split(";"))
                    .mapToDouble(Double::parseDouble).toArray();

            // TODO: If the model is updated with bias, retrieve and update here
            WeightVector userWeightVector = new WeightVector(new DenseVector(userFactors), 0.0);
            WeightVector itemWeightVector = new WeightVector(new DenseVector(itemFactors), 0.0);
            return new Tuple3<>(new Tuple2<>(userID, userWeightVector),
                    new Tuple2<>(itemID, itemWeightVector), rating);
        }

        @Override
        public void close() throws Exception {
            client.close();
        }
    }

    /**
     * Applies the SGD steps to update latent factors
     */
    private static class SGDRecommender implements
            MapFunction<Tuple3<Tuple2<Integer, WeightVector>, Tuple2<Integer, WeightVector>, Double>,
                    Tuple2<Tuple2<Integer, WeightVector>, Tuple2<Integer, WeightVector>>> {
        ParameterTool params;

        public SGDRecommender(ParameterTool params) {
            this.params = params;
        }

        @Override
        public Tuple2<Tuple2<Integer, WeightVector>, Tuple2<Integer, WeightVector>> map(
                Tuple3<Tuple2<Integer, WeightVector>, Tuple2<Integer, WeightVector>, Double> value) throws Exception {

            // hyper parameters for SGD online learning
            final double learningRate = params.getDouble("learningRate", 0.1);
            final double userRegularization = params.getDouble("userRegularization", 0.0);
            final double itemRegularization = params.getDouble("itemRegularization", 0.0);

            int userID = value.f0.f0;
            int itemID = value.f1.f0;
            double rating = value.f2;
            double userBias = value.f0.f1.intercept();
            double itemBias = value.f1.f1.intercept();
            Vector userVector = value.f0.f1.weights();
            Vector itemVector = value.f1.f1.weights();

            double prediction = userVector.dot(itemVector);
            double error = rating - prediction;

            // update bias
            userBias += learningRate * (error - userRegularization * userBias);
            itemBias += learningRate * (error - itemRegularization * itemBias);

            // update latent factors
            for (int i = 0; i < userVector.size(); i++) {
                double updatedLatentFactor = userVector.apply(i) + (learningRate *
                        (error * itemVector.apply(i) - userRegularization * userVector.apply(i)));
                userVector.update(i, updatedLatentFactor);
            }
            for (int i = 0; i < itemVector.size(); i++) {
                double updatedLatentFactor = itemVector.apply(i) + (learningRate *
                        (error * userVector.apply(i) - itemRegularization * itemVector.apply(i)));
                itemVector.update(i, updatedLatentFactor);
            }

            return new Tuple2<>(new Tuple2<>(userID, new WeightVector(userVector, userBias)),
                    new Tuple2<>(itemID, new WeightVector(itemVector, itemBias)));
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
            String tokens[] = value.split(params.get("fieldDelimiter", ","));
            return new Tuple3<>(Integer.parseInt(tokens[0]),
                    Integer.parseInt(tokens[1]),
                    Double.parseDouble(tokens[2]));
        }
    }

    /**
     * Prepare the output weight vectors in the model schema
     */
    private static class ModelUpdater implements FlatMapFunction<Tuple2<Tuple2<Integer, WeightVector>,
            Tuple2<Integer, WeightVector>>, String> {
        StringBuilder sb = new StringBuilder();

        @Override
        public void flatMap(Tuple2<Tuple2<Integer, WeightVector>, Tuple2<Integer, WeightVector>> value,
                            Collector<String> out) throws Exception {

            // prepare user record
            Vector userVector = value.f0.f1.weights();
            int userID = value.f0.f0;
            sb.setLength(0);
            for (int i = 0; i < userVector.size(); i++) {
                sb.append(userVector.apply(i));
                if (i != userVector.size() - 1) sb.append(";");
            }
            // TODO: Add bias to the user record in the model
            String userRecord = userID + ",U," + sb.toString();
            out.collect(userRecord);

            // prepare item record
            Vector itemVector = value.f1.f1.weights();
            int itemID = value.f1.f0;
            sb.setLength(0);
            for (int i = 0; i < itemVector.size(); i++) {
                sb.append(itemVector.apply(i));
                if (i != itemVector.size() - 1) sb.append(";");
            }
            // TODO: Add bias to the item record in the model
            String itemRecord = itemID + ",I," + sb.toString();
            out.collect(itemRecord);
        }
    }
}
