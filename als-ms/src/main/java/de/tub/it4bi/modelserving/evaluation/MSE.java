package de.tub.it4bi.modelserving.evaluation;

import de.tub.it4bi.modelserving.utils.QueryClientHelper;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.Optional;

/**
 * Calculate the mean squared error (MSE) for a given rating data set,
 * using the ALS model deployed as a queryable state.
 * Rating data is in the form:
 * userId <delimiter> itemID <delimiter> rating
 */
public class MSE {
    public static void main(String[] args) {
        // set up the environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);

        // read input data
        DataSet<Tuple3<Integer, Integer, Double>> inputDS = env
                .readCsvFile(params.getRequired("input"))
                .fieldDelimiter(params.get("fieldDelimiter", "\t"))
                .ignoreFirstLine()
                .types(Integer.class, Integer.class, Double.class);

        // query the model for predictions
        DataSet<Tuple2<Double, Double>> predictions = inputDS
                .groupBy(0)                   // assuming there are more userIDs than itemIDs
                .reduceGroup(new ModelScore(params)); // each userID is queried only once

        // calculate the mean squared error
        DataSet<Double> mse = predictions
                .map(new MapFunction<Tuple2<Double, Double>, Tuple2<Double, Long>>() {
                    @Override
                    public Tuple2<Double, Long> map(Tuple2<Double, Double> value) throws Exception {
                        return new Tuple2<>((value.f0 - value.f1) * (value.f0 - value.f1), 1l);
                    }
                }).reduce(new ReduceFunction<Tuple2<Double, Long>>() {
                    @Override
                    public Tuple2<Double, Long> reduce(Tuple2<Double, Long> left, Tuple2<Double, Long> right)
                            throws Exception {
                        return new Tuple2<>(left.f0 + right.f0, left.f1 + right.f1);
                    }
                }).map(new MapFunction<Tuple2<Double, Long>, Double>() {
                    @Override
                    public Double map(Tuple2<Double, Long> value) throws Exception {
                        return value.f0 / value.f1;
                    }
                });
        // Note: Lambda functions are not working in IntelliJ Idea.
        //.map(value -> new Tuple2<>((value.f0 - value.f1) * (value.f0 - value.f1), 1l))
        //.reduce((left, right) -> new Tuple2<>(left.f0 + right.f0, left.f1 + right.f1))
        //.map(value -> value.f0 / value.f1);

        //prepare the output
        try {
            if (params.has("output")) {
                mse.writeAsText(params.get("output"), FileSystem.WriteMode.OVERWRITE);
                env.execute("[ALS] mean-squared-error");
            } else {
                System.out.println("Printing result to stdout. Use --output to specify output path.");
                mse.print();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static class ModelScore extends
            RichGroupReduceFunction<Tuple3<Integer, Integer, Double>, Tuple2<Double, Double>> {

        QueryClientHelper<String, Tuple2<String, String>> client;
        ParameterTool params;

        public ModelScore(ParameterTool params) {
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
        }

        @Override
        public void close() throws Exception {
            client.close();
        }

        @Override
        public void reduce(Iterable<Tuple3<Integer, Integer, Double>> values,
                           Collector<Tuple2<Double, Double>> out) throws Exception {

            String userID;
            double[] userFactors = null;
            boolean isUserIdSet = false;

            for (Tuple3<Integer, Integer, Double> value : values) {
                if (!isUserIdSet) {
                    userID = String.valueOf(value.f0) + "-U";
                    Optional<Tuple2<String, String>> userRow = client.queryState("ALS_MODEL", userID);
                    if (userRow.isPresent()) {
                        userFactors = Arrays.stream(userRow.get().f1.split(";"))
                                .mapToDouble(Double::parseDouble).toArray();
                    } else {
                        System.err.println("No record found for the user ID: " + userID);
                        break;
                    }
                    isUserIdSet = true;
                }

                String itemID = String.valueOf(value.f1) + "-I";
                double rating = value.f2;

                Optional<Tuple2<String, String>> itemRow = client.queryState("ALS_MODEL", itemID);
                double prediction = 0.0;

                if (itemRow.isPresent()) {
                    double[] itemFactors = Arrays.stream(itemRow.get().f1.split(";"))
                            .mapToDouble(Double::parseDouble).toArray();
                    for (int i = 0; i < userFactors.length; i++) {
                        prediction += userFactors[i] * itemFactors[i];
                    }
                    out.collect(new Tuple2<>(rating, prediction));
                } else {
                    System.err.println("No record found for the itemID query: " + itemID);
                }
            }
        }
    }
}
