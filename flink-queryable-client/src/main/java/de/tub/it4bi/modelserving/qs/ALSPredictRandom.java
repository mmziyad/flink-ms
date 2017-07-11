package de.tub.it4bi.modelserving.qs;

import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.Arrays;
import java.util.Optional;
import java.util.Random;

/**
 * Creates random queries for ALS model serving using Flink queryable state.
 * Takes number of queries and bounds for feature IDs as main input parameters.
 * Measure the time taken to process each <user ID, item ID > prediction.
 */
public class ALSPredictRandom {

    public static void main(String[] args) throws Exception {

        ParameterTool params = ParameterTool.fromArgs(args);

        final JobID jobId = JobID.fromHexString(params.getRequired("jobId"));
        final String jobManagerHost = params.get("jobManagerHost", "localhost");
        final int jobManagerPort = params.getInt("jobManagerPort", 6123);
        final Time queryTimeout = Time.seconds(params.getInt("queryTimeout", 5));
        final int numQueries = params.getInt("numQueries", 1000);
        final int lowerItemId = params.getInt("lowerItemId", 0);
        final int upperItemId = params.getInt("upperItemId", Integer.MAX_VALUE);
        final int lowerUserId = params.getInt("lowerUserId", 0);
        final int upperUserId = params.getInt("upperUserId", Integer.MAX_VALUE);

        final StringSerializer keySerializer = StringSerializer.INSTANCE;
        final TypeSerializer<Tuple2<String, String>> valueSerializer =
                TypeInformation.of(new TypeHint<Tuple2<String, String>>() {
                }).createSerializer(new ExecutionConfig());

        Random r = new Random();
        BufferedWriter writer = new BufferedWriter(new FileWriter(params.getRequired("outputFile")));

        try (QueryClientHelper<String, Tuple2<String, String>> client = new QueryClientHelper<>(
                jobManagerHost,
                jobManagerPort,
                jobId,
                keySerializer,
                valueSerializer,
                queryTimeout)) {
            for (int i = 0; i < numQueries; i++) {
                int uId = r.nextInt(upperUserId - lowerUserId) + lowerUserId;
                int iId = r.nextInt(upperItemId - lowerItemId) + lowerItemId;
                String userID = uId + "-U";
                String itemID = iId + "-I";

                try {
                    long startTime = System.currentTimeMillis();
                    Optional<Tuple2<String, String>> userTuple = client.queryState("ALS_MODEL", userID);
                    Optional<Tuple2<String, String>> itemTuple = client.queryState("ALS_MODEL", itemID);

                    if (!userTuple.isPresent()) {
                        System.out.printf("User Factors do not exist in the model " +
                                "for the user: %s \n", uId);
                        i--;
                        continue;
                    }
                    if (!itemTuple.isPresent()) {
                        System.out.printf("Item Factors do not exist in the model " +
                                "for the item: %s \n", iId);
                        i--;
                        continue;
                    }
                    // create user vector
                    double[] userFactors = Arrays.stream(userTuple.get().f1.split(";"))
                            .mapToDouble(Double::parseDouble)
                            .toArray();
                    RealVector userVector = new ArrayRealVector(userFactors);

                    // create item vector
                    double[] itemFactors = Arrays.stream(itemTuple.get().f1.split(";"))
                            .mapToDouble(Double::parseDouble)
                            .toArray();
                    RealVector itemVector = new ArrayRealVector(itemFactors);

                    // prediction is the dot product of vectors
                    double prediction = userVector.dotProduct(itemVector);
                    long endTime = System.currentTimeMillis();
                    String outputLine = uId + "," + iId + "," +
                            prediction + "," + (endTime - startTime);
                    writer.write(outputLine);
                    writer.newLine();

                } catch (Exception e) {
                    System.out.println("Query failed because of the following Exception:");
                    e.printStackTrace();
                }
            }
        }
        writer.close();
        System.out.println("Output is written in the format:" +
                "User ID, Item ID, ALS prediction, Query time in milliseconds");
    }
}
