package de.tub.it4bi.modelserving.qs;

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
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Random;

/**
 * Creates random queries for the range partitioned SVM model serving using Flink queryable state.
 * Takes number of queries and max number of features as main input parameters.
 * Measure the time taken to process each sparse vector classification.
 */
public class RangePartitionSVMPredict {
    public static void main(String[] args) throws Exception {

        ParameterTool params = ParameterTool.fromArgs(args);
        final JobID jobId = JobID.fromHexString(params.getRequired("jobId"));
        final String jobManagerHost = params.get("jobManagerHost", "localhost");
        final int jobManagerPort = params.getInt("jobManagerPort", 6123);
        final boolean outputDecisionFunction = params.getBoolean("outputDecisionFunction", false);
        final double thresholdValue = params.getDouble("thresholdValue", 0.0);
        final Time queryTimeout = Time.seconds(params.getInt("queryTimeout", 5));
        final int numQueries = params.getInt("numQueries", 1000);
        final int maxNoOfFeatures = params.getInt("maxNoOfFeatures");
        final int minPercentageOfFeatures = params.getInt("minPercentageOfFeatures", 10);
        final int range = params.getInt("range", 1000);

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
                Map<Integer, Map<Integer, Double>> queryVector = new HashMap<>();
                int minVal = maxNoOfFeatures * minPercentageOfFeatures / 100;
                int noOfFeaturesInSparseVector = r.nextInt(maxNoOfFeatures - minVal) + minVal;

                for (int j = 0; j < noOfFeaturesInSparseVector; j++) {
                    int featureID = r.nextInt(maxNoOfFeatures) + 1;
                    double featureValue = r.nextDouble();
                    int modelKey = featureID / range;
                    if (queryVector.containsKey(modelKey)) {
                        queryVector.get(modelKey).put(featureID, featureValue);
                    } else {
                        queryVector.put(modelKey, new HashMap<>());
                        queryVector.get(modelKey).put(featureID, featureValue);
                    }
                }

                int querySize = 0;
                for (int key : queryVector.keySet()) {
                    querySize += queryVector.get(key).size();
                }

                double prediction;
                double rawValue = 0;
                long startTime = System.currentTimeMillis();
                for (int modelKey : queryVector.keySet()) {
                    try {
                        Optional<Tuple2<String, String>> modelVal = client.queryState("SVM_MODEL", String.valueOf(modelKey));
                        if (!modelVal.isPresent()) {
                            System.out.printf("The current Range of Keys %s do not exist in the model. \n", modelKey);
                            continue;
                        }

                        Map<Integer, Double> queryFeatureValMap = queryVector.get(modelKey);
                        String[] refVals = modelVal.get().f1.split(";");
                        Map<Integer, Double> refValMap = new HashMap<>();

                        for (String keyVal : refVals) {
                            String[] keyValPair = keyVal.split(":");
                            refValMap.put(Integer.parseInt(keyValPair[0]), Double.parseDouble(keyValPair[1]));
                        }

                        for (int feature : queryFeatureValMap.keySet()) {
                            if (refValMap.containsKey(feature))
                                rawValue += queryFeatureValMap.get(feature) * refValMap.get(feature);
                            else continue;
                        }
                    } catch (Exception e) {
                        System.out.println("current query failed because of the following Exception:");
                        e.printStackTrace();
                    }
                }

                if (outputDecisionFunction) {
                    prediction = rawValue;
                } else {
                    if (rawValue > thresholdValue) prediction = 1.0;
                    else prediction = -1.0;
                }
                long endTime = System.currentTimeMillis();

                String outputLine = i + "," + querySize + "," + prediction + "," + (endTime - startTime);
                writer.write(outputLine);
                writer.newLine();
            }
        }
        writer.close();
        System.out.println("Output is written in the format: " +
                "query ID, number of features in the query, prediction, query time in milliseconds");
    }
}
