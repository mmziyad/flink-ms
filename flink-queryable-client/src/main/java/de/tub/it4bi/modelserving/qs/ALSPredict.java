package de.tub.it4bi.modelserving.qs;

import jline.console.ConsoleReader;
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

import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Optional;

/**
 * ALS java client accepting point queries from console
 */
public class ALSPredict {

    public static void main(String[] args) throws Exception {

        if (args.length == 0) {
            throw new IllegalArgumentException("Missing required job ID argument. "
                    + "Usage: ./de.tub.it4bi.modelserving.qs.ALSPredict " +
                    "<jobID> [jobManagerHost] [jobManagerPort]");
        }
        String jobIdParam = args[0];

        // Configuration
        final String jobManagerHost = args.length > 1 ? args[1] : "localhost";
        final int jobManagerPort = args.length > 2 ? Integer.parseInt(args[2]) : 6123;

        System.out.println("Using JobManager " + jobManagerHost + ":" + jobManagerPort);

        final JobID jobId = JobID.fromHexString(jobIdParam);
        final StringSerializer keySerializer = StringSerializer.INSTANCE;
        final TypeSerializer<Tuple2<String, String>> valueSerializer = TypeInformation.of(
                new TypeHint<Tuple2<String, String>>() {
                }).createSerializer(new ExecutionConfig());
        final Time queryTimeout = Time.seconds(5);

        try (QueryClientHelper<String, Tuple2<String, String>> client = new QueryClientHelper<>(
                jobManagerHost,
                jobManagerPort,
                jobId,
                keySerializer,
                valueSerializer,
                queryTimeout)) {

            printUsage();
            ConsoleReader reader = new ConsoleReader();
            reader.setPrompt("$ ");
            PrintWriter out = new PrintWriter(reader.getOutput());

            String line;
            while ((line = reader.readLine()) != null) {
                String key = line.toUpperCase().trim();
                out.printf("[info] Querying the model for <user,item> pair '%s'\n", key);
                try {

                    String tokens[] = key.split(",");
                    String userID = tokens[0] + "-U";
                    String itemID = tokens[1] + "-I";

                    Optional<Tuple2<String, String>> userTuple = client.queryState("ALS_MODEL", userID);
                    Optional<Tuple2<String, String>> itemTuple = client.queryState("ALS_MODEL", itemID);

                    if (userTuple.isPresent() && itemTuple.isPresent()) {
                        // create user vector
                        double[] userFactors = Arrays.stream(userTuple.get().f1.split(";"))
                                .mapToDouble(Double::parseDouble).toArray();
                        RealVector userVector = new ArrayRealVector(userFactors);
                        // create item vector
                        double[] itemFactors = Arrays.stream(itemTuple.get().f1.split(";"))
                                .mapToDouble(Double::parseDouble).toArray();
                        RealVector itemVector = new ArrayRealVector(itemFactors);
                        // prediction is the dot product of vectors
                        double prediction = userVector.dotProduct(itemVector);
                        out.printf("ALS Prediction =  %f \n", prediction);
                    } else {
                        out.printf("User or Item Factors do not exist in the model for the query: ", key);
                    }
                } catch (Exception e) {
                    out.println("Query failed because of the following Exception:");
                    e.printStackTrace(out);
                }
            }
        }
    }

    private static void printUsage() {
        System.out.println("Enter <User,Item> to predict.");
    }
}
