package de.tub.it4bi.modelserving.qs;

import jline.console.ConsoleReader;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.PrintWriter;
import java.util.Optional;

/**
 * Created by zis on 06/05/17.
 */
public class SVMPredict {

    public static void main(String[] args) throws Exception {

        if (args.length == 0) {
            throw new IllegalArgumentException("Missing required job ID argument. "
                    + "Usage: ./de.tub.it4bi.modelserving.qs.ALSPredict <jobID> [jobManagerHost] [jobManagerPort]");
        }
        String jobIdParam = args[0];

        // Configuration
        final String jobManagerHost = args.length > 1 ? args[1] : "localhost";
        final int jobManagerPort = args.length > 2 ? Integer.parseInt(args[2]) : 6123;
        final boolean outputDecisionFunction = args.length > 3 ? Boolean.parseBoolean(args[3]) : false;
        final double thresholdValue = args.length > 4 ? Double.parseDouble(args[4]) : 0.0;

        System.out.println("Using JobManager " + jobManagerHost + ":" + jobManagerPort);

        final JobID jobId = JobID.fromHexString(jobIdParam);
        final StringSerializer keySerializer = StringSerializer.INSTANCE;

        final TypeSerializer<Tuple2<String, String>> valueSerializer =
                TypeInformation.of(new TypeHint<Tuple2<String, String>>() {
                }).createSerializer(new ExecutionConfig());/**/

        final Time queryTimeout = Time.seconds(5);

        try (// This helper is for convenience and not part of Flink
             QueryClientHelper<String, Tuple2<String, String>> client = new QueryClientHelper<>(
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
                // "4:0.161826 5:0.0758621 6:0.176471 11:0.428571 17:0.860463"
                out.printf("[info] Querying the model for vector '%s'\n", line);
                try {
                    String tokens[] = line.trim().split(" ");
                    double prediction;
                    double rawValue = 0;
                    for (String s : tokens) {
                        String pair[] = s.split(":");
                        String id = pair[0];
                        double val = Double.parseDouble(pair[1]);

                        Optional<Tuple2<String, String>> modelVal = client.queryState("SVM_MODEL", id);

                        if (modelVal.isPresent()) {
                            double refVal = Double.parseDouble(modelVal.get().f1);
                            rawValue += refVal * val;
                        } else {
                            out.printf("Could not find the value for feature ID: %s", id);
                        }
                    }
                    if (outputDecisionFunction) {
                        prediction = rawValue;
                    } else {
                        if (rawValue > thresholdValue) prediction = 1.0;
                        else prediction = -1.0;
                    }
                    out.printf("SVM Prediction =  %f \n", prediction);
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
