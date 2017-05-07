package de.tub.it4bi.modelserving.qs;

import jline.console.ConsoleReader;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;

import java.io.PrintWriter;
import java.util.Optional;

/**
 * Created by zis on 06/05/17.
 */
public class ALSPredict {

    public static void main(String[] args) throws Exception {

        if (args.length == 0) {
            throw new IllegalArgumentException("Missing required job ID argument. "
                    + "Usage: ./de.tub.it4bi.modelserving.qs.ALSPredict <jobID> [jobManagerHost] [jobManagerPort]");
        }
        String jobIdParam = args[0];

        // Configuration
        final String jobManagerHost = args.length > 1 ? args[1] : "localhost";
        final int jobManagerPort = args.length > 2 ? Integer.parseInt(args[2]) : 6123;

        System.out.println("Using JobManager " + jobManagerHost + ":" + jobManagerPort);

        final JobID jobId = JobID.fromHexString(jobIdParam);

        final StringSerializer keySerializer = StringSerializer.INSTANCE;

        ExecutionConfig config = new ExecutionConfig();
        TypeInformation<ArrayRealVector> info = TypeInformation.of(ArrayRealVector.class);
        final TypeSerializer<ArrayRealVector> valueSerializer = info.createSerializer(config);

        final Time queryTimeout = Time.seconds(5);

        try (
                // This helper is for convenience and not part of Flink
                QueryClientHelper<String, ArrayRealVector> client = new QueryClientHelper<>(
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
                out.printf("[info] Querying key '%s'\n", key);

                try {
                    long start = System.currentTimeMillis();
                    Optional<ArrayRealVector> factor = client.queryState("ALS_MODEL", key);
                    long end = System.currentTimeMillis();

                    long duration = Math.max(0, end - start);

                    if (factor.isPresent()) {
                        out.printf("%d (query took %d ms)\n", factor.get(), duration);
                    } else {
                        out.printf("Unknown key %s (query took %d ms)\n", key, duration);
                    }
                } catch (Exception e) {
                    out.println("Query failed because of the following Exception:");
                    e.printStackTrace(out);
                }
            }
        }
    }

    private static void printUsage() {
        System.out.println("Enter a key to query.");
        System.out.println();
        System.out.println("The keys are specified as ID-U or ID-I. eg: 12345-U , 6789-I");
        System.out.println();
    }
}
