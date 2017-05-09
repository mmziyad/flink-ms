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
        final TypeSerializer<Tuple2<String, String>> valueSerializer =
                TypeInformation.of(new TypeHint<Tuple2<String, String>>() {
                }).createSerializer(new ExecutionConfig());
        final Time queryTimeout = Time.seconds(5);

        try (
                // This helper is for convenience and not part of Flink
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
                String key = line.toUpperCase().trim();
                out.printf("[info] Querying key '%s'\n", key);

                try {
                    long start = System.currentTimeMillis();
                    Optional<Tuple2<String, String>> factor = client.queryState("ALS_MODEL", key);

                    if (factor.isPresent()) {
                        out.printf(key + " # " + factor.get().f1);
                    } else {
                        out.printf("Unknown key: ", key);
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
        System.out.println("The keys are specified as ID-U or ID-I. eg: 12345-U , 6789-I");
    }
}
