package de.tub.it4bi.modelserving.utils;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.query.QueryableStateClient;
import org.apache.flink.runtime.query.netty.UnknownKeyOrNamespace;
import org.apache.flink.runtime.query.netty.message.KvStateRequestSerializer;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.util.DataInputDeserializer;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * This is a wrapper around Flink's {@link QueryableStateClient} (as of Flink
 * 1.2.0) that hides the low-level type serialization details.
 * <p>
 * <p>Queries are executed synchronously via {@link #queryState(String, Object)}.
 *
 * @param <K> Type of the queried keys
 * @param <V> Type of the queried values
 */
public class QueryClientHelper<K, V> implements AutoCloseable {

    /**
     * ID of the job to query.
     */
    private final JobID jobId;

    /*** Serializer for the keys.*/
    private final TypeSerializer<K> keySerializer;

    /**
     * Serializer for the result values.
     */
    private final TypeSerializer<V> valueSerializer;

    /**
     * Timeout for each query. After this timeout, the query fails with a {@link TimeoutException}.
     */
    private final FiniteDuration queryTimeout;

    /**
     * The wrapper low-level {@link QueryableStateClient}.
     */
    private final QueryableStateClient client;

    /**
     * Creates the queryable state client wrapper.
     *
     * @param jobManagerHost  Host for JobManager communication
     * @param jobManagerPort  Port for JobManager communication.
     * @param jobId           ID of the job to query.
     * @param keySerializer   Serializer for keys.
     * @param valueSerializer Serializer for returned values.
     * @param queryTimeout    Timeout for queries.
     * @throws Exception Thrown if creating the {@link QueryableStateClient} fails.
     */
    public QueryClientHelper(
            String jobManagerHost,
            int jobManagerPort,
            JobID jobId,
            TypeSerializer<K> keySerializer,
            TypeSerializer<V> valueSerializer,
            Time queryTimeout) throws Exception {

        this.jobId = jobId;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.queryTimeout = new FiniteDuration(queryTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);

        Configuration config = new Configuration();
        config.setString(JobManagerOptions.ADDRESS, jobManagerHost);
        config.setInteger(JobManagerOptions.PORT, jobManagerPort);

        final HighAvailabilityServices highAvailabilityServices =
                HighAvailabilityServicesUtils.createHighAvailabilityServices(
                        config,
                        Executors.newSingleThreadScheduledExecutor(),
                        HighAvailabilityServicesUtils.AddressResolution.TRY_ADDRESS_RESOLUTION);

        this.client = new QueryableStateClient(config, highAvailabilityServices);
    }

    /**
     * Queries a state instance for the key.
     *
     * @param name Name of the state instance to query. This is the external name as given to {@link
     *             org.apache.flink.api.common.state.StateDescriptor#setQueryable(String)} or {@link
     *             org.apache.flink.streaming.api.datastream.KeyedStream#asQueryableState(String)}.
     * @param key  The key to query
     * @return The returned value if it is available
     */
    public Optional<V> queryState(String name, K key) throws Exception {
        if (name == null) {
            throw new NullPointerException("Name");
        }
        if (key == null) {
            throw new NullPointerException("Key");
        }

        // Serialize the key. The namespace is ignored as it's only relevant for
        // windows which are not yet exposed for queries.
        byte[] serializedKey = KvStateRequestSerializer.serializeKeyAndNamespace(
                key,
                keySerializer,
                VoidNamespace.INSTANCE,
                VoidNamespaceSerializer.INSTANCE);

        // Submit the query
        Future<byte[]> queryFuture = client.getKvState(jobId, name, key.hashCode(), serializedKey);

        try {
            // Wait for the result
            byte[] queryResult = Await.result(queryFuture, queryTimeout);

            DataInputDeserializer dis = new DataInputDeserializer(
                    queryResult,
                    0,
                    queryResult.length);

            V value = valueSerializer.deserialize(dis);

            return Optional.ofNullable(value);
        } catch (UnknownKeyOrNamespace e) {
            // The future is failed with this Exception if the key does not exist
            return Optional.empty();
        }
    }

    @Override
    public void close() throws Exception {
        client.shutDown();
    }
}