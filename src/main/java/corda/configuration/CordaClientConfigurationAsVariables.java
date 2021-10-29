package corda.configuration;

import net.corda.client.rpc.CordaRPCClientConfiguration;
import net.corda.core.internal.CordaUtilsKt;

import java.time.Duration;

public class CordaClientConfigurationAsVariables {

    public static final Duration CONNECTION_MAX_RETRY_INTERVAL = Duration.ofMinutes(3);
    public static final int MINIMUM_SERVER_PROTOCOL_VERSION = CordaUtilsKt.PLATFORM_VERSION;
    public static final boolean TRACK_RPC_CALL_SITES = false;
    public static final Duration REAP_INTERVAL = Duration.ofMinutes(1);
    public static final int OBSERVATION_EXECUTOR_POOL_SIZE = 4;
    public static final int CACHE_CONCURRENCY_LEVEL = 1;
    private static final Duration CONNECTION_RETRY_INTERVAL = Duration.ofMinutes(5);
    private static final double CONNECTION_RETRY_INTERVAL_MULTIPLIER = 1.5;
    private static final int MAX_RECONNECT_ATTEMPTS = -1;
    private static final int MAX_FILE_SIZE = 10485760;
    private static final Duration DEDUPLICATION_CACHE_EXPIRY = Duration.ofDays(1);

    public CordaRPCClientConfiguration getCustomClientConfiguration() {
        return new CordaRPCClientConfiguration(CONNECTION_MAX_RETRY_INTERVAL, MINIMUM_SERVER_PROTOCOL_VERSION,
                TRACK_RPC_CALL_SITES, REAP_INTERVAL, OBSERVATION_EXECUTOR_POOL_SIZE, CACHE_CONCURRENCY_LEVEL,
                CONNECTION_RETRY_INTERVAL, CONNECTION_RETRY_INTERVAL_MULTIPLIER, MAX_RECONNECT_ATTEMPTS,
                MAX_FILE_SIZE, DEDUPLICATION_CACHE_EXPIRY);
    }
}
