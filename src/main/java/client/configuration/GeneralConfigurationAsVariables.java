package client.configuration;

import client.blockchain.BlockchainFramework;
import client.client.ClientObject;
import client.commoninterfaces.IExecuteWorkload;
import client.commoninterfaces.IPrepareWorkload;
import client.statistics.GeneralMeasurementPoint;
import client.statistics.WriteStatisticObject;
import corda.workloads.ExecuteCordaWorkload;
import corda.workloads.PrepareCordaWorkload;

import java.util.AbstractQueue;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

public class GeneralConfigurationAsVariables {

    public static final int CLIENT_COUNT = 3;
    public static final List<Integer> CLIENT_WORKLOADS = new ArrayList<>(Arrays.asList(3));

    public static final boolean ENABLE_RATE_LIMITER_FOR_WORKLOAD = false;
    public static final List<Double> WORKLOADS_PER_SECOND = new ArrayList<>(Arrays.asList(1.0));

    public static final boolean CLIENT_EXECUTOR_POOL_IS_THREAD = false;
    public static final boolean WORKLOAD_POOL_IS_THREAD = false;

    public static final int MAIN_CORE_POOL_SIZE = 1;
    public static final int MAIN_MAXIMUM_POOL_SIZE = Integer.MAX_VALUE;
    public static final long MAIN_POOL_TIMEOUT = 60L;
    public static final TimeUnit MAIN_POOL_TIME_UNIT = TimeUnit.SECONDS;
    public static final Class<? extends AbstractQueue> MAIN_ABSTRACT_QUEUE = SynchronousQueue.class;

    public static final int CLIENT_CORE_POOL_SIZE = 1;
    public static final int CLIENT_MAXIMUM_POOL_SIZE = Integer.MAX_VALUE;
    public static final long CLIENT_POOL_TIMEOUT = 60L;
    public static final TimeUnit CLIENT_POOL_TIME_UNIT = TimeUnit.SECONDS;
    public static final Class<? extends AbstractQueue> CLIENT_ABSTRACT_QUEUE = SynchronousQueue.class;

    public static final BlockchainFramework BLOCKCHAIN_FRAMEWORK = BlockchainFramework.Corda;
    public static final List<Class<? extends IPrepareWorkload>> I_PREPARE_WORKLOADS = new ArrayList<>(Arrays.asList(
            PrepareCordaWorkload.class));
    public static final List<Class<? extends IExecuteWorkload>> I_EXECUTE_WORKLOADS =
            new ArrayList<>(Arrays.asList(ExecuteCordaWorkload.class));

    public static final boolean DEBUG_POOL = false;

    public static final boolean PRINT_STACK_TRACE = true;

    public static final String FIBER_CLIENT_EXECUTOR_POOL_NAME = "ClientExecutorFiberPool";

    public static final String FIBER_WORKLOAD_POOL_NAME = "WorkloadPool";

    public static final boolean WAIT_FOR_ALL_CLIENTS_TO_START = true;

    public static final int NUMBER_OF_WRITE_OPERATIONS = 0;
    public static final int NUMBER_OF_READ_OPERATIONS = 0;

    public static final int LISTENER_PRIORITY = 8;

    public static final List<ClientObject> CUSTOM_CLIENTS = new ArrayList<>();

    public static final String CLIENT_PREFIX = "client-";
    public static final long DEFAULT_ERROR_TIMESTAMP = -2;
    public static final boolean FORCE_QUIT_ON_EXCEPTION = false;

    public static final boolean EXCLUDE_FAILED_READ_REQUESTS = true;
    public static final boolean EXCLUDE_FAILED_WRITE_REQUESTS = true;
    public static final GeneralMeasurementPoint.GeneralMeasurementPointStart GENERAL_MEASUREMENT_POINT_START =
            GeneralMeasurementPoint.GeneralMeasurementPointStart.EXECUTE_WORKLOAD;
    public static final long SYSTEM_STATISTICS_REPOLL_INTERVAL = 3000;
    public static final boolean ENABLE_RESOURCE_MONITOR = true;
    public static final boolean RESOURCE_MONITOR_AS_THREAD = true;

    public static final boolean EXCLUDE_INVALID_VALUES = false;
    public static final boolean EXCLUDE_ERROR_VALUES = false;

    public static final WriteStatisticObject.NoteRateLimiter NOTE_RATE_LIMITER_WRITE =
            WriteStatisticObject.NoteRateLimiter.NO;

    public static final boolean DISTRIBUTED_CLIENT_HANDLING = false;
    public static final String RUN_ID = "runId";
    public static final boolean DISTRIBUTED_CLIENT_HANDLING_WAIT_FOR_CLIENT_THRESHOLD = false;
    public static final int NUMBER_OF_CLIENTS_THRESHOLD_DISTRIBUTED_CLIENT_HANDLING = 1;
    public static final long TIMEOUT_LISTENER = 10;
    public static final TimeUnit TIMEOUT_LISTENER_TIME_UNIT = TimeUnit.MINUTES;

    public static final boolean MEASURE_BY_BEFORE_RATE_LIMITER_TIME = false;
    public static final long CLIENT_RAMP_DOWN_TIME_AFTER_EXECUTION = 0;
    public static final long CLIENT_RAMP_UP_TIME_AFTER_PREPARATION = 0;
    public static final long CLIENT_RAMP_UP_TIME_BEFORE_PREPARATION = 0;
    public static final long CLIENT_RAMP_UP_TIME_BEFORE_WORKLOAD_EXECUTION = 0;
    public static final String LOG_PATH = "./logs"; //"E:\\BC-Test\\logs\\";
    public static final String DEFAULT_FILE_TYPE = ".csv";
    public static final String CLIENT_EXECUTOR_STATISTICS_FILE_NAME = "clientExecutorStats" + GeneralConfiguration.RUN_ID + DEFAULT_FILE_TYPE;
    public static final String DESCRIPTIVE_STATISTICS_FILE_NAME = "descriptiveStats" + GeneralConfiguration.RUN_ID + DEFAULT_FILE_TYPE;
    public static final String GENERAL_STATISTICS_FILE_NAME = "generalStats" + GeneralConfiguration.RUN_ID + DEFAULT_FILE_TYPE;
    public static final String LISTENER_STATISTICS_FILE_NAME = "listenerStats" + GeneralConfiguration.RUN_ID + DEFAULT_FILE_TYPE;
    public static final String WRITE_STATISTICS_FILE_NAME = "writeStats" + GeneralConfiguration.RUN_ID + DEFAULT_FILE_TYPE;
    public static final String READ_STATISTICS_FILE_NAME = "readStats" + GeneralConfiguration.RUN_ID + DEFAULT_FILE_TYPE;
    public static final String SYSTEM_STATISTICS_FILE_NAME = "systemStats" + GeneralConfiguration.RUN_ID + DEFAULT_FILE_TYPE;
    public static final String WORKLOAD_POOL_STATISTICS_FILE_NAME = "workloadPoolStats" + GeneralConfiguration.RUN_ID + DEFAULT_FILE_TYPE;
    public static final String CUSTOM_STATISTICS_FILE_NAME = "customStats" + GeneralConfiguration.RUN_ID + DEFAULT_FILE_TYPE;
    public static final String BLOCK_STATISTICS_FILE_NAME = "blockStats" + GeneralConfiguration.RUN_ID + DEFAULT_FILE_TYPE;
    public static final boolean EXCLUDE_EXISTING_VALUES = true;
    public static final char CSV_SEPARATOR = ',';
    public static final char CSV_REPLACEMENT_SEPARATOR = ';';

    public static final double DEFAULT_NAN = 0.0;

    public static final String HOST_ID = "cid";

    public static final boolean LISTENER_AS_THREAD = true;
    public static final boolean START_DATABASE_LISTENER_ASYNC = true;
    public static final long REPOLL_DATABASE_LISTENER = 500;

    public static final boolean WRITE_TO_DATABASE = true;
    public static final String DEFAULT_TIME_ZONE = "Europe/Berlin";
    public static final String DATABASE_DRIVER = "org.postgresql.Driver";
    public static final String DATABASE_URL = "jdbc:postgresql://localhost:5432/abpes";
    public static final String DATABASE_USER = "postgres";
    public static final String DATABASE_PASSWORD = "admin";
    public static final String TIME_OFFSET = "CET";

    public static GeneralMeasurementPoint.GeneralMeasurementPointEnd GeneralMeasurementPointEnd =
            GeneralMeasurementPoint.GeneralMeasurementPointEnd.END_OF_APPLICATION;

    static {
        setCustomClients();
    }

    private static void setCustomClients() {
        //CUSTOM_CLIENTS.add(new ClientObject(CLIENT_PREFIX, 2, 0, 0, ClientRole.GENERAL));
    }

}
