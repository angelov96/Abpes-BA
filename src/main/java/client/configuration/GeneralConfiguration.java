package client.configuration;

import client.blockchain.BlockchainFramework;
import client.client.ClientObject;
import client.commoninterfaces.IExecuteWorkload;
import client.commoninterfaces.IPrepareWorkload;
import client.statistics.GeneralMeasurementPoint;
import client.statistics.WriteStatisticObject;
import client.supplements.ExceptionHandler;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.convert.DefaultListDelimiterHandler;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.AbstractQueue;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class GeneralConfiguration {

    private static final Path CURRENT_RELATIVE_PATH = Paths.get("");
    private static final String CURRENT_ABSOLUTE_PATH = CURRENT_RELATIVE_PATH.toAbsolutePath().toString();
    public static final String FILE_NAME = "generalConfiguration.properties";
    public static final String FILE_PATH = "/configs/";

    static {
        try {
            PROPERTIES_CONFIGURATION =
                    new FileBasedConfigurationBuilder<>(PropertiesConfiguration.class)
                            .configure(new Parameters()
                                    .properties()
                                    .setBasePath(CURRENT_ABSOLUTE_PATH + FILE_PATH)
                                    .setFileName(FILE_NAME)
                                    .setListDelimiterHandler(new DefaultListDelimiterHandler(',')))
                            .getConfiguration();
        } catch (ConfigurationException ex) {
            ExceptionHandler.logException(ex);
        }

        setCustomClients();

    }

    private static PropertiesConfiguration PROPERTIES_CONFIGURATION;

    public static final List<Class<? extends IExecuteWorkload>> I_EXECUTE_WORKLOADS =
            new ArrayList<>();
    public static final List<ClientObject> CUSTOM_CLIENTS = new ArrayList<>();
    public static Class<? extends AbstractQueue> MAIN_ABSTRACT_QUEUE;
    public static Class<? extends AbstractQueue> CLIENT_ABSTRACT_QUEUE;

    public static final double DEFAULT_NAN = PROPERTIES_CONFIGURATION.getDouble("DEFAULT_NAN");

    public static final String HOST_ID = PROPERTIES_CONFIGURATION.getString("HOST_ID");

    public static List<Class<? extends IPrepareWorkload>> I_PREPARE_WORKLOADS = new ArrayList<>();
    public static final int CLIENT_COUNT = PROPERTIES_CONFIGURATION.getInt("CLIENT_COUNT");
    public static final List<Integer> CLIENT_WORKLOADS = PROPERTIES_CONFIGURATION.getList(Integer.class,
            "CLIENT_WORKLOADS");
    public static final boolean ENABLE_RATE_LIMITER_FOR_WORKLOAD = PROPERTIES_CONFIGURATION.getBoolean(
            "ENABLE_RATE_LIMITER_FOR_WORKLOAD");
    public static final List<Double> WORKLOADS_PER_SECOND = PROPERTIES_CONFIGURATION.getList(Double.class,
            "WORKLOADS_PER_SECOND");
    public static final boolean CLIENT_EXECUTOR_POOL_IS_THREAD = PROPERTIES_CONFIGURATION.getBoolean(
            "CLIENT_EXECUTOR_POOL_IS_THREAD");
    public static final boolean WORKLOAD_POOL_IS_THREAD = PROPERTIES_CONFIGURATION.getBoolean(
            "WORKLOAD_POOL_IS_THREAD");
    public static final int MAIN_CORE_POOL_SIZE = PROPERTIES_CONFIGURATION.getInt("MAIN_CORE_POOL_SIZE");
    public static final int MAIN_MAXIMUM_POOL_SIZE = PROPERTIES_CONFIGURATION.getInt("MAIN_MAXIMUM_POOL_SIZE");
    public static final long MAIN_POOL_TIMEOUT = PROPERTIES_CONFIGURATION.getLong("MAIN_POOL_TIMEOUT");
    public static final TimeUnit MAIN_POOL_TIME_UNIT = PROPERTIES_CONFIGURATION.get(TimeUnit.class,
            "MAIN_POOL_TIME_UNIT");
    public static final int CLIENT_CORE_POOL_SIZE = PROPERTIES_CONFIGURATION.getInt("CLIENT_CORE_POOL_SIZE");
    public static final int CLIENT_MAXIMUM_POOL_SIZE = PROPERTIES_CONFIGURATION.getInt("CLIENT_MAXIMUM_POOL_SIZE");
    public static final long CLIENT_POOL_TIMEOUT = PROPERTIES_CONFIGURATION.getLong("CLIENT_POOL_TIMEOUT");
    public static final TimeUnit CLIENT_POOL_TIME_UNIT = PROPERTIES_CONFIGURATION.get(TimeUnit.class,
            "CLIENT_POOL_TIME_UNIT");
    public static final BlockchainFramework BLOCKCHAIN_FRAMEWORK =
            PROPERTIES_CONFIGURATION.get(BlockchainFramework.class, "BLOCKCHAIN_FRAMEWORK");
    public static final boolean DEBUG_POOL = PROPERTIES_CONFIGURATION.getBoolean("DEBUG_POOL");
    public static final boolean PRINT_STACK_TRACE = PROPERTIES_CONFIGURATION.getBoolean("PRINT_STACK_TRACE");
    public static final String FIBER_CLIENT_EXECUTOR_POOL_NAME = PROPERTIES_CONFIGURATION.getString(
            "FIBER_CLIENT_EXECUTOR_POOL_NAME");
    public static final String FIBER_WORKLOAD_POOL_NAME = PROPERTIES_CONFIGURATION.getString(
            "FIBER_WORKLOAD_POOL_NAME");
    public static final boolean WAIT_FOR_ALL_CLIENTS_TO_START = PROPERTIES_CONFIGURATION.getBoolean(
            "WAIT_FOR_ALL_CLIENTS_TO_START");
    public static final int NUMBER_OF_WRITE_OPERATIONS = PROPERTIES_CONFIGURATION.getInt("NUMBER_OF_WRITE_OPERATIONS");
    public static final int NUMBER_OF_READ_OPERATIONS = PROPERTIES_CONFIGURATION.getInt("NUMBER_OF_READ_OPERATIONS");
    public static final String CLIENT_PREFIX = PROPERTIES_CONFIGURATION.getString("CLIENT_PREFIX");
    public static final long DEFAULT_ERROR_TIMESTAMP = PROPERTIES_CONFIGURATION.getLong("DEFAULT_ERROR_TIMESTAMP");
    public static final boolean FORCE_QUIT_ON_EXCEPTION = PROPERTIES_CONFIGURATION.getBoolean(
            "FORCE_QUIT_ON_EXCEPTION");
    public static final boolean EXCLUDE_FAILED_READ_REQUESTS = PROPERTIES_CONFIGURATION.getBoolean(
            "EXCLUDE_FAILED_READ_REQUESTS");
    public static final boolean EXCLUDE_FAILED_WRITE_REQUESTS = PROPERTIES_CONFIGURATION.getBoolean(
            "EXCLUDE_FAILED_WRITE_REQUESTS");
    public static final GeneralMeasurementPoint.GeneralMeasurementPointStart GENERAL_MEASUREMENT_POINT_START =
            PROPERTIES_CONFIGURATION.get(GeneralMeasurementPoint.GeneralMeasurementPointStart.class,
                    "GENERAL_MEASUREMENT_POINT_START");
    public static final long SYSTEM_STATISTICS_REPOLL_INTERVAL = PROPERTIES_CONFIGURATION.getLong(
            "SYSTEM_STATISTICS_REPOLL_INTERVAL");
    public static final boolean ENABLE_RESOURCE_MONITOR = PROPERTIES_CONFIGURATION.getBoolean(
            "ENABLE_RESOURCE_MONITOR");
    public static final boolean RESOURCE_MONITOR_AS_THREAD = PROPERTIES_CONFIGURATION.getBoolean(
            "RESOURCE_MONITOR_AS_THREAD");
    public static final boolean EXCLUDE_INVALID_VALUES = PROPERTIES_CONFIGURATION.getBoolean("EXCLUDE_INVALID_VALUES");
    public static final boolean EXCLUDE_ERROR_VALUES = PROPERTIES_CONFIGURATION.getBoolean("EXCLUDE_ERROR_VALUES");
    public static String notes = PROPERTIES_CONFIGURATION.getString("notes");
    public static final boolean MEASURE_BY_BEFORE_RATE_LIMITER_TIME = PROPERTIES_CONFIGURATION.getBoolean(
            "MEASURE_BY_BEFORE_RATE_LIMITER_TIME");
    public static final long CLIENT_RAMP_DOWN_TIME_AFTER_EXECUTION = PROPERTIES_CONFIGURATION.getLong(
            "CLIENT_RAMP_DOWN_TIME_AFTER_EXECUTION");
    public static final long CLIENT_RAMP_UP_TIME_AFTER_PREPARATION = PROPERTIES_CONFIGURATION.getLong(
            "CLIENT_RAMP_UP_TIME_AFTER_PREPARATION");
    public static final long CLIENT_RAMP_UP_TIME_BEFORE_PREPARATION = PROPERTIES_CONFIGURATION.getLong(
            "CLIENT_RAMP_UP_TIME_BEFORE_PREPARATION");
    public static final long CLIENT_RAMP_UP_TIME_BEFORE_WORKLOAD_EXECUTION = PROPERTIES_CONFIGURATION.getLong(
            "CLIENT_RAMP_UP_TIME_BEFORE_WORKLOAD_EXECUTION");
    public static final String LOG_PATH = PROPERTIES_CONFIGURATION.getString("LOG_PATH");
    public static final String RUN_ID = PROPERTIES_CONFIGURATION.getString("RUN_ID") ;
    public static final String DEFAULT_FILE_TYPE = PROPERTIES_CONFIGURATION.getString("DEFAULT_FILE_TYPE");
    public static final String CLIENT_EXECUTOR_STATISTICS_FILE_NAME = PROPERTIES_CONFIGURATION.getString(
            "CLIENT_EXECUTOR_STATISTICS_FILE_NAME") + GeneralConfiguration.RUN_ID + DEFAULT_FILE_TYPE;
    public static final String DESCRIPTIVE_STATISTICS_FILE_NAME = PROPERTIES_CONFIGURATION.getString(
            "DESCRIPTIVE_STATISTICS_FILE_NAME") + GeneralConfiguration.RUN_ID + DEFAULT_FILE_TYPE;
    public static final String GENERAL_STATISTICS_FILE_NAME = PROPERTIES_CONFIGURATION.getString(
            "GENERAL_STATISTICS_FILE_NAME") + GeneralConfiguration.RUN_ID + DEFAULT_FILE_TYPE;
    public static final String LISTENER_STATISTICS_FILE_NAME = PROPERTIES_CONFIGURATION.getString(
            "LISTENER_STATISTICS_FILE_NAME") + GeneralConfiguration.RUN_ID + DEFAULT_FILE_TYPE;
    public static final String WRITE_STATISTICS_FILE_NAME = PROPERTIES_CONFIGURATION.getString(
            "WRITE_STATISTICS_FILE_NAME") + GeneralConfiguration.RUN_ID + DEFAULT_FILE_TYPE;
    public static final String READ_STATISTICS_FILE_NAME = PROPERTIES_CONFIGURATION.getString(
            "READ_STATISTICS_FILE_NAME") + GeneralConfiguration.RUN_ID + DEFAULT_FILE_TYPE;
    public static final String SYSTEM_STATISTICS_FILE_NAME = PROPERTIES_CONFIGURATION.getString(
            "SYSTEM_STATISTICS_FILE_NAME") + GeneralConfiguration.RUN_ID + DEFAULT_FILE_TYPE;
    public static final String WORKLOAD_POOL_STATISTICS_FILE_NAME = PROPERTIES_CONFIGURATION.getString(
            "WORKLOAD_POOL_STATISTICS_FILE_NAME") + GeneralConfiguration.RUN_ID + DEFAULT_FILE_TYPE;
    public static final String CUSTOM_STATISTICS_FILE_NAME = PROPERTIES_CONFIGURATION.getString(
            "CUSTOM_STATISTICS_FILE_NAME") + GeneralConfiguration.RUN_ID + DEFAULT_FILE_TYPE;
    public static final String BLOCK_STATISTICS_FILE_NAME = PROPERTIES_CONFIGURATION.getString(
            "BLOCK_STATISTICS_FILE_NAME") + GeneralConfiguration.RUN_ID + DEFAULT_FILE_TYPE;
    public static final boolean EXCLUDE_EXISTING_VALUES = PROPERTIES_CONFIGURATION.getBoolean(
            "EXCLUDE_EXISTING_VALUES");
    public static final char CSV_SEPARATOR = PROPERTIES_CONFIGURATION.get(Character.class, "CSV_SEPARATOR");
    public static final char CSV_REPLACEMENT_SEPARATOR = PROPERTIES_CONFIGURATION.get(Character.class,
            "CSV_REPLACEMENT_SEPARATOR");

    public static final WriteStatisticObject.NoteRateLimiter NOTE_RATE_LIMITER_WRITE =
            PROPERTIES_CONFIGURATION.get(WriteStatisticObject.NoteRateLimiter.class,
                    "NOTE_RATE_LIMITER_WRITE");

    public static final boolean DISTRIBUTED_CLIENT_HANDLING = PROPERTIES_CONFIGURATION.getBoolean("DISTRIBUTED_CLIENT_HANDLING");
    public static final boolean DISTRIBUTED_CLIENT_HANDLING_WAIT_FOR_CLIENT_THRESHOLD = PROPERTIES_CONFIGURATION.getBoolean("DISTRIBUTED_CLIENT_HANDLING_WAIT_FOR_CLIENT_THRESHOLD");
    public static final int NUMBER_OF_CLIENTS_THRESHOLD_DISTRIBUTED_CLIENT_HANDLING = PROPERTIES_CONFIGURATION.getInt("NUMBER_OF_CLIENTS_THRESHOLD_DISTRIBUTED_CLIENT_HANDLING");
    public static final long TIMEOUT_LISTENER = PROPERTIES_CONFIGURATION.getLong("TIMEOUT_LISTENER");
    public static final TimeUnit TIMEOUT_LISTENER_TIME_UNIT = PROPERTIES_CONFIGURATION.get(TimeUnit.class,
            "TIMEOUT_LISTENER_TIME_UNIT");

    public static final boolean LISTENER_AS_THREAD = PROPERTIES_CONFIGURATION.getBoolean("LISTENER_AS_THREAD");
    public static final boolean START_DATABASE_LISTENER_ASYNC = PROPERTIES_CONFIGURATION.getBoolean("START_DATABASE_LISTENER_ASYNC");
    public static final long REPOLL_DATABASE_LISTENER = PROPERTIES_CONFIGURATION.getLong("REPOLL_DATABASE_LISTENER");

    public static final boolean WRITE_TO_DATABASE = PROPERTIES_CONFIGURATION.getBoolean("WRITE_TO_DATABASE");
    public static final String DEFAULT_TIME_ZONE = PROPERTIES_CONFIGURATION.getString("DEFAULT_TIME_ZONE");
    public static final String DATABASE_DRIVER = PROPERTIES_CONFIGURATION.getString("DATABASE_DRIVER");
    public static final String DATABASE_URL = PROPERTIES_CONFIGURATION.getString("DATABASE_URL");
    public static final String DATABASE_USER = PROPERTIES_CONFIGURATION.getString("DATABASE_USER");
    public static final String DATABASE_PASSWORD = PROPERTIES_CONFIGURATION.getString("DATABASE_PASSWORD");
    public static final String TIME_OFFSET = PROPERTIES_CONFIGURATION.getString("TIME_OFFSET");

    public static final int LISTENER_PRIORITY = PROPERTIES_CONFIGURATION.getInt("LISTENER_PRIORITY");

    public static final GeneralMeasurementPoint.GeneralMeasurementPointEnd GeneralMeasurementPointEnd =
            PROPERTIES_CONFIGURATION.get(
                    GeneralMeasurementPoint.GeneralMeasurementPointEnd.class, "GENERAL_MEASUREMENT_POINT_END");

    static {
        try {
            MAIN_ABSTRACT_QUEUE = (Class<? extends AbstractQueue>) Class.forName(PROPERTIES_CONFIGURATION.getString(
                    "MAIN_ABSTRACT_QUEUE"));
        } catch (ClassNotFoundException ex) {
            ExceptionHandler.logException(ex);
        }
    }

    static {
        try {
            CLIENT_ABSTRACT_QUEUE =
                    (Class<? extends AbstractQueue>) Class.forName(PROPERTIES_CONFIGURATION.getString(
                            "CLIENT_ABSTRACT_QUEUE"));
        } catch (ClassNotFoundException ex) {
            ExceptionHandler.logException(ex);
        }
    }

    static {
        try {

            for (final String i_prepare_workload : PROPERTIES_CONFIGURATION.getList(String.class,
                    "I_PREPARE_WORKLOADS")) {
                I_PREPARE_WORKLOADS.add((Class<? extends IPrepareWorkload>) Class.forName(i_prepare_workload));
            }
        } catch (ClassNotFoundException ex) {
            ExceptionHandler.logException(ex);
        }
    }

    static {
        try {

            for (final String i_execute_workload : PROPERTIES_CONFIGURATION.getList(String.class,
                    "I_EXECUTE_WORKLOADS")) {
                I_EXECUTE_WORKLOADS.add((Class<? extends IExecuteWorkload>) Class.forName(i_execute_workload));
            }
        } catch (ClassNotFoundException ex) {
            ExceptionHandler.logException(ex);
        }
    }

    private static void setCustomClients() {
        //CUSTOM_CLIENTS.add(new ClientObject(CLIENT_PREFIX, 2, 0, 0, ClientRole.GENERAL));
    }

}
