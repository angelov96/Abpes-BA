package client.statistics;

import client.blockchain.BlockchainStrategy;
import client.client.ClientObject;
import client.configuration.GeneralConfiguration;
import client.supplements.ExceptionHandler;
import org.apache.log4j.Logger;

import java.math.RoundingMode;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;

public class WorkloadPoolStatistics implements IStatistics {

    private static final AtomicInteger CUMULATIVE_COUNT = new AtomicInteger(0);
    private static final double CONVERSION = 1E9;
    private static final DateTimeFormatter FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssZ").withZone(ZoneId.of(GeneralConfiguration.DEFAULT_TIME_ZONE));
    private static final DecimalFormat DECIMAL_FORMAT = new DecimalFormat("0.00");
    private static final Logger LOG = Logger.getLogger(WorkloadPoolStatistics.class);
    private static final String[] HEADER = new String[]{
            "COMPLETE_START_TIME_BEFORE_RATE_LIMITER_FORMAT",
            "COMPLETE_START_TIME_FORMAT",
            "COMPLETE_END_TIME_FORMAT",
            "TOTAL_RUNTIME",
            "TOTAL_RUNTIME_BEFORE_RATE_LIMITER",
            "WORKLOAD_ID",
            "CLIENT_ID",
            "READ_REQUESTS",
            "WRITE_REQUESTS",
            "TOTAL_NUMBER_OF_REQUESTS",
            "EXCLUDE_FAILED_READ_REQUESTS",
            "EXCLUDE_FAILED_WRITE_REQUESTS",
            "FAILED_TOTAL",
            "SUCCESSFUL_TOTAL",
            "RPTU",
            "WPTU",
            "FPTU",
            "SPTU",
            "TPTU",
            "CPU_USAGE_BEFORE_RATE_LIMITER",
            "AVERAGE_CPU_USAGE_PER_CORE_BEFORE_RATE_LIMITER",
            "CPU_USAGE",
            "AVERAGE_CPU_USAGE_PER_CORE",
            "CUMULATIVE_COUNT",
            "CONVERSION",
            "BASIC_SYSTEM",
            "STATISTIC_TYPE",
            "CID"
    };
    private static final boolean MEASURE_BY_BEFORE_RATE_LIMITER_TIME =
            GeneralConfiguration.MEASURE_BY_BEFORE_RATE_LIMITER_TIME;
    private float totalCpuUsage;
    private float cpuUsage;
    private float totalCpuUsageBeforeRateLimiter;
    private float cpuUsageBeforeRateLimiter;
    private long startTime;
    private long endTime;
    private long startRuntime;
    private long startTimeBeforeRateLimiter;
    private long startRuntimeBeforeRateLimiter;
    private int workloadId;
    private ClientObject clientObject;
    private long endRuntime;
    private int failedTotal;
    private int totalNumberOfRequests;
    private boolean excludeFailedReadRequests;
    private boolean excludeFailedWriteRequests;
    private int successfulTotal;
    private int readRequests;
    private int writeRequests;

    public int getFailedTotal() {
        return failedTotal;
    }

    public void setFailedTotal(final int failedTotal) {
        this.failedTotal = failedTotal;
    }

    public int getTotalNumberOfRequests() {
        return totalNumberOfRequests;
    }

    public void setTotalNumberOfRequests(final int totalNumberOfRequests) {
        this.totalNumberOfRequests = totalNumberOfRequests;
    }

    public boolean isExcludeFailedReadRequests() {
        return excludeFailedReadRequests;
    }

    public void setExcludeFailedReadRequests(final boolean excludeFailedReadRequests) {
        this.excludeFailedReadRequests = excludeFailedReadRequests;
    }

    public boolean isExcludeFailedWriteRequests() {
        return excludeFailedWriteRequests;
    }

    public void setExcludeFailedWriteRequests(final boolean excludeFailedWriteRequests) {
        this.excludeFailedWriteRequests = excludeFailedWriteRequests;
    }

    public int getSuccessfulTotal() {
        return successfulTotal;
    }

    public void setSuccessfulTotal(final int successfulTotal) {
        this.successfulTotal = successfulTotal;
    }

    public int getReadRequests() {
        return readRequests;
    }

    public void setReadRequests(final int readRequests) {
        this.readRequests = readRequests;
    }

    public int getWriteRequests() {
        return writeRequests;
    }

    public void setWriteRequests(final int writeRequests) {
        this.writeRequests = writeRequests;
    }

    @SafeVarargs
    @Override
    public final <E> void writeStatistics(final E... params) {
        DECIMAL_FORMAT.setRoundingMode(RoundingMode.FLOOR);

        int cumulativeCount = CUMULATIVE_COUNT.incrementAndGet();

        if (GeneralConfiguration.WRITE_TO_DATABASE) {
            writeToDatabase(cumulativeCount);
        }

        List<Object> valuesToWrite = new ArrayList<>();

        valuesToWrite.add(getCompleteStartTimeBeforeRateLimiterFormatted());
        valuesToWrite.add(getCompleteStartTimeFormatted());
        valuesToWrite.add(getCompleteEndTimeFormatted());
        valuesToWrite.add(DECIMAL_FORMAT.format(getTotalRuntime()));
        valuesToWrite.add(DECIMAL_FORMAT.format(getTotalRuntimeBeforeRateLimiter()));
        valuesToWrite.add(workloadId);
        valuesToWrite.add(clientObject.getClientId());

        valuesToWrite.add(readRequests);
        valuesToWrite.add(writeRequests);
        valuesToWrite.add(totalNumberOfRequests);
        valuesToWrite.add(excludeFailedReadRequests);
        valuesToWrite.add(excludeFailedWriteRequests);
        valuesToWrite.add(failedTotal);
        valuesToWrite.add(successfulTotal);
        valuesToWrite.add(DECIMAL_FORMAT.format((readRequests /
                (MEASURE_BY_BEFORE_RATE_LIMITER_TIME ? getTotalRuntimeBeforeRateLimiter() : getTotalRuntime()))));
        valuesToWrite.add(DECIMAL_FORMAT.format((writeRequests /
                (MEASURE_BY_BEFORE_RATE_LIMITER_TIME ? getTotalRuntimeBeforeRateLimiter() : getTotalRuntime()))));
        valuesToWrite.add(DECIMAL_FORMAT.format((failedTotal /
                (MEASURE_BY_BEFORE_RATE_LIMITER_TIME ? getTotalRuntimeBeforeRateLimiter() : getTotalRuntime()))));
        valuesToWrite.add(DECIMAL_FORMAT.format((successfulTotal /
                (MEASURE_BY_BEFORE_RATE_LIMITER_TIME ? getTotalRuntimeBeforeRateLimiter() : getTotalRuntime()))));
        valuesToWrite.add(DECIMAL_FORMAT.format((totalNumberOfRequests /
                (MEASURE_BY_BEFORE_RATE_LIMITER_TIME ? getTotalRuntimeBeforeRateLimiter() : getTotalRuntime()))));

        valuesToWrite.add(DECIMAL_FORMAT.format(getTotalCpuUsageBeforeRateLimiter()));
        valuesToWrite.add(DECIMAL_FORMAT.format(getCpuUsageBeforeRateLimiter()));
        valuesToWrite.add(DECIMAL_FORMAT.format(getTotalCpuUsage()));
        valuesToWrite.add(DECIMAL_FORMAT.format(getCpuUsage()));

        valuesToWrite.add(cumulativeCount);
        valuesToWrite.add(CONVERSION);
        valuesToWrite.add(BlockchainStrategy.getBlockchainFrameworkAsString());
        valuesToWrite.add(getStatisticType());
        valuesToWrite.add(GeneralConfiguration.HOST_ID);

        WriteStatistics.prepareWrite(GeneralConfiguration.LOG_PATH + GeneralConfiguration.WORKLOAD_POOL_STATISTICS_FILE_NAME, HEADER, valuesToWrite);
    }

    public float getTotalCpuUsage() {
        return totalCpuUsage;
    }

    public void setTotalCpuUsage(final float totalCpuUsage) {
        this.totalCpuUsage = totalCpuUsage;
    }

    public float getCpuUsage() {
        return cpuUsage;
    }

    public void setCpuUsage(final float cpuUsage) {
        this.cpuUsage = cpuUsage;
    }

    public float getTotalCpuUsageBeforeRateLimiter() {
        return totalCpuUsageBeforeRateLimiter;
    }

    public void setTotalCpuUsageBeforeRateLimiter(final float totalCpuUsageBeforeRateLimiter) {
        this.totalCpuUsageBeforeRateLimiter = totalCpuUsageBeforeRateLimiter;
    }

    public float getCpuUsageBeforeRateLimiter() {
        return cpuUsageBeforeRateLimiter;
    }

    public void setCpuUsageBeforeRateLimiter(final float cpuUsageBeforeRateLimiter) {
        this.cpuUsageBeforeRateLimiter = cpuUsageBeforeRateLimiter;
    }

    public String getCompleteStartTimeBeforeRateLimiterFormatted() {
        return FORMATTER.format(Instant.ofEpochMilli(startTimeBeforeRateLimiter));
    }

    public double getTotalRuntimeBeforeRateLimiter() {
        return
                (endRuntime - startRuntimeBeforeRateLimiter) / CONVERSION;
    }

    public double getTotalRuntime() {
        return
                (endRuntime - startRuntime) / CONVERSION;
    }

    public String getCompleteStartTimeFormatted() {
        return FORMATTER.format(Instant.ofEpochMilli(startTime));
    }

    public String getCompleteEndTimeFormatted() {
        return FORMATTER.format(Instant.ofEpochMilli(endTime));
    }

    @Override
    public StatisticType getStatisticType() {
        return StatisticType.General;
    }

    public long getStartTimeBeforeRateLimiter() {
        return startTimeBeforeRateLimiter;
    }

    public void setStartTimeBeforeRateLimiter(final long startTimeBeforeRateLimiter) {
        this.startTimeBeforeRateLimiter = startTimeBeforeRateLimiter;
    }

    public long getStartRuntimeBeforeRateLimiter() {
        return startRuntimeBeforeRateLimiter;
    }

    public void setStartRuntimeBeforeRateLimiter(final long startRuntimeBeforeRateLimiter) {
        this.startRuntimeBeforeRateLimiter = startRuntimeBeforeRateLimiter;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(final long startTime) {
        this.startTime = startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(final long endTime) {
        this.endTime = endTime;
    }

    public long getStartRuntime() {
        return startRuntime;
    }

    public void setStartRuntime(final long startRuntime) {
        this.startRuntime = startRuntime;
    }

    public long getEndRuntime() {
        return endRuntime;
    }

    public void setEndRuntime(final long endRuntime) {
        this.endRuntime = endRuntime;
    }

    public ClientObject getClientObject() {
        return clientObject;
    }

    public void setClientObject(final ClientObject clientObject) {
        this.clientObject = clientObject;
    }

    public int getWorkloadId() {
        return workloadId;
    }

    public void setWorkloadId(final int workloadId) {
        this.workloadId = workloadId;
    }

    @SafeVarargs
    @Override
    public final <E> void printStatistics(final E... params) {
        LOG.info(toString());
    }

    @Override
    public String toString() {

        DECIMAL_FORMAT.setRoundingMode(RoundingMode.FLOOR);

        return "WorkloadPoolStatistics{" +
                "startTime=" + startTime +
                "; endTime=" + endTime +
                "; startRuntime=" + startRuntime +
                "; endRuntime=" + endRuntime +
                "; clientObject=" + clientObject +
                "; startTimeFormatted=" + getCompleteStartTimeFormatted() +
                "; endTimeFormatted=" + getCompleteEndTimeFormatted() +
                "; totalRuntime=" + DECIMAL_FORMAT.format(getTotalRuntime()) +
                "; startTimeBeforeRateLimiter=" + startTimeBeforeRateLimiter +
                "; startRuntimeBeforeRateLimiter=" + startRuntimeBeforeRateLimiter +
                "; totalRuntimeBeforeRateLimiter=" + DECIMAL_FORMAT.format(getTotalRuntimeBeforeRateLimiter()) +
                "; getCompleteStartTimeBeforeRateLimiter=" + getCompleteStartTimeBeforeRateLimiterFormatted() +
                "; workloadId=" + workloadId +
                "; readRequests=" + readRequests +
                "; writeRequests=" + writeRequests +
                "; totalNumberOfRequests=" + totalNumberOfRequests +
                "; excludeFailedReadRequests=" + excludeFailedReadRequests +
                "; excludeFailedWriteRequests=" + excludeFailedWriteRequests +
                "; failedTotal=" + failedTotal +
                "; successfulTotal=" + successfulTotal +
                "; RPTU=" + DECIMAL_FORMAT.format((readRequests /
                (MEASURE_BY_BEFORE_RATE_LIMITER_TIME ? getTotalRuntimeBeforeRateLimiter() : getTotalRuntime()))) +
                "; WPTU=" + DECIMAL_FORMAT.format((writeRequests /
                (MEASURE_BY_BEFORE_RATE_LIMITER_TIME ? getTotalRuntimeBeforeRateLimiter() : getTotalRuntime()))) +
                "; FPTU=" + DECIMAL_FORMAT.format((failedTotal /
                (MEASURE_BY_BEFORE_RATE_LIMITER_TIME ? getTotalRuntimeBeforeRateLimiter() : getTotalRuntime()))) +
                "; SPTU=" + DECIMAL_FORMAT.format((successfulTotal /
                (MEASURE_BY_BEFORE_RATE_LIMITER_TIME ? getTotalRuntimeBeforeRateLimiter() : getTotalRuntime()))) +
                "; TPTU=" + DECIMAL_FORMAT.format((totalNumberOfRequests /
                (MEASURE_BY_BEFORE_RATE_LIMITER_TIME ? getTotalRuntimeBeforeRateLimiter() : getTotalRuntime()))) +
                "; totalCpuUsageBeforeRateLimiter=" + DECIMAL_FORMAT.format(getTotalCpuUsageBeforeRateLimiter()) +
                "; averageCpuUsageBeforeRateLimiter=" + DECIMAL_FORMAT.format(getCpuUsageBeforeRateLimiter()) +
                "; totalCpuUsage=" + DECIMAL_FORMAT.format(getTotalCpuUsage()) +
                "; averageCpuUsage=" + DECIMAL_FORMAT.format(getCpuUsage()) +
                '}';
    }

    private void writeToDatabase(final int cumulativeCount) {

        Locale currentLocale = Locale.getDefault();
        DecimalFormatSymbols decimalFormatSymbols = new DecimalFormatSymbols(currentLocale);
        decimalFormatSymbols.setDecimalSeparator('.');
        decimalFormatSymbols.setGroupingSeparator('.');
        DecimalFormat decimalFormat = (DecimalFormat) DECIMAL_FORMAT.clone();
        decimalFormat.setDecimalFormatSymbols(decimalFormatSymbols);

        try {

            String query = "insert into workloadpoolstatistics (complete_start_time_before_rate_limiter_format, complete_start_time_format, " +
                    "complete_end_time_format, " +
                    "total_runtime, total_runtime_before_rate_limiter, workload_id, client_id, read_requests, write_requests, total_number_of_requests, " +
                    "exclude_failed_read_requests, exclude_failed_write_requests, failed_total, successful_total, " +
                    "rptu, wptu, fptu, sptu, tptu, cpu_usage_before_rate_limiter, average_cpu_usage_per_core_before_rate_limiter, cpu_usage, average_cpu_usage_per_core, cumulative_count, " +
                    "conversion_type, basic_system, statistic_type, cid, run_id)" +
                    "VALUES(to_timestamp(?::text, 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') at time zone '" + GeneralConfiguration.TIME_OFFSET + "' ," +
                    "to_timestamp(?::text, 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') at time zone '" + GeneralConfiguration.TIME_OFFSET + "' ," +
                    "to_timestamp(?::text, 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') at time zone '" + GeneralConfiguration.TIME_OFFSET +
                    "', ?, ?, ?, ?, ?, ?, ?, ?, ?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

            PreparedStatement preparedStatement = PrepareStatementCollection.addPreparedStatement(query);

            preparedStatement.setString(1, getCompleteStartTimeBeforeRateLimiterFormatted());
            preparedStatement.setString(2, getCompleteStartTimeFormatted());
            preparedStatement.setString(3, getCompleteEndTimeFormatted());
            preparedStatement.setDouble(4, Double.parseDouble(decimalFormat.format(getTotalRuntime())));
            preparedStatement.setDouble(5, Double.parseDouble(decimalFormat.format(getTotalRuntimeBeforeRateLimiter())));
            preparedStatement.setInt(6, workloadId);
            preparedStatement.setString(7, clientObject.getClientId());
            preparedStatement.setInt(8, readRequests);
            preparedStatement.setInt(9, writeRequests);
            preparedStatement.setInt(10, totalNumberOfRequests);
            preparedStatement.setBoolean(11, excludeFailedReadRequests);
            preparedStatement.setBoolean(12, excludeFailedWriteRequests);
            preparedStatement.setInt(13, failedTotal);
            preparedStatement.setInt(14, successfulTotal);
            preparedStatement.setDouble(15, Double.parseDouble(decimalFormat.format((readRequests /
                    (MEASURE_BY_BEFORE_RATE_LIMITER_TIME ? getTotalRuntimeBeforeRateLimiter() : getTotalRuntime())))));
            preparedStatement.setDouble(16, Double.parseDouble(decimalFormat.format((writeRequests /
                    (MEASURE_BY_BEFORE_RATE_LIMITER_TIME ? getTotalRuntimeBeforeRateLimiter() : getTotalRuntime())))));
            preparedStatement.setDouble(17, Double.parseDouble(decimalFormat.format((failedTotal /
                    (MEASURE_BY_BEFORE_RATE_LIMITER_TIME ? getTotalRuntimeBeforeRateLimiter() : getTotalRuntime())))));
            preparedStatement.setDouble(18, Double.parseDouble(decimalFormat.format((successfulTotal /
                    (MEASURE_BY_BEFORE_RATE_LIMITER_TIME ? getTotalRuntimeBeforeRateLimiter() : getTotalRuntime())))));
            preparedStatement.setDouble(19, Double.parseDouble(decimalFormat.format((totalNumberOfRequests /
                    (MEASURE_BY_BEFORE_RATE_LIMITER_TIME ? getTotalRuntimeBeforeRateLimiter() : getTotalRuntime())))));
            preparedStatement.setDouble(20, Double.parseDouble(decimalFormat.format(getTotalCpuUsageBeforeRateLimiter())));
            preparedStatement.setDouble(21, Double.parseDouble(decimalFormat.format(getCpuUsageBeforeRateLimiter())));
            preparedStatement.setDouble(22, Double.parseDouble(decimalFormat.format(getTotalCpuUsage())));
            preparedStatement.setDouble(23, Double.parseDouble(decimalFormat.format(getCpuUsage())));
            preparedStatement.setInt(24, cumulativeCount);
            preparedStatement.setDouble(25, CONVERSION);
            preparedStatement.setString(26, BlockchainStrategy.getBlockchainFrameworkAsString());
            preparedStatement.setString(27, getStatisticType().name());
            preparedStatement.setString(28, GeneralConfiguration.HOST_ID);
            preparedStatement.setString(29, GeneralConfiguration.RUN_ID);

            //preparedStatement.executeUpdate();
            preparedStatement.addBatch();

        } catch (SQLException ex) {
            ExceptionHandler.logException(ex);
        }
    }

}
