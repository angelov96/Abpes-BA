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

public class ClientExecutorStatistics implements IStatistics {

    private static final double CONVERSION = 1E9;
    private static final DecimalFormat DECIMAL_FORMAT = new DecimalFormat("0.00");
    private static final AtomicInteger CUMULATIVE_COUNT = new AtomicInteger(0);
    private static final DateTimeFormatter FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssZ").withZone(ZoneId.of(GeneralConfiguration.DEFAULT_TIME_ZONE));
    private static final Logger LOG = Logger.getLogger(ClientExecutorStatistics.class);
    private static final String[] HEADER = new String[]{
            "COMPLETE_START_TIME_FORMAT",
            "COMPLETE_END_TIME_FORMAT",
            "TOTAL_RUNTIME",
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
            "CPU_USAGE",
            "AVERAGE_CPU_USAGE_PER_CORE",
            "CUMULATIVE_COUNT",
            "CONVERSION",
            "BASIC_SYSTEM",
            "STATISTIC_TYPE",
            "CID"
    };
    private long startTime;
    private long endTime;
    private long startRuntime;
    private long endRuntime;
    private ClientObject clientObject;
    private int failedTotal;
    private int totalNumberOfRequests;
    private boolean excludeFailedReadRequests;
    private boolean excludeFailedWriteRequests;
    private int successfulTotal;
    private int readRequests;
    private int writeRequests;
    private float totalCpuUsage;
    private float cpuUsage;

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

    @SafeVarargs
    @Override
    public final <E> void writeStatistics(final E... params) {
        DECIMAL_FORMAT.setRoundingMode(RoundingMode.FLOOR);

        int cumulativeCount = CUMULATIVE_COUNT.incrementAndGet();

        if (GeneralConfiguration.WRITE_TO_DATABASE) {
            writeToDatabase(cumulativeCount);
        }

        List<Object> valuesToWrite = new ArrayList<>();

        valuesToWrite.add(getCompleteStartTimeFormatted());
        valuesToWrite.add(getCompleteEndTimeFormatted());
        valuesToWrite.add(DECIMAL_FORMAT.format(getTotalRuntime()));
        valuesToWrite.add(clientObject.getClientId());

        valuesToWrite.add(readRequests);
        valuesToWrite.add(writeRequests);
        valuesToWrite.add(totalNumberOfRequests);
        valuesToWrite.add(excludeFailedReadRequests);
        valuesToWrite.add(excludeFailedWriteRequests);
        valuesToWrite.add(failedTotal);
        valuesToWrite.add(successfulTotal);
        valuesToWrite.add(DECIMAL_FORMAT.format((readRequests / getTotalRuntime())));
        valuesToWrite.add(DECIMAL_FORMAT.format((writeRequests / getTotalRuntime())));
        valuesToWrite.add(DECIMAL_FORMAT.format((failedTotal / getTotalRuntime())));
        valuesToWrite.add(DECIMAL_FORMAT.format((successfulTotal / getTotalRuntime())));
        valuesToWrite.add(DECIMAL_FORMAT.format((totalNumberOfRequests / getTotalRuntime())));
        valuesToWrite.add(DECIMAL_FORMAT.format(getTotalCpuUsage()));
        valuesToWrite.add(DECIMAL_FORMAT.format(getCpuUsage()));

        valuesToWrite.add(cumulativeCount);
        valuesToWrite.add(CONVERSION);
        valuesToWrite.add(BlockchainStrategy.getBlockchainFrameworkAsString());
        valuesToWrite.add(getStatisticType());
        valuesToWrite.add(GeneralConfiguration.HOST_ID);

        WriteStatistics.prepareWrite(GeneralConfiguration.LOG_PATH + GeneralConfiguration.CLIENT_EXECUTOR_STATISTICS_FILE_NAME, HEADER, valuesToWrite);
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

    public double getTotalRuntime() {
        return (endRuntime - startRuntime) / CONVERSION;
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

    @SafeVarargs
    @Override
    public final <E> void printStatistics(final E... params) {
        LOG.info(toString());
    }

    @Override
    public String toString() {

        DECIMAL_FORMAT.setRoundingMode(RoundingMode.FLOOR);

        return "ClientExecutorStatistics{" +
                "startTime=" + startTime +
                "; endTime=" + endTime +
                "; startRuntime=" + startRuntime +
                "; endRuntime=" + endRuntime +
                "; clientObject=" + clientObject +
                "; startTimeFormatted=" + getCompleteStartTimeFormatted() +
                "; endTimeFormatted=" + getCompleteEndTimeFormatted() +
                "; totalRuntime=" + DECIMAL_FORMAT.format(getTotalRuntime()) +
                "; readRequests=" + readRequests +
                "; writeRequests=" + writeRequests +
                "; totalNumberOfRequests=" + totalNumberOfRequests +
                "; excludeFailedReadRequests=" + excludeFailedReadRequests +
                "; excludeFailedWriteRequests=" + excludeFailedWriteRequests +
                "; failedTotal=" + failedTotal +
                "; successfulTotal=" + successfulTotal +
                "; RPTU=" + DECIMAL_FORMAT.format((readRequests / getTotalRuntime())) +
                "; WPTU=" + DECIMAL_FORMAT.format((writeRequests / getTotalRuntime())) +
                "; FPTU=" + DECIMAL_FORMAT.format((failedTotal / getTotalRuntime())) +
                "; SPTU=" + DECIMAL_FORMAT.format((successfulTotal / getTotalRuntime())) +
                "; TPTU=" + DECIMAL_FORMAT.format((totalNumberOfRequests / getTotalRuntime())) +
                "; totalCpuUsage=" + DECIMAL_FORMAT.format(getTotalCpuUsage()) +
                "; averageCpuUsage=" + DECIMAL_FORMAT.format(getCpuUsage()) +
                '}';
    }

    public int getFailedTotal() {
        return failedTotal;
    }

    public void setFailedTotal(final int failedTotal) {
        this.failedTotal = failedTotal;
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

    public int getSuccessfulTotal() {
        return successfulTotal;
    }

    public void setSuccessfulTotal(final int successfulTotal) {
        this.successfulTotal = successfulTotal;
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

    private void writeToDatabase(final int cumulativeCount) {

        Locale currentLocale = Locale.getDefault();
        DecimalFormatSymbols decimalFormatSymbols = new DecimalFormatSymbols(currentLocale);
        decimalFormatSymbols.setDecimalSeparator('.');
        decimalFormatSymbols.setGroupingSeparator('.');
        DecimalFormat decimalFormat = (DecimalFormat) DECIMAL_FORMAT.clone();
        decimalFormat.setDecimalFormatSymbols(decimalFormatSymbols);

        try {

            String query = "insert into clientexecutorstatistics (complete_start_time_format, " +
                    "complete_end_time_format, " +
                    "total_runtime, client_id, read_requests, write_requests, total_number_of_requests, " +
                    "exclude_failed_read_requests, exclude_failed_write_requests, failed_total, successful_total, " +
                    "rptu, wptu, fptu, sptu, tptu, cpu_usage, average_cpu_usage_per_core, cumulative_count, " +
                    "conversion_type, basic_system, statistic_type, cid, run_id)" +
                    "VALUES(to_timestamp(?::text, 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') at time zone '" + GeneralConfiguration.TIME_OFFSET + "' ," +
                    "to_timestamp(?::text, 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') at time zone '" + GeneralConfiguration.TIME_OFFSET + "', ?, ?, ?, ?, ?, ?, ?, ?, ?" +
                    ",?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

            PreparedStatement preparedStatement = PrepareStatementCollection.addPreparedStatement(query);
            preparedStatement.setString(1, getCompleteStartTimeFormatted());
            preparedStatement.setString(2, getCompleteEndTimeFormatted());
            preparedStatement.setDouble(3, Double.parseDouble(decimalFormat.format(getTotalRuntime())));
            preparedStatement.setString(4, clientObject.getClientId());
            preparedStatement.setInt(5, readRequests);
            preparedStatement.setInt(6, writeRequests);
            preparedStatement.setInt(7, totalNumberOfRequests);
            preparedStatement.setBoolean(8, excludeFailedReadRequests);
            preparedStatement.setBoolean(9, excludeFailedWriteRequests);
            preparedStatement.setInt(10, failedTotal);
            preparedStatement.setInt(11, successfulTotal);
            preparedStatement.setDouble(12,
                    Double.parseDouble(decimalFormat.format((readRequests / getTotalRuntime()))));
            preparedStatement.setDouble(13,
                    Double.parseDouble(decimalFormat.format((writeRequests / getTotalRuntime()))));
            preparedStatement.setDouble(14,
                    Double.parseDouble(decimalFormat.format((failedTotal / getTotalRuntime()))));
            preparedStatement.setDouble(15,
                    Double.parseDouble(decimalFormat.format((successfulTotal / getTotalRuntime()))));
            preparedStatement.setDouble(16,
                    Double.parseDouble(decimalFormat.format((totalNumberOfRequests / getTotalRuntime()))));
            preparedStatement.setDouble(17, Double.parseDouble(decimalFormat.format(getTotalCpuUsage())));
            preparedStatement.setDouble(18, Double.parseDouble(decimalFormat.format(getCpuUsage())));
            preparedStatement.setInt(19, cumulativeCount);
            preparedStatement.setDouble(20, CONVERSION);
            preparedStatement.setString(21, BlockchainStrategy.getBlockchainFrameworkAsString());
            preparedStatement.setString(22, getStatisticType().toString());
            preparedStatement.setString(23, GeneralConfiguration.HOST_ID);
            preparedStatement.setString(24, GeneralConfiguration.RUN_ID);

            //preparedStatement.executeUpdate();
            preparedStatement.addBatch();

        } catch (SQLException ex) {
            ExceptionHandler.logException(ex);
        }
    }


}
