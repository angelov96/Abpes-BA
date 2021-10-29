package client.statistics;

import client.blockchain.BlockchainStrategy;
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

public class GeneralStatisticObject implements IStatistics {

    private static final DecimalFormat DECIMAL_FORMAT = new DecimalFormat("0.00");

    private static final AtomicInteger CUMULATIVE_COUNT = new AtomicInteger(0);
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssZ").withZone(ZoneId.of(GeneralConfiguration.DEFAULT_TIME_ZONE));
    private static final double CONVERSION = 1E9;
    private static final Logger LOG = Logger.getLogger(GeneralStatisticObject.class);
    private static final String[] HEADER = new String[]{
            "CURRENT_TIME_START_FORMAT",
            "CURRENT_TIME_END_FORMAT",
            "CLIENT_COUNTER",
            "READ_REQUESTS",
            "WRITE_REQUESTS",
            "TOTAL_NUMBER_OF_REQUESTS",
            "EXCLUDE_FAILED_READ_REQUESTS",
            "EXCLUDE_FAILED_WRITE_REQUESTS",
            "FAILED_TOTAL",
            "SUCCESSFUL_TOTAL",
            "NUMBER_OF_WORKLOADS",
            "COMPLETE_START_TIME_FORMAT",
            "COMPLETE_END_TIME_FORMAT",
            "TOTAL_RUNTIME",
            "MEASUREMENT_RUNTIME",
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
            "NOTES",
            "CID"
    };
    private long currentTimeStart;
    private long currentTimeEnd;
    private long completeStartRuntime;
    private long completeEndRuntime;
    private long completeStartTime;
    private long completeEndTime;
    private long startTime;
    private long endTime;
    private int clientCounter;
    private int readRequests;
    private int writeRequests;
    private int totalNumberOfRequests;
    private boolean excludeFailedReadRequests;
    private boolean excludeFailedWriteRequests;
    private int failedTotal;
    private int successfulTotal;
    private int numberOfWorkloads;
    private float totalCpuUsage;
    private float cpuUsage;

    public long getCompleteStartRuntime() {
        return completeStartRuntime;
    }

    public void setCompleteStartRuntime(final long completeStartRuntime) {
        this.completeStartRuntime = completeStartRuntime;
    }

    public long getCompleteEndRuntime() {
        return completeEndRuntime;
    }

    public void setCompleteEndRuntime(final long completeEndRuntime) {
        this.completeEndRuntime = completeEndRuntime;
    }

    public long getCompleteStartTime() {
        return completeStartTime;
    }

    public void setCompleteStartTime(final long completeStartTime) {
        this.completeStartTime = completeStartTime;
    }

    public long getCompleteEndTime() {
        return completeEndTime;
    }

    public void setCompleteEndTime(final long completeEndTime) {
        this.completeEndTime = completeEndTime;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(final long startTime, final boolean overwrite) {
        if (!overwrite) {
            this.startTime = startTime;
        }
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(final long endTime, final boolean overwrite) {
        if (!overwrite) {
            this.endTime = endTime;
        }
    }

    public long getCurrentTimeStart() {
        return currentTimeStart;
    }

    public void setCurrentTimeStart(final long currentTimeStart, final boolean overwrite) {
        if (!overwrite) {
            this.currentTimeStart = currentTimeStart;
        }
    }

    public long getCurrentTimeEnd() {
        return currentTimeEnd;
    }

    public void setCurrentTimeEnd(final long currentTimeEnd, final boolean overwrite) {
        if (!overwrite) {
            this.currentTimeEnd = currentTimeEnd;
        }
    }

    public int getClientCounter() {
        return clientCounter;
    }

    public void setClientCounter(final int clientCounter) {
        this.clientCounter = clientCounter;
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

    public int getFailedTotal() {
        return failedTotal;
    }

    public void setFailedTotal(final int failedTotal) {
        this.failedTotal = failedTotal;
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

    public int getTotalNumberOfRequests() {
        return totalNumberOfRequests;
    }

    public void setTotalNumberOfRequests(final int totalNumberOfRequests) {
        this.totalNumberOfRequests = totalNumberOfRequests;
    }

    public int getNumberOfWorkloads() {
        return numberOfWorkloads;
    }

    public void setNumberOfWorkloads(final int numberOfWorkloads) {
        this.numberOfWorkloads = numberOfWorkloads;
    }

    @SafeVarargs
    @Override
    public final <E> void printStatistics(final E... params) {
        LOG.info(toString());
    }

    @Override
    public String toString() {

        DECIMAL_FORMAT.setRoundingMode(RoundingMode.FLOOR);

        return "GeneralStatisticObject{" +
                "currentTimeStart=" + currentTimeStart +
                "; currentTimeStartFormatted= " + getCurrentTimeStartFormatted() +
                "; currentTimeEnd=" + currentTimeEnd +
                "; currentTimeEndFormatted=" + getCurrentTimeEndFormatted() +
                "; startTime=" + startTime +
                "; endTime=" + endTime +
                "; clientCounter=" + clientCounter +
                "; readRequests=" + readRequests +
                "; writeRequests=" + writeRequests +
                "; totalNumberOfRequests=" + totalNumberOfRequests +
                "; excludeFailedReadRequests=" + excludeFailedReadRequests +
                "; excludeFailedWriteRequests=" + excludeFailedWriteRequests +
                "; failedTotal=" + failedTotal +
                "; successfulTotal=" + successfulTotal +
                "; numberOfWorkloads=" + numberOfWorkloads +
                "; completeStartRuntime=" + completeStartRuntime +
                "; completeEndRuntime=" + completeEndRuntime +
                "; completeEndTime=" + completeEndTime +
                "; completeStartTime=" + completeStartTime +
                "; completeStartTimeFormatted=" + getCompleteStartTimeFormatted() +
                "; completeEndTimeFormatted=" + getCompleteEndTimeFormatted() +
                "; totalRuntime=" + DECIMAL_FORMAT.format(getTotalRuntime()) +
                "; measurementRuntime " + DECIMAL_FORMAT.format(getMeasurementRuntime()) +
                "; RPTU=" + DECIMAL_FORMAT.format((readRequests / getMeasurementRuntime())) +
                "; WPTU=" + DECIMAL_FORMAT.format((writeRequests / getMeasurementRuntime())) +
                "; FPTU=" + DECIMAL_FORMAT.format((failedTotal / getMeasurementRuntime())) +
                "; SPTU=" + DECIMAL_FORMAT.format((successfulTotal / getMeasurementRuntime())) +
                "; TPTU=" + DECIMAL_FORMAT.format((totalNumberOfRequests / getMeasurementRuntime())) +
                "; totalCpuUsage=" + DECIMAL_FORMAT.format(getTotalCpuUsage()) +
                "; averageCpuUsage=" + DECIMAL_FORMAT.format(getCpuUsage()) +
                "; notes= " + GeneralConfiguration.notes +
                '}';
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

    public String getCompleteStartTimeFormatted() {
        return FORMATTER.format(Instant.ofEpochMilli(completeStartTime));
    }

    public String getCompleteEndTimeFormatted() {
        return FORMATTER.format(Instant.ofEpochMilli(completeEndTime));
    }

    public String getCurrentTimeStartFormatted() {
        return FORMATTER.format(Instant.ofEpochMilli(currentTimeStart));
    }

    public String getCurrentTimeEndFormatted() {
        return FORMATTER.format(Instant.ofEpochMilli(currentTimeEnd));
    }

    public double getMeasurementRuntime() {
        return (endTime - startTime) / CONVERSION;
    }

    public double getTotalRuntime() {
        return
                (completeEndRuntime - completeStartRuntime) / CONVERSION;
    }

    @SafeVarargs
    @Override
    public final <E> void writeStatistics(final E... params) {
        DECIMAL_FORMAT.setRoundingMode(RoundingMode.FLOOR);

        int cumulativeCount = CUMULATIVE_COUNT.incrementAndGet();

        if(GeneralConfiguration.WRITE_TO_DATABASE) {
            writeToDatabase(cumulativeCount);
        }

        List<Object> valuesToWrite = new ArrayList<>();

        valuesToWrite.add(getCurrentTimeStartFormatted());
        valuesToWrite.add(getCurrentTimeEndFormatted());
        valuesToWrite.add(clientCounter);
        valuesToWrite.add(readRequests);
        valuesToWrite.add(writeRequests);
        valuesToWrite.add(totalNumberOfRequests);
        valuesToWrite.add(excludeFailedReadRequests);
        valuesToWrite.add(excludeFailedWriteRequests);
        valuesToWrite.add(failedTotal);
        valuesToWrite.add(successfulTotal);
        valuesToWrite.add(numberOfWorkloads);
        valuesToWrite.add(getCompleteStartTimeFormatted());
        valuesToWrite.add(getCompleteEndTimeFormatted());
        valuesToWrite.add(DECIMAL_FORMAT.format(getTotalRuntime()));
        valuesToWrite.add(DECIMAL_FORMAT.format(getMeasurementRuntime()));
        valuesToWrite.add(DECIMAL_FORMAT.format((readRequests / getMeasurementRuntime())));
        valuesToWrite.add(DECIMAL_FORMAT.format((writeRequests / getMeasurementRuntime())));
        valuesToWrite.add(DECIMAL_FORMAT.format((failedTotal / getMeasurementRuntime())));
        valuesToWrite.add(DECIMAL_FORMAT.format((successfulTotal / getMeasurementRuntime())));
        valuesToWrite.add(DECIMAL_FORMAT.format((totalNumberOfRequests / getMeasurementRuntime())));
        valuesToWrite.add(DECIMAL_FORMAT.format(getTotalCpuUsage()));
        valuesToWrite.add(DECIMAL_FORMAT.format(getCpuUsage()));
        valuesToWrite.add(cumulativeCount);
        valuesToWrite.add(CONVERSION);
        valuesToWrite.add(BlockchainStrategy.getBlockchainFrameworkAsString());
        valuesToWrite.add(getStatisticType());
        valuesToWrite.add(GeneralConfiguration.notes);
        valuesToWrite.add(GeneralConfiguration.HOST_ID);

        WriteStatistics.prepareWrite(GeneralConfiguration.LOG_PATH + GeneralConfiguration.GENERAL_STATISTICS_FILE_NAME, HEADER, valuesToWrite);
    }

    private void writeToDatabase(final int cumulativeCount) {

        Locale currentLocale = Locale.getDefault();
        DecimalFormatSymbols decimalFormatSymbols = new DecimalFormatSymbols(currentLocale);
        decimalFormatSymbols.setDecimalSeparator('.');
        decimalFormatSymbols.setGroupingSeparator('.');
        DecimalFormat decimalFormat = (DecimalFormat) DECIMAL_FORMAT.clone();
        decimalFormat.setDecimalFormatSymbols(decimalFormatSymbols);

        try (java.sql.Connection connection = client.database.Connection.getConnection()) {

            String query = "insert into generalstatistics (CURRENT_TIME_START_FORMAT, CURRENT_TIME_END_FORMAT, CLIENT_COUNTER, " +
                    "READ_REQUESTS, WRITE_REQUESTS, TOTAL_NUMBER_OF_REQUESTS, EXCLUDE_FAILED_READ_REQUESTS, " +
                    "EXCLUDE_FAILED_WRITE_REQUESTS, FAILED_TOTAL, SUCCESSFUL_TOTAL, NUMBER_OF_WORKLOADS, " +
                    "COMPLETE_START_TIME_FORMAT, COMPLETE_END_TIME_FORMAT, TOTAL_RUNTIME, MEASUREMENT_RUNTIME, RPTU, WPTU, FPTU, " +
                    "SPTU, TPTU, CPU_USAGE, AVERAGE_CPU_USAGE_PER_CORE, CUMULATIVE_COUNT, CONVERSION_TYPE, BASIC_SYSTEM, " +
                    "STATISTIC_TYPE, NOTES, CID, RUN_ID) VALUES(to_timestamp(?::text, 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') at time zone '" + GeneralConfiguration.TIME_OFFSET + "' ," +
                    "to_timestamp(?::text, 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') at time zone '" + GeneralConfiguration.TIME_OFFSET + "', ?, ?, ?, ?, ?, ?, ?, ?, ?, " +
                    "to_timestamp(?::text, 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') at time zone '"  + GeneralConfiguration.TIME_OFFSET + "', " +
                    "to_timestamp(?::text, 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') at time zone '"  + GeneralConfiguration.TIME_OFFSET +
                    "',?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

            PreparedStatement preparedStatement = connection.prepareStatement(query);
            preparedStatement.setString(1, getCurrentTimeStartFormatted());
            preparedStatement.setString(2, getCurrentTimeEndFormatted());
            preparedStatement.setInt(3, clientCounter);
            preparedStatement.setInt(4, readRequests);
            preparedStatement.setInt(5, writeRequests);
            preparedStatement.setInt(6, totalNumberOfRequests);
            preparedStatement.setBoolean(7, excludeFailedReadRequests);
            preparedStatement.setBoolean(8, excludeFailedWriteRequests);
            preparedStatement.setInt(9, failedTotal);
            preparedStatement.setInt(10, successfulTotal);
            preparedStatement.setInt(11, numberOfWorkloads);
            preparedStatement.setString(12, getCompleteStartTimeFormatted());
            preparedStatement.setString(13, getCompleteEndTimeFormatted());
            preparedStatement.setDouble(14, Double.parseDouble(decimalFormat.format(getTotalRuntime())));
            preparedStatement.setDouble(15, Double.parseDouble(decimalFormat.format(getMeasurementRuntime())));
            preparedStatement.setDouble(16, Double.parseDouble(decimalFormat.format((readRequests / getMeasurementRuntime()))));
            preparedStatement.setDouble(17, Double.parseDouble(decimalFormat.format((writeRequests / getMeasurementRuntime()))));
            preparedStatement.setDouble(18, Double.parseDouble(decimalFormat.format((failedTotal / getMeasurementRuntime()))));
            preparedStatement.setDouble(19, Double.parseDouble(decimalFormat.format((successfulTotal / getMeasurementRuntime()))));
            preparedStatement.setDouble(20, Double.parseDouble(decimalFormat.format((totalNumberOfRequests / getMeasurementRuntime()))));
            preparedStatement.setDouble(21, Double.parseDouble(decimalFormat.format(getTotalCpuUsage())));
            preparedStatement.setDouble(22, Double.parseDouble(decimalFormat.format(getCpuUsage())));
            preparedStatement.setInt(23, cumulativeCount);
            preparedStatement.setDouble(24, CONVERSION);
            preparedStatement.setString(25, BlockchainStrategy.getBlockchainFrameworkAsString());
            preparedStatement.setString(26, getStatisticType().toString());
            preparedStatement.setString(27, GeneralConfiguration.notes);
            preparedStatement.setString(28, GeneralConfiguration.HOST_ID);
            preparedStatement.setString(29, GeneralConfiguration.RUN_ID);

            preparedStatement.executeUpdate();

            preparedStatement.close();
        } catch(SQLException ex) {
            ExceptionHandler.logException(ex);
        }
    }

    @Override
    public StatisticType getStatisticType() {
        return StatisticType.General;
    }
}
