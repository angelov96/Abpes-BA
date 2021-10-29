package client.statistics;

import client.blockchain.BlockchainStrategy;
import client.commoninterfaces.IBlockchainPayload;
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
import java.util.stream.Collectors;

public abstract class ReadStatisticObject implements IStatistics {

    public static final StatisticType STATISTIC_TYPE = StatisticType.Read;
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssZ").withZone(ZoneId.of(GeneralConfiguration.DEFAULT_TIME_ZONE));
    private static final IBlockchainPayload.Payload_Type PAYLOAD_TYPE = IBlockchainPayload.Payload_Type.READ;
    private static final DecimalFormat DECIMAL_FORMAT = new DecimalFormat("0.00");
    private static final AtomicInteger CUMULATIVE_COUNT = new AtomicInteger(0);
    private static final String[] HEADER = new String[]{
            "START_TIME_FORMAT",
            "END_TIME_FORMAT",
            "START_TIME",
            "END_TIME",
            "LATENCY",
            "REQUEST_ID_OF_WORKLOAD",
            "CLIENT_ID",
            "REQUEST_NUMBER",
            "FAILED_REQUEST",
            "SPECIFIC_PAYLOAD_TYPE",
            "PAYLOAD_TYPE",
            "ERROR_MESSAGES",
            "HAD_ERROR",
            "PARTICIPATING_SERVERS",
            "NUMBER_OF_ERROR_MESSAGES",
            "CUMULATIVE_COUNT",
            "CONVERSION",
            "IS_CONSISTENT",
            "BASIC_SYSTEM",
            "STATISTIC_TYPE",
            "CID"
    };
    private static final double CONVERSION = 1E9;
    private static final Logger LOG = Logger.getLogger(ReadStatisticObject.class);
    private final List<String> errorMessages = new ArrayList<>();
    private final List<String> participatingServers = new ArrayList<>();
    private long startTime;
    private long endTime;
    private String requestId;
    private String clientId;
    private int requestNumber;
    private boolean failedRequest;
    private long currentTimeStart;
    private long currentTimeEnd;

    public int getNumberOfErrors() {
        return numberOfErrors;
    }

    public void setNumberOfErrors(final int numberOfErrors) {
        this.numberOfErrors = numberOfErrors;
    }

    private int numberOfErrors;


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
        valuesToWrite.add(DECIMAL_FORMAT.format(getConvertedStartTime()));
        valuesToWrite.add(DECIMAL_FORMAT.format(getConvertedEndTime()));
        valuesToWrite.add(DECIMAL_FORMAT.format(getLatency()));
        valuesToWrite.add(requestId);
        valuesToWrite.add(clientId);
        valuesToWrite.add(requestNumber);
        valuesToWrite.add(failedRequest);
        valuesToWrite.add(specificPayloadTypes.toString());
        valuesToWrite.add(PAYLOAD_TYPE);

        valuesToWrite.add(sanitizeErrorList());
        valuesToWrite.add(isCompletedWithError());
        valuesToWrite.add(getParticipatingServers());
        valuesToWrite.add(getNumberOfErrors());
        valuesToWrite.add(cumulativeCount);
        valuesToWrite.add(CONVERSION);
        valuesToWrite.add(isConsistent());
        valuesToWrite.add(BlockchainStrategy.getBlockchainFrameworkAsString());
        valuesToWrite.add(getStatisticType());
        valuesToWrite.add(GeneralConfiguration.HOST_ID);

        WriteStatistics.prepareWrite(GeneralConfiguration.LOG_PATH + GeneralConfiguration.READ_STATISTICS_FILE_NAME,
                HEADER, valuesToWrite);

    }

    public List<String> getParticipatingServers() {
        return participatingServers;
    }

    public double getLatency() {
        if (ErrorValues.getErrorValueList().contains(endTime)) {
            return endTime;
        } else {
            return (endTime - startTime) / CONVERSION;
        }
    }

    public double getConvertedStartTime() {
        return (startTime);
    }

    public double getConvertedEndTime() {
        if (ErrorValues.getErrorValueList().contains(endTime)) {
            return endTime;
        } else {
            return (endTime) /*/ CONVERSION */;
        }
    }

    public String getCurrentTimeStartFormatted() {
        return FORMATTER.format(Instant.ofEpochMilli(currentTimeStart));
    }

    public String getCurrentTimeEndFormatted() {
        return FORMATTER.format(Instant.ofEpochMilli(currentTimeEnd));
    }

    public boolean isCompletedWithError() {
        return !errorMessages.isEmpty();
    }

    public boolean isConsistent() {
        if (startTime > endTime) {
            return false;
        }
        if (currentTimeStart > currentTimeEnd) {
            return false;
        }
        return true;
    }

    @Override
    public StatisticType getStatisticType() {
        return STATISTIC_TYPE;
    }

    public boolean isFailedRequest() {
        return failedRequest;
    }

    public void setFailedRequest(final boolean failedRequest) {
        this.failedRequest = failedRequest;
    }

    public int getRequestNumber() {
        return requestNumber;
    }

    public void setRequestNumber(final int requestNumber) {
        this.requestNumber = requestNumber;
    }

    public long getCurrentTimeStart() {
        return currentTimeStart;
    }

    public void setCurrentTimeStart(final long currentTimeStart) {
        this.currentTimeStart = currentTimeStart;
    }

    public long getCurrentTimeEnd() {
        return currentTimeEnd;
    }

    public void setCurrentTimeEnd(final long currentTimeEnd) {
        this.currentTimeEnd = currentTimeEnd;
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

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(final String requestId) {
        this.requestId = requestId;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(final String clientId) {
        this.clientId = clientId;
    }

    public List<String> getErrorMessages() {
        return errorMessages;
    }

    public List<String> getSpecificPayloadTypeList() {
        return specificPayloadTypes;
    }

    private final List<String> specificPayloadTypes = new ArrayList<>();

    @SafeVarargs
    @Override
    public final <E> void printStatistics(final E... params) {
        LOG.info(toString());
    }

    @Override
    public String toString() {
        return "ReadStatisticObject{" +
                "errorMessages=" + errorMessages +
                "; errorMessageSize=" + getErrorMessagesSize() +
                "; startTime=" + startTime +
                "; endTime=" + endTime +
                "; requestId='" + requestId + '\'' +
                "; clientId='" + clientId + '\'' +
                "; requestNumber=" + requestNumber +
                "; currentTimeStart=" + currentTimeStart +
                "; currentTimeEnd=" + currentTimeEnd +
                "; completedWithError=" + isCompletedWithError() +
                "; payloadType=" + PAYLOAD_TYPE +
                "; isFailedRequest=" + failedRequest +
                "; participatingServers=" + participatingServers +
                "; currentTimeStartFormatted=" + getCurrentTimeStartFormatted() +
                "; currentTimeEndFormatted=" + getCurrentTimeEndFormatted() +
                "; specificPayloadTypes=" + specificPayloadTypes.toString() +
                '}';
    }

    public int getErrorMessagesSize() {
        return errorMessages.size();
    }

    private void writeToDatabase(final int cumulativeCount) {

        Locale currentLocale = Locale.getDefault();
        DecimalFormatSymbols decimalFormatSymbols = new DecimalFormatSymbols(currentLocale);
        decimalFormatSymbols.setDecimalSeparator('.');
        decimalFormatSymbols.setGroupingSeparator('.');
        DecimalFormat decimalFormat = (DecimalFormat) DECIMAL_FORMAT.clone();
        decimalFormat.setDecimalFormatSymbols(decimalFormatSymbols);

        try {

            String query = "insert into readstatisticobject (START_TIME_FORMAT, END_TIME_FORMAT, START_TIME, END_TIME, " +
                    "LATENCY, REQUEST_ID_OF_WORKLOAD, CLIENT_ID, REQUEST_NUMBER, FAILED_REQUEST, PAYLOAD_TYPE, " +
                    "ERROR_MESSAGES, HAD_ERROR, PARTICIPATING_SERVERS, CUMULATIVE_COUNT, CONVERSION, IS_CONSISTENT, " +
                    "BASIC_SYSTEM, STATISTIC_TYPE, CID, RUN_ID, SPECIFIC_PAYLOAD_TYPES, NUMBER_OF_ERROR_MESSAGES) VALUES(to_timestamp(?::text, 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') at time zone '" + GeneralConfiguration.TIME_OFFSET + "' ," +
                    "to_timestamp(?::text, 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') at time zone '" + GeneralConfiguration.TIME_OFFSET +
                    "',?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

            PreparedStatement preparedStatement = PrepareStatementCollection.addPreparedStatement(query);
            preparedStatement.setString(1, getCurrentTimeStartFormatted());
            preparedStatement.setString(2, getCurrentTimeEndFormatted());
            preparedStatement.setDouble(3, Double.parseDouble(decimalFormat.format(getConvertedStartTime())));
            preparedStatement.setDouble(4, Double.parseDouble(decimalFormat.format(getConvertedEndTime())));
            preparedStatement.setDouble(5, Double.parseDouble(decimalFormat.format(getLatency())));
            preparedStatement.setString(6, requestId);
            preparedStatement.setString(7, clientId);
            preparedStatement.setInt(8, requestNumber);
            preparedStatement.setBoolean(9, failedRequest);
            preparedStatement.setString(10, PAYLOAD_TYPE.name());
            preparedStatement.setString(11, sanitizeErrorList().toString());
            preparedStatement.setBoolean(12, isCompletedWithError());
            preparedStatement.setString(13, getParticipatingServers().toString());
            preparedStatement.setInt(14, cumulativeCount);
            preparedStatement.setDouble(15, CONVERSION);
            preparedStatement.setBoolean(16, isConsistent());
            preparedStatement.setString(17, BlockchainStrategy.getBlockchainFrameworkAsString());
            preparedStatement.setString(18, getStatisticType().toString());
            preparedStatement.setString(19, GeneralConfiguration.HOST_ID);
            preparedStatement.setString(20, GeneralConfiguration.RUN_ID);
            preparedStatement.setString(21, specificPayloadTypes.toString());
            preparedStatement.setInt(22, getErrorMessagesSize());

            //preparedStatement.executeUpdate();
            preparedStatement.addBatch();

        } catch(SQLException ex) {
            ExceptionHandler.logException(ex);
        }
    }

    private List<String> sanitizeErrorList() {
        return errorMessages.stream()
                .map(str -> str.replaceAll(Character.toString(GeneralConfiguration.CSV_SEPARATOR),
                        Character.toString(GeneralConfiguration.CSV_REPLACEMENT_SEPARATOR)))
                .collect(Collectors.toList());
    }

}
