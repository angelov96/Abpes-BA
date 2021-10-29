package client.statistics;

import client.blockchain.BlockchainStrategy;
import client.configuration.GeneralConfiguration;
import client.supplements.ExceptionHandler;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.log4j.Logger;
import org.hibernate.cfg.NotYetImplementedException;

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
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class ListenerStatisticObject implements IStatistics {

    public static final StatisticType STATISTIC_TYPE = StatisticType.Listener;
    private static final int DEFAULT_INVALID_VALUE = -1;
    private static final int DEFAULT_EXISTING_VALUE = -3;
    private static final DateTimeFormatter FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssZ").withZone(ZoneId.of(GeneralConfiguration.DEFAULT_TIME_ZONE));
    private static final DecimalFormat DECIMAL_FORMAT = new DecimalFormat("0.00");
    private static final AtomicInteger CUMULATIVE_COUNT = new AtomicInteger(0);
    private static final double CONVERSION = 1E9;
    private static final String[] HEADER = new String[]{
            "START_TIME",
            "END_TIME",
            "LATENCY",
            "IS_INVALID",
            "IS_EXISTING",
            "HAS_ERROR",
            "IS_VALID",
            "CLIENT_ID",
            "EVENT_KEY",
            "OBTAINED_EVENTS_TOTAL_MAP_SIZE",
            "OBTAINED_EVENTS_MAP_SIZE",
            "INVALID_VALUE_COUNTER",
            "EXISTING_VALUE_COUNTER",
            "ERROR_VALUE_COUNTER",
            "VALID_COUNTER",
            "SET_THRESHOLD",
            "EXPECTED_THRESHOLD",
            "TOTAL_THRESHOLD",
            "CUMULATIVE_COUNT",
            "CONVERSION",
            "BASIC_SYSTEM",
            "STATISTIC_TYPE",
            "CID",
            "START_TIME_FORMAT",
            "END_TIME_FORMAT"
    };
    private static final Logger LOG = Logger.getLogger(ListenerStatisticObject.class);
    private Map<String, Map<String, MutablePair<Long, Long>>> obtainedEventsMap;
    private double setThreshold;
    private double setTotalThreshold;
    private int invalidValueCounter;
    private int existingValueCounter;
    private int errorValueCounter;
    private int validCounter;
    private int totalMapSize;
    private long startTime;
    private long endTime;
    private double expectedThreshold;

    public int getExistingValueCounter() {
        return existingValueCounter;
    }

    public void setExistingValueCounter(final int existingValueCounter) {
        this.existingValueCounter = existingValueCounter;
    }

    public double getSetTotalThreshold() {
        return setTotalThreshold;
    }

    public void setSetTotalThreshold(final double setTotalThreshold) {
        this.setTotalThreshold = setTotalThreshold;
    }

    public int getInvalidValueCounter() {
        return invalidValueCounter;
    }

    public void setInvalidValueCounter(final int invalidValueCounter) {
        this.invalidValueCounter = invalidValueCounter;
    }

    public int getErrorValueCounter() {
        return errorValueCounter;
    }

    public void setErrorValueCounter(final int errorValueCounter) {
        this.errorValueCounter = errorValueCounter;
    }

    public int getValidCounter() {
        return validCounter;
    }

    public void setValidCounter(final int validCounter) {
        this.validCounter = validCounter;
    }

    public int getTotalMapSize() {
        return totalMapSize;
    }

    public void setTotalMapSize(final int totalMapSize) {
        this.totalMapSize = totalMapSize;
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
        setValues(valuesToWrite, cumulativeCount);
    }

    public List<Object> setValues(final List<Object> valueList, final Object cumulativeCount) {

        for (final Map.Entry<String, Map<String, MutablePair<Long, Long>>> stringMapEntry :
                obtainedEventsMap.entrySet()) {
            totalMapSize += stringMapEntry.getValue().size();
            for (final Map.Entry<String, MutablePair<Long, Long>> stringMutablePairEntry :
                    stringMapEntry.getValue().entrySet()) {

                MutablePair<Long, Long> value = stringMutablePairEntry.getValue();
                startTime = value.getLeft();
                endTime = value.getRight();
                valueList.add(DECIMAL_FORMAT.format(getConvertedStartTime()));
                valueList.add(DECIMAL_FORMAT.format(getConvertedEndTime()));
                valueList.add(DECIMAL_FORMAT.format(getLatency()));

                Long rightValue = stringMutablePairEntry.getValue().getRight();
                if (rightValue == DEFAULT_INVALID_VALUE) {
                    invalidValueCounter++;
                    valueList.add(true);
                } else {
                    valueList.add(false);
                }

                if (rightValue == DEFAULT_EXISTING_VALUE) {
                    existingValueCounter++;
                    valueList.add(true);
                } else {
                    valueList.add(false);
                }

                if (rightValue == GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP) {
                    errorValueCounter++;
                    valueList.add(true);
                } else {
                    valueList.add(false);
                }
                if (rightValue != DEFAULT_INVALID_VALUE && rightValue != GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP
                        && rightValue != DEFAULT_EXISTING_VALUE) {
                    validCounter++;
                    valueList.add(true);
                } else {
                    valueList.add(false);
                }

                valueList.add(stringMapEntry.getKey());
                valueList.add(stringMutablePairEntry.getKey());
                valueList.add(totalMapSize);
                valueList.add(obtainedEventsMap.size());
                valueList.add(invalidValueCounter);
                valueList.add(existingValueCounter);
                valueList.add(errorValueCounter);
                valueList.add(validCounter);
                valueList.add(DECIMAL_FORMAT.format(setThreshold));
                valueList.add(DECIMAL_FORMAT.format(expectedThreshold));
                valueList.add(DECIMAL_FORMAT.format(setTotalThreshold));
                valueList.add(cumulativeCount);
                valueList.add(CONVERSION);
                valueList.add(BlockchainStrategy.getBlockchainFrameworkAsString());
                valueList.add(getStatisticType());
                valueList.add(GeneralConfiguration.HOST_ID);
                MutablePair<Long, Long> formattedTimes =
                        ListenerReferenceValues.getTimeMap().get(stringMapEntry.getKey()).get(stringMutablePairEntry.getKey());

                if (stringMapEntry.getKey() == null ||
                        stringMutablePairEntry.getKey() == null ||
                        ListenerReferenceValues.getTimeMap().get(stringMapEntry.getKey()) == null ||
                        ListenerReferenceValues.getTimeMap().get(stringMapEntry.getKey()).get(stringMutablePairEntry.getKey()) == null ||
                        formattedTimes == null ||
                        formattedTimes.getLeft() == null ||
                        formattedTimes.getRight() == null) {

                    LOG.error("ListenerReferenceValues contains null elements (local): " +
                            (stringMapEntry.getKey() == null) + " : " +
                            (stringMutablePairEntry.getKey() == null) + " : " +
                            (ListenerReferenceValues.getTimeMap().get(stringMapEntry.getKey()) == null) + " : " +
                            (ListenerReferenceValues.getTimeMap().get(stringMapEntry.getKey()).get(stringMutablePairEntry.getKey()) == null) + " : " +
                            (formattedTimes == null) + " : " +
                            (formattedTimes != null && formattedTimes.getLeft() == null) + " : " +
                            (formattedTimes != null && formattedTimes.getRight() == null)
                    );

                    if (formattedTimes == null || formattedTimes.getLeft() == null && formattedTimes.getRight() == null) {
                        valueList.add(FORMATTER.format(Instant.ofEpochMilli(0)));
                        valueList.add(FORMATTER.format(Instant.ofEpochMilli(0)));
                    } else if (formattedTimes.getLeft() == null) {
                        valueList.add(FORMATTER.format(Instant.ofEpochMilli(0)));
                        valueList.add(FORMATTER.format(Instant.ofEpochMilli(formattedTimes.getRight())));
                    } else if (formattedTimes.getRight() == null) {
                        valueList.add(FORMATTER.format(Instant.ofEpochMilli(formattedTimes.getLeft())));
                        valueList.add(FORMATTER.format(Instant.ofEpochMilli(0)));
                    } else {
                        LOG.error("Unhandled case");
                        throw new NotYetImplementedException("Not yet implemented");
                    }
                } else {
                    valueList.add(FORMATTER.format(Instant.ofEpochMilli(formattedTimes.getLeft())));
                    valueList.add(FORMATTER.format(Instant.ofEpochMilli(formattedTimes.getRight())));
                }

                //valueList.add(FORMATTER.format(Instant.ofEpochMilli(formattedTimes.getLeft())));
                //valueList.add(FORMATTER.format(Instant.ofEpochMilli(formattedTimes.getRight())));

                WriteStatistics.prepareWrite(GeneralConfiguration.LOG_PATH + GeneralConfiguration.LISTENER_STATISTICS_FILE_NAME, HEADER, valueList);
                valueList.clear();

            }
        }
        return valueList;
    }

    @Override
    public StatisticType getStatisticType() {
        return STATISTIC_TYPE;
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

    public List<Double> getLatencyValues(final boolean excludeInvalidValues, final boolean excludeErrorValues,
                                         final boolean excludeExistingValues) {
        List<Double> valueList = new ArrayList<>();

        for (final Map.Entry<String, Map<String, MutablePair<Long, Long>>> stringMapEntry :
                obtainedEventsMap.entrySet()) {
            for (final Map.Entry<String, MutablePair<Long, Long>> stringMutablePairEntry :
                    stringMapEntry.getValue().entrySet()) {

                MutablePair<Long, Long> value = stringMutablePairEntry.getValue();
                startTime = value.getLeft();
                endTime = value.getRight();

                Long rightValue = stringMutablePairEntry.getValue().getRight();
                if (rightValue == DEFAULT_INVALID_VALUE && excludeInvalidValues) {
                    valueList.add((double) DEFAULT_INVALID_VALUE);
                }
                if (rightValue == GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP && excludeErrorValues) {
                    valueList.add((double) GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP);
                }
                if (rightValue == DEFAULT_EXISTING_VALUE && excludeExistingValues) {
                    valueList.add((double) DEFAULT_EXISTING_VALUE);
                }
                if (rightValue != DEFAULT_INVALID_VALUE && rightValue != GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP
                        && rightValue != DEFAULT_EXISTING_VALUE) {
                    valueList.add(getLatency());
                }
            }
        }
        return valueList;
    }

    public Map<String, Map<String, MutablePair<Long, Long>>> getObtainedEventsMap() {
        return obtainedEventsMap;
    }

    public synchronized void setObtainedEventsMap(
            final Map<String, Map<String, MutablePair<Long, Long>>> obtainedEventsMap) {
        this.obtainedEventsMap = obtainedEventsMap;
    }

    public double getSetThreshold() {
        return setThreshold;
    }

    public void setSetThreshold(final double setThreshold) {
        this.setThreshold = setThreshold;
    }

    public double getExpectedThreshold() {
        return expectedThreshold;
    }

    public void setExpectedThreshold(final double expectedThreshold) {
        this.expectedThreshold = expectedThreshold;
    }

    @SafeVarargs
    @Override
    public final <E> void printStatistics(final E... params) {
        LOG.info(toString());
    }

    @Override
    public String toString() {

        DECIMAL_FORMAT.setRoundingMode(RoundingMode.FLOOR);

        return "ListenerStatisticObject{" +
                "obtainedEventsMap=" + this.obtainedEventsMap +
                ", obtainedEventsMapSize=" + this.obtainedEventsMap.size() +
                ", obtainedEventsTotalMapSize=" + totalMapSize +
                ", setThreshold=" + DECIMAL_FORMAT.format(setThreshold) +
                ", invalidValueCounter=" + invalidValueCounter +
                ", existingValueCounter=" + existingValueCounter +
                ", errorValueCounter=" + errorValueCounter +
                ", validCounter=" + validCounter +
                ", expectedThreshold=" + DECIMAL_FORMAT.format(expectedThreshold) +
                ", setTotalThreshold=" + DECIMAL_FORMAT.format(setTotalThreshold) +
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

            String query = "insert into listenerstatisticobject (start_time, end_time, latency, is_invalid, " +
                    "is_existing, has_error, is_valid, client_id, event_key, obtained_events_total_map_size, " +
                    "obtained_events_map_size, invalid_value_counter, existing_value_counter, error_value_counter, " +
                    "valid_counter, set_threshold, expected_threshold, total_threshold, cumulative_count, conversion," +
                    " basic_system, statistic_type, cid, run_id, start_time_format, end_time_format)" +
                    "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " +
                    "to_timestamp(?::text, 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') at time zone '" + GeneralConfiguration.TIME_OFFSET + "', " +
                    "to_timestamp(?::text, 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') at time zone '" + GeneralConfiguration.TIME_OFFSET + "')";

            for (final Map.Entry<String, Map<String, MutablePair<Long, Long>>> stringMapEntry :
                    obtainedEventsMap.entrySet()) {
                totalMapSize += stringMapEntry.getValue().size();
                for (final Map.Entry<String, MutablePair<Long, Long>> stringMutablePairEntry :
                        stringMapEntry.getValue().entrySet()) {

                    PreparedStatement preparedStatement = PrepareStatementCollection.addPreparedStatement(query);

                    MutablePair<Long, Long> value = stringMutablePairEntry.getValue();
                    startTime = value.getLeft();
                    endTime = value.getRight();
                    preparedStatement.setDouble(1, Double.parseDouble(decimalFormat.format(getConvertedStartTime())));
                    preparedStatement.setDouble(2, Double.parseDouble(decimalFormat.format(getConvertedEndTime())));
                    preparedStatement.setDouble(3, Double.parseDouble(decimalFormat.format(getLatency())));

                    Long rightValue = stringMutablePairEntry.getValue().getRight();
                    if (rightValue == DEFAULT_INVALID_VALUE) {
                        invalidValueCounter++;
                        preparedStatement.setBoolean(4, true);
                    } else {
                        preparedStatement.setBoolean(4, false);
                    }

                    if (rightValue == DEFAULT_EXISTING_VALUE) {
                        existingValueCounter++;
                        preparedStatement.setBoolean(5, true);
                    } else {
                        preparedStatement.setBoolean(5, false);
                    }

                    if (rightValue == GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP) {
                        errorValueCounter++;
                        preparedStatement.setBoolean(6, true);
                    } else {
                        preparedStatement.setBoolean(6, false);
                    }
                    if (rightValue != DEFAULT_INVALID_VALUE && rightValue != GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP
                            && rightValue != DEFAULT_EXISTING_VALUE) {
                        validCounter++;
                        preparedStatement.setBoolean(7, true);
                    } else {
                        preparedStatement.setBoolean(7, false);
                    }

                    preparedStatement.setString(8, stringMapEntry.getKey());
                    preparedStatement.setString(9, stringMutablePairEntry.getKey());
                    preparedStatement.setInt(10, totalMapSize);
                    preparedStatement.setInt(11, obtainedEventsMap.size());
                    preparedStatement.setInt(12, invalidValueCounter);
                    preparedStatement.setInt(13, existingValueCounter);
                    preparedStatement.setInt(14, errorValueCounter);
                    preparedStatement.setInt(15, validCounter);
                    preparedStatement.setDouble(16, Double.parseDouble(decimalFormat.format(setThreshold)));
                    preparedStatement.setDouble(17, Double.parseDouble(decimalFormat.format(expectedThreshold)));
                    preparedStatement.setDouble(18, Double.parseDouble(decimalFormat.format(setTotalThreshold)));
                    preparedStatement.setInt(19, cumulativeCount);
                    preparedStatement.setDouble(20, CONVERSION);
                    preparedStatement.setString(21, BlockchainStrategy.getBlockchainFrameworkAsString());
                    preparedStatement.setString(22, getStatisticType().name());
                    preparedStatement.setString(23, GeneralConfiguration.HOST_ID);
                    preparedStatement.setString(24, GeneralConfiguration.RUN_ID);
                    MutablePair<Long, Long> formattedTimes =
                            ListenerReferenceValues.getTimeMap().get(stringMapEntry.getKey()).get(stringMutablePairEntry.getKey());

                    if (stringMapEntry.getKey() == null ||
                            stringMutablePairEntry.getKey() == null ||
                            ListenerReferenceValues.getTimeMap().get(stringMapEntry.getKey()) == null ||
                            ListenerReferenceValues.getTimeMap().get(stringMapEntry.getKey()).get(stringMutablePairEntry.getKey()) == null ||
                            formattedTimes == null ||
                            formattedTimes.getLeft() == null ||
                            formattedTimes.getRight() == null) {

                        LOG.error("ListenerReferenceValues contains null elements: " +
                                (stringMapEntry.getKey() == null) + " : " +
                                (stringMutablePairEntry.getKey() == null) + " : " +
                                (ListenerReferenceValues.getTimeMap().get(stringMapEntry.getKey()) == null) + " : " +
                                (ListenerReferenceValues.getTimeMap().get(stringMapEntry.getKey()).get(stringMutablePairEntry.getKey()) == null) + " : " +
                                (formattedTimes == null) + " : " +
                                (formattedTimes != null && formattedTimes.getLeft() == null) + " : " +
                                (formattedTimes != null && formattedTimes.getRight() == null)
                        );

                        if (formattedTimes == null || formattedTimes.getLeft() == null && formattedTimes.getRight() == null) {
                            preparedStatement.setString(25, FORMATTER.format(Instant.ofEpochMilli(0)));
                            preparedStatement.setString(26, FORMATTER.format(Instant.ofEpochMilli(0)));
                        } else if (formattedTimes.getLeft() == null) {
                            preparedStatement.setString(25,
                                    FORMATTER.format(Instant.ofEpochMilli(0)));
                            preparedStatement.setString(26,
                                    FORMATTER.format(Instant.ofEpochMilli(formattedTimes.getRight())));
                        } else if (formattedTimes.getRight() == null) {
                            preparedStatement.setString(25,
                                    FORMATTER.format(Instant.ofEpochMilli(formattedTimes.getLeft())));
                            preparedStatement.setString(26,
                                    FORMATTER.format(Instant.ofEpochMilli(0)));
                        } else {
                            LOG.error("Unhandled case");
                            throw new NotYetImplementedException("Not yet implemented");
                        }
                    } else {
                        preparedStatement.setString(25,
                                FORMATTER.format(Instant.ofEpochMilli(formattedTimes.getLeft())));
                        preparedStatement.setString(26,
                                FORMATTER.format(Instant.ofEpochMilli(formattedTimes.getRight())));
                    }

                    //preparedStatement.executeUpdate();
                    preparedStatement.addBatch();

                }
            }
        } catch (SQLException ex) {
            ExceptionHandler.logException(ex);
        }
    }

}
