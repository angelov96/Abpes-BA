package client.statistics;

import client.configuration.GeneralConfiguration;
import client.supplements.ExceptionHandler;
import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;
import org.apache.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.math.RoundingMode;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.String.format;

public class DescriptiveStatistics {

    private static final Logger LOG = Logger.getLogger(DescriptiveStatistics.class);
    private static final String[] HEADER = new String[]{"PERCENTILE_10",
            "PERCENTILE_25", "PERCENTILE_50", "PERCENTILE_75",
            "PERCENTILE_90", "MEAN", "MAX", "MIN", "IQR",
            "RANGE", "VARIANCE", "STANDARD", "KURTOSIS", "SKEWNESS", "N", "BASIC_SYSTEM", "STATISTIC_TYPE", "CID"};
    private static final boolean EXCLUDE_INVALID_VALUES = true;
    private static final boolean EXCLUDE_ERROR_VALUES = true;
    private static final boolean EXCLUDE_EXISTING_VALUES = true;
    private static final DecimalFormat DECIMAL_FORMAT = new DecimalFormat("0.00");

    private DescriptiveStatistics() {
    }

    @SafeVarargs
    public static <E> void printStatistics(final E... params) {
        List<Object> valueList = prepareStatisticList(params);
        LOG.info(valueList);
    }

    @SafeVarargs
    @NotNull
    private static <E> List<Object> prepareStatisticList(final E... params) {
        List<Double> list = removeValues(params[0]);

        IStatistics.StatisticType statisticType = (IStatistics.StatisticType) params[1];
        String blockchainFramework = (String) params[2];
        List<Object> valueList = new ArrayList<>();

        valueList.add(DECIMAL_FORMAT.format(checkForNaN(getPercentile(list, 10))));
        valueList.add(DECIMAL_FORMAT.format(checkForNaN(getPercentile(list, 25))));
        valueList.add(DECIMAL_FORMAT.format(checkForNaN(getPercentile(list, 50))));
        valueList.add(DECIMAL_FORMAT.format(checkForNaN(getPercentile(list, 75))));
        valueList.add(DECIMAL_FORMAT.format(checkForNaN(getPercentile(list, 90))));
        valueList.add(DECIMAL_FORMAT.format(checkForNaN(getMean(list))));
        valueList.add(DECIMAL_FORMAT.format(checkForNaN(getMax(list))));
        valueList.add(DECIMAL_FORMAT.format(checkForNaN(getMin(list))));
        valueList.add(DECIMAL_FORMAT.format(checkForNaN(getIqr(list))));
        valueList.add(DECIMAL_FORMAT.format(checkForNaN(getRange(list))));
        valueList.add(DECIMAL_FORMAT.format(checkForNaN(getVariance(list))));
        valueList.add(DECIMAL_FORMAT.format(checkForNaN(getStandardDeviation(list))));
        valueList.add(DECIMAL_FORMAT.format(checkForNaN(getKurtosis(list))));
        valueList.add(DECIMAL_FORMAT.format(checkForNaN(getSkewness(list))));
        valueList.add(DECIMAL_FORMAT.format(checkForNaN(getN(list))));
        valueList.add(blockchainFramework);
        valueList.add(statisticType.name());
        valueList.add(GeneralConfiguration.HOST_ID);
        return valueList;
    }

    private static <E> List<Double> removeValues(final E param) {
        List<Double> list = (List<Double>) param;

        if (EXCLUDE_INVALID_VALUES) {
            AtomicInteger i = new AtomicInteger();
            boolean removed = list.removeIf(entry -> {

                if (entry == ErrorValues.getDefaultInvalidValue()) {
                    LOG.error("Removed invalid value " + entry);
                    i.getAndIncrement();
                    return true;
                }
                return false;
            });
            if (removed) {
                int numberOfInvalidValues = i.get();
                LOG.error("Removed " + numberOfInvalidValues + " invalid value" + (numberOfInvalidValues > 1 ? "s" : ""));
            }
        }
        if (EXCLUDE_ERROR_VALUES) {
            AtomicInteger i = new AtomicInteger();
            boolean removed = list.removeIf(entry -> {
                if (entry == ErrorValues.getDefaultErrorTimestamp()) {
                    LOG.error("Removed error value " + entry);
                    i.getAndIncrement();
                    return true;
                }
                return false;
            });
            if (removed) {
                LOG.error("Removed " + i.get() + " error values");
            }
        }
        if (EXCLUDE_EXISTING_VALUES) {
            AtomicInteger i = new AtomicInteger();
            boolean removed = list.removeIf(entry -> {
                if (entry == ErrorValues.getDefaultExistingValue()) {
                    LOG.error("Removed existing value " + entry);
                    i.getAndIncrement();
                    return true;
                }
                return false;
            });
            if (removed) {
                LOG.error("Removed " + i.get() + " existing values");
            }
        }
        return list;
    }

    private static double checkForNaN(final double valueOf) {
        if(Double.isNaN(valueOf)) {
            return GeneralConfiguration.DEFAULT_NAN;
        }
        return valueOf;
        //return "NaN".equals(valueOf) ? "0.0" : roundTo(valueOf);
    }

    private static String roundTo(final String valueOf) {
        return format("%.2f", Double.valueOf(valueOf));
    }

    public static double getStandardDeviation(final List<Double> list) {
        StandardDeviation standardDeviation = new StandardDeviation();
        double[] doubles = list.stream().mapToDouble(i -> i).toArray();
        return standardDeviation.evaluate(doubles);
    }

    public static double getMin(final List<Double> list) {
        return getDescriptiveStatistics(list).getMin();
    }

    public static double getMax(final List<Double> list) {
        return getDescriptiveStatistics(list).getMax();
    }

    public static double getRange(final List<Double> list) {
        return getMax(list) - getMin(list);
    }

    public static double getPercentile(final List<Double> list, final double percentile) {
        return getDescriptiveStatistics(list).getPercentile(percentile);
    }

    private static org.apache.commons.math3.stat.descriptive.DescriptiveStatistics getDescriptiveStatistics(final List<Double> list) {
        double[] doubles = list.stream().mapToDouble(i -> i).toArray();
        return new org.apache.commons.math3.stat.descriptive.DescriptiveStatistics(doubles);
    }

    public static double getMean(final List<Double> list) {
        return getDescriptiveStatistics(list).getMean();
    }

    public static double getKurtosis(final List<Double> list) {
        return getDescriptiveStatistics(list).getKurtosis();
    }

    public static double getSkewness(final List<Double> list) {
        return getDescriptiveStatistics(list).getSkewness();
    }

    public static double getN(final List<Double> list) {
        return getDescriptiveStatistics(list).getN();
    }

    public static double getVariance(final List<Double> list) {
        return getDescriptiveStatistics(list).getVariance();
    }

    public static double getIqr(final List<Double> list) {
        return getDescriptiveStatistics(list).getPercentile(75) - getDescriptiveStatistics(list).getPercentile(25);
    }

    @SafeVarargs
    private static <E> void writeToDatabase(final E... params) {

        Locale currentLocale = Locale.getDefault();
        DecimalFormatSymbols decimalFormatSymbols = new DecimalFormatSymbols(currentLocale);
        decimalFormatSymbols.setDecimalSeparator('.');
        decimalFormatSymbols.setGroupingSeparator('.');
        DecimalFormat decimalFormat = (DecimalFormat) DECIMAL_FORMAT.clone();
        decimalFormat.setDecimalFormatSymbols(decimalFormatSymbols);

        List<Double> list = removeValues(params[0]);

        IStatistics.StatisticType statisticType = (IStatistics.StatisticType) params[1];
        String blockchainFramework = (String) params[2];

        try (java.sql.Connection connection = client.database.Connection.getConnection()){

            String query = "insert into descriptivestatistics (PERCENTILE_10, PERCENTILE_25, PERCENTILE_50, " +
                    "PERCENTILE_75, PERCENTILE_90, MEAN, MAX, MIN, IQR, RANGE, VARIANCE, STANDARD, KURTOSIS, " +
                    "SKEWNESS, N, BASIC_SYSTEM, STATISTIC_TYPE, CID, RUN_ID) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

            PreparedStatement preparedStatement = connection.prepareStatement(query);
            preparedStatement.setDouble(1, Double.parseDouble(decimalFormat.format(checkForNaN(getPercentile(list, 10)))));
            preparedStatement.setDouble(2, Double.parseDouble(decimalFormat.format(checkForNaN(getPercentile(list, 25)))));
            preparedStatement.setDouble(3, Double.parseDouble(decimalFormat.format(checkForNaN(getPercentile(list, 50)))));
            preparedStatement.setDouble(4, Double.parseDouble(decimalFormat.format(checkForNaN(getPercentile(list, 75)))));
            preparedStatement.setDouble(5, Double.parseDouble(decimalFormat.format(checkForNaN(getPercentile(list, 90)))));
            preparedStatement.setDouble(6, Double.parseDouble(decimalFormat.format(checkForNaN(getMean(list)))));
            preparedStatement.setDouble(7, Double.parseDouble(decimalFormat.format(checkForNaN(getMax(list)))));
            preparedStatement.setDouble(8, Double.parseDouble(decimalFormat.format(checkForNaN(getMin(list)))));
            preparedStatement.setDouble(9, Double.parseDouble(decimalFormat.format(checkForNaN(getIqr(list)))));
            preparedStatement.setDouble(10, Double.parseDouble(decimalFormat.format(checkForNaN(getRange(list)))));
            preparedStatement.setDouble(11, Double.parseDouble(decimalFormat.format(checkForNaN(getVariance(list)))));
            preparedStatement.setDouble(12, Double.parseDouble(decimalFormat.format(checkForNaN(getStandardDeviation(list)))));
            preparedStatement.setDouble(13, Double.parseDouble(decimalFormat.format(checkForNaN(getKurtosis(list)))));
            preparedStatement.setDouble(14, Double.parseDouble(decimalFormat.format(checkForNaN(getSkewness(list)))));
            preparedStatement.setDouble(15, Double.parseDouble(decimalFormat.format(checkForNaN(getN(list)))));
            preparedStatement.setString(16, blockchainFramework);
            preparedStatement.setString(17, statisticType.name());
            preparedStatement.setString(18, GeneralConfiguration.HOST_ID);
            preparedStatement.setString(19, GeneralConfiguration.RUN_ID);

            preparedStatement.executeUpdate();

            preparedStatement.close();

        } catch(SQLException ex) {
            ExceptionHandler.logException(ex);
        }
    }


    @SafeVarargs
    public static <E> void writeStatistics(final E... params) {

        DECIMAL_FORMAT.setRoundingMode(RoundingMode.FLOOR);

        if(GeneralConfiguration.WRITE_TO_DATABASE) {
            writeToDatabase(params);
        }

        List<Object> valueList = prepareStatisticList(params);
        WriteStatistics.prepareWrite(GeneralConfiguration.LOG_PATH + GeneralConfiguration.DESCRIPTIVE_STATISTICS_FILE_NAME, HEADER, valueList);
    }
}
