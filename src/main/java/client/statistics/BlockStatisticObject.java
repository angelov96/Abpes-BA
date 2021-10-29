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

public abstract class BlockStatisticObject implements IStatistics {

    public static final StatisticType STATISTIC_TYPE = StatisticType.Block;
    private static final DecimalFormat DECIMAL_FORMAT = new DecimalFormat("0.00");
    private static final AtomicInteger CUMULATIVE_COUNT = new AtomicInteger(0);
    private static final double CONVERSION = 1E9;
    private static final String[] HEADER = new String[]{
            "BLOCK_ID",
            "RECEIVED_TIME",
            "CLIENT_ID",
            "NUMBER_OF_TRANSACTIONS",
            "NUMBER_OF_ACTIONS",
            "BLOCK_NUM",
            "TX_IDS_IN_BLOCK",
            "CUMULATIVE_COUNT",
            "BASIC_SYSTEM",
            "STATISTIC_TYPE",
            "CID"
    };

    private static final Logger LOG = Logger.getLogger(BlockStatisticObject.class);

    public String getBlockId() {
        return blockId;
    }

    public void setBlockId(final String blockId) {
        this.blockId = blockId;
    }

    public long getReceivedTime() {
        return receivedTime;
    }

    public void setReceivedTime(final long receivedTime) {
        this.receivedTime = receivedTime;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(final String clientId) {
        this.clientId = clientId;
    }

    public int getNumberOfTransactions() {
        return numberOfTransactions;
    }

    public void setNumberOfTransactions(final int numberOfTransactions) {
        this.numberOfTransactions = numberOfTransactions;
    }

    public int getNumberOfActions() {
        return numberOfActions;
    }

    public void setNumberOfActions(final int numberOfActions) {
        this.numberOfActions = numberOfActions;
    }

    private String blockId;
    private long receivedTime;
    private String clientId;
    private int numberOfTransactions;
    private int numberOfActions;

    public List<String> getTxIdList() {
        return txIdList;
    }

    private final List<String> txIdList = new ArrayList<>();

    public long getBlockNum() {
        return blockNum;
    }

    public void setBlockNum(final long blockNum) {
        this.blockNum = blockNum;
    }

    private long blockNum;

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

    private static final DateTimeFormatter FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssZ").withZone(ZoneId.of(GeneralConfiguration.DEFAULT_TIME_ZONE));

    public List<Object> setValues(final List<Object> valueList, final Object cumulativeCount) {

        valueList.add(blockId);
        valueList.add(FORMATTER.format(Instant.ofEpochMilli(receivedTime)));
        valueList.add(clientId);
        valueList.add(numberOfTransactions);
        valueList.add(numberOfActions);
        valueList.add(blockNum);
        valueList.add(txIdList.toString());
        valueList.add(cumulativeCount);
        valueList.add(CONVERSION);
        valueList.add(BlockchainStrategy.getBlockchainFrameworkAsString());
        valueList.add(getStatisticType());
        valueList.add(GeneralConfiguration.HOST_ID);

        WriteStatistics.prepareWrite(GeneralConfiguration.LOG_PATH + GeneralConfiguration.BLOCK_STATISTICS_FILE_NAME,
                HEADER, valueList);
        valueList.clear();

        return valueList;
    }

    @Override
    public StatisticType getStatisticType() {
        return STATISTIC_TYPE;
    }

    @SafeVarargs
    @Override
    public final <E> void printStatistics(final E... params) {
        LOG.info(toString());
    }

    @Override
    public String toString() {

        DECIMAL_FORMAT.setRoundingMode(RoundingMode.FLOOR);

        return "BlockStatisticObject{" +
                "blockId=" + this.blockId +
                ", receivedTime=" + this.receivedTime +
                ", numberOfTransactions=" + this.numberOfTransactions +
                ", numberOfActions=" + this.numberOfActions +
                ", blockNum=" + this.blockNum +
                ", txIdList=" + this.txIdList.toString() +
                ", clientId=" + this.clientId +
                '}';
    }

    public String getReceivedTimeFormatted() {
        return FORMATTER.format(Instant.ofEpochMilli(receivedTime));
    }

    private void writeToDatabase(final int cumulativeCount) {

        Locale currentLocale = Locale.getDefault();
        DecimalFormatSymbols decimalFormatSymbols = new DecimalFormatSymbols(currentLocale);
        decimalFormatSymbols.setDecimalSeparator('.');
        decimalFormatSymbols.setGroupingSeparator('.');
        DecimalFormat decimalFormat = (DecimalFormat) DECIMAL_FORMAT.clone();
        decimalFormat.setDecimalFormatSymbols(decimalFormatSymbols);

        try {

            String query = "insert into blockstatisticobject (block_id, received_time, client_id, " +
                    "number_of_transactions, number_of_actions, block_num, tx_id_list, cumulative_count, basic_system, statistic_type, cid, " +
                    "run_id)" +
                    "VALUES(?, " +
                    "to_timestamp(?::text, 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') at time zone '" + GeneralConfiguration.TIME_OFFSET
                    + "', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

            PreparedStatement preparedStatement = PrepareStatementCollection.addPreparedStatement(query);

            preparedStatement.setString(1, blockId);
            preparedStatement.setString(2, getReceivedTimeFormatted());
            preparedStatement.setString(3, clientId);
            preparedStatement.setInt(4, numberOfTransactions);
            preparedStatement.setInt(5, numberOfActions);
            preparedStatement.setLong(6, blockNum);
            preparedStatement.setString(7, txIdList.toString());
            preparedStatement.setInt(8, cumulativeCount);
            preparedStatement.setString(9, BlockchainStrategy.getBlockchainFrameworkAsString());
            preparedStatement.setString(10, getStatisticType().name());
            preparedStatement.setString(11, GeneralConfiguration.HOST_ID);
            preparedStatement.setString(12, GeneralConfiguration.RUN_ID);

            //preparedStatement.executeUpdate();
            preparedStatement.addBatch();

        } catch (SQLException ex) {
            ExceptionHandler.logException(ex);
        }
    }

}
