package client.statistics;

import client.blockchain.BlockchainStrategy;
import client.configuration.GeneralConfiguration;
import client.supplements.ExceptionHandler;
import org.apache.log4j.Logger;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public abstract class CustomStatisticObject<E1> implements IStatistics {

    public static final StatisticType STATISTIC_TYPE = StatisticType.Custom;
    private static final Logger LOG = Logger.getLogger(CustomStatisticObject.class);

    @SafeVarargs
    @Override
    public final <E> void writeStatistics(final E... params) {

        if (GeneralConfiguration.WRITE_TO_DATABASE) {
            writeToDatabase();
        }

        List<E> valuesToWrite = new ArrayList<>();

        valuesToWrite.add((E) getSharedId());
        valuesToWrite.add((E) getId());

        valuesToWrite.add((E) BlockchainStrategy.getBlockchainFrameworkAsString());
        valuesToWrite.add((E) getStatisticType());
        valuesToWrite.add((E) GeneralConfiguration.HOST_ID);

        if (getValue() instanceof List) {
            List<E> valueList = (List<E>) getValue();
            for (final Object o : valueList) {
                valuesToWrite.add((E) o);
            }
        } else {
            valuesToWrite.add((E) getValue());
        }

        WriteStatistics.prepareWrite(GeneralConfiguration.LOG_PATH + GeneralConfiguration.CUSTOM_STATISTICS_FILE_NAME
                , null, valuesToWrite);

    }

    public abstract String getId();

    public abstract void setId(final String id);

    public abstract String getSharedId();

    public abstract void setSharedId(final String sharedId);

    public abstract E1 getValue();

    public abstract void setValue(final E1 value);

    @Override
    public StatisticType getStatisticType() {
        return STATISTIC_TYPE;
    }

    @SafeVarargs
    @Override
    public final <E> void printStatistics(final E... params) {
        LOG.info(toString());
    }

    private <E> void writeToDatabase() {

        try {

            String query = "insert into customstatisticobject (shared_id, id, basic_system, statistic_type, value, cid, run_id)" +
                    "VALUES(?, ?, ?, ?, ?, ?, ?)";

            PreparedStatement preparedStatement = PrepareStatementCollection.addPreparedStatement(query);

            preparedStatement.setString(1, getSharedId());
            preparedStatement.setString(2, getId());
            preparedStatement.setString(3, BlockchainStrategy.getBlockchainFrameworkAsString());
            preparedStatement.setString(4, getStatisticType().name());
            preparedStatement.setString(5, GeneralConfiguration.RUN_ID);


            if (getValue() instanceof List) {
                List<E> valueList = (List<E>) getValue();
                preparedStatement.setString(5, valueList.toString());
            } else {
                preparedStatement.setString(5, String.valueOf(getValue()));
            }

            preparedStatement.setString(6, GeneralConfiguration.HOST_ID);

            //preparedStatement.executeUpdate();
            preparedStatement.addBatch();

        } catch (SQLException ex) {
            ExceptionHandler.logException(ex);
        }
    }

}
