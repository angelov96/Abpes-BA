package client.statistics;

import client.supplements.ExceptionHandler;
import co.paralleluniverse.fibers.Suspendable;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PrepareStatementCollection {

    //private static Connection connection;

    public static Map<String, ImmutablePair<PreparedStatement, Connection>> getPreparedStatementMap() {
        return PREPARED_STATEMENT_MAP;
    }

    private static final Map<String, ImmutablePair<PreparedStatement, Connection>> PREPARED_STATEMENT_MAP =
            new ConcurrentHashMap<>();

/*    static {
        Connection connection = client.database.Connection.getConnection();
        try {
            connection.setAutoCommit(false);
        } catch (SQLException ex) {
            ExceptionHandler.logException(ex);
        }
        PrepareStatementCollection.connection = connection;
    }*/

    @Suspendable
    public static PreparedStatement addPreparedStatement(final String query) {

        ImmutablePair<PreparedStatement, Connection> preparedStatementConnectionImmutablePair =
                PREPARED_STATEMENT_MAP.computeIfAbsent(query, i -> {
                    try {
                        Connection connection = client.database.Connection.getConnection();
                        connection.setAutoCommit(false);
                        return ImmutablePair.of(connection.prepareStatement(query), connection);
                    } catch (SQLException ex) {
                        ExceptionHandler.logException(ex);
                        return null;
                    }
                });
        return preparedStatementConnectionImmutablePair.getLeft();
    }
}
