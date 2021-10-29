package client.database;

import client.configuration.GeneralConfiguration;
import client.supplements.ExceptionHandler;
import co.paralleluniverse.fibers.SuspendExecution;
import co.paralleluniverse.fibers.Suspendable;
import co.paralleluniverse.strands.Strand;
import com.impossibl.postgres.api.jdbc.PGNotificationListener;
import com.impossibl.postgres.jdbc.PGDataSource;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class Listener {

    private static final Logger LOG = Logger.getLogger(Listener.class);

    public CompletableFuture<Boolean> getIsSubscribed() {
        return isSubscribed;
    }

    private final CompletableFuture<Boolean> isSubscribed = new CompletableFuture<>();

    @Suspendable
    private static PGNotificationListener addAsyncListener(final CompletableFuture<Boolean> completableFuture) {
        return new PGNotificationListener() {

            @Override
            public void notification(int processId, String channelName, String payload) {

                LOG.debug("Received payload: " + payload + " channel: " + channelName + " pid: " + processId);

                if (GeneralConfiguration.NUMBER_OF_CLIENTS_THRESHOLD_DISTRIBUTED_CLIENT_HANDLING == Integer.parseInt(payload)) {
                    completableFuture.complete(true);
                    LOG.info("Number of client threshold reached");
                }
            }
        };
    }

    @Suspendable
    public void startAsyncListener(final CompletableFuture<Boolean> completableFuture) {

        try {
            Class.forName(GeneralConfiguration.DATABASE_DRIVER);
        } catch (ClassNotFoundException ex) {
            ExceptionHandler.logException(ex);
        }
        com.impossibl.postgres.jdbc.PGDataSource dataSource = new PGDataSource();
        dataSource.setUser(GeneralConfiguration.DATABASE_USER);
        dataSource.setPassword(GeneralConfiguration.DATABASE_PASSWORD);
        dataSource.setUrl(GeneralConfiguration.DATABASE_URL.replace("postgresql", "pgsql"));

        try (com.impossibl.postgres.api.jdbc.PGConnection connection =
                     (com.impossibl.postgres.api.jdbc.PGConnection) dataSource.getConnection()) {

            connection.addNotificationListener(addAsyncListener(completableFuture));

            Statement statement = connection.createStatement();
            statement.execute("LISTEN current_client_counter");
            statement.close();

            isSubscribed.complete(true);
            completableFuture.get(GeneralConfiguration.TIMEOUT_LISTENER,
                    GeneralConfiguration.TIMEOUT_LISTENER_TIME_UNIT);

        } catch (SQLException | InterruptedException | ExecutionException | TimeoutException ex) {
            ExceptionHandler.logException(ex);
        }
    }

    @Suspendable
    public void startListenerSync(final CompletableFuture<Boolean> completableFuture) {

        try {
            Class.forName(GeneralConfiguration.DATABASE_DRIVER);
        } catch (ClassNotFoundException ex) {
            ExceptionHandler.logException(ex);
        }

        try (Connection connection = DriverManager.getConnection(GeneralConfiguration.DATABASE_URL, GeneralConfiguration.DATABASE_USER, GeneralConfiguration.DATABASE_PASSWORD)) {
            org.postgresql.PGConnection pgConnection = (org.postgresql.PGConnection) connection;

            Statement statementPrepare = connection.createStatement();
            statementPrepare.execute("LISTEN current_client_counter");
            statementPrepare.close();

            isSubscribed.complete(true);

            while (true) {
                Statement pollStatement = connection.createStatement();
                ResultSet resultSet = pollStatement.executeQuery("SELECT 1");
                resultSet.close();
                pollStatement.close();

                org.postgresql.PGNotification[] notifications = pgConnection.getNotifications();
                if (notifications != null) {
                    for (final org.postgresql.PGNotification notification : notifications) {

                        LOG.debug("Received payload: " + notification.getParameter() + " channel: " + notification.getName() + " pid: " + notification.getPID());

                        if (GeneralConfiguration.NUMBER_OF_CLIENTS_THRESHOLD_DISTRIBUTED_CLIENT_HANDLING == Integer.parseInt(notification.getParameter())) {
                            LOG.info("Number of client threshold reached");
                            completableFuture.complete(true);
                            return;
                        }

                    }
                }
                Strand.sleep(GeneralConfiguration.REPOLL_DATABASE_LISTENER);
            }

        } catch (SQLException | InterruptedException | SuspendExecution ex) {
                ExceptionHandler.logException(ex);
            }
    }

}
