package quorum.helper;

import client.configuration.GeneralConfiguration;
import client.supplements.ExceptionHandler;
import co.paralleluniverse.fibers.Suspendable;
import org.apache.log4j.Logger;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ServerSelectionStrategyHelper {

    private static final Logger LOG = Logger.getLogger(ServerSelectionStrategyHelper.class);

    @Suspendable
    public static List<String> getFixed(final String node) {

        List<String> servers = new ArrayList<>();

        try (java.sql.Connection connection = client.database.Connection.getConnection()) {

            String query = "SELECT node FROM node_occupancy WHERE run_id='" + GeneralConfiguration.RUN_ID + "' AND" +
                    " node ='" + node +  "'";

            PreparedStatement preparedStatement = connection.prepareStatement(query);

            ResultSet resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {
                servers.add(resultSet.getString("node"));
                LOG.info("Added server: " + resultSet.getString("node"));
            }

            resultSet.close();
            preparedStatement.close();
        } catch (SQLException ex) {
            ExceptionHandler.logException(ex);
        }
        return servers;
    }

    @Suspendable
    public static List<String> getUnused() {

        List<String> servers = new ArrayList<>();

        try (java.sql.Connection connection = client.database.Connection.getConnection()) {

            String query = "SELECT node FROM node_occupancy WHERE run_id='" + GeneralConfiguration.RUN_ID + "' AND" +
                    " used_by_counter = 0";

            PreparedStatement preparedStatement = connection.prepareStatement(query);

            ResultSet resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {
                servers.add(resultSet.getString("node"));
                LOG.info("Added server: " + resultSet.getString("node"));
            }

            resultSet.close();
            preparedStatement.close();
        } catch (SQLException ex) {
            ExceptionHandler.logException(ex);
        }
        return servers;
    }

    @Suspendable
    public static List<String> selectRandom(final int numberOfServers) {

        List<String> servers = new ArrayList<>();

        try (java.sql.Connection connection = client.database.Connection.getConnection()) {

            String query = "SELECT node FROM node_occupancy WHERE run_id='" + GeneralConfiguration.RUN_ID + "' ORDER " +
                    "BY random() limit " +
                    numberOfServers;

            PreparedStatement preparedStatement = connection.prepareStatement(query);

            ResultSet resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {
                servers.add(resultSet.getString("node"));
                LOG.info("Added server: " + resultSet.getString("node"));
            }

            resultSet.close();
            preparedStatement.close();
        } catch (SQLException ex) {
            ExceptionHandler.logException(ex);
        }
        return servers;
    }

    @Suspendable
    public static void setServer(final String serverAddress) {

        try (java.sql.Connection connection = client.database.Connection.getConnection()) {
            int initialValue = 0;
            String query = "INSERT INTO node_occupancy AS no (node, used_by_counter, run_id)" +
                    "VALUES (?, ?, ?)" +
                    "ON CONFLICT (node) DO UPDATE " +
                    "SET used_by_counter = no.used_by_counter + " + initialValue;

            PreparedStatement preparedStatement = connection.prepareStatement(query);
            preparedStatement.setString(1, serverAddress);
            preparedStatement.setLong(2, initialValue);
            preparedStatement.setString(3, GeneralConfiguration.RUN_ID);

            preparedStatement.executeUpdate();

            preparedStatement.close();
        } catch (SQLException ex) {
            ExceptionHandler.logException(ex);
        }
    }

    @Suspendable
    public static List<String> selectRoundRobin() {

        List<String> servers = new ArrayList<>();

        try (java.sql.Connection connection = client.database.Connection.getConnection()) {
            String query =
                    "UPDATE node_occupancy AS no SET used_by_counter=no.used_by_counter + 1 WHERE run_id='" + GeneralConfiguration.RUN_ID + "'" +
                    "AND no.node=(SELECT node FROM node_occupancy WHERE" +
                    " run_id='" + GeneralConfiguration.RUN_ID + "' ORDER BY used_by_counter ASC LIMIT " +
                    "1)" +
                    "RETURNING node";

            PreparedStatement preparedStatement = connection.prepareStatement(query);

            ResultSet resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {
                servers.add(resultSet.getString("node"));
                LOG.info("Added server: " + resultSet.getString("node"));
            }

            resultSet.close();
            preparedStatement.close();
        } catch (SQLException ex) {
            ExceptionHandler.logException(ex);
        }
        return servers;
    }

}
