package client.database;

import client.configuration.GeneralConfiguration;
import client.supplements.ExceptionHandler;
import co.paralleluniverse.fibers.Suspendable;

import java.sql.DriverManager;
import java.sql.SQLException;

public class Connection {

    @Suspendable
    public static java.sql.Connection getConnection() {
        java.sql.Connection connection = null;
        try {
            Class.forName(GeneralConfiguration.DATABASE_DRIVER);
            String url = GeneralConfiguration.DATABASE_URL;
            connection = DriverManager.getConnection(url, GeneralConfiguration.DATABASE_USER, GeneralConfiguration.DATABASE_PASSWORD);
        } catch (ClassNotFoundException | SQLException ex) {
            ExceptionHandler.logException(ex);
        }
        return connection;
    }

}
