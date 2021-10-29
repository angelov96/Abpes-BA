package additions;

import client.configuration.GeneralConfiguration;
import client.executors.MainExecutor;
import client.statistics.WriteStatistics;
import client.supplements.ExceptionHandler;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class GeneralStatisticQueries {

    private static final Logger LOG = Logger.getLogger(MainExecutor.class);

    public static void main(final String... args) {

        Path currentRelativePath = Paths.get("");
        String currentAbsolutePath = currentRelativePath.toAbsolutePath().toString();
        String filePath = "/configs/";
        String logPropertiesName = "log4j.properties";
        PropertyConfigurator.configureAndWatch(currentAbsolutePath + filePath + logPropertiesName, 60 * 1000);

        try {
            Class.forName(GeneralConfiguration.DATABASE_DRIVER);
        } catch (ClassNotFoundException ex) {
            ExceptionHandler.logException(ex);
        }

        try (java.sql.Connection connection = client.database.Connection.getConnection()) {

            getWriteTps(connection);

        } catch (SQLException ex) {
            ExceptionHandler.logException(ex);
        }
    }

    private static void getWriteTps(final java.sql.Connection connection) throws SQLException {
        String[] header = new String[]{"TPS", "LATENCY", "NUMBER_OF_REQUESTS_TOTAL", "BASIC_SYSTEM", "RUN_ID"};

        List<List<String>> statisticList = new ArrayList<>();

        for (int i = 0; i < 1; i++) {

            String likeVal = "keyValue/set";
            String basicSystem = "Corda";

            String query = "select min(start_time_format), max(end_time_format), " +
                    "(round((count(*) / COALESCE(NULLIF((max(extract(epoch from end_time_format)) - min(extract(epoch " +
                    "from start_time_format))),0), 1))::numeric, 2)) as tps, (round((sum(latency)/count(*))::numeric, 2)) as latency, " +
                    "count(*) as count, basic_system, run_id " +
                    "from writestatisticobject " +
                    "where run_id = (select run_id from writestatisticobject where basic_system='" + basicSystem + "' and specific_payload_types like '%" + likeVal + "%' group by run_id order by max(uid) desc " +
                    "limit 1 offset " + i + ")group by basic_system, run_id";

                    /*"select min(start_time_format), max(end_time_format),\n" +
                    "(round(count(*) / (max(extract(epoch from end_time_format)) - min(extract(epoch from " +
                    "start_time_format)))), 2) as tps, " +
                    "(round(sum(latency)/count(*)), 2) as latency, " +
                    "count(*) as count, " +
                    "basic_system, run_id " +
                    "from writestatisticobject " +
                    "where run_id = (select run_id from writestatisticobject group by run_id order by max(uid) desc " +
                    "limit 1 offset " + i + ")" +
                    "group by basic_system, run_id";*/

            LOG.debug("Query: " + query);

            PreparedStatement preparedStatement = connection.prepareStatement(query);
            ResultSet resultSet = preparedStatement.executeQuery();
            preparedStatement.close();

            while (resultSet.next()) {
                System.out.println(resultSet.getString("tps")
                        .replaceAll("\\(", "")
                        .replaceAll("\\)", "") + " | " + (resultSet.getString("latency")
                        .replaceAll("\\(", "")
                        .replaceAll("\\)", "")) + " | " + resultSet.getString("count")
                        + " | " + resultSet.getString("basic_system")
                        + " | " + resultSet.getString("run_id"));

                List<String> valueList = new ArrayList<>();
                valueList.add(resultSet.getString("tps")
                        .replaceAll("\\(", "")
                        .replaceAll("\\)", ""));
                valueList.add((resultSet.getString("latency")
                        .replaceAll("\\(", "")
                        .replaceAll("\\)", "")));
                valueList.add(resultSet.getString("count"));
                valueList.add(resultSet.getString("basic_system"));
                valueList.add(resultSet.getString("run_id"));

                statisticList.add(valueList);
            }

            resultSet.close();
        }

        Collections.reverse(statisticList);
        statisticList.forEach(statistics ->
                WriteStatistics.prepareWrite(GeneralConfiguration.LOG_PATH + "r.csv", header, statistics));

    }

}
