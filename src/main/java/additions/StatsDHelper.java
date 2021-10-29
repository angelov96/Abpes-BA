package additions;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.jboss.resteasy.client.jaxrs.ResteasyClientBuilder;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

import static java.lang.String.format;

/**
 * This class is used standalone in order to get various statistics from StatsD/Graphite.
 * See for example: https://community.grafana.com/t/y-axis-values-change-depending-on-time-range/5429/7 why this is
 * necessary.
 * Also see the used API documentation here: https://graphite.readthedocs.io/en/latest/render_api.html
 */
public class StatsDHelper {
    private static final Logger LOG = Logger.getLogger(StatsDHelper.class);
    private static final short HTTP_CODE_OK = 200;
    // The interval which will be used for gathering data.
    private static final Interval INTERVAL = Interval.BETWEEN;
    // The URL of StatsD/Graphite.
    private static final String URL = "http://10.159.4.53/";
    // The period of hours which should be returned. One means the last hour is returned, two means the last two
    //hours are returned.
    // Should be set to a value higher than the period of time to gather.
    private static final String PERIOD = "10hours";
    // Whether zeros should be ignored.
    private static final boolean IGNORE_ZEROS = true;
    // The QUERY parameter
    private static final String QUERY = "sumSeries(stats.peer0.peerone.com.ledger.transaction_count.vm5" +
            ".ENDORSER_TRANSACTION.server_1_0.MVCC_READ_CONFLICT%2Cstats.peer0.peerone.com.ledger.transaction_count" +
            ".vm5.ENDORSER_TRANSACTION.server_1_0.PHANTOM_READ_CONFLICT%2Cstats.peer0.peerone.com.ledger" +
            ".transaction_count.vm5.ENDORSER_TRANSACTION.server_1_0.VALID)";
    private static final List<Double> TX_COUNT = new LinkedList<>();
    private static final List<Long> TIMESTAMP = new LinkedList<>();
    // The date time the QUERY should start
    private static final long DATE_TIME_START = 1553538010;
    // The date time the QUERY should end
    private static final long DATE_TIME_END = 1553538170;
    // You could optionally set additional query parameters like &from=19%3A20_20190325&until=19%3A23_20190325
    //(URL_DECODE: &from=19:20_20190325&until=19:23_20190325)
    private static final String ADDITIONAL_QUERY = "";

    private StatsDHelper() {
    }

    /**
     * A simple main method which executes the creation of various statistics gathered from StatsD/Graphite.
     *
     * @param args sample arguments
     */
    public static void main(final String... args) {
        PropertyConfigurator.configureAndWatch("log4j.client.properties", 60 * 1000);
        Client client = ResteasyClientBuilder.newBuilder().build();
        prepareAndSetValues(client);

        try {
            LOG.info(checkForNaN(Collections.max(TX_COUNT).toString()) + " max tx");
            LOG.info(checkForNaN(Collections.min(TX_COUNT).toString()) + " min tx");
            double average = TX_COUNT.stream().mapToDouble(val -> val).average().orElse(0.0);
            LOG.info(checkForNaN(String.valueOf(average)) + " average tx");
        } catch (NoSuchElementException e) {
            LOG.error("Element not found, please check your configuration");
        } finally {
            client.close();
        }
    }

    private static void prepareAndSetValues(final Client client) {
        String url = URL + "render?target=" + QUERY + "&format=json&from=-" + PERIOD + "&" + ADDITIONAL_QUERY;
        JSONArray jsonObj = setRestClientValues(client, url);
        JSONObject jsonObject = jsonObj.getJSONObject(0);
        JSONArray datapoints = (JSONArray) jsonObject.get("datapoints");
        for (final Object o : datapoints) {
            JSONArray jsonArray = (JSONArray) o;
            TIMESTAMP.add(Long.parseLong(jsonArray.get(1).toString()));
            setStatisticalValues(jsonArray);
        }
    }

    private static void setStatisticalValues(final JSONArray jsonArray) {
        long timestamp = Long.parseLong(jsonArray.get(1).toString());
        double txCount = ("null".equals(jsonArray.get(0).toString()) ? 0.0 :
                Double.parseDouble(jsonArray.get(0).toString()));
        switch (INTERVAL) {
            case AFTER:
                if (timestamp > DATE_TIME_START) {
                    addToList(txCount);
                }
                break;
            case BEFORE:
                if (timestamp < DATE_TIME_END) {
                    addToList(txCount);
                }
                break;
            case BETWEEN:
                if (timestamp > DATE_TIME_START && timestamp < DATE_TIME_END) {
                    addToList(txCount);
                }
                break;
            default:
                LOG.error("Unknown interval type set");
                break;
        }
    }

    private static void addToList(final double count) {
        if (IGNORE_ZEROS && count > 0.0) {
            TX_COUNT.add(count);
        } else if (IGNORE_ZEROS && count == 0.0) {
            LOG.error("Zero value ignored");
        } else {
            TX_COUNT.add(count);
        }
    }

    private static JSONArray setRestClientValues(final Client client, final String txUrl) {
        WebTarget target = client.target(txUrl);
        checkStatusCode(target.request().get());
        String response = target.request().get(String.class);

        return new JSONArray(response);
    }

    private static void checkStatusCode(final Response response) {
        if (response.getStatus() == HTTP_CODE_OK) {
            LOG.debug("Request successful");
        } else {
            LOG.error("Request was not successful: " + response.getStatus());
        }
        response.close();
    }

    private static String checkForNaN(final String valueOf) {
        return "NaN".equals(valueOf) ? "0.0" : roundTo(valueOf);
    }

    private static String roundTo(final String valueOf) {
        return format("%.2f", Double.valueOf(valueOf));
    }

    enum Interval {
        BEFORE, AFTER, BETWEEN
    }

}
