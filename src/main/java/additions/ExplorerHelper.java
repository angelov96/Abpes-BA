package additions;

import client.supplements.ExceptionHandler;
import com.csvreader.CsvWriter;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.jboss.resteasy.client.jaxrs.ResteasyClient;
import org.jboss.resteasy.client.jaxrs.ResteasyClientBuilder;
import org.jboss.resteasy.client.jaxrs.ResteasyWebTarget;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.ws.rs.core.Response;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import static java.lang.String.format;

public class ExplorerHelper {

    private static final Logger LOG = Logger.getLogger(ExplorerHelper.class);
    private static final short HTTP_CODE_OK = 200;
    // The name of the channel to query.
    private static final String CHANNEL_NAME = "vm";
    private static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    // The interval which will be used for gathering data.
    private static final Interval INTERVAL = Interval.BETWEEN;
    private static final List<Integer> TX_COUNT = new LinkedList<>();
    // The URL of the Hyperledger Blockchain Explorer.
    private static final String URL = "http://10.28.56.37:3333/";
    // The time unit to use. Currently minutes and hours are supported.
    private static final TimeUnit UNIT = TimeUnit.MINUTE;
    // The period of hours which should be returned. One means the last hour is returned, two means the last two hours are returned.

    // Should be set to a value higher than the period of time to gather.
    private static final int PERIOD = 100;
    // Whether zeros should be ignored.
    private static final boolean IGNORE_ZEROS = true;
    // The name of the file to create.
    private static final String LOG_FILE = "testScenario3-4+5-HLF-ExplorerHelper.csv";
    // The separator to use for the CSV file.
    private static final char SEPARATOR = ',';
    private static Date dateTimeStart;
    private static Date dateTimeEnd;

    static {
        try {
            // The start time to capture.
            dateTimeStart = DATE_FORMAT.parse("2019-03-11T12:38:59.000Z");
        } catch (ParseException ex) {
            ExceptionHandler.logException(ex);
        }
    }

    static {
        try {
            // The end time to capture.
            dateTimeEnd = DATE_FORMAT.parse("2019-03-11T15:27:49.000Z");
        } catch (ParseException ex) {
            ExceptionHandler.logException(ex);
        }
    }

    private ExplorerHelper() {
    }

    /**
     * A simple main method which executes the creation of various statistics gathered from the Hyperledger
     * Blockchain Explorer.
     *
     * @param args sample arguments
     */

    public static void main(final String... args) {
        PropertyConfigurator.configureAndWatch("log4j.client.properties", 60 * 1000);
        ResteasyClient client = (ResteasyClient) ResteasyClientBuilder.newBuilder().build();
        String genesisHash = getGenesisBlockHashForChannel(CHANNEL_NAME, client);

        if (!"".equals(genesisHash)) {
            setStatisticalValues(genesisHash, client);

            LOG.info(Collections.max(TX_COUNT) + " max tx");
            LOG.info(Collections.min(TX_COUNT) + " min tx");
            double average = TX_COUNT.stream().mapToInt(val -> val).average().orElse(0.0);
            LOG.info(average + " average tx");
            double averagePerSecond = 0.0;
            if (UNIT == TimeUnit.MINUTE) {
                averagePerSecond = average / 60;
                LOG.info((averagePerSecond) + " average tx / second");
            } else if (UNIT == TimeUnit.HOUR) {
                averagePerSecond = average / 3600;
                LOG.info((averagePerSecond) + " average tx / second");
            }
            writeCsvFile(Collections.max(TX_COUNT).doubleValue(), Collections.min(TX_COUNT).doubleValue(), average,
    averagePerSecond);
        }
        client.close();
    }

    private static void setStatisticalValues(final String genesisHash, final ResteasyClient client) {
        String timeUnit = (UNIT == TimeUnit.MINUTE ? "txByMinute" : "txByHour");
        String txUrl = URL + "/api/" + timeUnit + "/" + genesisHash + "/" + PERIOD;
        JSONObject jsonObj = setRestClientValues(client, txUrl);
        JSONArray rows = jsonObj.getJSONArray("rows");
        for (final Object row : rows) {
            JSONObject jsonObject = (JSONObject) row;
            DateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
            try {
                Date datetime = simpleDateFormat.parse(jsonObject.getString("datetime"));
                int count = jsonObject.getInt("count");
                switch (INTERVAL) {
                    case AFTER:
                        if (datetime.after(dateTimeStart)) {
                            addToList(count);
                        }
                        break;
                    case BEFORE:
                        if (datetime.before(dateTimeEnd)) {
                            addToList(count);
                        }
                        break;
                    case BETWEEN:
                        if (datetime.after(dateTimeStart) && datetime.before(dateTimeEnd)) {
                            addToList(count);
                        }
                        break;
                    default:
                        LOG.error("Unknown interval type set");
                        break;
                }
            } catch (ParseException ex) {
                ExceptionHandler.logException(ex);
            }
        }
    }

    private static JSONObject setRestClientValues(final ResteasyClient client, final String txUrl) {
        ResteasyWebTarget target = client.target(txUrl);
        checkStatusCode(target.request().get());
        String response = target.request().get(String.class);

        return new JSONObject(response);
    }

    private static void addToList(final int count) {
        if (IGNORE_ZEROS && count > 0) {
            TX_COUNT.add(count);
        } else if (IGNORE_ZEROS && count == 0) {
            LOG.error("Zero value ignored");
        } else {
            TX_COUNT.add(count);
        }
    }

    private static String getGenesisBlockHashForChannel(final String channelName, final ResteasyClient client) {
        String url = URL + "/api/channels/info";
        JSONObject jsonObj = setRestClientValues(client, url);
        JSONArray channels = jsonObj.getJSONArray("channels");
        String genesisHash = "";
        for (final Object channel : channels) {
            JSONObject jsonObject = (JSONObject) channel;
            if (jsonObject.getString("channelname").equals(channelName)) {
                genesisHash = jsonObject.getString("channel_genesis_hash");
            }
        }
        LOG.error("Genesis hash obtained for channel [" + channelName + "] " + genesisHash);
        return genesisHash;
    }

    private static void checkStatusCode(final Response response) {
        if (response.getStatus() == HTTP_CODE_OK) {
            LOG.debug("Request successful");
        } else {
            LOG.error("Request was not successful: " + response.getStatus());
        }
        response.close();
    }

    private static final String CONFIG_PATH = "./test/";
    private static final String FILE_PREFIX = "explorer-";

    private static void writeCsvFile(final double max, final double min, final Double average, final Double
    averagePerSecond) {

        String path = CONFIG_PATH + FILE_PREFIX + LOG_FILE;
        boolean exists = new File(path).exists();
        try {
            FileOutputStream fileOutputStream = new FileOutputStream(path, true);

            CsvWriter csvWriter = createCsvWriter(fileOutputStream);

            if (!exists) {
                String[] headerRecord = new String[]{"MAX TX", "MIN TX", "MEAN TX/" + (UNIT == TimeUnit.MINUTE ?
    "minute" : "hour"), "MEAN TX/second", "TIME_UNIT", "INTERVAL_START", "INTERVAL_END", "ZEROS_IGNORED"};
                csvWriter.writeRecord(headerRecord);
            }

            csvWriter.write(checkForNaN(String.valueOf(max)));
            csvWriter.write(checkForNaN(String.valueOf(min)));
            csvWriter.write(checkForNaN(String.valueOf(average)));
            csvWriter.write(checkForNaN(String.valueOf(averagePerSecond)));
            csvWriter.write(UNIT == TimeUnit.MINUTE ? "minute" : "hour");
            csvWriter.write(String.valueOf(dateTimeStart.toString()));
            csvWriter.write(String.valueOf(dateTimeEnd.toString()));
            csvWriter.write(String.valueOf(IGNORE_ZEROS));
            csvWriter.endRecord();

            csvWriter.close();
            fileOutputStream.close();
        } catch (IOException ex) {
            ExceptionHandler.logException(ex);
        }
    }

    private static CsvWriter createCsvWriter(final FileOutputStream fileOutputStream) {
        CsvWriter csvWriter = new CsvWriter(fileOutputStream, SEPARATOR, StandardCharsets.UTF_8);
        csvWriter.setTextQualifier('"');
        csvWriter.setForceQualifier(true);
        return csvWriter;
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

    enum TimeUnit {
        MINUTE, HOUR
    }

}
