package client.statistics;

import client.configuration.GeneralConfiguration;
import client.supplements.ExceptionHandler;
import com.csvreader.CsvWriter;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static java.lang.String.format;

public class WriteStatistics {
    private static final Logger LOG = Logger.getLogger(WriteStatistics.class);

    public static <E> void prepareWrite(final String fileName, final String[] header, final List<E> values) {
        try {
            boolean exists = new File(fileName).exists();
            FileOutputStream fileOutputStream = new FileOutputStream(fileName, true);

            CsvWriter csvWriter = createCsvWriter(fileOutputStream);

            if (header != null && !exists) {
                csvWriter.writeRecord(header);
            }

            for (final E value : values) {
                write(csvWriter, value);
            }

            csvWriter.endRecord();

            csvWriter.close();
            fileOutputStream.close();
        } catch (IOException ex) {
            ExceptionHandler.logException(ex);
        }
    }

    private static <E> void write(final CsvWriter csvWriter, final E value) {
        try {
            csvWriter.write(checkForNaN(String.valueOf(value)));
        } catch (IOException ex) {
            ExceptionHandler.logException(ex);
        }
    }

    private static String checkForNaN(final String valueOf) {
        return "NaN".equals(valueOf) ? "0.0" : valueOf;
    }

    private static CsvWriter createCsvWriter(final FileOutputStream fileOutputStream) {
        CsvWriter csvWriter = new CsvWriter(fileOutputStream, GeneralConfiguration.CSV_SEPARATOR,
                StandardCharsets.UTF_8);
        csvWriter.setTextQualifier('"');
        csvWriter.setForceQualifier(true);
        return csvWriter;
    }

    private static String roundTo(final String valueOf) {
        boolean creatable = NumberUtils.isCreatable(valueOf);
        if (creatable) {
            return format("%.2f", Double.valueOf(valueOf));
        }
        return valueOf;
    }
}
