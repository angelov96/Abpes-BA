package client.miscellaneous;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.nio.file.Path;
import java.nio.file.Paths;

public class Ascii {

    private static final Logger LOG = Logger.getLogger(Ascii.class);

    public static void main(final String... args) {

        Path currentRelativePath = Paths.get("");
        String currentAbsolutePath = currentRelativePath.toAbsolutePath().toString();
        String filePath = "/configs/";
        String logPropertiesName = "log4j.properties";

        PropertyConfigurator.configureAndWatch(currentAbsolutePath + filePath + logPropertiesName, 60 * 1000);

        StringBuilder s = new StringBuilder();
        for (int i = 32; i <= 126; i++) {
            s.append(Character.toChars(i));
        }
        LOG.info(s.toString());
    }

}