package client.miscellaneous;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.file.Path;
import java.nio.file.Paths;

class ReflectionTest {

    static final Integer f = 1;

    private static final Logger LOG = Logger.getLogger(ReflectionTest.class);

    public static void main(final String... args) throws NoSuchFieldException, IllegalAccessException {

        Path currentRelativePath = Paths.get("");
        String currentAbsolutePath = currentRelativePath.toAbsolutePath().toString();
        String filePath = "/configs/";
        String logPropertiesName = "log4j.properties";

        PropertyConfigurator.configureAndWatch(currentAbsolutePath + filePath + logPropertiesName, 60 * 1000);

        ReflectionTest.set();
        LOG.info("F: " + ReflectionTest.f);
        LOG.info("F.class: " + ReflectionTest.f.getClass());
    }

    static public void set() throws NoSuchFieldException, IllegalAccessException {
        Field field = ReflectionTest.class.getDeclaredField("f");

        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);

        Field fieldType = Field.class.getDeclaredField("type");
        fieldType.setAccessible(true);
        fieldType.set(field, String.class);

        field.setAccessible(true);
        field.set(null, "test");
    }

}