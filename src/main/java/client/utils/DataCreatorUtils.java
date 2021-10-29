package client.utils;

import org.apache.commons.lang3.RandomStringUtils;

public class DataCreatorUtils {

    private static final String ASCII = "!\"#$%&'()*+,-./0123456789:;" +
            "<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~";

    private DataCreatorUtils() {
    }

    public static String randomString(final int length, final boolean letters, final boolean numbers) {
        return RandomStringUtils.random(length, letters, numbers);
    }

    public static String fixedString(final int from, final int to) {
        StringBuilder stringBuilder = new StringBuilder(to - from);
        String[] split = ASCII.split("");
        for (int i = from; i < to; i++) {
            stringBuilder.append(split[i % split.length]);
        }
        return stringBuilder.toString();
    }

}
