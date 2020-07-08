package org.embulk.output.snowflake;

import java.util.Random;

public class SnowflakeUtils {
    public static final String SOURCES =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz1234567890";

    public static String randomString(int length){
        Random rand = new Random();
        char[] text = new char[length];
        for (int i = 0; i < length; i++) {
            text[i] = SOURCES.charAt(rand.nextInt(SOURCES.length()));
        }
        return new String(text);
    }
}
