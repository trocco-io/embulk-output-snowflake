package org.embulk.output.snowflake;

import java.util.Random;

public class SnowflakeUtils {
  public static final String SOURCES =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz1234567890";

  public static String randomString(int length) {
    Random rand = new Random();
    char[] text = new char[length];
    for (int i = 0; i < length; i++) {
      text[i] = SOURCES.charAt(rand.nextInt(SOURCES.length()));
    }
    return new String(text);
  }

  // SnowflakeSQLException は JDBC ドライバの 3.x と 4.x でパッケージが異なり、型では受けられない。
  private static final String LEGACY_SNOWFLAKE_SQL_EXCEPTION =
      "net.snowflake.client.jdbc.SnowflakeSQLException";
  private static final String API_SNOWFLAKE_SQL_EXCEPTION =
      "net.snowflake.client.api.exception.SnowflakeSQLException";

  /**
   * 例外が SnowflakeSQLException（またはそのサブクラス）かどうかを、3.x / 4.x のどちらのドライバでも判定する。 Snowflake 以外の
   * SQLException を誤って対象にしないよう、クラス名で継承関係をたどる。
   */
  public static boolean isSnowflakeSQLException(Throwable t) {
    for (Class<?> c = t.getClass(); c != null; c = c.getSuperclass()) {
      String name = c.getName();
      if (LEGACY_SNOWFLAKE_SQL_EXCEPTION.equals(name) || API_SNOWFLAKE_SQL_EXCEPTION.equals(name)) {
        return true;
      }
    }
    return false;
  }
}
