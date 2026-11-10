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

  // SnowflakeSQLException is in a different package between JDBC driver 3.x and 4.x, so it can't be
  // caught by type.
  private static final String LEGACY_SNOWFLAKE_SQL_EXCEPTION =
      "net.snowflake.client.jdbc.SnowflakeSQLException";
  private static final String API_SNOWFLAKE_SQL_EXCEPTION =
      "net.snowflake.client.api.exception.SnowflakeSQLException";

  /**
   * Returns whether the throwable is a SnowflakeSQLException (or a subclass) of either driver
   * generation, 3.x or 4.x. It walks the class hierarchy by name so that other SQLExceptions are
   * not matched by mistake.
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
