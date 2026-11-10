package org.embulk.output.snowflake;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.SQLException;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import org.junit.Test;

public class TestSnowflakeUtils {
  private static class SubSnowflakeSQLException extends SnowflakeSQLException {
    SubSnowflakeSQLException() {
      super("query-id", "reason", "S0000", 390195);
    }
  }

  @Test
  public void isSnowflakeSQLExceptionMatchesSnowflakeSQLException() {
    SQLException ex = new SnowflakeSQLException("query-id", "reason", "S0000", 390195);

    assertTrue(SnowflakeUtils.isSnowflakeSQLException(ex));
    // The error code is read through SQLException#getErrorCode, so it must be kept as is.
    assertEquals(390195, ex.getErrorCode());
  }

  @Test
  public void isSnowflakeSQLExceptionMatchesSubclass() {
    assertTrue(SnowflakeUtils.isSnowflakeSQLException(new SubSnowflakeSQLException()));
  }

  // 4.x: the same class name in another package (a test stub of the real one, see
  // src/test/java/net/snowflake/client/api)
  private static class SubApiSnowflakeSQLException
      extends net.snowflake.client.api.exception.SnowflakeSQLException {
    SubApiSnowflakeSQLException() {
      super("query-id", "reason", "S0000", 390195);
    }
  }

  @Test
  public void isSnowflakeSQLExceptionMatchesApiSnowflakeSQLException() {
    SQLException ex =
        new net.snowflake.client.api.exception.SnowflakeSQLException(
            "query-id", "reason", "S0000", 390195);

    assertTrue(SnowflakeUtils.isSnowflakeSQLException(ex));
    // The error code is read through SQLException#getErrorCode, so it must be kept as is.
    assertEquals(390195, ex.getErrorCode());
  }

  @Test
  public void isSnowflakeSQLExceptionMatchesApiSubclass() {
    assertTrue(SnowflakeUtils.isSnowflakeSQLException(new SubApiSnowflakeSQLException()));
  }

  @Test
  public void isSnowflakeSQLExceptionDoesNotMatchPlainSQLException() {
    assertFalse(SnowflakeUtils.isSnowflakeSQLException(new SQLException("reason", "S0000", 1)));
  }

  @Test
  public void isSnowflakeSQLExceptionDoesNotMatchOtherThrowable() {
    assertFalse(SnowflakeUtils.isSnowflakeSQLException(new RuntimeException("reason")));
  }
}
