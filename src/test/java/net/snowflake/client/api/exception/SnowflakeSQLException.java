package net.snowflake.client.api.exception;

import java.sql.SQLException;

/**
 * Test stub of the Snowflake JDBC 4.x {@code SnowflakeSQLException}, with the same package, name
 * and superclass as the real one (checked against 4.4.1). Keep it in sync with the real API.
 */
public class SnowflakeSQLException extends SQLException {
  public SnowflakeSQLException(String queryId, String reason, String sqlState, int vendorCode) {
    super(reason, sqlState, vendorCode);
  }
}
