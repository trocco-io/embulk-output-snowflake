package net.snowflake.client.api.connection;

import java.io.InputStream;
import java.sql.SQLException;

/**
 * Test stub of the Snowflake JDBC 4.x {@code SnowflakeConnection}, with the same package, name and
 * signatures as the real one (checked against 4.4.1). Only the 3.x driver is on the test class
 * path, so this lets the tests cover the code path for 4.x. Keep it in sync with the real API.
 */
public interface SnowflakeConnection {
  void uploadStream(String stageName, String destFileName, InputStream inputStream)
      throws SQLException;

  void uploadStream(
      String stageName, String destFileName, InputStream inputStream, UploadStreamConfig config)
      throws SQLException;
}
