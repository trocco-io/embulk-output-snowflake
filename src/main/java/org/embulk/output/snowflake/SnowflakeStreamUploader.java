package org.embulk.output.snowflake;

import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Uploads a stream to a stage with either Snowflake JDBC driver 3.x or 4.x.
 *
 * <p>In 4.x the public classes moved to {@code net.snowflake.client.api.*} and the signature of
 * {@code uploadStream} changed. The driver version is chosen per connection (3.x and 4.x are used
 * at the same time), so this class depends on neither generation at compile time. It uses whichever
 * {@code SnowflakeConnection} is loadable at runtime, via reflection.
 *
 * <p>Argument mapping (the public 4.x method delegates to the same internal implementation as 3.x):
 *
 * <ul>
 *   <li>3.x: {@code uploadStream(stageName, destPrefix, inputStream, destFileName, compressData)}
 *   <li>4.x: {@code uploadStream(stageName, destFileName, inputStream,
 *       UploadStreamConfig(destPrefix, compressData))}. {@code compressData} defaults to {@code
 *       true} in 4.x, so it is always passed explicitly.
 * </ul>
 */
final class SnowflakeStreamUploader {
  // 3.x
  private static final String LEGACY_CONNECTION = "net.snowflake.client.jdbc.SnowflakeConnection";
  // 4.x
  private static final String API_CONNECTION =
      "net.snowflake.client.api.connection.SnowflakeConnection";
  private static final String API_UPLOAD_CONFIG =
      "net.snowflake.client.api.connection.UploadStreamConfig";

  private SnowflakeStreamUploader() {}

  static void upload(
      Connection connection,
      String stageName,
      String destPrefix,
      InputStream inputStream,
      String destFileName,
      boolean compressData)
      throws SQLException {
    ClassLoader loader = SnowflakeStreamUploader.class.getClassLoader();
    try {
      Class<?> apiConnection = findClass(API_CONNECTION, loader);
      if (apiConnection != null) {
        uploadWithApi(
            connection,
            apiConnection,
            loader,
            stageName,
            destPrefix,
            inputStream,
            destFileName,
            compressData);
      } else {
        uploadWithLegacy(
            connection,
            Class.forName(LEGACY_CONNECTION, false, loader),
            stageName,
            destPrefix,
            inputStream,
            destFileName,
            compressData);
      }
    } catch (InvocationTargetException e) {
      // Rethrow what the driver threw with its original type, even through reflection
      Throwable cause = e.getCause();
      if (cause instanceof SQLException) {
        throw (SQLException) cause;
      }
      if (cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      }
      if (cause instanceof Error) {
        throw (Error) cause;
      }
      throw new IllegalStateException(cause);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(
          "Unsupported Snowflake JDBC driver: neither "
              + API_CONNECTION
              + " (4.x) nor "
              + LEGACY_CONNECTION
              + " (3.x) is usable",
          e);
    }
  }

  private static void uploadWithLegacy(
      Connection connection,
      Class<?> legacyConnection,
      String stageName,
      String destPrefix,
      InputStream inputStream,
      String destFileName,
      boolean compressData)
      throws SQLException, ReflectiveOperationException {
    Object snowflakeConnection = connection.unwrap(legacyConnection);
    Method uploadStream =
        legacyConnection.getMethod(
            "uploadStream",
            String.class,
            String.class,
            InputStream.class,
            String.class,
            boolean.class);
    uploadStream.invoke(
        snowflakeConnection, stageName, destPrefix, inputStream, destFileName, compressData);
  }

  private static void uploadWithApi(
      Connection connection,
      Class<?> apiConnection,
      ClassLoader loader,
      String stageName,
      String destPrefix,
      InputStream inputStream,
      String destFileName,
      boolean compressData)
      throws SQLException, ReflectiveOperationException {
    Class<?> uploadConfig = Class.forName(API_UPLOAD_CONFIG, false, loader);
    Object builder = uploadConfig.getMethod("builder").invoke(null);
    Class<?> builderClass = builder.getClass();
    builder = builderClass.getMethod("setDestPrefix", String.class).invoke(builder, destPrefix);
    builder =
        builderClass.getMethod("setCompressData", boolean.class).invoke(builder, compressData);
    Object config = builderClass.getMethod("build").invoke(builder);

    Object snowflakeConnection = connection.unwrap(apiConnection);
    Method uploadStream =
        apiConnection.getMethod(
            "uploadStream", String.class, String.class, InputStream.class, uploadConfig);
    uploadStream.invoke(snowflakeConnection, stageName, destFileName, inputStream, config);
  }

  private static Class<?> findClass(String name, ClassLoader loader) {
    try {
      return Class.forName(name, false, loader);
    } catch (ClassNotFoundException e) {
      return null;
    }
  }
}
