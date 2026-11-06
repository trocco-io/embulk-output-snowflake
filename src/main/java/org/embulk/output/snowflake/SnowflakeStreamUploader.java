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
 * at the same time), so this class depends on neither generation at compile time. It uses, via
 * reflection, the {@code SnowflakeConnection} that the live connection actually implements.
 *
 * <p>Argument mapping (the public 4.x method delegates to an internal method that takes the same
 * five parameters as the 3.x one; for the stage names and prefixes this plugin uses the behavior is
 * equivalent, although 4.x trims {@code destPrefix} and treats a blank one as the stage root):
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
      // Choose by what the live connection implements, not only by which classes are loadable: the
      // class loader may see both generations at once (for example, a 3.x driver jar bundled with
      // the plugin next to a 4.x jar given by driver_path).
      Class<?> apiConnection = implementedBy(connection, API_CONNECTION, loader);
      Class<?> legacyConnection =
          apiConnection != null ? null : implementedBy(connection, LEGACY_CONNECTION, loader);
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
      } else if (legacyConnection != null) {
        uploadWithLegacy(
            connection,
            legacyConnection,
            stageName,
            destPrefix,
            inputStream,
            destFileName,
            compressData);
      } else {
        throw new IllegalStateException(
            "Unsupported Snowflake JDBC driver: "
                + connection.getClass().getName()
                + " implements neither "
                + API_CONNECTION
                + " (4.x) nor "
                + LEGACY_CONNECTION
                + " (3.x)");
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
          "Unsupported Snowflake JDBC driver: the expected uploadStream API was not found on "
              + connection.getClass().getName(),
          e);
    }
  }

  /** Returns the interface if the class is loadable and the connection implements it. */
  private static Class<?> implementedBy(Connection connection, String name, ClassLoader loader)
      throws SQLException {
    Class<?> iface = findClass(name, loader);
    return iface != null && connection.isWrapperFor(iface) ? iface : null;
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
    Method builderFactory = uploadConfig.getMethod("builder");
    Object builder = builderFactory.invoke(null);
    // Use the declared (public) return type rather than the runtime class of the instance
    Class<?> builderClass = builderFactory.getReturnType();
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
