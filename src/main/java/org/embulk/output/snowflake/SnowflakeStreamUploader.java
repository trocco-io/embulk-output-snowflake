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
 * <p>This class only adapts to the driver that was actually loaded. Which driver is loaded is
 * decided by the class loader, not here: the jar given by {@code driver_path} is appended to the
 * end of the plugin's class path, so a driver jar bundled in the plugin's own class path is found
 * first (see "JDBC driver" in the README).
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

  /** How to call {@code uploadStream} on one generation of the driver. */
  private interface Uploader {
    void upload(
        Connection connection,
        String stageName,
        String destPrefix,
        InputStream inputStream,
        String destFileName,
        boolean compressData)
        throws SQLException, ReflectiveOperationException;
  }

  private SnowflakeStreamUploader() {}

  /**
   * Checks that uploads work with the driver behind the connection, and fails right away if they do
   * not, so that a job does not read and write all its records first and only then fail at the
   * first upload. Nothing is cached: {@link #upload} resolves again for each upload, which costs a
   * few reflection calls next to a network upload.
   *
   * @throws IllegalStateException if the driver is neither a supported 3.x nor 4.x driver
   */
  static void prepare(Connection connection) throws SQLException {
    resolve(connection);
  }

  static void upload(
      Connection connection,
      String stageName,
      String destPrefix,
      InputStream inputStream,
      String destFileName,
      boolean compressData)
      throws SQLException {
    Uploader uploader = resolve(connection);
    try {
      uploader.upload(connection, stageName, destPrefix, inputStream, destFileName, compressData);
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
          "Unsupported Snowflake JDBC driver: failed to call uploadStream on "
              + connection.getClass().getName(),
          e);
    }
  }

  private static Uploader resolve(Connection connection) throws SQLException {
    // Look the interfaces up in the class loader of the connection: a connection can only
    // implement an interface that its own class loader can see, and that loader is not
    // necessarily the plugin's.
    ClassLoader loader = connection.getClass().getClassLoader();
    try {
      // Choose by what the live connection implements, not only by which classes are loadable: the
      // class loader may see both generations at once even though the connection belongs to one.
      Class<?> apiConnection = implementedBy(connection, API_CONNECTION, loader);
      if (apiConnection != null) {
        return apiUploader(apiConnection);
      }
      Class<?> legacyConnection = implementedBy(connection, LEGACY_CONNECTION, loader);
      if (legacyConnection != null) {
        return legacyUploader(legacyConnection);
      }
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(
          "Unsupported Snowflake JDBC driver: the expected uploadStream API was not found on "
              + connection.getClass().getName(),
          e);
    }
    throw new IllegalStateException(
        "Unsupported Snowflake JDBC driver: "
            + connection.getClass().getName()
            + " implements neither "
            + API_CONNECTION
            + " (4.x) nor "
            + LEGACY_CONNECTION
            + " (3.x)");
  }

  /** Returns the interface if it is loadable and the connection implements it. */
  private static Class<?> implementedBy(Connection connection, String name, ClassLoader loader)
      throws SQLException {
    Class<?> iface = findClass(name, loader);
    return iface != null && connection.isWrapperFor(iface) ? iface : null;
  }

  private static Class<?> findClass(String name, ClassLoader loader) {
    try {
      return Class.forName(name, false, loader);
    } catch (ClassNotFoundException | LinkageError e) {
      // LinkageError (e.g. NoClassDefFoundError) means the class was found but cannot be linked,
      // for example because a type it refers to is missing. It must not stop the other generation
      // from being tried.
      return null;
    }
  }

  private static Uploader legacyUploader(Class<?> legacyConnection) throws NoSuchMethodException {
    Method uploadStream =
        legacyConnection.getMethod(
            "uploadStream",
            String.class,
            String.class,
            InputStream.class,
            String.class,
            boolean.class);
    return (connection, stageName, destPrefix, inputStream, destFileName, compressData) ->
        uploadStream.invoke(
            connection.unwrap(legacyConnection),
            stageName,
            destPrefix,
            inputStream,
            destFileName,
            compressData);
  }

  private static Uploader apiUploader(Class<?> apiConnection) throws ReflectiveOperationException {
    // UploadStreamConfig is in the same jar as the interface, so load it from the same place
    Class<?> uploadConfig = Class.forName(API_UPLOAD_CONFIG, false, apiConnection.getClassLoader());
    Method builderFactory = uploadConfig.getMethod("builder");
    // Use the declared (public) return type rather than the runtime class of the instance
    Class<?> builderClass = builderFactory.getReturnType();
    Method setDestPrefix = builderClass.getMethod("setDestPrefix", String.class);
    Method setCompressData = builderClass.getMethod("setCompressData", boolean.class);
    Method build = builderClass.getMethod("build");
    Method uploadStream =
        apiConnection.getMethod(
            "uploadStream", String.class, String.class, InputStream.class, uploadConfig);
    return (connection, stageName, destPrefix, inputStream, destFileName, compressData) -> {
      Object builder = builderFactory.invoke(null);
      builder = setDestPrefix.invoke(builder, destPrefix);
      builder = setCompressData.invoke(builder, compressData);
      Object config = build.invoke(builder);
      uploadStream.invoke(
          connection.unwrap(apiConnection), stageName, destFileName, inputStream, config);
    };
  }
}
