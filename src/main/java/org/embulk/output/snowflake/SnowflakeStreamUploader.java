package org.embulk.output.snowflake;

import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Snowflake JDBC ドライバの 3.x / 4.x の両方で、ステージへのストリームアップロードを行う。
 *
 * <p>4.x では公開クラスが {@code net.snowflake.client.api.*} へ移動し、{@code uploadStream} のシグネチャも変わった。 接続情報ごとに
 * 3.x と 4.x のドライバが混在するため、コンパイル時にはどちらのクラスにも依存せず、 実行時にロードできている方のクラスをリフレクションで使う。
 *
 * <p>引数の対応（4.x の公開メソッドは内部で 3.x と同じ実装に委譲している）:
 *
 * <ul>
 *   <li>3.x: {@code uploadStream(stageName, destPrefix, inputStream, destFileName, compressData)}
 *   <li>4.x: {@code uploadStream(stageName, destFileName, inputStream,
 *       UploadStreamConfig(destPrefix, compressData))}。{@code compressData} の既定値は {@code true}
 *       なので、必ず明示的に渡す
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
      // リフレクション越しでも、ドライバが投げた例外は元の型のまま伝える
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
