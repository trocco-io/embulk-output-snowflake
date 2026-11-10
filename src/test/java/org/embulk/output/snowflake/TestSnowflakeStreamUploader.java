package org.embulk.output.snowflake;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import net.snowflake.client.api.connection.UploadStreamConfig;
import net.snowflake.client.jdbc.SnowflakeConnection;
import org.junit.Test;

/**
 * Tests both driver generations. The 3.x driver is the one on the test class path; the 4.x API is
 * covered with stubs of the same package, name and signatures under {@code
 * src/test/java/net/snowflake/client/api} (the real 4.x driver is not on the test class path). Both
 * generations are therefore visible to the class loader in every test, as when a 3.x jar and a 4.x
 * jar are present together.
 */
public class TestSnowflakeStreamUploader {
  private static final String API_CONNECTION =
      "net.snowflake.client.api.connection.SnowflakeConnection";

  private static class Call {
    final Object[] args;
    final Class<?>[] parameterTypes;

    Call(Object[] args, Class<?>[] parameterTypes) {
      this.args = args;
      this.parameterTypes = parameterTypes;
    }
  }

  /** A connection that implements the given SnowflakeConnection and records uploadStream calls. */
  private static Connection fakeConnection(
      ClassLoader loader, Class<?> snowflakeConnection, List<Call> calls, Throwable toThrow) {
    InvocationHandler handler =
        (proxy, method, args) -> {
          switch (method.getName()) {
            case "unwrap":
              if (((Class<?>) args[0]).isInstance(proxy)) {
                return proxy;
              }
              throw new SQLException("not a wrapper for " + args[0]);
            case "isWrapperFor":
              return ((Class<?>) args[0]).isInstance(proxy);
            case "uploadStream":
              calls.add(new Call(args, method.getParameterTypes()));
              if (toThrow != null) {
                throw toThrow;
              }
              return null;
            case "toString":
              return "fake-connection";
            case "hashCode":
              return 1;
            case "equals":
              return proxy == args[0];
            default:
              throw new UnsupportedOperationException(method.getName());
          }
        };
    return (Connection)
        Proxy.newProxyInstance(
            loader, new Class<?>[] {Connection.class, snowflakeConnection}, handler);
  }

  private static Connection fakeConnection(
      Class<?> snowflakeConnection, List<Call> calls, Throwable toThrow) {
    return fakeConnection(
        TestSnowflakeStreamUploader.class.getClassLoader(), snowflakeConnection, calls, toThrow);
  }

  private static Connection fakeLegacyConnection(List<Call> calls, Throwable toThrow) {
    return fakeConnection(SnowflakeConnection.class, calls, toThrow);
  }

  private static Connection fakeApiConnection(List<Call> calls, Throwable toThrow)
      throws ClassNotFoundException {
    return fakeConnection(Class.forName(API_CONNECTION), calls, toThrow);
  }

  /** A connection that implements neither the 3.x nor the 4.x SnowflakeConnection. */
  private static Connection fakeUnsupportedConnection() {
    return (Connection)
        Proxy.newProxyInstance(
            TestSnowflakeStreamUploader.class.getClassLoader(),
            new Class<?>[] {Connection.class},
            (proxy, method, args) -> {
              switch (method.getName()) {
                case "isWrapperFor":
                  return false;
                case "toString":
                  return "fake-connection";
                case "hashCode":
                  return 1;
                case "equals":
                  return proxy == args[0];
                default:
                  throw new UnsupportedOperationException(method.getName());
              }
            });
  }

  // 3.x

  @Test
  public void uploadCallsLegacyUploadStreamWithTheSameArgumentsAsBefore() throws Exception {
    List<Call> calls = new ArrayList<>();
    Connection connection = fakeLegacyConnection(calls, null);
    InputStream in = new ByteArrayInputStream(new byte[] {1, 2, 3});

    SnowflakeStreamUploader.upload(connection, "@stage", "/prefix", in, "file.csv.gz", false);

    assertEquals(1, calls.size());
    Call call = calls.get(0);
    // uploadStream(stageName, destPrefix, inputStream, destFileName, compressData)
    assertEquals(5, call.args.length);
    assertEquals("@stage", call.args[0]);
    assertEquals("/prefix", call.args[1]);
    assertSame(in, call.args[2]);
    assertEquals("file.csv.gz", call.args[3]);
    assertEquals(Boolean.FALSE, call.args[4]);
    // The overload with a boolean (not the deprecated one with a long) must be the one called.
    assertEquals(boolean.class, call.parameterTypes[4]);
  }

  @Test
  public void uploadChoosesLegacyPathWhenTheApiClassesAreAlsoVisible() throws Exception {
    // Both generations are visible to the class loader (precondition of this test) ...
    Class.forName(API_CONNECTION);
    List<Call> calls = new ArrayList<>();
    // ... but the connection implements only the 3.x interface, so the 3.x path must be used.
    Connection connection = fakeLegacyConnection(calls, null);

    SnowflakeStreamUploader.upload(
        connection, "@stage", "/prefix", new ByteArrayInputStream(new byte[0]), "f.csv.gz", false);

    assertEquals(1, calls.size());
    assertEquals(5, calls.get(0).args.length);
  }

  // 4.x

  @Test
  public void uploadCallsApiUploadStreamWithDestFileNameAndExplicitCompressData() throws Exception {
    List<Call> calls = new ArrayList<>();
    Connection connection = fakeApiConnection(calls, null);
    InputStream in = new ByteArrayInputStream(new byte[] {1, 2, 3});

    SnowflakeStreamUploader.upload(connection, "@stage", "/prefix", in, "file.csv.gz", false);

    assertEquals(1, calls.size());
    Call call = calls.get(0);
    // uploadStream(stageName, destFileName, inputStream, UploadStreamConfig)
    assertEquals(4, call.args.length);
    assertEquals("@stage", call.args[0]);
    // The 2nd argument is the file name, not the prefix
    assertEquals("file.csv.gz", call.args[1]);
    assertSame(in, call.args[2]);
    UploadStreamConfig config = (UploadStreamConfig) call.args[3];
    assertEquals("/prefix", config.getDestPrefix());
    // compressData defaults to true in 4.x; the files are already gzipped, so false must be set.
    assertFalse(config.isCompressData());
  }

  @Test
  public void uploadPassesCompressDataTrueToApiUploadStream() throws Exception {
    List<Call> calls = new ArrayList<>();
    Connection connection = fakeApiConnection(calls, null);

    SnowflakeStreamUploader.upload(
        connection, "@stage", "/prefix", new ByteArrayInputStream(new byte[0]), "f.csv.gz", true);

    assertTrue(((UploadStreamConfig) calls.get(0).args[3]).isCompressData());
  }

  @Test
  public void uploadRethrowsSQLExceptionFromApiDriverUnwrapped() throws Exception {
    SQLException thrown = new SQLException("boom", "S0000", 42);
    Connection connection = fakeApiConnection(new ArrayList<>(), thrown);

    try {
      SnowflakeStreamUploader.upload(
          connection,
          "@stage",
          "/prefix",
          new ByteArrayInputStream(new byte[0]),
          "f.csv.gz",
          false);
      fail("SQLException should be thrown");
    } catch (SQLException e) {
      assertSame(thrown, e);
    }
  }

  // Selection and class loading

  @Test
  public void uploadFallsBackToLegacyWhenTheApiClassFailsToLink() throws Exception {
    // A class loader on which looking up the 4.x interface fails the way a class that is found but
    // cannot be linked does (NoClassDefFoundError, a LinkageError). The connection is created in
    // this class loader, so it is the first one the uploader asks.
    ClassLoader failingLoader =
        new ClassLoader(TestSnowflakeStreamUploader.class.getClassLoader()) {
          @Override
          protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            if (API_CONNECTION.equals(name)) {
              throw new NoClassDefFoundError(name);
            }
            return super.loadClass(name, resolve);
          }
        };
    List<Call> calls = new ArrayList<>();
    Connection connection = fakeConnection(failingLoader, SnowflakeConnection.class, calls, null);

    SnowflakeStreamUploader.upload(
        connection, "@stage", "/prefix", new ByteArrayInputStream(new byte[0]), "f.csv.gz", false);

    // The Error did not escape, and the 3.x path was used
    assertEquals(1, calls.size());
    assertEquals(5, calls.get(0).args.length);
  }

  @Test
  public void uploadFailsClearlyForConnectionOfAnUnsupportedDriver() throws Exception {
    Connection connection = fakeUnsupportedConnection();

    try {
      SnowflakeStreamUploader.upload(
          connection,
          "@stage",
          "/prefix",
          new ByteArrayInputStream(new byte[0]),
          "f.csv.gz",
          false);
      fail("IllegalStateException should be thrown");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("Unsupported Snowflake JDBC driver"));
      assertTrue(e.getMessage().contains(connection.getClass().getName()));
    }
  }

  // Failing fast

  @Test
  public void prepareFailsForConnectionOfAnUnsupportedDriver() throws Exception {
    Connection connection = fakeUnsupportedConnection();

    try {
      SnowflakeStreamUploader.prepare(connection);
      fail("IllegalStateException should be thrown");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("Unsupported Snowflake JDBC driver"));
    }
  }

  @Test
  public void prepareAcceptsConnectionsOfBothGenerationsWithoutUploading() throws Exception {
    List<Call> calls = new ArrayList<>();

    SnowflakeStreamUploader.prepare(fakeLegacyConnection(calls, null));
    SnowflakeStreamUploader.prepare(fakeApiConnection(calls, null));

    assertTrue(calls.isEmpty());
  }
}
