package org.embulk.output.snowflake;

import static org.junit.Assert.assertEquals;
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
import net.snowflake.client.jdbc.SnowflakeConnection;
import org.junit.Test;

/**
 * Tests the 3.x path only, because the 3.x driver is the one on the test classpath. The real 4.x
 * API is not on the test classpath, so the 4.x path is not covered by these tests.
 */
public class TestSnowflakeStreamUploader {
  private static class Call {
    final Object[] args;
    final Class<?>[] parameterTypes;

    Call(Object[] args, Class<?>[] parameterTypes) {
      this.args = args;
      this.parameterTypes = parameterTypes;
    }
  }

  /** A connection that implements the 3.x SnowflakeConnection and records uploadStream calls. */
  private static Connection fakeLegacyConnection(List<Call> calls, Throwable toThrow) {
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
            TestSnowflakeStreamUploader.class.getClassLoader(),
            new Class<?>[] {Connection.class, SnowflakeConnection.class},
            handler);
  }

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
  public void uploadRethrowsSQLExceptionFromDriverUnwrapped() throws Exception {
    SQLException thrown = new SQLException("boom", "S0000", 42);
    Connection connection = fakeLegacyConnection(new ArrayList<>(), thrown);

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

  @Test
  public void uploadRethrowsRuntimeExceptionFromDriverUnwrapped() throws Exception {
    IllegalArgumentException thrown = new IllegalArgumentException("bad argument");
    Connection connection = fakeLegacyConnection(new ArrayList<>(), thrown);

    try {
      SnowflakeStreamUploader.upload(
          connection,
          "@stage",
          "/prefix",
          new ByteArrayInputStream(new byte[0]),
          "f.csv.gz",
          false);
      fail("IllegalArgumentException should be thrown");
    } catch (IllegalArgumentException e) {
      assertSame(thrown, e);
    }
  }

  @Test
  public void uploadFailsClearlyForConnectionOfAnUnsupportedDriver() throws Exception {
    // A connection that implements neither the 3.x nor the 4.x SnowflakeConnection
    Connection connection =
        (Connection)
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
}
