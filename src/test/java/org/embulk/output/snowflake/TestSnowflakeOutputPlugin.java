package org.embulk.output.snowflake;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.embulk.EmbulkSystemProperties;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.input.file.LocalFileInputPlugin;
import org.embulk.output.SnowflakeOutputPlugin;
import org.embulk.output.SnowflakeOutputPlugin.SnowflakePluginTask;
import org.embulk.parser.csv.CsvParserPlugin;
import org.embulk.spi.FileInputPlugin;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.ParserPlugin;
import org.embulk.test.TestingEmbulk;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.config.TaskMapper;
import org.embulk.util.config.modules.ZoneIdModule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestSnowflakeOutputPlugin {
  private static final EmbulkSystemProperties EMBULK_SYSTEM_PROPERTIES =
      EmbulkSystemProperties.of(new Properties());

  @Rule
  public TestingEmbulk embulk =
      TestingEmbulk.builder()
          .setEmbulkSystemProperties(EMBULK_SYSTEM_PROPERTIES)
          .registerPlugin(FileInputPlugin.class, "file", LocalFileInputPlugin.class)
          .registerPlugin(ParserPlugin.class, "csv", CsvParserPlugin.class)
          .registerPlugin(OutputPlugin.class, "snowflake", SnowflakeOutputPlugin.class)
          .build();

  @Rule public TemporaryFolder testFolder = new TemporaryFolder();

  private static final ConfigMapperFactory CONFIG_MAPPER_FACTORY =
      ConfigMapperFactory.builder()
          .addDefaultModules()
          .addModule(ZoneIdModule.withLegacyNames())
          .build();
  private static final ConfigMapper CONFIG_MAPPER = CONFIG_MAPPER_FACTORY.createConfigMapper();
  private static final TaskMapper TASK_MAPPER = CONFIG_MAPPER_FACTORY.createTaskMapper();

  private Logger logger = LoggerFactory.getLogger(TestSnowflakeOutputPlugin.class);

  private static final String TEST_SNOWFLAKE_HOST =
      Optional.ofNullable(System.getenv("EMBULK_OUTPUT_SNOWFLAKE_TEST_HOST")).orElse("localhost");
  private static final String TEST_SNOWFLAKE_USER =
      Optional.ofNullable(System.getenv("EMBULK_OUTPUT_SNOWFLAKE_TEST_USER")).orElse("user");
  private static final String TEST_SNOWFLAKE_PASSWORD =
      Optional.ofNullable(System.getenv("EMBULK_OUTPUT_SNOWFLAKE_TEST_PASSWORD"))
          .orElse("password");
  private static final String TEST_SNOWFLAKE_WAREHOUSE =
      Optional.ofNullable(System.getenv("EMBULK_OUTPUT_SNOWFLAKE_TEST_WAREHOUSE"))
          .orElse("warehouse");
  private static final String TEST_SNOWFLAKE_DB =
      Optional.ofNullable(System.getenv("EMBULK_OUTPUT_SNOWFLAKE_TEST_DATABASE")).orElse("db");
  private static final String TEST_SNOWFLAKE_SCHEMA =
      Optional.ofNullable(System.getenv("EMBULK_OUTPUT_SNOWFLAKE_TEST_SCHEMA")).orElse("schema");
  public static final Properties TEST_PROPERTIES;

  static {
    Properties props = new Properties();
    props.setProperty("host", TEST_SNOWFLAKE_HOST);
    props.setProperty("user", TEST_SNOWFLAKE_USER);
    props.setProperty("password", TEST_SNOWFLAKE_PASSWORD);
    props.setProperty("warehouse", TEST_SNOWFLAKE_WAREHOUSE);
    props.setProperty("db", TEST_SNOWFLAKE_DB);
    props.setProperty("schema", TEST_SNOWFLAKE_SCHEMA);
    TEST_PROPERTIES = props;
  }

  private interface ThrowableConsumer<T> extends Consumer<T> {
    @Override
    default void accept(T t) {
      try {
        acceptThrows(t);
      } catch (Throwable e) {
        throw new RuntimeException(e);
      }
    }

    void acceptThrows(T t) throws Throwable;
  }

  // select
  private void runQuery(String query, ThrowableConsumer<ResultSet> f) {
    // load driver
    try {
      Class.forName("net.snowflake.client.jdbc.SnowflakeDriver");
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    String uri = String.format("jdbc:snowflake://%s", TEST_PROPERTIES.getProperty("host"));
    try (Connection conn = DriverManager.getConnection(uri, TEST_PROPERTIES);
        Statement stmt = conn.createStatement();
        ResultSet rset = stmt.executeQuery(query); ) {
      f.accept(rset);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private ThrowableConsumer<ResultSet> foreachResult(ThrowableConsumer<ResultSet> f) {
    return rs -> {
      try {
        while (rs.next()) {
          f.accept(rs);
        }
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    };
  }

  @Test
  public void testConfigDefault() throws Exception {
    final ConfigSource config =
        CONFIG_MAPPER_FACTORY
            .newConfigSource()
            .set("type", "snowflake")
            .set("host", TEST_SNOWFLAKE_HOST)
            .set("database", TEST_SNOWFLAKE_DB)
            .set("warehouse", TEST_SNOWFLAKE_WAREHOUSE)
            .set("mode", "insert")
            .set("table", "test");
    final SnowflakePluginTask task = CONFIG_MAPPER.map(config, SnowflakePluginTask.class);

    assertEquals(Optional.empty(), task.getDriverPath());
    assertEquals("", task.getUser());
    assertEquals("", task.getPassword());
    assertEquals("public", task.getSchema());
    assertEquals(false, task.getDeleteStage());
  }

  @Test
  public void testConfigExceptions() throws Exception {
    final ConfigSource config =
        CONFIG_MAPPER_FACTORY
            .newConfigSource()
            .set("type", "snowflake")
            .set("host", TEST_SNOWFLAKE_HOST)
            .set("database", TEST_SNOWFLAKE_DB)
            .set("warehouse", TEST_SNOWFLAKE_WAREHOUSE)
            .set("mode", "insert")
            .set("table", "test");

    assertThrows(
        ConfigException.class,
        () -> {
          ConfigSource c = config.deepCopy();
          c.remove("host");
          CONFIG_MAPPER.map(c, SnowflakePluginTask.class);
        });
    assertThrows(
        ConfigException.class,
        () -> {
          ConfigSource c = config.deepCopy();
          c.remove("database");
          CONFIG_MAPPER.map(c, SnowflakePluginTask.class);
        });
    assertThrows(
        ConfigException.class,
        () -> {
          ConfigSource c = config.deepCopy();
          c.remove("warehouse");
          CONFIG_MAPPER.map(c, SnowflakePluginTask.class);
        });
  }

  private List<String> lines(String header, String... data) {
    // Remove hacking double column after merging https://github.com/embulk/embulk/pull/1476.
    return Stream.concat(
            Stream.of(header + ",_hack_double:double"), Stream.of(data).map(s -> s + ",1.0"))
        .collect(Collectors.toList());
  }

  @Test
  public void testRuntimeReplaceStringTable() throws IOException {
    File in = testFolder.newFile(SnowflakeUtils.randomString(8) + ".csv");
    String[] data = new String[] {"aaa", "bbb", "ccc"};
    Files.write(in.toPath(), lines("_c0:string", data));

    final ConfigSource config =
        CONFIG_MAPPER_FACTORY
            .newConfigSource()
            .set("type", "snowflake")
            .set("user", TEST_SNOWFLAKE_USER)
            .set("password", TEST_SNOWFLAKE_PASSWORD)
            .set("host", TEST_SNOWFLAKE_HOST)
            .set("database", TEST_SNOWFLAKE_DB)
            .set("warehouse", TEST_SNOWFLAKE_WAREHOUSE)
            .set("schema", TEST_SNOWFLAKE_SCHEMA)
            .set("mode", "replace")
            .set("table", "test");
    embulk.runOutput(config, in.toPath());

    String tableName =
        String.format("\"%s\".\"%s\".\"%s\"", TEST_SNOWFLAKE_DB, TEST_SNOWFLAKE_SCHEMA, "test");
    runQuery(
        "select count(1) from " + tableName,
        foreachResult(
            rs -> {
              assertEquals(3, rs.getInt(1));
            }));
    List<String> results = new ArrayList();
    runQuery(
        "select \"_c0\" from " + tableName + " order by 1",
        foreachResult(
            rs -> {
              results.add(rs.getString(1));
            }));
    for (int i = 0; i < results.size(); i++) {
      assertEquals(data[i], results.get(i));
    }
  }
}
