package org.embulk.output.snowflake;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.embulk.EmbulkEmbed;
import org.embulk.EmbulkSystemProperties;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.exec.PartialExecutionException;
import org.embulk.input.file.LocalFileInputPlugin;
import org.embulk.output.SnowflakeOutputPlugin;
import org.embulk.output.SnowflakeOutputPlugin.SnowflakePluginTask;
import org.embulk.parser.csv.CsvParserPlugin;
import org.embulk.spi.FileInputPlugin;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.ParserPlugin;
import org.embulk.spi.type.Types;
import org.embulk.test.TestingEmbulk;
import org.embulk.test.TestingEmbulk.RunResult;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.config.TaskMapper;
import org.embulk.util.config.modules.ZoneIdModule;
import org.embulk.util.config.units.ColumnConfig;
import org.embulk.util.config.units.SchemaConfig;
import org.junit.*;
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
  private static final String TEST_TABLE_PREFIX =
      String.format("test_%d_", System.currentTimeMillis());
  private static final String TEST_DB_SUFFIX =
      String.format("_test_%d", System.currentTimeMillis());

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
    props.setProperty("MULTI_STATEMENT_COUNT", "0");
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

  private void runQuery(String query) {
    runQuery(query, foreachResult(rs_ -> {}));
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

  private String generateTableFullName(String targetTableName) {
    return String.format(
        "\"%s\".\"%s\".\"%s\"", TEST_SNOWFLAKE_DB, TEST_SNOWFLAKE_SCHEMA, targetTableName);
  }

  private String generateTemporaryTableName() {
    return TEST_TABLE_PREFIX + UUID.randomUUID().toString().replace("-", "");
  }

  private void dropAllTemporaryTables() {
    runQuery(
        String.format(
            "select table_name from information_schema.tables where table_schema = '%s' AND table_name LIKE '%s%%'",
            TEST_PROPERTIES.getProperty("schema"), TEST_TABLE_PREFIX),
        foreachResult(
            rs -> {
              String tableName = rs.getString(1);
              runQuery(
                  String.format(
                      "drop table if exists \"%s\".\"%s\"", TEST_SNOWFLAKE_SCHEMA, tableName),
                  foreachResult(rs_ -> {}));
            }));
  }

  private String generateTemporaryDatabaseName() {
    return TEST_SNOWFLAKE_DB + TEST_DB_SUFFIX + UUID.randomUUID().toString().replace("-", "");
  }

  private void dropAllTemporaryDatabases() {
    runQuery(
        String.format("show databases like '%s%%'", TEST_SNOWFLAKE_DB + TEST_DB_SUFFIX),
        foreachResult(
            rs -> {
              String databaseName = rs.getString("name");

              runQuery(
                  String.format("drop database if exists \"%s\"", databaseName),
                  foreachResult(rs_ -> {}));
            }));
  }

  private File generateTestFile(String... data) throws IOException {
    File in = testFolder.newFile(SnowflakeUtils.randomString(8) + ".csv");
    List<String> lines = Stream.of(data).collect(Collectors.toList());
    Files.write(in.toPath(), lines);
    return in;
  }

  private ConfigSource generateConfig(
      String targetTableName, String mode, String matchByColumnName) {
    return CONFIG_MAPPER_FACTORY
        .newConfigSource()
        .set("type", "snowflake")
        .set("user", TEST_SNOWFLAKE_USER)
        .set("password", TEST_SNOWFLAKE_PASSWORD)
        .set("host", TEST_SNOWFLAKE_HOST)
        .set("database", TEST_SNOWFLAKE_DB)
        .set("warehouse", TEST_SNOWFLAKE_WAREHOUSE)
        .set("schema", TEST_SNOWFLAKE_SCHEMA)
        .set("mode", mode)
        .set("match_by_column_name", matchByColumnName)
        .set("table", targetTableName);
  }

  private void assertSelectResults(String tableName, String query, Stream<String> expectedStream) {
    List<String> expected = expectedStream.collect(Collectors.toList());
    runQuery(
        String.format("select count(*) from %s;", tableName),
        foreachResult(rs -> assertEquals(expected.size(), rs.getInt(1))));

    int columnCount = expected.get(0).split(",").length;
    List<String> results = new ArrayList<>();
    runQuery(
        query,
        foreachResult(
            rs -> {
              List<String> result = new ArrayList<>();
              for (int i = 1; i <= columnCount; i++) {
                result.add(rs.getString(i));
              }
              results.add(String.join(",", result));
            }));
    for (int i = 0; i < results.size(); i++) {
      assertEquals(expected.get(i), results.get(i));
    }
  }

  private void assertSelectResults(
      String tableName, Stream<String> columns, Stream<String> expected) {
    String columnString =
        columns.map(x -> String.format("\"%s\"", x)).collect(Collectors.joining(","));
    String query = String.format("select %s from %s order by 1", columnString, tableName);
    assertSelectResults(tableName, query, expected);
  }

  private void assertSelectResults(String tableName, String columns, String... expected) {
    assertSelectResults(tableName, Arrays.stream(columns.split(",")), Arrays.stream(expected));
  }

  @After
  public void after() {
    dropAllTemporaryTables();
    dropAllTemporaryDatabases();
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
    assertEquals("", task.getRole());
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
    String[] data = new String[] {"aaa", "bbb", "ccc", "あああ", "雪"};
    Files.write(in.toPath(), lines("_c0:string", data));

    final String tableName = generateTemporaryTableName();
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
            .set("table", tableName);
    embulk.runOutput(config, in.toPath());

    String fullTableName =
        String.format("\"%s\".\"%s\".\"%s\"", TEST_SNOWFLAKE_DB, TEST_SNOWFLAKE_SCHEMA, tableName);
    runQuery(
        "select count(1) from " + fullTableName,
        foreachResult(
            rs -> {
              assertEquals(data.length, rs.getInt(1));
            }));
    List<String> results = new ArrayList();
    runQuery(
        "select \"_c0\" from " + fullTableName + " order by 1",
        foreachResult(
            rs -> {
              results.add(rs.getString(1));
            }));
    for (int i = 0; i < results.size(); i++) {
      assertEquals(data[i], results.get(i));
    }
  }

  @Test
  public void testRuntimeReplaceLongTable() throws IOException {
    File in = testFolder.newFile(SnowflakeUtils.randomString(8) + ".csv");
    Long[] data = new Long[] {1L, 2L, 3L};
    Files.write(
        in.toPath(),
        lines("_c0:long", Stream.of(data).map(String::valueOf).toArray(String[]::new)));

    final String tableName = generateTemporaryTableName();
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
            .set("table", tableName);
    embulk.runOutput(config, in.toPath());

    String fullTableName =
        String.format("\"%s\".\"%s\".\"%s\"", TEST_SNOWFLAKE_DB, TEST_SNOWFLAKE_SCHEMA, tableName);
    runQuery(
        "select count(1) from " + fullTableName,
        foreachResult(
            rs -> {
              assertEquals(data.length, rs.getInt(1));
            }));
    List<Long> results = new ArrayList();
    runQuery(
        "select \"_c0\" from " + fullTableName + " order by 1",
        foreachResult(
            rs -> {
              results.add(rs.getLong(1));
            }));
    for (int i = 0; i < results.size(); i++) {
      assertEquals(data[i], results.get(i));
    }
  }

  @Test
  public void testRuntimeReplaceDoubleTable() throws IOException {
    File in = testFolder.newFile(SnowflakeUtils.randomString(8) + ".csv");
    Double[] data = new Double[] {1.1d, 2.2d, 3.3d};
    Files.write(
        in.toPath(),
        lines("_c0:double", Stream.of(data).map(String::valueOf).toArray(String[]::new)));

    final String tableName = generateTemporaryTableName();
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
            .set("table", tableName);
    embulk.runOutput(config, in.toPath());

    String fullTableName =
        String.format("\"%s\".\"%s\".\"%s\"", TEST_SNOWFLAKE_DB, TEST_SNOWFLAKE_SCHEMA, tableName);
    runQuery(
        "select count(1) from " + fullTableName,
        foreachResult(
            rs -> {
              assertEquals(data.length, rs.getInt(1));
            }));
    List<Double> results = new ArrayList();
    runQuery(
        "select \"_c0\" from " + fullTableName + " order by 1",
        foreachResult(
            rs -> {
              results.add(rs.getDouble(1));
            }));
    for (int i = 0; i < results.size(); i++) {
      assertEquals(data[i], results.get(i));
    }
  }

  @Test
  public void testRuntimeReplaceBooleanTable() throws IOException {
    File in = testFolder.newFile(SnowflakeUtils.randomString(8) + ".csv");
    Boolean[] data = new Boolean[] {false, true};
    Files.write(
        in.toPath(),
        lines("_c0:boolean", Stream.of(data).map(String::valueOf).toArray(String[]::new)));

    final String tableName = generateTemporaryTableName();
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
            .set("table", tableName);
    embulk.runOutput(config, in.toPath());

    String fullTableName =
        String.format("\"%s\".\"%s\".\"%s\"", TEST_SNOWFLAKE_DB, TEST_SNOWFLAKE_SCHEMA, tableName);
    runQuery(
        "select count(1) from " + fullTableName,
        foreachResult(
            rs -> {
              assertEquals(data.length, rs.getInt(1));
            }));
    List<Boolean> results = new ArrayList();
    runQuery(
        "select \"_c0\" from " + fullTableName + " order by 1",
        foreachResult(
            rs -> {
              results.add(rs.getBoolean(1));
            }));
    for (int i = 0; i < results.size(); i++) {
      assertEquals(data[i], results.get(i));
    }
  }

  @Test
  public void testExecuteMultiQuery() throws IOException {
    File in = testFolder.newFile(SnowflakeUtils.randomString(8) + ".csv");
    List<String> lines =
        Stream.of("c0:double, c1:string", "0.0,aaa", "0.1,bbb", "1.2,ccc")
            .collect(Collectors.toList());
    Files.write(in.toPath(), lines);

    String targetTableName = generateTemporaryTableName();
    String targetTableFullName =
        String.format(
            "\"%s\".\"%s\".\"%s\"", TEST_SNOWFLAKE_DB, TEST_SNOWFLAKE_SCHEMA, targetTableName);
    runQuery(
        String.format("create table %s (c0 FLOAT, c1 STRING)", targetTableFullName),
        foreachResult(rs_ -> {}));

    String temporaryTableName = generateTemporaryTableName();

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
            .set("table", temporaryTableName)
            .set(
                "after_load",
                String.format(
                    "insert into \"%s\" select * from \"%s\"; drop table \"%s\";",
                    targetTableName, temporaryTableName, temporaryTableName));
    embulk.runOutput(config, in.toPath());

    runQuery(
        String.format("select count(*) from %s;", targetTableFullName),
        foreachResult(
            rs -> {
              assertEquals(3, rs.getInt(1));
            }));
  }

  @Test
  public void testRuntimeInsertStringTable() throws IOException {
    File in = testFolder.newFile(SnowflakeUtils.randomString(8) + ".csv");
    List<String> lines =
        Stream.of("c0:double,c1:string", "0.0,aaa", "0.1,bbb", "1.2,ccc")
            .collect(Collectors.toList());
    Files.write(in.toPath(), lines);

    // When multiple databases which have tables of same name exists, this plugin had a bug to try
    // to create invalid temporary tables.
    // So I added the test case for that.
    // ref: https://github.com/trocco-io/embulk-output-snowflake/pull/41

    final String anotherDbName = generateTemporaryDatabaseName();
    runQuery(String.format("create database \"%s\"", anotherDbName), foreachResult(rs_ -> {}));

    final String tableName = generateTemporaryTableName();
    String anotherDBFullTableName =
        String.format("\"%s\".\"%s\".\"%s\"", anotherDbName, TEST_SNOWFLAKE_SCHEMA, tableName);
    runQuery(
        String.format("create table %s (c0 FLOAT, c1 STRING)", anotherDBFullTableName),
        foreachResult(rs_ -> {}));

    String fullTableName =
        String.format("\"%s\".\"%s\".\"%s\"", TEST_SNOWFLAKE_DB, TEST_SNOWFLAKE_SCHEMA, tableName);

    runQuery(
        String.format("create table %s (c0 FLOAT, c1 STRING)", fullTableName),
        foreachResult(rs_ -> {}));

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
            .set("mode", "insert")
            .set("table", tableName);
    embulk.runOutput(config, in.toPath());

    runQuery(
        "select count(1) from " + fullTableName,
        foreachResult(
            rs -> {
              assertEquals(3, rs.getInt(1));
            }));
  }

  @Test
  public void testRuntimeMergeTable() throws IOException {
    File in = testFolder.newFile(SnowflakeUtils.randomString(8) + ".csv");
    List<String> lines =
        Stream.of("c0:double,c1:double,c2:string", "1,1,aaa", "1,2,bbb", "1,3,ccc")
            .collect(Collectors.toList());
    Files.write(in.toPath(), lines);

    String targetTableName = generateTemporaryTableName();
    String targetTableFullName =
        String.format(
            "\"%s\".\"%s\".\"%s\"", TEST_SNOWFLAKE_DB, TEST_SNOWFLAKE_SCHEMA, targetTableName);
    runQuery(
        String.format(
            "create table %s (\"c0\" NUMBER, \"c1\" NUMBER, \"c2\" STRING)", targetTableFullName),
        foreachResult(rs_ -> {}));
    runQuery(
        String.format("insert into %s values (1, 1, 'ddd'), (1, 4, 'eee');", targetTableFullName),
        foreachResult(rs_ -> {}));

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
            .set("mode", "merge")
            .set("merge_keys", new ArrayList<String>(Arrays.asList("c0", "c1")))
            .set("table", targetTableName);
    embulk.runOutput(config, in.toPath());

    runQuery(
        String.format("select count(*) from %s;", targetTableFullName),
        foreachResult(
            rs -> {
              assertEquals(4, rs.getInt(1));
            }));
    List<String> results = new ArrayList();
    runQuery(
        "select \"c0\",\"c1\",\"c2\" from " + targetTableFullName + " order by 1, 2",
        foreachResult(
            rs -> {
              results.add(rs.getString(1) + "," + rs.getString(2) + "," + rs.getString(3));
            }));
    List<String> expected =
        Stream.of("1,1,aaa", "1,2,bbb", "1,3,ccc", "1,4,eee").collect(Collectors.toList());
    for (int i = 0; i < results.size(); i++) {
      assertEquals(expected.get(i), results.get(i));
    }
  }

  @Test
  public void testRuntimeMergeTableWithMergeRule() throws IOException {
    File in = testFolder.newFile(SnowflakeUtils.randomString(8) + ".csv");
    List<String> lines =
        Stream.of("c0:double,c1:string", "0.0,aaa", "0.1,bbb", "1.2,ccc")
            .collect(Collectors.toList());
    Files.write(in.toPath(), lines);

    String targetTableName = generateTemporaryTableName();
    String targetTableFullName =
        String.format(
            "\"%s\".\"%s\".\"%s\"", TEST_SNOWFLAKE_DB, TEST_SNOWFLAKE_SCHEMA, targetTableName);
    runQuery(
        String.format("create table %s (\"c0\" FLOAT, \"c1\" STRING)", targetTableFullName),
        foreachResult(rs_ -> {}));
    runQuery(
        String.format("insert into %s values (0.0, 'ddd'), (1.3, 'eee');", targetTableFullName),
        foreachResult(rs_ -> {}));

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
            .set("mode", "merge")
            .set("merge_keys", new ArrayList<String>(Arrays.asList("c0")))
            .set(
                "merge_rule", new ArrayList<String>(Arrays.asList("\"c1\" = T.\"c1\" || S.\"c1\"")))
            .set("table", targetTableName);
    embulk.runOutput(config, in.toPath());

    runQuery(
        String.format("select count(*) from %s;", targetTableFullName),
        foreachResult(
            rs -> {
              assertEquals(4, rs.getInt(1));
            }));
    List<String> results = new ArrayList();
    runQuery(
        "select \"c0\",\"c1\" from " + targetTableFullName + " order by 1",
        foreachResult(
            rs -> {
              results.add(rs.getString(1) + "," + rs.getString(2));
            }));
    List<String> expected =
        Stream.of("0.0,dddaaa", "0.1,bbb", "1.2,ccc", "1.3,eee").collect(Collectors.toList());
    for (int i = 0; i < results.size(); i++) {
      assertEquals(expected.get(i), results.get(i));
    }
  }

  @Test
  public void testRuntimeMergeTableWithoutMergeKey() throws IOException {
    File in = testFolder.newFile(SnowflakeUtils.randomString(8) + ".csv");
    List<String> lines =
        Stream.of("c0:double,c1:string", "0.0,aaa", "0.1,bbb", "1.2,ccc")
            .collect(Collectors.toList());
    Files.write(in.toPath(), lines);

    String targetTableName = generateTemporaryTableName();
    String targetTableFullName =
        String.format(
            "\"%s\".\"%s\".\"%s\"", TEST_SNOWFLAKE_DB, TEST_SNOWFLAKE_SCHEMA, targetTableName);
    runQuery(
        String.format(
            "create table %s (\"c0\" FLOAT PRIMARY KEY, \"c1\" STRING)", targetTableFullName),
        foreachResult(rs_ -> {}));
    runQuery(
        String.format("insert into %s values (0.0, 'ddd'), (1.3, 'eee');", targetTableFullName),
        foreachResult(rs_ -> {}));

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
            .set("mode", "merge")
            .set("table", targetTableName);
    embulk.runOutput(config, in.toPath());

    runQuery(
        String.format("select count(*) from %s;", targetTableFullName),
        foreachResult(
            rs -> {
              assertEquals(4, rs.getInt(1));
            }));
    List<String> results = new ArrayList();
    runQuery(
        "select \"c0\",\"c1\" from " + targetTableFullName + " order by 1",
        foreachResult(
            rs -> {
              results.add(rs.getString(1) + "," + rs.getString(2));
            }));
    List<String> expected =
        Stream.of("0.0,aaa", "0.1,bbb", "1.2,ccc", "1.3,eee").collect(Collectors.toList());
    for (int i = 0; i < results.size(); i++) {
      assertEquals(expected.get(i), results.get(i));
    }
  }

  private void execRuntimeForMatchByColumnName(
      String mode,
      String matchByColumnName,
      String csvColumns,
      String tableColumns,
      String expectedColumns)
      throws IOException {
    final String targetTableName = generateTemporaryTableName();
    final String targetTableFullName = generateTableFullName(targetTableName);
    final Stream<String> csvColumnStream = Arrays.stream(csvColumns.split(","));

    final String header = csvColumnStream.map(x -> x + ":double").collect(Collectors.joining(","));
    final File in = generateTestFile(header, "100,1", "200,2", "300,3");

    if (tableColumns != null) {
      Stream<String> tableColumnStream = Arrays.stream(tableColumns.split(","));
      Stream<String> tableColumnStreamWithType =
          tableColumnStream.map(x -> String.format("\"%s\" DOUBLE", x));
      String queryColumns = tableColumnStreamWithType.collect(Collectors.joining(","));
      runQuery(String.format("create table %s (%s)", targetTableFullName, queryColumns));
    }

    ConfigSource config = generateConfig(targetTableName, mode, matchByColumnName);
    if (mode.equals("merge")) {
      String mergeColumns = tableColumns != null ? tableColumns : csvColumns;
      config.set("merge_keys", Arrays.stream(mergeColumns.split(",")).collect(Collectors.toList()));
    }
    embulk.runOutput(config, in.toPath());

    Stream<String> expected = Arrays.stream(expectedColumns.split(","));
    assertSelectResults(
        targetTableFullName, expected, Stream.of("100.0,1.0", "200.0,2.0", "300.0,3.0"));
  }

  private PartialExecutionException assertEmbulkThrows(ConfigSource config, File in) {
    return assertThrows(
        PartialExecutionException.class, () -> embulk.runOutput(config, in.toPath()));
  }

  private void assertErrorMessageIncludeInputSchemaColumnNotFound(
      PartialExecutionException exception, String... missingColumns) {
    String msg = exception.getCause().getMessage();
    String columns = String.join(", ", missingColumns);
    String regex =
        String.format(".*input schema column %s is not found in target table.*", columns);
    assertTrue(msg, msg.matches(regex));
  }

  private void assertErrorMessageExcludeInputSchemaColumnNotFound(
      PartialExecutionException exception) {
    String msg = exception.getCause().getMessage();
    String regex = ".*input schema column .* is not found in target table.*";
    assertFalse(msg, msg.matches(regex));
  }

  @Test
  public void testRuntimeWithMatchByColumnNameInvalid() {
    assertThrows(
        PartialExecutionException.class,
        () -> execRuntimeForMatchByColumnName("insert", "invalid", "c0", "c0", "c0"));
  }

  // MatchByColumnName = None
  @Test
  public void testRuntimeWithMatchByColumnNameNoneInsert() throws IOException {
    execRuntimeForMatchByColumnName("insert", "none", "c0,c1", "C1,c0", "C1,c0");
  }

  @Test
  public void testRuntimeWithMatchByColumnNameNoneInsertDirect() throws IOException {
    execRuntimeForMatchByColumnName("insert_direct", "none", "c0,c1", "C1,c0", "C1,c0");
  }

  @Test
  public void testRuntimeWithMatchByColumnNameNoneTruncateInsert() throws IOException {
    execRuntimeForMatchByColumnName("truncate_insert", "none", "c0,c1", "C1,c0", "C1,c0");
  }

  @Test
  public void testRuntimeWithMatchByColumnNameNoneReplace() throws IOException {
    execRuntimeForMatchByColumnName("replace", "none", "c0,c1", "C1,c0", "c0,c1");
  }

  @Test
  public void testRuntimeWithMatchByColumnNameNoneMerge() throws IOException {
    execRuntimeForMatchByColumnName("merge", "none", "c0,c1", "C1,c0", "C1,c0");
  }

  @Test
  public void testRuntimeWithMatchByColumnNameCaseNoneWhenOnlyPresentColumnInCSV()
      throws IOException {
    final String targetTableName = generateTemporaryTableName();
    final String targetTableFullName = generateTableFullName(targetTableName);

    File in = generateTestFile("c1:double,c2:double,c3:double,c0:double", "100,1,,10000");
    runQuery(String.format("create table %s (\"c0\" DOUBLE, \"c2\" DOUBLE)", targetTableFullName));

    ConfigSource config = generateConfig(targetTableName, "insert", "none");
    embulk.runOutput(config, in.toPath());
    assertSelectResults(targetTableFullName, "c0,c2", "1.0,10000.0");
  }

  @Test
  public void testRuntimeWithMatchByColumnNameNoneWhenOnlyPresentColumnInTable()
      throws IOException {
    final String targetTableName = generateTemporaryTableName();
    final String targetTableFullName = generateTableFullName(targetTableName);

    File in = generateTestFile("c0:double", "100");
    runQuery(String.format("create table %s (\"c0\" DOUBLE, \"c1\" DOUBLE)", targetTableFullName));

    ConfigSource config = generateConfig(targetTableName, "insert", "none");
    PartialExecutionException exception = assertEmbulkThrows(config, in);
    assertTrue(
        exception.getCause().getCause().getCause()
            instanceof net.snowflake.client.jdbc.SnowflakeSQLException);
  }

  // MatchByColumnName = CaseSensitive

  // table present
  @Test
  public void testRuntimeWithMatchByColumnNameCaseSensitiveInsert() throws IOException {
    execRuntimeForMatchByColumnName("insert", "case_sensitive", "c0,c1", "c1,c0", "c0,c1");
  }

  @Test
  public void testRuntimeWithMatchByColumnNameCaseSensitiveInsertDirect() throws IOException {
    execRuntimeForMatchByColumnName("insert_direct", "case_sensitive", "c0,c1", "c1,c0", "c0,c1");
  }

  @Test
  public void testRuntimeWithMatchByColumnNameCaseSensitiveTruncateInsert() throws IOException {
    execRuntimeForMatchByColumnName("truncate_insert", "case_sensitive", "c0,c1", "c1,c0", "c0,c1");
  }

  @Test
  public void testRuntimeWithMatchByColumnNameCaseSensitiveReplace() throws IOException {
    execRuntimeForMatchByColumnName("replace", "case_sensitive", "c0,c1", "c1,c0", "c0,c1");
  }

  @Test
  public void testRuntimeWithMatchByColumnNameCaseSensitiveMerge() throws IOException {
    execRuntimeForMatchByColumnName("merge", "case_sensitive", "c0,c1", "c1,c0", "c0,c1");
  }

  @Test
  public void
      testRuntimeWithMatchByColumnNameCaseSensitiveWhenOnlyPresentColumnInCSVByCaseSensitive()
          throws IOException {
    final String targetTableName = generateTemporaryTableName();
    final String targetTableFullName = generateTableFullName(targetTableName);

    File in = generateTestFile("c1:double,c0:double,c3:double,c2:double", "100,1,,10000");
    runQuery(
        String.format(
            "create table %s (\"c0\" DOUBLE, \"C1\" DOUBLE, \"c2\" DOUBLE, \"C3\" DOUBLE)",
            targetTableFullName));

    ConfigSource config = generateConfig(targetTableName, "insert", "case_sensitive");
    PartialExecutionException exception = assertEmbulkThrows(config, in);
    assertErrorMessageIncludeInputSchemaColumnNotFound(exception, "c1", "c3");
  }

  // Error
  @Test
  public void testRuntimeWithMatchByColumnNameCaseSensitiveWhenOnlyPresentColumnInCSVBySkip()
      throws IOException {
    final String targetTableName = generateTemporaryTableName();
    final String targetTableFullName = generateTableFullName(targetTableName);

    File in = generateTestFile("c1:double,c0:double,c3:double,c2:double", "100,1,,10000");
    runQuery(String.format("create table %s (\"c0\" DOUBLE, \"c2\" DOUBLE)", targetTableFullName));

    ConfigSource config = generateConfig(targetTableName, "insert", "case_sensitive");
    PartialExecutionException exception = assertEmbulkThrows(config, in);
    assertErrorMessageIncludeInputSchemaColumnNotFound(exception, "c1", "c3");
  }

  @Test
  public void testRuntimeWithMatchByColumnNameCaseSensitiveWhenOnlyPresentColumnInTable()
      throws IOException {
    final String targetTableName = generateTemporaryTableName();
    final String targetTableFullName = generateTableFullName(targetTableName);

    File in = generateTestFile("c0:double", "100");
    runQuery(String.format("create table %s (\"c0\" DOUBLE, \"c1\" DOUBLE)", targetTableFullName));

    ConfigSource config = generateConfig(targetTableName, "insert", "case_sensitive");

    embulk.runOutput(config, in.toPath());
    assertSelectResults(targetTableFullName, "c0,c1", "100.0,null");
  }

  // MatchByColumnName = CaseInsensitive

  // table present
  @Test
  public void testRuntimeWithMatchByColumnNameCaseInsensitiveInsert() throws IOException {
    execRuntimeForMatchByColumnName("insert", "case_insensitive", "c0,c1", "C1,c0", "c0,C1");
  }

  @Test
  public void testRuntimeWithMatchByColumnNameCaseInsensitiveInsertDirect() throws IOException {
    execRuntimeForMatchByColumnName("insert_direct", "case_insensitive", "c0,c1", "C1,c0", "c0,C1");
  }

  @Test
  public void testRuntimeWithMatchByColumnNameCaseInsensitiveTruncateInsert() throws IOException {
    execRuntimeForMatchByColumnName(
        "truncate_insert", "case_insensitive", "c0,c1", "C1,c0", "c0,C1");
  }

  @Test
  public void testRuntimeWithMatchByColumnNameCaseInsensitiveReplace() throws IOException {
    execRuntimeForMatchByColumnName("replace", "case_insensitive", "c0,c1", "C1,c0", "c0,c1");
  }

  @Test
  public void testRuntimeWithMatchByColumnNameCaseInsensitiveMerge() throws IOException {
    execRuntimeForMatchByColumnName("merge", "case_insensitive", "c0,c1", "C1,c0", "c0,C1");
  }

  // Error
  @Test
  public void testRuntimeWithMatchByColumnNameCaseInsensitiveWhenOnlyPresentColumnInCSVBySkip()
      throws IOException {
    final String targetTableName = generateTemporaryTableName();
    final String targetTableFullName = generateTableFullName(targetTableName);

    File in = generateTestFile("c1:double,c0:double,c3:double,c2:double", "100,1,,10000");
    runQuery(String.format("create table %s (\"C0\" DOUBLE, \"c2\" DOUBLE)", targetTableFullName));

    ConfigSource config = generateConfig(targetTableName, "insert", "case_insensitive");
    PartialExecutionException exception = assertEmbulkThrows(config, in);
    assertErrorMessageIncludeInputSchemaColumnNotFound(exception, "c1", "c3");
  }

  @Test
  public void testRuntimeWithMatchByColumnNameCaseInsensitiveWhenOnlyPresentColumnInTable()
      throws IOException {
    final String targetTableName = generateTemporaryTableName();
    final String targetTableFullName = generateTableFullName(targetTableName);

    File in = generateTestFile("c0:double", "100");
    runQuery(String.format("create table %s (\"C0\" DOUBLE, \"c1\" DOUBLE)", targetTableFullName));

    ConfigSource config = generateConfig(targetTableName, "insert", "case_insensitive");
    embulk.runOutput(config, in.toPath());
    assertSelectResults(targetTableFullName, "C0,c1", "100.0,null");
  }

  @Ignore(
      "This test takes so long time because it needs to create more than 1000 tables, so ignored...")
  @Test(expected = Test.None.class /* no exception expected */)
  public void testRunnableEvenIfMoreThan1001TasksRun() throws IOException {
    File in = testFolder.newFile(SnowflakeUtils.randomString(8) + ".csv");
    List<String> lines = Stream.of("aaa", "bbb", "ccc").collect(Collectors.toList());
    Files.write(in.toPath(), lines);

    final String tableName = generateTemporaryTableName();
    final ConfigSource parserConfig =
        CONFIG_MAPPER_FACTORY
            .newConfigSource()
            .set("charset", "UTF-8")
            .set("newline", "LF")
            .set("type", "csv")
            .set("delimiter", ",")
            .set("quote", "\"")
            .set("escape", "\"")
            .set(
                "columns",
                new SchemaConfig(
                    Stream.of(
                            new ColumnConfig(
                                "c0", Types.STRING, CONFIG_MAPPER_FACTORY.newConfigSource()))
                        .collect(Collectors.toList())));
    final ConfigSource inConfig =
        CONFIG_MAPPER_FACTORY
            .newConfigSource()
            .set("type", "file")
            .set("path_prefix", in.getAbsolutePath())
            .set("parser", parserConfig);
    final ConfigSource outConfig =
        CONFIG_MAPPER_FACTORY
            .newConfigSource()
            .set("type", "snowflake")
            .set("user", TEST_SNOWFLAKE_USER)
            .set("password", TEST_SNOWFLAKE_PASSWORD)
            .set("host", TEST_SNOWFLAKE_HOST)
            .set("database", TEST_SNOWFLAKE_DB)
            .set("warehouse", TEST_SNOWFLAKE_WAREHOUSE)
            .set("schema", TEST_SNOWFLAKE_SCHEMA)
            .set("mode", "insert")
            .set("table", tableName);
    final ConfigSource execConfig =
        CONFIG_MAPPER_FACTORY.newConfigSource().set("min_output_tasks", 1001);

    final EmbulkEmbed embed = TestingEmbulkHack.getEmbulkEmbed(embulk);
    final ConfigSource config =
        embed
            .newConfigLoader()
            .newConfigSource()
            .set("exec", execConfig)
            .set("in", inConfig)
            .set("out", outConfig);

    RunResult result = (RunResult) embed.run(config);
    assertTrue(result.getIgnoredExceptions().isEmpty());
  }
}
