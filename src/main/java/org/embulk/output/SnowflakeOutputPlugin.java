package org.embulk.output;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.TaskSource;
import org.embulk.output.jdbc.AbstractJdbcOutputPlugin;
import org.embulk.output.jdbc.BatchInsert;
import org.embulk.output.jdbc.JdbcColumn;
import org.embulk.output.jdbc.JdbcColumnOption;
import org.embulk.output.jdbc.JdbcOutputConnection;
import org.embulk.output.jdbc.JdbcOutputConnector;
import org.embulk.output.jdbc.JdbcSchema;
import org.embulk.output.jdbc.MergeConfig;
import org.embulk.output.jdbc.TableIdentifier;
import org.embulk.output.snowflake.SnowflakeCopyBatchInsert;
import org.embulk.output.snowflake.SnowflakeOutputConnection;
import org.embulk.output.snowflake.SnowflakeOutputConnector;
import org.embulk.output.snowflake.StageIdentifier;
import org.embulk.output.snowflake.StageIdentifierHolder;
import org.embulk.spi.Column;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.Schema;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.util.retryhelper.RetryExecutor;
import org.embulk.util.retryhelper.RetryGiveupException;
import org.embulk.util.retryhelper.Retryable;

public class SnowflakeOutputPlugin extends AbstractJdbcOutputPlugin {
  private StageIdentifier stageIdentifier;

  public interface SnowflakePluginTask extends PluginTask {
    @Config("driver_path")
    @ConfigDefault("null")
    public Optional<String> getDriverPath();

    @Config("host")
    public String getHost();

    @Config("user")
    @ConfigDefault("\"\"")
    public String getUser();

    @Config("password")
    @ConfigDefault("\"\"")
    public String getPassword();

    @Config("database")
    public String getDatabase();

    @Config("warehouse")
    public String getWarehouse();

    @Config("schema")
    @ConfigDefault("\"public\"")
    public String getSchema();

    @Config("delete_stage")
    @ConfigDefault("false")
    public boolean getDeleteStage();
  }

  @Override
  protected Class<? extends PluginTask> getTaskClass() {
    return SnowflakePluginTask.class;
  }

  @Override
  protected Features getFeatures(PluginTask task) {
    return new Features()
        .setMaxTableNameLength(127)
        .setSupportedModes(
            new HashSet<>(
                Arrays.asList(Mode.INSERT, Mode.INSERT_DIRECT, Mode.TRUNCATE_INSERT, Mode.REPLACE)))
        .setIgnoreMergeKeys(false);
  }

  @Override
  protected JdbcOutputConnector getConnector(PluginTask task, boolean retryableMetadataOperation) {
    SnowflakePluginTask t = (SnowflakePluginTask) task;

    loadDriver("net.snowflake.client.jdbc.SnowflakeDriver", t.getDriverPath());

    String url = String.format("jdbc:snowflake://%s", t.getHost());

    Properties props = new Properties();

    props.setProperty("user", t.getUser());
    props.setProperty("password", t.getPassword());
    props.setProperty("warehouse", t.getWarehouse());
    props.setProperty("db", t.getDatabase());
    props.setProperty("schema", t.getSchema());

    props.putAll(t.getOptions());

    logConnectionProperties(url, props);

    return new SnowflakeOutputConnector(url, props, t.getTransactionIsolation());
  }

  @Override
  public ConfigDiff resume(
      TaskSource taskSource, Schema schema, int taskCount, OutputPlugin.Control control) {
    throw new UnsupportedOperationException("snowflake output plugin does not support resuming");
  }

  @Override
  protected void doCommit(JdbcOutputConnection con, PluginTask task, int taskCount)
      throws SQLException {
    super.doCommit(con, task, taskCount);
    SnowflakeOutputConnection snowflakeCon = (SnowflakeOutputConnection) con;

    SnowflakePluginTask t = (SnowflakePluginTask) task;
    if (this.stageIdentifier == null) {
      this.stageIdentifier = StageIdentifierHolder.getStageIdentifier(t);
    }

    if (t.getDeleteStage()) {
      snowflakeCon.runDropStage(this.stageIdentifier);
    }
  }

  @Override
  protected void doBegin(
      JdbcOutputConnection con, PluginTask task, final Schema schema, int taskCount)
      throws SQLException {
    if (schema.getColumnCount() == 0) {
      throw new ConfigException("No column.");
    }

    Mode mode = task.getMode();
    logger.info("Using {} mode", mode);

    if (mode.commitBySwapTable() && task.getBeforeLoad().isPresent()) {
      throw new ConfigException(
          String.format("%s mode does not support 'before_load' option.", mode));
    }

    String actualTable;
    if (con.tableExists(task.getTable())) {
      actualTable = task.getTable();
    } else {
      String upperTable = task.getTable().toUpperCase();
      String lowerTable = task.getTable().toLowerCase();
      if (con.tableExists(upperTable)) {
        if (con.tableExists(lowerTable)) {
          throw new ConfigException(
              String.format(
                  "Cannot specify table '%s' because both '%s' and '%s' exist.",
                  task.getTable(), upperTable, lowerTable));
        } else {
          actualTable = upperTable;
        }
      } else {
        if (con.tableExists(lowerTable)) {
          actualTable = lowerTable;
        } else {
          actualTable = task.getTable();
        }
      }
    }
    // need to get database name
    SnowflakePluginTask sfTask = (SnowflakePluginTask) task;
    task.setActualTable(
        new TableIdentifier(sfTask.getDatabase(), con.getSchemaName(), actualTable));

    Optional<JdbcSchema> initialTargetTableSchema =
        mode.ignoreTargetTableSchema()
            ? Optional.<JdbcSchema>empty()
            : newJdbcSchemaFromTableIfExists(con, task.getActualTable());

    // TODO get CREATE TABLE statement from task if set
    JdbcSchema newTableSchema =
        applyColumnOptionsToNewTableSchema(
            initialTargetTableSchema.orElseGet(
                new Supplier<JdbcSchema>() {
                  public JdbcSchema get() {
                    return newJdbcSchemaForNewTable(schema);
                  }
                }),
            task.getColumnOptions());

    // create intermediate tables
    if (!mode.isDirectModify()) {
      // create the intermediate tables here
      task.setIntermediateTables(
          Optional.<List<TableIdentifier>>of(
              createIntermediateTables(con, task, taskCount, newTableSchema)));
    } else {
      // direct modify mode doesn't need intermediate tables.
      task.setIntermediateTables(Optional.<List<TableIdentifier>>empty());
      if (task.getBeforeLoad().isPresent()) {
        // executeSql is private need to cast
        SnowflakeOutputConnection sfCon = (SnowflakeOutputConnection) con;
        sfCon.executeSql(task.getBeforeLoad().get());
      }
    }

    // build JdbcSchema from a table
    JdbcSchema targetTableSchema;
    if (initialTargetTableSchema.isPresent()) {
      targetTableSchema = initialTargetTableSchema.get();
      task.setNewTableSchema(Optional.<JdbcSchema>empty());
    } else if (task.getIntermediateTables().isPresent()
        && !task.getIntermediateTables().get().isEmpty()) {
      TableIdentifier firstItermTable = task.getIntermediateTables().get().get(0);
      targetTableSchema = newJdbcSchemaFromTableIfExists(con, firstItermTable).get();
      task.setNewTableSchema(Optional.of(newTableSchema));
    } else {
      // also create the target table if not exists
      // CREATE TABLE IF NOT EXISTS xyz
      con.createTableIfNotExists(
          task.getActualTable(),
          newTableSchema,
          task.getCreateTableConstraint(),
          task.getCreateTableOption());
      targetTableSchema = newJdbcSchemaFromTableIfExists(con, task.getActualTable()).get();
      task.setNewTableSchema(Optional.<JdbcSchema>empty());
    }
    task.setTargetTableSchema(matchSchemaByColumnNames(schema, targetTableSchema));

    // validate column_options
    newColumnSetters(
        newColumnSetterFactory(null, task.getDefaultTimeZone()), // TODO create a dummy BatchInsert
        task.getTargetTableSchema(),
        schema,
        task.getColumnOptions());

    // normalize merge_key parameter for merge modes
    if (mode.isMerge()) {
      Optional<List<String>> mergeKeys = task.getMergeKeys();
      if (task.getFeatures().getIgnoreMergeKeys()) {
        if (mergeKeys.isPresent()) {
          throw new ConfigException("This output type does not accept 'merge_key' option.");
        }
        task.setMergeKeys(Optional.<List<String>>of(Collections.emptyList()));
      } else if (mergeKeys.isPresent()) {
        if (task.getMergeKeys().get().isEmpty()) {
          throw new ConfigException("Empty 'merge_keys' option is invalid.");
        }
        for (String key : mergeKeys.get()) {
          if (!targetTableSchema.findColumn(key).isPresent()) {
            throw new ConfigException(
                String.format("Merge key '%s' does not exist in the target table.", key));
          }
        }
      } else {
        final ArrayList<String> builder = new ArrayList<>();
        for (JdbcColumn column : targetTableSchema.getColumns()) {
          if (column.isUniqueKey()) {
            builder.add(column.getName());
          }
        }
        task.setMergeKeys(Optional.<List<String>>of(Collections.unmodifiableList(builder)));
        if (task.getMergeKeys().get().isEmpty()) {
          throw new ConfigException(
              "Merging mode is used but the target table does not have primary keys. Please set merge_keys option.");
        }
      }
      logger.info("Using merge keys: {}", task.getMergeKeys().get());
    } else {
      task.setMergeKeys(Optional.<List<String>>empty());
    }
  }

  @Override
  protected BatchInsert newBatchInsert(PluginTask task, Optional<MergeConfig> mergeConfig)
      throws IOException, SQLException {
    if (mergeConfig.isPresent()) {
      throw new UnsupportedOperationException(
          "Snowflake output plugin doesn't support 'merge_direct' mode.");
    }

    SnowflakePluginTask t = (SnowflakePluginTask) task;
    // TODO: put some where executes once
    if (this.stageIdentifier == null) {
      SnowflakeOutputConnection snowflakeCon =
          (SnowflakeOutputConnection) getConnector(task, true).connect(true);
      this.stageIdentifier = StageIdentifierHolder.getStageIdentifier(t);
      snowflakeCon.runCreateStage(this.stageIdentifier);
    }

    return new SnowflakeCopyBatchInsert(getConnector(task, true), this.stageIdentifier, false);
  }

  // borrow code from jdbc to fix TablIdentifer in doBegin
  private static JdbcSchema applyColumnOptionsToNewTableSchema(
      JdbcSchema schema, final Map<String, JdbcColumnOption> columnOptions) {
    return new JdbcSchema(
        schema.getColumns().stream()
            .map(
                c -> {
                  JdbcColumnOption option = columnOptionOf(columnOptions, c.getName());
                  if (option.getType().isPresent()) {
                    return JdbcColumn.newTypeDeclaredColumn(
                        c.getName(),
                        Types.OTHER, // sqlType, isNotNull, and isUniqueKey are ignored
                        option.getType().get(),
                        false,
                        false);
                  }
                  return c;
                })
            .collect(Collectors.toList()));
  }

  // borrow code from jdbc to fix TablIdentifer in doBegin
  private static JdbcColumnOption columnOptionOf(
      Map<String, JdbcColumnOption> columnOptions, String columnName) {
    return Optional.ofNullable(columnOptions.get(columnName))
        .orElseGet(
            // default column option
            new Supplier<JdbcColumnOption>() {
              public JdbcColumnOption get() {
                return CONFIG_MAPPER.map(
                    CONFIG_MAPPER_FACTORY.newConfigSource(), JdbcColumnOption.class);
              }
            });
  }

  // borrow code from jdbc to fix TablIdentifer in doBegin
  private List<TableIdentifier> createIntermediateTables(
      final JdbcOutputConnection con,
      final PluginTask task,
      final int taskCount,
      final JdbcSchema newTableSchema)
      throws SQLException {
    try {
      return buildRetryExecutor(task)
          .run(
              new Retryable<List<TableIdentifier>>() {
                private TableIdentifier table;
                private ArrayList<TableIdentifier> intermTables;

                @Override
                public List<TableIdentifier> call() throws Exception {
                  intermTables = new ArrayList<>();
                  if (task.getMode().tempTablePerTask()) {
                    String tableNameFormat =
                        generateIntermediateTableNameFormat(
                            task.getActualTable().getTableName(),
                            con,
                            taskCount,
                            task.getFeatures().getMaxTableNameLength(),
                            task.getFeatures().getTableNameLengthSemantics());
                    for (int taskIndex = 0; taskIndex < taskCount; taskIndex++) {
                      String tableName = String.format(tableNameFormat, taskIndex);
                      table = buildIntermediateTableId(con, task, tableName);
                      // if table already exists, SQLException will be thrown
                      con.createTable(
                          table,
                          newTableSchema,
                          task.getCreateTableConstraint(),
                          task.getCreateTableOption());
                      intermTables.add(table);
                    }
                  } else {
                    String tableName =
                        generateIntermediateTableNamePrefix(
                            task.getActualTable().getTableName(),
                            con,
                            0,
                            task.getFeatures().getMaxTableNameLength(),
                            task.getFeatures().getTableNameLengthSemantics());
                    table = buildIntermediateTableId(con, task, tableName);
                    con.createTable(
                        table,
                        newTableSchema,
                        task.getCreateTableConstraint(),
                        task.getCreateTableOption());
                    intermTables.add(table);
                  }
                  return Collections.unmodifiableList(intermTables);
                }

                @Override
                public boolean isRetryableException(Exception exception) {
                  if (exception instanceof SQLException) {
                    try {
                      // true means that creating table failed because the table already exists.
                      return con.tableExists(table);
                    } catch (SQLException e) {
                    }
                  }
                  return false;
                }

                @Override
                public void onRetry(
                    Exception exception, int retryCount, int retryLimit, int retryWait)
                    throws RetryGiveupException {
                  logger.info("Try to create intermediate tables again because already exist");
                  try {
                    dropTables();
                  } catch (SQLException e) {
                    throw new RetryGiveupException(e);
                  }
                }

                @Override
                public void onGiveup(Exception firstException, Exception lastException)
                    throws RetryGiveupException {
                  try {
                    dropTables();
                  } catch (SQLException e) {
                    logger.warn("Cannot delete intermediate table", e);
                  }
                }

                private void dropTables() throws SQLException {
                  for (TableIdentifier table : intermTables) {
                    con.dropTableIfExists(table);
                  }
                }
              });
    } catch (RetryGiveupException e) {
      throw new RuntimeException(e);
    }
  }

  private static RetryExecutor buildRetryExecutor(PluginTask task) {
    return RetryExecutor.retryExecutor()
        .withRetryLimit(task.getRetryLimit())
        .withInitialRetryWait(task.getRetryWait())
        .withMaxRetryWait(task.getMaxRetryWait());
  }

  // borrow code from jdbc to fix TablIdentifer in doBegin
  private JdbcSchema matchSchemaByColumnNames(Schema inputSchema, JdbcSchema targetTableSchema) {
    final ArrayList<JdbcColumn> jdbcColumns = new ArrayList<>();

    for (Column column : inputSchema.getColumns()) {
      Optional<JdbcColumn> c = targetTableSchema.findColumn(column.getName());
      jdbcColumns.add(c.orElse(JdbcColumn.skipColumn()));
    }

    return new JdbcSchema(Collections.unmodifiableList(jdbcColumns));
  }
}
