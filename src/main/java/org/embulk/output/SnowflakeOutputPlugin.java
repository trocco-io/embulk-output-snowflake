package org.embulk.output;

import java.io.IOException;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import org.embulk.config.ConfigDiff;
import org.embulk.config.TaskSource;
import org.embulk.output.jdbc.AbstractJdbcOutputPlugin;
import org.embulk.output.jdbc.BatchInsert;
import org.embulk.output.jdbc.JdbcColumn;
import org.embulk.output.jdbc.JdbcOutputConnection;
import org.embulk.output.jdbc.JdbcOutputConnector;
import org.embulk.output.jdbc.JdbcSchema;
import org.embulk.output.jdbc.JdbcUtils;
import org.embulk.output.jdbc.MergeConfig;
import org.embulk.output.jdbc.TableIdentifier;
import org.embulk.output.snowflake.SnowflakeCopyBatchInsert;
import org.embulk.output.snowflake.SnowflakeOutputConnection;
import org.embulk.output.snowflake.SnowflakeOutputConnector;
import org.embulk.output.snowflake.StageIdentifier;
import org.embulk.output.snowflake.StageIdentifierHolder;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.Schema;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;

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
    super.doBegin(con, task, schema, taskCount);
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
      SnowflakeOutputConnection snowflakeCon = (SnowflakeOutputConnection) getConnector(task, true).connect(true);
      this.stageIdentifier = StageIdentifierHolder.getStageIdentifier(t);
      snowflakeCon.runCreateStage(this.stageIdentifier);
    }

    return new SnowflakeCopyBatchInsert(getConnector(task, true), this.stageIdentifier, false);
  }

  public Optional<JdbcSchema> newJdbcSchemaFromTableIfExists(JdbcOutputConnection connection,
      TableIdentifier table) throws SQLException {
    if (!connection.tableExists(table)) {
      // DatabaseMetaData.getPrimaryKeys fails if table does not exist
      return Optional.empty();
    }

    DatabaseMetaData dbm = connection.getMetaData();
    String escape = dbm.getSearchStringEscape();

    ResultSet rs = dbm.getPrimaryKeys(table.getDatabase(), table.getSchemaName(), table.getTableName());
    final HashSet<String> primaryKeysBuilder = new HashSet<>();
    try {
      while (rs.next()) {
        primaryKeysBuilder.add(rs.getString("COLUMN_NAME"));
      }
    } finally {
      rs.close();
    }
    final Set<String> primaryKeys = Collections.unmodifiableSet(primaryKeysBuilder);

    final ArrayList<JdbcColumn> builder = new ArrayList<>();
    logger.info("{}", JdbcUtils.escapeSearchString(table.getDatabase(), escape));
    logger.info("{}", JdbcUtils.escapeSearchString(table.getSchemaName(), escape));
    logger.info("{}", JdbcUtils.escapeSearchString(table.getTableName(), escape));
    rs = dbm.getColumns(
        JdbcUtils.escapeSearchString(table.getDatabase(), escape),
        JdbcUtils.escapeSearchString(table.getSchemaName(), escape),
        JdbcUtils.escapeSearchString(table.getTableName(), escape),
        null);
    try {
      while (rs.next()) {
        String columnName = rs.getString("COLUMN_NAME");
        logger.info("{}", columnName);
        String simpleTypeName = rs.getString("TYPE_NAME").toUpperCase(Locale.ENGLISH);
        boolean isUniqueKey = primaryKeys.contains(columnName);
        int sqlType = rs.getInt("DATA_TYPE");
        int colSize = rs.getInt("COLUMN_SIZE");
        int decDigit = rs.getInt("DECIMAL_DIGITS");
        if (rs.wasNull()) {
          decDigit = -1;
        }
        int charOctetLength = rs.getInt("CHAR_OCTET_LENGTH");
        boolean isNotNull = "NO".equals(rs.getString("IS_NULLABLE"));
        // rs.getString("COLUMN_DEF") // or null // TODO
        builder.add(JdbcColumn.newGenericTypeColumn(
            columnName, sqlType, simpleTypeName, colSize, decDigit, charOctetLength, isNotNull, isUniqueKey));
        // We can't get declared column name using JDBC API.
        // Subclasses need to overwrite it.
      }
    } finally {
      rs.close();
    }
    final List<JdbcColumn> columns = Collections.unmodifiableList(builder);
    if (columns.isEmpty()) {
      return Optional.empty();
    } else {
      return Optional.of(new JdbcSchema(columns));
    }
  }

}
