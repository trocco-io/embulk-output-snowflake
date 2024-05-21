package org.embulk.output;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Types;
import java.util.*;
import net.snowflake.client.jdbc.internal.org.bouncycastle.operator.OperatorCreationException;
import net.snowflake.client.jdbc.internal.org.bouncycastle.pkcs.PKCSException;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskSource;
import org.embulk.output.jdbc.*;
import org.embulk.output.snowflake.PrivateKeyReader;
import org.embulk.output.snowflake.SnowflakeCopyBatchInsert;
import org.embulk.output.snowflake.SnowflakeOutputConnection;
import org.embulk.output.snowflake.SnowflakeOutputConnector;
import org.embulk.output.snowflake.StageIdentifier;
import org.embulk.output.snowflake.StageIdentifierHolder;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.Schema;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;

public class SnowflakeOutputPlugin extends AbstractJdbcOutputPlugin {
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

    @Config("privateKey")
    @ConfigDefault("\"\"")
    String getPrivateKey();

    @Config("private_key_passphrase")
    @ConfigDefault("\"\"")
    String getPrivateKeyPassphrase();

    @Config("database")
    public String getDatabase();

    @Config("warehouse")
    public String getWarehouse();

    @Config("schema")
    @ConfigDefault("\"public\"")
    public String getSchema();

    @Config("role")
    @ConfigDefault("\"\"")
    public String getRole();

    @Config("delete_stage")
    @ConfigDefault("false")
    public boolean getDeleteStage();

    @Config("max_upload_retries")
    @ConfigDefault("3")
    public int getMaxUploadRetries();

    @Config("empty_field_as_null")
    @ConfigDefault("true")
    public boolean getEmtpyFieldAsNull();

    @Config("delete_stage_on_error")
    @ConfigDefault("false")
    public boolean getDeleteStageOnError();
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
                Arrays.asList(
                    Mode.INSERT,
                    Mode.INSERT_DIRECT,
                    Mode.TRUNCATE_INSERT,
                    Mode.REPLACE,
                    Mode.MERGE)))
        .setIgnoreMergeKeys(false);
  }

  @Override
  protected JdbcOutputConnector getConnector(PluginTask task, boolean retryableMetadataOperation) {
    SnowflakePluginTask t = (SnowflakePluginTask) task;

    loadDriver("net.snowflake.client.jdbc.SnowflakeDriver", t.getDriverPath());

    String url = String.format("jdbc:snowflake://%s", t.getHost());

    Properties props = new Properties();

    props.setProperty("user", t.getUser());
    if (!t.getPassword().isEmpty()) {
      props.setProperty("password", t.getPassword());
    } else if (!t.getPrivateKey().isEmpty()) {
      try {
        props.put(
            "privateKey", PrivateKeyReader.get(t.getPrivateKey(), t.getPrivateKeyPassphrase()));
      } catch (IOException | OperatorCreationException | PKCSException e) {
        // Since this method is not allowed to throw any checked exception,
        // wrap it with ConfigException, which is unchecked.
        throw new ConfigException(e);
      }
    }

    props.setProperty("warehouse", t.getWarehouse());
    props.setProperty("db", t.getDatabase());
    props.setProperty("schema", t.getSchema());
    if (!t.getRole().isEmpty()) {
      props.setProperty("role", t.getRole());
    }

    // When CLIENT_METADATA_REQUEST_USE_CONNECTION_CTX is false (default),
    // getMetaData().getColumns() returns columns of the tables which table name is
    // same in all databases.
    // So, set this parameter true.
    // https://github.com/snowflakedb/snowflake-jdbc/blob/032bdceb408ebeedb1a9ad4edd9ee6cf7c6bb470/src/main/java/net/snowflake/client/jdbc/SnowflakeDatabaseMetaData.java#L1261-L1269
    props.setProperty("CLIENT_METADATA_REQUEST_USE_CONNECTION_CTX", "true");
    props.setProperty("MULTI_STATEMENT_COUNT", "0");

    props.putAll(t.getOptions());

    logConnectionProperties(url, props);

    return new SnowflakeOutputConnector(url, props, t.getTransactionIsolation());
  }

  @Override
  public ConfigDiff transaction(
      ConfigSource config, Schema schema, int taskCount, OutputPlugin.Control control) {
    PluginTask task = CONFIG_MAPPER.map(config, this.getTaskClass());
    SnowflakePluginTask t = (SnowflakePluginTask) task;
    StageIdentifier stageIdentifier = StageIdentifierHolder.getStageIdentifier(t);
    ConfigDiff configDiff;
    SnowflakeOutputConnection snowflakeCon = null;

    try {
      snowflakeCon = (SnowflakeOutputConnection) getConnector(task, true).connect(true);
      snowflakeCon.runCreateStage(stageIdentifier);
      configDiff = super.transaction(config, schema, taskCount, control);
      Thread.sleep(6 * 60 * 1000); // 6 minutes
      if (t.getDeleteStage()) {
        runDropStage(snowflakeCon, stageIdentifier, task);
      }
    } catch (Exception e) {
      if (t.getDeleteStage() && t.getDeleteStageOnError()) {
        try {
          runDropStage(snowflakeCon, stageIdentifier, task);
        } catch (SQLException ex) {
          throw new RuntimeException(ex);
        }
      }
      throw new RuntimeException(e);
    }

    return configDiff;
  }

  private void runDropStage(
      SnowflakeOutputConnection snowflakeCon, StageIdentifier stageIdentifier, PluginTask task)
      throws SQLException {
    try {
      snowflakeCon.runDropStage(stageIdentifier);
    } catch (RuntimeException ex) {
      if (ex.getMessage().startsWith("Authentication token has expired.")) {
        // INFO: If runCreateStage consumed a lot of time, authentication might be expired.
        //       In this case, retry to drop stage.
        snowflakeCon = (SnowflakeOutputConnection) getConnector(task, true).connect(true);
        snowflakeCon.runDropStage(stageIdentifier);
      }
    }
  }

  @Override
  public ConfigDiff resume(
      TaskSource taskSource, Schema schema, int taskCount, OutputPlugin.Control control) {
    throw new UnsupportedOperationException("snowflake output plugin does not support resuming");
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
    SnowflakePluginTask pluginTask = (SnowflakePluginTask) task;

    return new SnowflakeCopyBatchInsert(
        getConnector(task, true),
        StageIdentifierHolder.getStageIdentifier(pluginTask),
        false,
        pluginTask.getMaxUploadRetries(),
        pluginTask.getEmtpyFieldAsNull());
  }

  @Override
  protected void logConnectionProperties(String url, Properties props) {
    Properties maskedProps = new Properties();
    for (Object keyObj : props.keySet()) {
      String key = (String) keyObj;
      if (key.equals("password")) {
        maskedProps.setProperty(key, "***");
      } else if (key.equals("proxyPassword")) {
        maskedProps.setProperty(key, "***");
      } else if (key.equals("privateKey")) {
        maskedProps.setProperty(key, "***");
      } else {
        maskedProps.setProperty(key, props.getProperty(key));
      }
    }
    logger.info("Connecting to {} options {}", url, maskedProps);
  }

  // TODO This is almost copy from AbstractJdbcOutputPlugin excepting type of JSON -> OBJECT
  //      AbstractJdbcOutputPlugin should have better extensibility.
  @Override
  protected JdbcSchema newJdbcSchemaForNewTable(Schema schema) {
    final ArrayList<JdbcColumn> columns = new ArrayList<>();
    for (Column c : schema.getColumns()) {
      final String columnName = c.getName();
      c.visit(
          new ColumnVisitor() {
            public void booleanColumn(Column column) {
              columns.add(
                  JdbcColumn.newGenericTypeColumn(
                      columnName, Types.BOOLEAN, "BOOLEAN", 1, 0, false, false));
            }

            public void longColumn(Column column) {
              columns.add(
                  JdbcColumn.newGenericTypeColumn(
                      columnName, Types.BIGINT, "BIGINT", 22, 0, false, false));
            }

            public void doubleColumn(Column column) {
              columns.add(
                  JdbcColumn.newGenericTypeColumn(
                      columnName, Types.FLOAT, "DOUBLE PRECISION", 24, 0, false, false));
            }

            public void stringColumn(Column column) {
              columns.add(
                  JdbcColumn.newGenericTypeColumn(
                      columnName,
                      Types.CLOB,
                      "CLOB",
                      4000,
                      0,
                      false,
                      false)); // TODO size type param
            }

            public void jsonColumn(Column column) {
              columns.add(
                  JdbcColumn.newGenericTypeColumn(
                      columnName,
                      Types.OTHER,
                      "VARIANT",
                      4000,
                      0,
                      false,
                      false)); // TODO size type param
            }

            public void timestampColumn(Column column) {
              columns.add(
                  JdbcColumn.newGenericTypeColumn(
                      columnName,
                      Types.TIMESTAMP,
                      "TIMESTAMP",
                      26,
                      0,
                      false,
                      false)); // size type param is from postgresql
            }
          });
    }
    return new JdbcSchema(Collections.unmodifiableList(columns));
  }
}
