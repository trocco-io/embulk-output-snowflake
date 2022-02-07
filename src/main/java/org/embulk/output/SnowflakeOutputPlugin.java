package org.embulk.output;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Properties;
import org.embulk.config.ConfigDiff;
import org.embulk.config.TaskSource;
import org.embulk.output.jdbc.AbstractJdbcOutputPlugin;
import org.embulk.output.jdbc.BatchInsert;
import org.embulk.output.jdbc.JdbcOutputConnection;
import org.embulk.output.jdbc.JdbcOutputConnector;
import org.embulk.output.jdbc.MergeConfig;
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

    props.setProperty("CLIENT_METADATA_REQUEST_USE_CONNECTION_CTX", "true");

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
}
