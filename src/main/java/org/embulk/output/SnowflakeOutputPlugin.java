package org.embulk.output;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import org.embulk.config.*;
import org.embulk.output.jdbc.*;
import org.embulk.output.snowflake.*;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.Schema;

public class SnowflakeOutputPlugin
        extends AbstractJdbcOutputPlugin
{
    private StageIdentifier stageIdentifier;

    public interface SnowflakePluginTask extends PluginTask {
        @Config("driver_path")
        @ConfigDefault("null")
        public Optional<String> getDriverPath();

        @Config("host")
        public String getHost();

        @Config("user")
        @ConfigDefault("\"KEN\"")
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

        @Config("role")
        @ConfigDefault("\"PUBLIC\"")
        public String getRole();

        @Config("delete_stage")
        @ConfigDefault("false")
        public boolean getDeleteStage();
    }

    @Override
    protected Class<? extends PluginTask> getTaskClass()
    {
        return SnowflakePluginTask.class;
    }

    @Override
    protected Features getFeatures(PluginTask task)
    {
        return new Features()
                .setMaxTableNameLength(127)
                .setSupportedModes(ImmutableSet.of(Mode.INSERT, Mode.INSERT_DIRECT, Mode.TRUNCATE_INSERT, Mode.REPLACE))
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
        props.setProperty("role", t.getRole());

        props.putAll(t.getOptions());

        logConnectionProperties(url, props);

        return new SnowflakeOutputConnector(url, props, t.getTransactionIsolation());
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource,
            Schema schema, int taskCount,
            OutputPlugin.Control control)
    {
        throw new UnsupportedOperationException("snowflake output plugin does not support resuming");
    }

    @Override
    protected void doBegin(JdbcOutputConnection con,
                           PluginTask task, final Schema schema, int taskCount) throws SQLException
    {
        super.doBegin(con,task,schema,taskCount);
    }


    @Override
    protected void doCommit(JdbcOutputConnection con, PluginTask task, int taskCount)
            throws SQLException {
        super.doCommit(con, task, taskCount);
        SnowflakeOutputConnection snowflakeCon = (SnowflakeOutputConnection)con;

        SnowflakePluginTask t = (SnowflakePluginTask) task;
        if (this.stageIdentifier == null) {
            this.stageIdentifier = StageIdentifierHolder.getStageIdentifier(t);
        }

        if (t.getDeleteStage()){
            snowflakeCon.runDropStage(this.stageIdentifier);
        }
    }

    @Override
    protected BatchInsert newBatchInsert(PluginTask task, Optional<MergeConfig> mergeConfig) throws IOException, SQLException
    {
        if (mergeConfig.isPresent()) {
            throw new UnsupportedOperationException("Snowflake output plugin doesn't support 'merge_direct' mode.");
        }

        SnowflakePluginTask t = (SnowflakePluginTask) task;
        // TODO: put some where executes once
        if (this.stageIdentifier == null){
            SnowflakeOutputConnection snowflakeCon = (SnowflakeOutputConnection) getConnector(task, true).connect(true);
            this.stageIdentifier = StageIdentifierHolder.getStageIdentifier(t);
            snowflakeCon.runCreateStage(this.stageIdentifier);
        }

        return new SnowflakeCopyBatchInsert(getConnector(task, true), this.stageIdentifier,false);
    }
}
