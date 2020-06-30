package org.embulk.output.snowflake;

import com.google.common.base.Optional;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.output.jdbc.AbstractJdbcOutputPlugin;

public interface SnowflakePluginTask extends AbstractJdbcOutputPlugin.PluginTask {
    @Config("driver_path")
    @ConfigDefault("null")
    public Optional<String> getDriverPath();


    @Config("host")
    public String getHost();

    @Config("port")
    @ConfigDefault("443")
    public int getPort();

    @Config("user")
    public String getUser();

    @Config("password")
    @ConfigDefault("\"\"")
    public String getPassword();

    @Config("database")
    public String getDatabase();

    @Config("schema")
    @ConfigDefault("\"public\"")
    public String getSchema();
}
