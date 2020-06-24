Embulk::JavaPlugin.register_output(
  "snowflake", "org.embulk.output.snowflake.SnowflakeOutputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
