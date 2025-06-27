package org.embulk.output;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Types;
import java.util.*;
import java.util.function.BiFunction;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.jdbc.internal.org.bouncycastle.operator.OperatorCreationException;
import net.snowflake.client.jdbc.internal.org.bouncycastle.pkcs.PKCSException;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskSource;
import org.embulk.output.jdbc.*;
import org.embulk.output.s3.JdbcLogUploader;
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

    @Config("max_copy_retries")
    @ConfigDefault("3")
    public int getMaxCopyRetries();

    @Config("empty_field_as_null")
    @ConfigDefault("true")
    public boolean getEmtpyFieldAsNull();

    @Config("delete_stage_on_error")
    @ConfigDefault("false")
    public boolean getDeleteStageOnError();

    @Config("match_by_column_name")
    @ConfigDefault("\"none\"")
    public MatchByColumnName getMatchByColumnName();

    @Config("upload_jdbc_log_to_s3")
    @ConfigDefault("false")
    public boolean getUploadJdbcLogToS3();

    @Config("s3_bucket")
    @ConfigDefault("null")
    public Optional<String> getS3Bucket();

    @Config("s3_prefix")
    @ConfigDefault("null")
    public Optional<String> getS3Prefix();

    @Config("s3_region")
    @ConfigDefault("null")
    public Optional<String> getS3Region();

    @Config("s3_access_key_id")
    @ConfigDefault("null")
    public Optional<String> getS3AccessKeyId();

    @Config("s3_secret_access_key")
    @ConfigDefault("null")
    public Optional<String> getS3SecretAccessKey();

    public void setCopyIntoTableColumnNames(String[] columnNames);

    public String[] getCopyIntoTableColumnNames();

    public void setCopyIntoCSVColumnNumbers(int[] columnNumbers);

    public int[] getCopyIntoCSVColumnNumbers();

    public enum MatchByColumnName {
      CASE_SENSITIVE,
      CASE_INSENSITIVE,
      NONE;

      @JsonValue
      @Override
      public String toString() {
        return name().toLowerCase(Locale.ENGLISH);
      }

      @JsonCreator
      public static MatchByColumnName fromString(String value) {
        switch (value) {
          case "case_sensitive":
            return CASE_SENSITIVE;
          case "case_insensitive":
            return CASE_INSENSITIVE;
          case "none":
            return NONE;
          default:
            throw new ConfigException(
                String.format(
                    "Unknown match_by_column_name '%s'. Supported values are case_sensitive, case_insensitive, none",
                    value));
        }
      }
    }
  }

  // error codes which need reauthenticate
  // ref:
  // https://github.com/snowflakedb/snowflake-jdbc/blob/v3.13.26/src/main/java/net/snowflake/client/jdbc/SnowflakeUtil.java#L42
  private static final int ID_TOKEN_EXPIRED_GS_CODE = 390110;
  private static final int SESSION_NOT_EXIST_GS_CODE = 390111;
  private static final int MASTER_TOKEN_NOTFOUND = 390113;
  private static final int MASTER_EXPIRED_GS_CODE = 390114;
  private static final int MASTER_TOKEN_INVALID_GS_CODE = 390115;
  private static final int ID_TOKEN_INVALID_LOGIN_REQUEST_GS_CODE = 390195;

  private static final String ENCOUNTERED_COMMUNICATION_ERROR_MESSAGE =
      "JDBC driver encountered communication error";

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

    if (t.getUploadJdbcLogToS3()) {
      props.setProperty("tracing", "ALL");
    }

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
      if (t.getDeleteStage()) {
        runDropStageWithRecovery(snowflakeCon, stageIdentifier, task);
      }
    } catch (Exception e) {
      if (e instanceof SQLException) {
        String message = e.getMessage();
        if (message != null
            && message.contains(ENCOUNTERED_COMMUNICATION_ERROR_MESSAGE)
            && t.getUploadJdbcLogToS3() == true) {
          final Optional<String> s3Bucket = t.getS3Bucket();
          final Optional<String> s3Prefix = t.getS3Prefix();
          final Optional<String> s3Region = t.getS3Region();
          final Optional<String> s3AccessKeyId = t.getS3AccessKeyId();
          final Optional<String> s3SecretAccessKey = t.getS3SecretAccessKey();
          if (!s3Bucket.isPresent() || !s3Prefix.isPresent() || !s3Region.isPresent()) {
            logger.warn("s3_bucket, s3_prefix, and s3_region must be set when upload_jdbc_log_to_s3 is true");
          } else {
            try (JdbcLogUploader jdbcLogUploader = new JdbcLogUploader(s3Bucket.get(), s3Prefix.get(), s3Region.get(), s3AccessKeyId.orElse(null), s3SecretAccessKey.orElse(null))) {
              // snowflake_jdbc*.log で最新のファイルを探してアップロード
              String tmpDir = System.getProperty("java.io.tmpdir", "/tmp");
              File logDir = new File(tmpDir);
              File[] logFiles =
                  logDir.listFiles(
                      (dir, name) -> name.startsWith("snowflake_jdbc") && name.endsWith(".log"));
              if (logFiles != null && logFiles.length > 0) {
                // 最終更新日時が新しいファイルを選択
                Optional<File> latest =
                    Arrays.stream(logFiles).max(Comparator.comparingLong(File::lastModified));
                if (latest.isPresent()) {
                  jdbcLogUploader.uploadIfExists(latest.get());
                }
              } else {
                logger.warn("No snowflake_jdbc*.log file found in {} for upload", tmpDir);
              }
            } catch (Exception uploadException) {
              logger.warn("Failed to upload JDBC log to S3: {}", uploadException.getMessage());
            }
          }
        }
      }
      if (t.getDeleteStage() && t.getDeleteStageOnError()) {
        try {
          runDropStageWithRecovery(snowflakeCon, stageIdentifier, task);
        } catch (SQLException ex) {
          throw new RuntimeException(ex);
        }
      }
      throw new RuntimeException(e);
    }

    return configDiff;
  }

  private void runDropStageWithRecovery(
      SnowflakeOutputConnection snowflakeCon, StageIdentifier stageIdentifier, PluginTask task)
      throws SQLException {
    try {
      snowflakeCon.runDropStage(stageIdentifier);
    } catch (SnowflakeSQLException ex) {
      // INFO: Don't handle only SnowflakeReauthenticationRequest here
      //       because SnowflakeSQLException with following error codes may be thrown in some cases.

      logger.info("SnowflakeSQLException was caught: ({}) {}", ex.getErrorCode(), ex.getMessage());

      switch (ex.getErrorCode()) {
        case ID_TOKEN_EXPIRED_GS_CODE:
        case SESSION_NOT_EXIST_GS_CODE:
        case MASTER_TOKEN_NOTFOUND:
        case MASTER_EXPIRED_GS_CODE:
        case MASTER_TOKEN_INVALID_GS_CODE:
        case ID_TOKEN_INVALID_LOGIN_REQUEST_GS_CODE:
          // INFO: If runCreateStage consumed a lot of time, authentication might be expired.
          //       In this case, retry to drop stage.
          snowflakeCon = (SnowflakeOutputConnection) getConnector(task, true).connect(true);
          snowflakeCon.runDropStage(stageIdentifier);
          break;
        default:
          throw ex;
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

    SnowflakePluginTask pluginTask = (SnowflakePluginTask) task;
    SnowflakePluginTask.MatchByColumnName matchByColumnName = pluginTask.getMatchByColumnName();
    if (matchByColumnName == SnowflakePluginTask.MatchByColumnName.NONE) {
      pluginTask.setCopyIntoCSVColumnNumbers(new int[0]);
      pluginTask.setCopyIntoTableColumnNames(new String[0]);
      return;
    }

    List<String> copyIntoTableColumnNames = new ArrayList<>();
    List<Integer> copyIntoCSVColumnNumbers = new ArrayList<>();
    JdbcSchema targetTableSchema = pluginTask.getTargetTableSchema();
    BiFunction<String, String, Boolean> compare =
        matchByColumnName == SnowflakePluginTask.MatchByColumnName.CASE_SENSITIVE
            ? String::equals
            : String::equalsIgnoreCase;
    int columnNumber = 1;
    for (int i = 0; i < targetTableSchema.getCount(); i++) {
      JdbcColumn targetColumn = targetTableSchema.getColumn(i);
      if (targetColumn.isSkipColumn()) {
        continue;
      }
      Column schemaColumn = schema.getColumn(i);
      if (compare.apply(schemaColumn.getName(), targetColumn.getName())) {
        copyIntoTableColumnNames.add(targetColumn.getName());
        copyIntoCSVColumnNumbers.add(columnNumber);
      }
      columnNumber += 1;
    }
    pluginTask.setCopyIntoTableColumnNames(copyIntoTableColumnNames.toArray(new String[0]));
    pluginTask.setCopyIntoCSVColumnNumbers(
        copyIntoCSVColumnNumbers.stream().mapToInt(i -> i).toArray());
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
        pluginTask.getCopyIntoTableColumnNames(),
        pluginTask.getCopyIntoCSVColumnNumbers(),
        false,
        pluginTask.getMaxUploadRetries(),
        pluginTask.getMaxCopyRetries(),
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
