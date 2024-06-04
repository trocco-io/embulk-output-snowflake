package org.embulk.output.snowflake;

import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Optional;

import net.snowflake.client.jdbc.SnowflakeConnection;
import org.embulk.output.jdbc.JdbcColumn;
import org.embulk.output.jdbc.JdbcOutputConnection;
import org.embulk.output.jdbc.JdbcSchema;
import org.embulk.output.jdbc.MergeConfig;
import org.embulk.output.jdbc.TableIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnowflakeOutputConnection extends JdbcOutputConnection {
  private final Logger logger = LoggerFactory.getLogger(SnowflakeOutputConnection.class);

  public SnowflakeOutputConnection(Connection connection) throws SQLException {
    super(connection, null);
  }

  public void runCopy(
      TableIdentifier tableIdentifier,
      StageIdentifier stageIdentifier,
      String filename,
      String[] tableColumnNames,
      int[] csvColumnNumbers,
      String delimiterString,
      boolean emptyFieldAsNull)
      throws SQLException {
    String sql =
        tableColumnNames != null && tableColumnNames.length > 0
            ? buildCopySQL(
                tableIdentifier,
                stageIdentifier,
                filename,
                tableColumnNames,
                csvColumnNumbers,
                delimiterString,
                emptyFieldAsNull)
            : buildCopySQL(
                tableIdentifier, stageIdentifier, filename, delimiterString, emptyFieldAsNull);

    runUpdate(sql);
  }

  public void runCreateStage(StageIdentifier stageIdentifier) throws SQLException {
    String sql = buildCreateStageSQL(stageIdentifier);
    runUpdate(sql);
    logger.info("SQL: {}", sql);
  }

  public void runDropStage(StageIdentifier stageIdentifier) throws SQLException {
    String sql = buildDropStageSQL(stageIdentifier);
    runUpdate(sql);
    logger.info("SQL: {}", sql);
  }

  public void runUploadFile(
      StageIdentifier stageIdentifier, String filename, FileInputStream fileInputStream)
      throws SQLException {
    connection
        .unwrap(SnowflakeConnection.class)
        .uploadStream(
            stageIdentifier.getStageName(),
            stageIdentifier.getDestPrefix().orElse("/"),
            fileInputStream,
            filename + ".csv.gz",
            false);
  }

  public void runDeleteStageFile(StageIdentifier stageIdentifier, String filename)
      throws SQLException {
    String sql = buildDeleteStageFileSQL(stageIdentifier, filename);
    runUpdate(sql);
  }

  protected void runUpdate(String sql) throws SQLException {
    Statement stmt = connection.createStatement();
    try {
      stmt.executeUpdate(sql);
    } finally {
      stmt.close();
    }
  }

  @Override
  protected String buildColumnTypeName(JdbcColumn c) {
    switch (c.getSimpleTypeName()) {
      case "CLOB":
        return "VARCHAR(65535)";
      case "NUMBER":
        // ref. https://docs.snowflake.com/en/sql-reference/data-types-numeric.html
        return String.format("NUMBER(%d,%d)", c.getSizeTypeParameter(), c.getScaleTypeParameter());
      default:
        return super.buildColumnTypeName(c);
    }
  }

  @Override
  protected String buildCollectMergeSql(
      List<TableIdentifier> fromTables,
      JdbcSchema schema,
      TableIdentifier toTable,
      MergeConfig mergeConfig)
      throws SQLException {
    StringBuilder sb = new StringBuilder();

    sb.append("MERGE INTO ");
    sb.append(quoteTableIdentifier(toTable));
    sb.append(" AS T ");
    sb.append(" USING (");
    for (int i = 0; i < fromTables.size(); i++) {
      if (i != 0) {
        sb.append(" UNION ALL ");
      }
      sb.append(" SELECT ");
      sb.append(buildColumns(schema, ""));
      sb.append(" FROM ");
      sb.append(quoteTableIdentifier(fromTables.get(i)));
    }
    sb.append(") AS S");
    sb.append(" ON (");
    for (int i = 0; i < mergeConfig.getMergeKeys().size(); i++) {
      if (i != 0) {
        sb.append(" AND ");
      }
      String mergeKey = quoteIdentifierString(mergeConfig.getMergeKeys().get(i));
      sb.append("T.");
      sb.append(mergeKey);
      sb.append(" = S.");
      sb.append(mergeKey);
    }
    sb.append(")");
    sb.append(" WHEN MATCHED THEN");
    sb.append(" UPDATE SET ");
    if (mergeConfig.getMergeRule().isPresent()) {
      for (int i = 0; i < mergeConfig.getMergeRule().get().size(); i++) {
        if (i != 0) {
          sb.append(", ");
        }
        sb.append(mergeConfig.getMergeRule().get().get(i));
      }
    } else {
      for (int i = 0; i < schema.getCount(); i++) {
        if (i != 0) {
          sb.append(", ");
        }
        String column = quoteIdentifierString(schema.getColumnName(i));
        sb.append(column);
        sb.append(" = S.");
        sb.append(column);
      }
    }
    sb.append(" WHEN NOT MATCHED THEN");
    sb.append(" INSERT (");
    sb.append(buildColumns(schema, ""));
    sb.append(") VALUES (");
    sb.append(buildColumns(schema, "S."));
    sb.append(");");

    return sb.toString();
  }

  protected String buildCreateStageSQL(StageIdentifier stageIdentifier) {
    StringBuilder sb = new StringBuilder();
    sb.append("CREATE STAGE IF NOT EXISTS ");
    quoteStageIdentifier(sb, stageIdentifier);
    sb.append(";");
    return sb.toString();
  }

  protected String buildDropStageSQL(StageIdentifier stageIdentifier) {
    StringBuilder sb = new StringBuilder();
    sb.append("DROP STAGE IF EXISTS ");
    quoteStageIdentifier(sb, stageIdentifier);
    sb.append(";");
    return sb.toString();
  }

  protected void quoteStageIdentifier(StringBuilder sb, StageIdentifier stageIdentifier) {
    sb.append(stageIdentifier.getDatabase());
    sb.append(".");
    sb.append(stageIdentifier.getSchemaName());
    sb.append(".");
    sb.append(stageIdentifier.getStageName());
  }

  protected String buildCopySQL(
      TableIdentifier tableIdentifier,
      StageIdentifier stageIdentifier,
      String snowflakeStageFileName,
      String delimiterString,
      boolean emptyFieldAsNull) {
    StringBuilder sb = new StringBuilder();
    sb.append("COPY INTO ");
    quoteTableIdentifier(sb, tableIdentifier);
    sb.append(" FROM ");
    quoteInternalStoragePath(sb, stageIdentifier, snowflakeStageFileName);
    sb.append(" FILE_FORMAT = ( TYPE = CSV FIELD_DELIMITER = '");
    sb.append(delimiterString);
    sb.append("'");
    if (!emptyFieldAsNull) {
      sb.append(" EMPTY_FIELD_AS_NULL = FALSE");
    }
    sb.append(" );");
    return sb.toString();
  }

  protected String buildCopySQL(
      TableIdentifier tableIdentifier,
      StageIdentifier stageIdentifier,
      String snowflakeStageFileName,
      String[] tableColumnNames,
      int[] csvColumnNumbers,
      String delimiterString,
      boolean emptyFieldAsNull) {
    // Data load with transformation
    // Correspondence between CSV column numbers and table column names can be specified.
    // https://docs.snowflake.com/ja/sql-reference/sql/copy-into-table

    StringBuilder sb = new StringBuilder();
    sb.append("COPY INTO ");
    quoteTableIdentifier(sb, tableIdentifier);
    sb.append(" (");
    for (int i = 0; i < tableColumnNames.length; i++) {
      if (i != 0) {
        sb.append(", ");
      }
      String column = quoteIdentifierString(tableColumnNames[i]);
      sb.append(column);
    }
    sb.append(" ) FROM ( SELECT ");
    for (int i = 0; i < csvColumnNumbers.length; i++) {
      if (i != 0) {
        sb.append(", ");
      }
      sb.append("t.$");
      sb.append(csvColumnNumbers[i]);
    }
    sb.append(" from ");
    quoteInternalStoragePath(sb, stageIdentifier, snowflakeStageFileName);
    sb.append(" t ) ");
    sb.append(" FILE_FORMAT = ( TYPE = CSV FIELD_DELIMITER = '");
    sb.append(delimiterString);
    sb.append("'");
    if (!emptyFieldAsNull) {
      sb.append(" EMPTY_FIELD_AS_NULL = FALSE");
    }
    sb.append(" );");
    return sb.toString();
  }

  protected String buildDeleteStageFileSQL(
      StageIdentifier stageIdentifier, String snowflakeStageFileName) {
    StringBuilder sb = new StringBuilder();
    sb.append("REMOVE ");
    quoteInternalStoragePath(sb, stageIdentifier, snowflakeStageFileName);
    sb.append(';');
    return sb.toString();
  }

  protected String quoteInternalStoragePath(
      StringBuilder sb, StageIdentifier stageIdentifier, String snowflakeStageFileName) {
    sb.append("@");
    quoteStageIdentifier(sb, stageIdentifier);
    if (stageIdentifier.getDestPrefix().isPresent()) {
      sb.append("/");
      sb.append(stageIdentifier.getDestPrefix().get());
    }
    sb.append("/");
    sb.append(snowflakeStageFileName);
    sb.append(".csv.gz");
    return sb.toString();
  }

  private String buildColumns(JdbcSchema schema, String prefix) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < schema.getCount(); i++) {
      if (i != 0) {
        sb.append(", ");
      }
      sb.append(prefix);
      sb.append(quoteIdentifierString(schema.getColumnName(i)));
    }
    return sb.toString();
  }

  @Override
  protected String buildCreateTableIfNotExistsSql(TableIdentifier table, JdbcSchema schema, Optional<String> tableConstraint, Optional<String> tableOption) {
    StringBuilder sb = new StringBuilder();
    sb.append("CREATE TRANSIENT TABLE IF NOT EXISTS ");
    this.quoteTableIdentifier(sb, table);
    sb.append(this.buildCreateTableSchemaSql(schema, tableConstraint));
    if (tableOption.isPresent()) {
      sb.append(" ");
      sb.append((String)tableOption.get());
    }

    return sb.toString();
  }

  @Override
  protected String buildCreateTableSql(TableIdentifier table, JdbcSchema schema, Optional<String> tableConstraint, Optional<String> tableOption) {
    StringBuilder sb = new StringBuilder();
    sb.append("CREATE TRANSIENT TABLE ");
    this.quoteTableIdentifier(sb, table);
    sb.append(this.buildCreateTableSchemaSql(schema, tableConstraint));
    if (tableOption.isPresent()) {
      sb.append(" ");
      sb.append((String)tableOption.get());
    }

    return sb.toString();
  }
}
