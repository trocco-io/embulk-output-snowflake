package org.embulk.output.snowflake;

import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import net.snowflake.client.jdbc.SnowflakeConnection;
import org.embulk.output.jdbc.JdbcColumn;
import org.embulk.output.jdbc.JdbcOutputConnection;
import org.embulk.output.jdbc.TableIdentifier;

public class SnowflakeOutputConnection extends JdbcOutputConnection {
  public SnowflakeOutputConnection(Connection connection) throws SQLException {
    super(connection, null);
  }

  public void runCopy(
      TableIdentifier tableIdentifier,
      StageIdentifier stageIdentifier,
      String filename,
      String delimiterString,
      boolean emptyFieldAsNull)
      throws SQLException {
    String sql = buildCopySQL(tableIdentifier, stageIdentifier, filename, delimiterString, emptyFieldAsNull);
    runUpdate(sql);
  }

  public void runCreateStage(StageIdentifier stageIdentifier) throws SQLException {
    String sql = buildCreateStageSQL(stageIdentifier);
    runUpdate(sql);
  }

  public void runDropStage(StageIdentifier stageIdentifier) throws SQLException {
    String sql = buildDropStageSQL(stageIdentifier);
    runUpdate(sql);
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
    sb.append("') ");
    if (!emptyFieldAsNull){
      sb.append("EMPTY_FIELD_AS_NULL = FALSE");
    }
    sb.append(";");
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
}
