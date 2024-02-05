package org.embulk.output.snowflake;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import org.embulk.output.jdbc.JdbcColumn;
import org.embulk.output.jdbc.JdbcOutputConnector;
import org.embulk.output.jdbc.JdbcSchema;
import org.embulk.output.jdbc.TableIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnowflakeMatchByColumnNameCopyBatchInsert extends SnowflakeCopyBatchInsert {
  private final Logger logger =
      LoggerFactory.getLogger(SnowflakeMatchByColumnNameCopyBatchInsert.class);

  private final JdbcSchema existingJdbcSchema;

  private final List<Integer> indexes = new ArrayList<>();

  private final List<CheckedRunnable<IOException>> setColumnRunnables = new ArrayList<>();

  public SnowflakeMatchByColumnNameCopyBatchInsert(
      JdbcOutputConnector connector,
      StageIdentifier stageIdentifier,
      boolean deleteStageFile,
      int maxUploadRetries,
      boolean emptyFieldAsNull,
      JdbcSchema existingJdbcSchema)
      throws IOException {
    super(connector, stageIdentifier, deleteStageFile, maxUploadRetries, emptyFieldAsNull);
    this.existingJdbcSchema = existingJdbcSchema;
  }

  @Override
  public void prepare(TableIdentifier loadTable, JdbcSchema insertSchema) throws SQLException {
    super.prepare(loadTable, insertSchema);
    prepareIndexes(insertSchema);
  }

  private void prepareIndexes(JdbcSchema insertSchema) {
    for (int i = 0; i < existingJdbcSchema.getCount(); i++) {
      JdbcColumn existingColumn = existingJdbcSchema.getColumn(i);
      String existingColumnName = existingColumn.getName();
      for (int j = 0; j < insertSchema.getCount(); j++) {
        JdbcColumn insertColumn = insertSchema.getColumn(j);
        String insertColumnName = insertColumn.getName();
        if (insertColumnName.equalsIgnoreCase(existingColumnName)) {
          indexes.add(j);
          break;
        }
      }
    }
    if (indexes.size() != insertSchema.getCount()) {
      throw new UnsupportedOperationException(
          "Input column names does not match output column names.");
    }
  }

  @Override
  public void add() throws IOException {
    indexes.forEach(i -> setColumnRunnables.get(i).run());
    setColumnRunnables.clear();
    super.add();
  }

  @Override
  public void setNull(int sqlType) throws IOException {
    setColumnRunnables.add(() -> super.setNull(sqlType));
  }

  @Override
  public void setBoolean(boolean v) throws IOException {
    setColumnRunnables.add(() -> super.setBoolean(v));
  }

  @Override
  public void setByte(byte v) throws IOException {
    setColumnRunnables.add(() -> super.setByte(v));
  }

  @Override
  public void setShort(short v) throws IOException {
    setColumnRunnables.add(() -> super.setShort(v));
  }

  @Override
  public void setInt(int v) throws IOException {
    setColumnRunnables.add(() -> super.setInt(v));
  }

  @Override
  public void setLong(long v) throws IOException {
    setColumnRunnables.add(() -> super.setLong(v));
  }

  @Override
  public void setFloat(float v) throws IOException {
    setColumnRunnables.add(() -> super.setFloat(v));
  }

  @Override
  public void setDouble(double v) throws IOException {
    setColumnRunnables.add(() -> super.setDouble(v));
  }

  @Override
  public void setBigDecimal(BigDecimal v) throws IOException {
    setColumnRunnables.add(() -> super.setBigDecimal(v));
  }

  @Override
  public void setString(String v) throws IOException {
    setColumnRunnables.add(() -> super.setString(v));
  }

  @Override
  public void setNString(String v) throws IOException {
    setColumnRunnables.add(() -> super.setNString(v));
  }

  @Override
  public void setBytes(byte[] v) throws IOException {
    setColumnRunnables.add(() -> super.setBytes(v));
  }

  @Override
  public void setSqlDate(Instant v, Calendar cal) throws IOException {
    setColumnRunnables.add(() -> super.setSqlDate(v, cal));
  }

  @Override
  public void setSqlTime(Instant v, Calendar cal) throws IOException {
    setColumnRunnables.add(() -> super.setSqlTime(v, cal));
  }

  @Override
  public void setSqlTimestamp(Instant v, Calendar cal) throws IOException {
    setColumnRunnables.add(() -> super.setSqlTimestamp(v, cal));
  }

  @FunctionalInterface
  private interface CheckedRunnable<E extends Exception> extends Runnable {

    @Override
    default void run() throws RuntimeException {
      try {
        runThrows();
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }

    void runThrows() throws E;
  }
}
