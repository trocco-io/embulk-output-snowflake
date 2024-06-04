package org.embulk.output.jdbc.setter;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;
import org.embulk.output.jdbc.BatchInsert;
import org.embulk.output.jdbc.JdbcColumn;
import org.msgpack.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NullColumnSetter extends ColumnSetter {
  protected static final Logger logger = LoggerFactory.getLogger(NullColumnSetter.class);

  public NullColumnSetter(BatchInsert batch, JdbcColumn column, DefaultValueSetter defaultValue) {
    super(batch, column, defaultValue);
  }

  @Override
  public void booleanValue(boolean v) throws IOException, SQLException {
    defaultValue.setNull();
  }

  @Override
  public void longValue(long v) throws IOException, SQLException {
    logger.info("NullColumnSetter: setNull({})", v);
    defaultValue.setNull();
  }

  @Override
  public void doubleValue(double v) throws IOException, SQLException {
    defaultValue.setNull();
  }

  @Override
  public void stringValue(String v) throws IOException, SQLException {
    defaultValue.setNull();
  }

  @Override
  public void timestampValue(final Instant v) throws IOException, SQLException {
    defaultValue.setNull();
  }

  @Override
  public void nullValue() throws IOException, SQLException {
    defaultValue.setNull();
  }

  @Override
  public void jsonValue(Value v) throws IOException, SQLException {
    defaultValue.setNull();
  }
}
