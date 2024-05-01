package org.embulk.output.jdbc.setter;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;
import org.embulk.output.jdbc.BatchInsert;
import org.embulk.output.jdbc.JdbcColumn;
import org.embulk.util.timestamp.TimestampFormatter;
import org.msgpack.value.Value;

public class StringColumnSetter extends ColumnSetter {
  private final TimestampFormatter timestampFormatter;

  public StringColumnSetter(
      BatchInsert batch,
      JdbcColumn column,
      DefaultValueSetter defaultValue,
      TimestampFormatter timestampFormatter) {
    super(batch, column, defaultValue);
    this.timestampFormatter = timestampFormatter;
  }

  @Override
  public void nullValue() throws IOException, SQLException {
    defaultValue.setString();
  }

  @Override
  public void booleanValue(boolean v) throws IOException, SQLException {
    batch.setString(Boolean.toString(v));
  }

  @Override
  public void longValue(long v) throws IOException, SQLException {
    batch.setString(Long.toString(v));
  }

  @Override
  public void doubleValue(double v) throws IOException, SQLException {
    batch.setString(Double.toString(v));
  }

  @Override
  public void stringValue(String v) throws IOException, SQLException {
    batch.setString(v);
  }

  @Override
  public void timestampValue(final Instant v) throws IOException, SQLException {
    batch.setString(timestampFormatter.format(v));
  }

  @Override
  public void jsonValue(Value v) throws IOException, SQLException {
    batch.setString(v.toJson());
  }
}
