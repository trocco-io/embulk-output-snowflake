package org.embulk.output.jdbc.setter;

import org.embulk.output.jdbc.BatchInsert;
import org.embulk.output.jdbc.JdbcColumn;
import org.msgpack.value.Value;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Calendar;

public class SqlDateColumnSetter
        extends ColumnSetter
{
    protected final Calendar calendar;

    public SqlDateColumnSetter(BatchInsert batch, JdbcColumn column,
            DefaultValueSetter defaultValue,
            Calendar calendar)
    {
        super(batch, column, defaultValue);
        this.calendar = calendar;
    }

    @Override
    public void nullValue() throws IOException, SQLException
    {
        defaultValue.setSqlDate();
    }

    @Override
    public void booleanValue(boolean v) throws IOException, SQLException
    {
        defaultValue.setSqlDate();
    }

    @Override
    public void longValue(long v) throws IOException, SQLException
    {
        defaultValue.setSqlDate();
    }

    @Override
    public void doubleValue(double v) throws IOException, SQLException
    {
        defaultValue.setSqlDate();
    }

    @Override
    public void stringValue(String v) throws IOException, SQLException
    {
        defaultValue.setSqlDate();
    }

    @Override
    public void timestampValue(final Instant v) throws IOException, SQLException
    {
        batch.setSqlDate(v, calendar);
    }

    @Override
    public void jsonValue(Value v) throws IOException, SQLException
    {
        defaultValue.setSqlDate();
    }
}