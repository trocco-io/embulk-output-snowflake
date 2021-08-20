package org.embulk.output.snowflake;

import org.embulk.output.jdbc.AbstractJdbcOutputConnector;
import org.embulk.output.jdbc.JdbcOutputConnection;
import org.embulk.output.jdbc.TransactionIsolation;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Properties;

public class SnowflakeOutputConnector extends AbstractJdbcOutputConnector
{
    private final String url;
    private final Properties properties;

    public SnowflakeOutputConnector(String url, Properties properties,
                                     Optional<TransactionIsolation> transactionIsolation)
    {
        super(transactionIsolation);

        this.url = url;
        this.properties = properties;
    }

    @Override
    protected JdbcOutputConnection connect() throws SQLException
    {
        Connection c = DriverManager.getConnection(url, properties);
        try {
            SnowflakeOutputConnection con = new SnowflakeOutputConnection(c);
            c = null;
            return con;
        } finally {
            if (c != null) {
                c.close();
            }
        }
    }

}
