package org.embulk.output.snowflake;

import org.embulk.output.jdbc.JdbcOutputConnection;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class SnowflakeOutputConnection extends JdbcOutputConnection {
    public SnowflakeOutputConnection(Connection connection)
            throws SQLException
    {
        super(connection, null);
    }

    public Connection getConnection() {
        return connection;
    }

    public void runCopy(String sql) throws SQLException
    {
        Statement stmt = connection.createStatement();
        try {
            stmt.executeUpdate(sql);
        } finally {
            stmt.close();
        }
    }

    // TODO: merge with runCopy
    public void runUpdate(String sql) throws SQLException
    {
        Statement stmt = connection.createStatement();
        try {
            stmt.executeUpdate(sql);
        } finally {
            stmt.close();
        }
    }
}
