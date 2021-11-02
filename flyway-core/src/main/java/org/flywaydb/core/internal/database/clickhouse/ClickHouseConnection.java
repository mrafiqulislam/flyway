package org.flywaydb.core.internal.database.clickhouse;

import lombok.CustomLog;
import org.flywaydb.core.internal.database.base.Connection;

import java.sql.SQLException;

@CustomLog
public class ClickHouseConnection extends Connection<ClickHouseDatabase> {

    public ClickHouseConnection(ClickHouseDatabase database, java.sql.Connection connection) {
        super(database, connection);
    }

    @Override
    protected String getCurrentSchemaNameOrSearchPath() throws SQLException {
        return getJdbcTemplate().getConnection().getCatalog();
    }

    @Override
    public void doChangeCurrentSchemaOrSearchPathTo(String schema) throws SQLException {
        getJdbcTemplate().getConnection().setCatalog(schema);
    }

    @Override
    public ClickHouseSchema getSchema(String name) {
        return new ClickHouseSchema(jdbcTemplate, database, name);
    }
}
