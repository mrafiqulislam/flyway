package org.flywaydb.core.internal.database.clickhouse;

import org.flywaydb.core.internal.database.base.Table;
import org.flywaydb.core.internal.jdbc.JdbcTemplate;

import java.sql.SQLException;

/**
 * ClickHouse-specific table.
 */
public class ClickHouseTable extends Table<ClickHouseDatabase, ClickHouseSchema> {

    /**
     * Creates a new table.
     *
     * @param jdbcTemplate The Jdbc Template for communicating with the DB.
     * @param database     The database-specific support.
     * @param schema       The schema this table lives in.
     * @param name         The name of the table.
     */
    public ClickHouseTable(JdbcTemplate jdbcTemplate, ClickHouseDatabase database, ClickHouseSchema schema, String name) {
        super(jdbcTemplate, database, schema, name);
    }

    @Override
    protected void doDrop() throws SQLException {
        jdbcTemplate.executeStatement("DROP TABLE " + database.quote(schema.getName(), name));
    }

    @Override
    protected boolean doExists() throws SQLException {
        int count = jdbcTemplate.queryForInt(
                "SELECT COUNT() FROM system.tables WHERE database = ? AND name = ?",
                schema.getName(), name);
        return count > 0;
    }

    @Override
    protected void doLock() throws SQLException {
        // No support for locking
    }
}
