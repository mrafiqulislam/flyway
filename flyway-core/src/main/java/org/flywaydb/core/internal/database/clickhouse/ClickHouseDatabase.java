package org.flywaydb.core.internal.database.clickhouse;

import lombok.CustomLog;
import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.internal.database.base.Database;
import org.flywaydb.core.internal.database.base.Table;
import org.flywaydb.core.internal.jdbc.JdbcConnectionFactory;
import org.flywaydb.core.internal.jdbc.StatementInterceptor;
import org.flywaydb.core.internal.util.StringUtils;

import java.sql.Connection;
import java.sql.SQLException;

@CustomLog
public class ClickHouseDatabase extends Database<ClickHouseConnection> {

    public ClickHouseDatabase(Configuration configuration, JdbcConnectionFactory jdbcConnectionFactory, StatementInterceptor statementInterceptor) {
        super(configuration, jdbcConnectionFactory, statementInterceptor);
    }

    @Override
    protected ClickHouseConnection doGetConnection(Connection connection) {
        return new ClickHouseConnection(this, connection);
    }

    @Override
    public void ensureSupported() {

    }

    @Override
    public boolean supportsDdlTransactions() {
        return false;
    }

    @Override
    public boolean supportsChangingCurrentSchema() {
        return true;
    }

    @Override
    public String getBooleanTrue() {
        return "1";
    }

    @Override
    public String getBooleanFalse() {
        return "0";
    }

    @Override
    protected String doQuote(String identifier) {
        return identifier;
    }

    @Override
    public boolean catalogIsSchema() {
        return true;
    }

    @Override
    public boolean useSingleConnection() {
        return true;
    }

    @Override
    public String getRawCreateScript(Table table, boolean baseline) {
        String clusterName = configuration.getClickHouseClusterName();
        if (StringUtils.hasText(clusterName)) {
            return "CREATE TABLE " + table + " ON CLUSTER " + clusterName + "(" +
                    "    installed_rank Int32," +
                    "    version Nullable(String)," +
                    "    description String," +
                    "    type String," +
                    "    script String," +
                    "    checksum Nullable(Int32)," +
                    "    installed_by String," +
                    "    installed_on DateTime DEFAULT now()," +
                    "    execution_time Int32," +
                    "    success UInt8," +
                    "    CONSTRAINT success CHECK success in (0,1)" +
                    ")" +
                    " ENGINE = ReplicatedMergeTree(" +
                    "   '/clickhouse/tables/{shard${namespace}}/" + table + "'," +
                    "   '{replica${namespace}}'" +
                    " )" +
                    " PARTITION BY tuple()" +
                    " ORDER BY (installed_rank);" +
                    (baseline ? getBaselineStatement(table) + ";" : "");
        } else {
            return "CREATE TABLE " + table + "(" +
                    "    installed_rank Int32," +
                    "    version Nullable(String)," +
                    "    description String," +
                    "    type String," +
                    "    script String," +
                    "    checksum Nullable(Int32)," +
                    "    installed_by String," +
                    "    installed_on DateTime DEFAULT now()," +
                    "    execution_time Int32," +
                    "    success UInt8," +
                    "    CONSTRAINT success CHECK success in (0,1)" +
                    ")" +
                    " ENGINE = TinyLog;" +
                    (baseline ? getBaselineStatement(table) + ";" : "");
        }
    }

    /**
     * ClickHouse does not support deleting and updating,
     * so, we perform the deletion by copying only those rows that satisfy the condition into the newly created table
     */
    @Override
    protected String getRawDeleteScript(Table table) {
        String backupTableName = quote(table.getSchema().getName(), table.getName() + "_backup");

        return "DROP TABLE IF EXISTS " + backupTableName + ";" +
                "RENAME TABLE " + table + " TO " + backupTableName + ";" +
                getRawCreateScript(table, false) + ";" +
                "INSERT INTO " + table + " SELECT * FROM " + backupTableName + " WHERE success > 0 ORDER BY installed_rank;" +
                "DROP TABLE " + backupTableName + ";";
    }

    @Override
    protected String doGetCurrentUser() throws SQLException {
        return getMainConnection().getJdbcTemplate().queryForString("SELECT currentUser()");
    }
}
