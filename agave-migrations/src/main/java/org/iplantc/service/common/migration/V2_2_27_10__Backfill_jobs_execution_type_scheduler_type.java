/**
 *
 */
package org.iplantc.service.common.migration;

import org.flywaydb.core.api.FlywayException;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.*;

/**
 * Populates the schedulerType and executionType columns in the jobs table with the values currently
 * defined for the software and execution system associated with the job.
 *
 * @author dooley
 */
public class V2_2_27_10__Backfill_jobs_execution_type_scheduler_type extends BaseJavaMigration {

    @Override
    public Integer getChecksum() {
        MessageDigest m;
        try {
            m = MessageDigest.getInstance("SHA1");
            m.update(getClass().getSimpleName().getBytes());

            return new BigInteger(1, m.digest()).intValue();
        } catch (NoSuchAlgorithmException e) {
            throw new FlywayException("Unable to determine checksum for migration class");
        }
    }

    @Override
    public void migrate(Context context) throws Exception {
        context.getConnection().setAutoCommit(false);

        System.out.println("Starting to backfill jobs table.");
        backfillSchedulerTypeAndExecutionType(context.getConnection());
        System.out.println("Finished backfilling jobs table.");
    }

    /**
     * Populates the execution_type column in the jobs table.
     *
     * @param connection the current database connectino passed in by Flyway
     * @throws SQLException if things go horribly wrong
     */
    private void backfillSchedulerTypeAndExecutionType(Connection connection) throws SQLException {

        // This is our job update query. We will update all job records for a given software id in a single query.
        // This query will be updated in batch as we finish processing a batch of software records.
        String jobSql;
        jobSql = "UPDATE jobs set execution_type = ?, scheduler_type = ? where tenant_id = ? and software_name = ?";

        int fetchSize = 100;
        int i = 0;
        try (PreparedStatement updateStmt = connection.prepareStatement(jobSql); Statement stmt = connection.createStatement()) {

            // Count the number of software records so we can monitor progress.
            ResultSet rs = stmt.executeQuery("SELECT count(id) FROM softwares");
            rs.next();
            long softwareTableSize = rs.getLong(1);

            // Query all the software records, batching in groups of {@code fetchSize}
            String sql = "SELECT s.tenant_id, s.name, s.version, s.publicly_available, s.revision_count, s.execution_type, e.scheduler_type FROM softwares s left join executionsystems e on s.system_id = e.id";
            stmt.setFetchSize(fetchSize);
            ResultSet softwareResultSet = stmt.executeQuery(sql);

            // Iterate over the result set, creating job update records for each software record and running them
            // when the batch of software results is complete.
            while (softwareResultSet.next()) {
                i++;
                String tenantId = softwareResultSet.getString(1);
                String name = softwareResultSet.getString(2);
                String version = softwareResultSet.getString(3);
                boolean publiclyAvailable = softwareResultSet.getBoolean(4);
                int revisionCount = softwareResultSet.getInt(5);
                String executionType = softwareResultSet.getString(6);
                String schedulerType = softwareResultSet.getString(7);
                if (executionType.equals("CLI")) {
                    schedulerType = "FORK";
                }
                // Here we preserve the same construct used to generate the sofware id in our domain classes.
                String softwareUniqueName = String.format("%s-%s%s", name, version, publiclyAvailable ? "u" + revisionCount : "");

                // populate the query, observing tenancy since software unique names are only so within a tenant.
                updateStmt.setString(1, executionType);
                updateStmt.setString(2, schedulerType);
                updateStmt.setString(3, tenantId);
                updateStmt.setString(4, softwareUniqueName);
                updateStmt.addBatch();

                // If we are at the last record in the currently fetched result set, then execute all the statements.
                if (i % fetchSize == 0) {
                    updateStmt.executeBatch();
                    // progress report just so we have it
                    System.out.println(String.format("[%d/%d] Migrating jobs table...", i, softwareTableSize));
                }
            }
            // Execute any remaining queries in case the number of software records was not a multiple of the
            // fetch size.
            updateStmt.executeBatch();
        }
    }
}
