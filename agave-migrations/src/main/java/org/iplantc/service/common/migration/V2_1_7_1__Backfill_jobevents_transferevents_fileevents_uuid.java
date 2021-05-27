/**
 *
 */
package org.iplantc.service.common.migration;

import org.flywaydb.core.api.FlywayException;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.iplantc.service.common.migration.utils.BackfillUtil;
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.common.uuid.UUIDType;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Backfills valid {@link AgaveUUID} into each row of the jobevents and transfertasks tables
 * and sets the columns as unique indexes.
 *
 * @author dooley
 *
 */
public class V2_1_7_1__Backfill_jobevents_transferevents_fileevents_uuid extends BaseJavaMigration {

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

        System.out.println("Starting to backfill jobevents table.");
        BackfillUtil.backfillAgaveUUID(context.getConnection(), "jobevents", UUIDType.JOB_EVENT);
        System.out.println("Finished backfilling jobevents table.");


        System.out.println("Starting to backfill transfertasks table.");
//    	BackfillUtil.backfillAgaveUUID(connection, "transfertasks", UUIDType.TRANSFER);
        System.out.println("Finished backfilling transfertasks table.");

        System.out.println("Started adding jobevents uuid index.");
//    	ColumnUtil.addUniqueIndex(connection, "jobevents");
        System.out.println("Finished adding jobevents uuid index.");

//        ColumnUtil.addUniqueIndex(connection, "transfertasks");

    }
}
