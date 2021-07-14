package org.iplantc.service.io;

import org.iplantc.service.io.dao.LogicalFileDao;
import org.iplantc.service.io.model.LogicalFile;
import org.testng.annotations.Test;

import java.util.List;

import static org.testng.Assert.assertEquals;

@Test(groups={"integration"})
public class DatabaseCleanupIT extends BaseTestCase{
    @Test
    public void testDatabaseCleanup(){
        clearLogicalFiles();

        List<LogicalFile> clearedFiles = LogicalFileDao.getAll();
        assertEquals(clearedFiles.size(), 0);

    }

}
