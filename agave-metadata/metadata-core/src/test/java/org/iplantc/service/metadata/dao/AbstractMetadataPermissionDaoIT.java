package org.iplantc.service.metadata.dao;

import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.iplantc.service.common.persistence.HibernateUtil;
import org.iplantc.service.metadata.exceptions.MetadataException;
import org.iplantc.service.metadata.exceptions.MetadataQueryException;
import org.iplantc.service.metadata.model.MetadataPermission;
import org.iplantc.service.metadata.model.enumerations.PermissionType;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.UUID;

public abstract class AbstractMetadataPermissionDaoIT implements IMetadataPermissionDaoIT {
    protected String TEST_OWNER = "testuser";
    protected String TEST_SHARED_OWNER = "testshareuser";

    @BeforeClass
    public void beforeClass() throws Exception {
        clearPermissions();
    }

    @AfterMethod
    public void afterMethod() throws Exception
    {
        clearPermissions();
    }

    /**
     * Clears all permissions in the given table
     * @throws Exception
     */
    private void clearPermissions() throws Exception {
        Session session = null;
        try
        {
            HibernateUtil.beginTransaction();
            session = HibernateUtil.getSession();
            session.clear();
            HibernateUtil.disableAllFilters();

            //noinspection JpaQlInspection
            session.createQuery("delete MetadataSchemaPermission").executeUpdate();
            //noinspection JpaQlInspection
            session.createQuery("delete MetadataPermission").executeUpdate();
        }
        catch (HibernateException ex)
        {
            throw new MetadataQueryException(ex);
        }
        finally
        {
            try {
                if (session != null) {
                    session.close();
                }
            } catch (Exception ignore) {}
        }
    }

}
