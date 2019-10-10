package org.iplantc.service.systems.model;

import org.hibernate.Session;
import org.iplantc.service.common.persistence.HibernateUtil;
import org.testng.annotations.Test;

@Test(groups = {"integration"})
public class PersistedSystemsModelTestCommon extends SystemsModelTestCommon {

    protected void clearSystems() throws Exception {
        Session session = null;
        try {
            HibernateUtil.beginTransaction();
            session = HibernateUtil.getSession();
            session.clear();
            HibernateUtil.disableAllFilters();
            @SuppressWarnings("JpaQlInspection") String query = "DELETE RemoteSystem";

            session.createQuery(query).executeUpdate();
            session.flush();
        } catch (Throwable t) {
            t.printStackTrace();
        } finally {
            try {
                HibernateUtil.commitTransaction();
            } catch (Exception ignored) {
            }
        }
    }
}
