package org.iplantc.service.systems.model;

import org.hibernate.Session;
import org.iplantc.service.common.persistence.HibernateUtil;

public class PersistedSystemsModelTestCommon extends SystemsModelTestCommon {

    protected void clearSystems() throws Exception {
        Session session = null;
        try {
            HibernateUtil.beginTransaction();
            session = HibernateUtil.getSession();
            session.clear();
            HibernateUtil.disableAllFilters();

            session.createQuery("delete RemoteSystem").executeUpdate();
            session.createQuery("delete BatchQueue").executeUpdate();
            session.createQuery("delete StorageConfig").executeUpdate();
            session.createQuery("delete LoginConfig").executeUpdate();
            session.createQuery("delete AuthConfig").executeUpdate();
            session.createQuery("delete SystemRole").executeUpdate();
            session.createQuery("delete CredentialServer").executeUpdate();
            session.createQuery("delete SystemHistoryEvent").executeUpdate();
            session.flush();
        } catch (Throwable t) {
            t.printStackTrace();
        } finally {
            try { HibernateUtil.commitTransaction(); } catch (Exception ignored) {}
        }
    }
}
