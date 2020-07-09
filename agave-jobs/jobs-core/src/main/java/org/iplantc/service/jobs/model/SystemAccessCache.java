package org.iplantc.service.jobs.model;

import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Retains a map of the last access times for a system
 */
public class SystemAccessCache {
    final private static ConcurrentHashMap<String, SystemAccess> accessCach = new ConcurrentHashMap<>();

    /**
     * Returns the last time the system was accessed, or null of it has not been accessed recently.
     * @param uuid the uuid of the system to fetch the access timestamp
     * @return timestamp of the last successful access to that system
     */
    public Instant getLastSuccess(String uuid) {
        SystemAccess systemAccess = accessCach.getOrDefault(uuid, new SystemAccess());
        if (systemAccess.getLastSuccess() != null && systemAccess.getLastSuccess().isBefore(Instant.now().minusSeconds(1800))) {
            setLastSuccess(uuid, null);
            return null;
        } else {
            return systemAccess.getLastSuccess();
        }
    }

    /**
     * Updates the last time the system was successfully accessed.
     *
     * @param uuid the uuid of the system access to update
     * @param instant the last successful access timestamp
     */
    public void setLastSuccess(String uuid, Instant instant) {
        SystemAccess systemAccess = accessCach.getOrDefault(uuid, new SystemAccess());
        systemAccess.setLastSuccess(instant);
        systemAccess.setLastAccess(instant);
        accessCach.put(uuid, systemAccess);
    }

    /**
     * Updates the last time the system was accessed, success for fail.
     *
     * @param uuid the uuid of the system access to update
     * @param instant the last access timestamp
     */
    public void setLastAccesss(String uuid, Instant instant) {
        SystemAccess systemAccess = accessCach.getOrDefault(uuid, new SystemAccess());
        systemAccess.setLastAccess(instant);
        accessCach.put(uuid, systemAccess);
    }

    class SystemAccess {
        private Instant lastAccess;
        private Instant lastSuccess;

        public Instant getLastAccess() {
            return lastAccess;
        }

        public void setLastAccess(Instant lastAccess) {
            this.lastAccess = lastAccess;
        }

        public Instant getLastSuccess() {
            return lastSuccess;
        }

        public void setLastSuccess(Instant lastSuccess) {
            this.lastSuccess = lastSuccess;
        }
    }


}
