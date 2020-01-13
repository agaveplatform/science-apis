package org.iplantc.service.systems.model.enumerations;

import org.iplantc.service.systems.model.RemoteSystem;
import org.iplantc.service.systems.model.SystemRole;

public enum RoleType implements Comparable<RoleType>
{
	/**
	 * Represents the absence of any permission. This value cannot be explicitly assigned. Instead, by assigning
	 * this value, any existing {@link RoleType} will be removed.
	 */
	NONE,
	/**
	 * Represents read-only access to a system. Users have the capability to view the system information and browse
	 * the file system, but cannot make any changes or share the system with anyone else.
	 */
	GUEST,
	/**
	 * Provides read and execution capabilities to the system. Users with the {@link #USER} role can interact with the
	 * file system, run jobs, and grant file permissions to individual files, but cannot register apps to the system
	 * or grant system roles to others.
	 */
	USER,
	/**
	 * Provides all {@link #USER} capabilities plus the ability to register apps to this system. Does not grant the
	 * ability to grant system roles to others.
	 */
	PUBLISHER,
	/**
	 * Provides all {@link #PUBLISHER} capabilities plus the ability to grant system roles to others. Does not grant
	 * the ability to assign ownership.
	 */
	ADMIN,
	/**
	 * Provides full capability do manage, edit, share, and assign ownership of a {@link RemoteSystem} to other users.
	 */
	OWNER;

	/**
	 * Checks for {@link #ADMIN} or above
	 * @return true of permission equals {@link #ADMIN} or above
	 */
	public boolean canAdmin()
    {
        return this == ADMIN || this == OWNER;
    }

	/**
	 * Checks for {@link #PUBLISHER} or {@link #canAdmin()}
	 * @return true of permission equals {@link #PUBLISHER} or above
	 */
	public boolean canPublish() {
        return this == PUBLISHER || canAdmin();
    }

	/**
	 * Checks for {@link #USER} or {@link #canPublish()}
	 * @return true of permission equals {@link #USER} or above
	 */
    public boolean canUse() {
		return this == USER || canPublish();
	}

	/**
	 * Checks for {@link #GUEST} or {@link #canUse()}
	 * @return true of permission equals {@link #GUEST} or above
	 */
	public boolean canRead() {
        return this == GUEST || canUse();
    }

	/**
	 * Returns integer value of the {@link RoleType}. Values ascend in value from 0 for {@link #NONE} to 5
	 * @return true of permission equals {@link #PUBLISHER} or above
	 */
    public int intVal() {
		if (this.equals(ADMIN)) {
			return 5;
		} else if (this.equals(OWNER)) {
			return 4;
		} else if (this.equals(PUBLISHER)) {
			return 3;
		} else if (this.equals(USER)) {
			return 2;
		} else if (this.equals(GUEST)) {
			return 1;
		} else {
			return 0;
		}
	}
	
	public static String supportedValuesAsString()
	{
		return NONE + ", " + USER + ", " + PUBLISHER + ", " + OWNER + ", " + ADMIN;
	}
	
	@Override
	public String toString() {
		return name();
	}
}
