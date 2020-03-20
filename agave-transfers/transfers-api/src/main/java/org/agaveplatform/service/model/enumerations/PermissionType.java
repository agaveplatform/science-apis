package org.agaveplatform.service.model.enumerations;


import org.iplantc.service.common.Settings;
import org.iplantc.service.systems.model.RemoteSystem;
import org.iplantc.service.systems.model.enumerations.StorageProtocolType;

/**
 * Enumerated values representing all available permissions that may be set on file items at the Agave level.
 * These are map to permission model of a {@link org.iplantc.service.systems.model.enumerations.StorageProtocolType}
 * on a case by case basis within the concerte {@link org.iplantc.service.transfer.RemoteDataClientPermissionProvider}
 * for a {@link org.iplantc.service.transfer.RemoteDataClient}.
 * <p>
 * <em>Note:</em> When set on a directory/collection without recursive behavior, the permission will apply only to the
 * directory itself. This allows for direct listing and stat operations on the directory but will not apply to any of
 * file items within the directory.
 */
public enum PermissionType implements Comparable<PermissionType>
{
	/**
	 * No permissions at all. This cannot be explitly set, but rather is an indication of no permission of any kind
	 */
	NONE,
	/**
	 * Read-only permission. This grants permission to read the content, but not edit or execute
	 */
	READ,
	/**
	 * Write-only permission. This grants permission to edit the content and delete, but not read or execute.
	 */
	WRITE,
	/**
	 * Execute-only permission. This grants permission to edit the content and delete, but not read or write.
	 * This may not have any meaning depending on the underlying {@link StorageProtocolType} of the {@link RemoteSystem}
	 */
	EXECUTE,
	/**
	 * {@link #READ} and {@link #WRITE} permission. This grants the ability to read and edit the content of a file,
	 * but not execute it.
	 */
	READ_WRITE,
	/**
	 * {@link #READ} and {@link #EXECUTE} permission. This grants the ability to read and execute the content of a file,
	 * but not change its content. The {@link #EXECUTE} permission may not have any meaning depending on the underlying
	 * {@link StorageProtocolType} of the {@link RemoteSystem}
	 */
	READ_EXECUTE,
	/**
	 * {@link #WRITE} and {@link #EXECUTE} permission. This grants the ability to edit content and execute a file item,
	 * but not read its content. The {@link #EXECUTE} permission may not have any meaning depending on the underlying
	 * {@link StorageProtocolType} of the {@link RemoteSystem}
	 */
	WRITE_EXECUTE,
	/**
	 * All permissions. This is a wilcard meaning every {@link PermissionType} except for {@link #NONE} and
	 * {@link #OWNER}.
	 */
	ALL,
	/**
	 * Indicates ownership of the file. Implementations of the {@link StorageProtocolType} may refer to this as
	 * "creator" or "author" within their terminology. Here we use this to differentiate from the {@link #ALL}
	 * permission which is not allowed to grant ownership to another user.
	 * <p>
	 * Please note that the {@link Settings#WORLD_USER_USERNAME} and {@link Settings#PUBLIC_USER_USERNAME} cannot be
	 * granted owner permission.
	 */
	OWNER,
	/**
	 * Grants permission to read the permissions on a file item. {@link #WRITE_PERMISSION} is prohibited.
	 */
	READ_PERMISSION,
	/**
	 * Grants permission to create, edit, and delete the permissions on a file item. {@link #READ_PERMISSION} is prohibited.
	 */
	WRITE_PERMISSION,
	/**
	 * Grants both {@link #READ_PERMISSION} and {@link #WRITE_PERMISSION} allowing the user to fully manage the permissions of a file item.
	 */
	READ_WRITE_PERMISSION;

	/**
	 * Checks for the existence direct or implied {@link #READ}. This includes {@link #ALL} and {@link #OWNER}
	 * @return true if permission has read ability
	 */
	public boolean canRead() {
		return (this.equals(ALL) || this.equals(OWNER) ||
				this.equals(READ) ||
				this.equals(READ_PERMISSION) ||
				this.equals(READ_WRITE) ||
				this.equals(READ_WRITE_PERMISSION) ||
				this.equals(READ_EXECUTE));
	}

	/**
	 * Checks for the existence direct or implied {@link #WRITE}. This includes {@link #ALL} and {@link #OWNER}
	 * @return true if permission has write ability
	 */
	public boolean canWrite() {
		return (this.equals(ALL) || this.equals(OWNER) ||
				this.equals(WRITE) ||
				this.equals(WRITE_PERMISSION) ||
				this.equals(READ_WRITE) ||
				this.equals(READ_WRITE_PERMISSION) ||
				this.equals(WRITE_EXECUTE));
	}

	/**
	 * Checks for the existence direct or implied {@link #EXECUTE}. This includes {@link #ALL} and {@link #OWNER}
	 * @return true if permission has execute ability
	 */
	public boolean canExecute() {
		return (this.equals(ALL) || this.equals(OWNER) ||
				this.equals(EXECUTE) ||
				this.equals(READ_EXECUTE) ||
				this.equals(WRITE_EXECUTE));
	}

	/**
	 * Checks for the ability to share the file item with another user. Currnetly only {@link #OWNER} has this ability.
	 * @return true if permission has sharing ability
	 */
	public boolean canShare() {
		return this.equals(OWNER);
	}

	/**
	 * Mergest the newPermission with the current one, maintaining the ability of both. If {@link #NONE} is provided,
	 * no change will be made.
	 * @return combination of the two permission values.
	 */
	public PermissionType add(PermissionType newPermission)
	{
		if (newPermission.equals(this))
		{
			return newPermission;
		}
		else if (newPermission.equals(OWNER) || this.equals(OWNER))
		{
			return OWNER;
		}
		else if (newPermission.equals(ALL) || this.equals(ALL))
		{
			return ALL;
		}
		else if (newPermission.canRead() && newPermission.canWrite())
		{
			if (canExecute())
				return ALL;
			else
				return READ_WRITE;
		}
		else if (newPermission.canRead() && newPermission.canExecute())
		{
			if (canWrite())
				return ALL;
			else
				return READ_EXECUTE;
		}
		else if (newPermission.canWrite() && newPermission.canExecute())
		{
			if (canRead())
				return ALL;
			else
				return READ_WRITE;
		}
		else if (newPermission.canRead())
		{
			if (canWrite() && canExecute())
				return ALL;
			else if (canWrite())
				return READ_WRITE;
			else if (canExecute())
				return READ_EXECUTE;
			else
				return READ;
		}
		else if (newPermission.canWrite())
		{
			if (canRead() && canExecute())
				return ALL;
			else if (canRead())
				return READ_WRITE;
			else if (canExecute())
				return WRITE_EXECUTE;
			else
				return WRITE;
		}
		else if (newPermission.canExecute())
		{
			if (canRead() && canWrite())
				return ALL;
			else if (canRead())
				return READ_EXECUTE;
			else if (canWrite())
				return WRITE_EXECUTE;
			else
				return EXECUTE;
		}
		else {
			return this;
		}
	}

	/**
	 * Removes the previous permission from the current. If the new and current permission are equivalent, the result
	 * will be {@link #NONE}.
	 * @return the current permission minus the new permission
	 */
	public PermissionType remove(PermissionType newPermission)
	{
		if (newPermission.equals(this))
		{
			return NONE;
		}
		else if (newPermission.equals(ALL) || this.equals(ALL))
		{
			return NONE;
		}
		else if (newPermission.canRead() && newPermission.canWrite())
		{
			if (canExecute())
				return EXECUTE;
			else
				return NONE;
		}
		else if (newPermission.canRead() && newPermission.canExecute())
		{
			if (canWrite())
				return WRITE;
			else
				return NONE;
		}
		else if (newPermission.canWrite() && newPermission.canExecute())
		{
			if (canRead())
				return READ;
			else
				return NONE;
		}
		else if (newPermission.canRead())
		{
			if (canWrite() && canExecute())
				return WRITE_EXECUTE;
			else if (canWrite())
				return WRITE;
			else if (canExecute())
				return EXECUTE;
			else
				return NONE;
		}
		else if (newPermission.canWrite())
		{
			if (canRead() && canExecute())
				return READ_EXECUTE;
			else if (canRead())
				return READ;
			else if (canExecute())
				return EXECUTE;
			else
				return NONE;
		}
		else if (newPermission.canExecute())
		{
			if (canRead() && canWrite())
				return READ_WRITE;
			else if (canRead())
				return READ;
			else if (canWrite())
				return WRITE;
			else
				return NONE;
		}
		else {
			return NONE;
		}
	}

	/**
	 * Returns the numeric owner value of a unix permission code. Potential values are 0-7.
	 * @return true if permission has read ability
	 */
	public int getUnixValue()
	{
		if (this.equals(EXECUTE)) {
			return 1;
		} else if (this.equals(WRITE) || this.equals(WRITE_PERMISSION)) {
			return 2;
		} else if (this.equals(WRITE_EXECUTE)) {
			return 3;
		} else if (this.equals(READ) || this.equals(READ_PERMISSION)) {
			return 4;
		} else if (this.equals(READ_EXECUTE)) {
			return 5;
		} else if (this.equals(READ_WRITE) || this.equals(READ_WRITE_PERMISSION)) {
			return 6;
		} else if (this.equals(ALL)) {
			return 7;
		} else {
			return 0;
		}
	}

	/**
	 * Returns a comma-separated list of all permissions.
	 *
	 * @return string representation of all values
	 */
	public static String supportedValuesAsString()
	{
		return ALL + ", " + READ + ", " + WRITE + ", " + READ_WRITE + ", " + EXECUTE + ", " + READ_EXECUTE + ", " + WRITE_EXECUTE + ", " + NONE;
	}

	/**
	 * Compares the permission of this permission to another
	 * @param permission
	 * @return -1 if less, 0 if equal, 1 if greater.
	 */
	public int compareUnixValueTo(PermissionType permission) {
		return Integer.valueOf(this.getUnixValue()).compareTo(Integer.valueOf(permission.getUnixValue()));
	}

}

