/**
 * 
 */
package org.iplantc.service.systems.model.enumerations;

import org.iplantc.service.systems.model.ExecutionSystem;
import org.iplantc.service.systems.model.StorageConfig;
import org.iplantc.service.systems.model.StorageSystem;

/**
 * The types of systems one may register.
 * @author dooley
 *
 */
public enum RemoteSystemType
{
	/**
	 * {@link ExecutionSystem} supporting code execution through one or more {@link ExecutionType}. Execution systems
	 * also support data access through their {@link StorageConfig}, but cannot hold the assets of a Software resource.
	 */
	EXECUTION,
	/**
	 * {@link StorageSystem} supporting pure data management without any code execution support.
	 */
	STORAGE,
	/**
	 * Represents third-party service used for creating, storing, and managing authentication credentials used
	 * to interact with external systems and services.
	 */
	AUTH;

	@Override
	public String toString() {
		return name();
	}
}
