package org.iplantc.service.systems.model.enumerations;

import org.iplantc.service.systems.model.RemoteSystem;

/**
 * Enumerated values for the available statuses a {@link RemoteSystem} can have. Each system has a custom
 * human readable expression associated with it.
 */
public enum SystemStatusType
{
	UP("The system is up"), 
	DOWN("The system is down"), 
	UNKNOWN("The system status is unknown"), 
	MAINTENANCE("The system is under maintenance");
	
	private final String expression;

	/**
	 * Private constructor for the enumerated class. We do not allow custom {@link SystemStatusType}
	 * @param expression the human readable phrase for the system
	 */
    SystemStatusType(String expression) {
		this.expression = expression;
	}

	@Override
	public String toString() {
		return name();
	}
	
	/**
	 * Returns a human readable expression describing
	 * the system status. ex. "The system is up"
	 * @return
	 */
	public String getExpression() {
		return this.expression;
	}
}
