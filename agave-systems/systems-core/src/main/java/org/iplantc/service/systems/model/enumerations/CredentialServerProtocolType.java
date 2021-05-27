package org.iplantc.service.systems.model.enumerations;

import org.iplantc.service.transfer.irods.IRODS;
import org.iplantc.service.transfer.irods4.IRODS4;

/**
 * Enumerates the types of remote credential servers supported by an {@link RemoteSystemType#AUTH} system.
 */
public enum CredentialServerProtocolType
{
	/**
	 * Kerberose login, primarily used with {@link IRODS} and {@link IRODS4} support
	 */
	KERBEROSE,
	/**
	 * OAuth2 support. Requires a user password, JWT, or device token flow.
	 */
	OAUTH2,
	/**
	 * OAuth for MyProxy. Oauth1 compliant proxy for MyProxy.
	 * @deprecated
	 */
	OA4MP,
	/**
	 * Virtual Organization Management Service. Primarily popular outside the US at this point.
	 * @deprecated
	 */
	VOMS,
	/**
	 * MyProxy server. Provides X509 short-lived credentials for GSI authentiation. No longer widely used.
	 * @deprecated
	 */
	MYPROXY,
	/**
	 * MyProxy Gateway. Acts as a proxy to a MyProxy server providing credential refresh and advanced auditing.
	 * @deprecated
	 */
	MPG,
	/**
	 * Represents the absence of a known protocol.
	 */
	NONE;
	
	@Override
	public String toString() {
		return name();
	}
}
