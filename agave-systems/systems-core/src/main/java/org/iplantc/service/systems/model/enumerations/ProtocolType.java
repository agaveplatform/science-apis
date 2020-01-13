package org.iplantc.service.systems.model.enumerations;

/**
 * Interface providing a method for the {@link LoginProtocolType} and {@link StorageProtocolType} enumerated
 * value to resolve whether it supports a given {@link AuthConfigType}.
 * @see LoginProtocolType
 * @see StorageProtocolType
 */
public interface ProtocolType {

	/**
	 * Verifies whether the protocol supports the provided LoginCredentialType.
	 * 
	 * @param type the {@link AuthConfigType} to check.
	 * @return true if supported, false otherwise.
	 */
	public boolean accepts(AuthConfigType type);

	/**
	 * The name of the {@link ProtocolType}
	 * @return
	 */
	public String name();
}
