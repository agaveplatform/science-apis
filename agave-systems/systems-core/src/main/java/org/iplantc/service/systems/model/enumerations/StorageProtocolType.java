package org.iplantc.service.systems.model.enumerations;

import org.iplantc.service.systems.model.StorageConfig;
import org.iplantc.service.systems.model.StorageSystem;

/**
 * Represents the supported ways to connect to an {@link StorageSystem} to orchestrate the execution of job. While
 * the {@link ExecutionType} determines the taxonomy of execution and the {@link SchedulerType} determines how the
 * job will eventually be run, the {@link StorageProtocolType} defines the communication mechanism to be used between
 * Agave and the remote {@link StorageSystem}.
 * <p>
 * Because there are multiple ways to communicate with a remote system to invoke tasks, there is a many-to-many
 * relationship between a {@link StorageProtocolType} and {@link ExecutionType}. Likewise, there are frequently multiple
 * ways in which to authenticate communication. Thus, there is also a many to many relationship between
 * {@link LoginProtocolType} and {@link AuthConfigType}.
 *
 * @author dooley
 * @see StorageConfig
 */
public enum StorageProtocolType implements ProtocolType {
	GRIDFTP, FTP, SFTP, IRODS, IRODS4, LOCAL, AZURE, S3, SWIFT, HTTP, HTTPS;

	@Override
	public boolean accepts(AuthConfigType type)
	{
		if (this == GRIDFTP) {
			return (type == AuthConfigType.X509);
		} else if (this == FTP) {
			return (type == AuthConfigType.PASSWORD || type == AuthConfigType.ANONYMOUS);
		} else if (this == SFTP) {
			return (type == AuthConfigType.PASSWORD) || type == AuthConfigType.SSHKEYS;
		} else if (this == IRODS || this == IRODS4) {
			return type == AuthConfigType.PASSWORD || type == AuthConfigType.PAM || type == AuthConfigType.X509;
		} else if (this == LOCAL) {
			return type == AuthConfigType.LOCAL;
		} else if (this == AZURE
				|| this == S3
//				|| this == SWIFT)
				) {
			return type == AuthConfigType.APIKEYS;
		} else {
			return false;
		}
	}
	
	/**
	 * Returns whether this {@link StorageProtocolType} supports 
	 * specifying the authentication parameters in the URL. This is 
	 * used to determine whether Agave can transfer a URL with this
	 * protocol value given as the schema. 
	 * @return true if the sytem can be authenticated and accessed from a
	 * URL, false otherwise.
	 */
	public boolean allowsURLAuth() {
	    if (this == GRIDFTP) {
            return false;
        } else if (this == FTP) {
            return true;
        } else if (this == SFTP) {
            return true;
        } else if (this == IRODS || this == IRODS4) {
            return false;
        } else if (this == LOCAL) {
            return false;
        } else if (this == AZURE
                || this == S3
//              || this == SWIFT)
                ) {
            return false;
        } else return this == HTTP || this == HTTPS;
	}
	
	@Override
	public String toString() {
		return name();
	}
}

