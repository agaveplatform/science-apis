package org.iplantc.service.systems.model.enumerations;

import org.iplantc.service.systems.model.AuthConfig;
import org.iplantc.service.systems.model.ExecutionSystem;
import org.iplantc.service.systems.model.LoginConfig;

/**
 * Represents the supported ways to connect to an {@link ExecutionSystem} to orchestrate the execution of job. While
 * the {@link ExecutionType} determines the taxonomy of execution and the {@link SchedulerType} determines how the
 * job will eventually be run, the {@link LoginProtocolType} defines the communication mechanism to be used between
 * Agave and the remote {@link ExecutionSystem}.
 * <p>
 * Because there are multiple ways to communicate with a remote system to invoke tasks, there is a many-to-many
 * relationship between a {@link LoginProtocolType} and {@link ExecutionType}. Likewise, there are frequently multiple
 * ways in which to authenticate communication. Thus, there is also a many to many relationship between
 * {@link LoginProtocolType} and {@link AuthConfigType}.
 *
 * @author dooley
 * @see LoginConfig
 */
public enum LoginProtocolType implements ProtocolType {
    /**
     * Indicates authentication via a 3rd party API. {@link AuthConfig} using this login protocl type
     */
    API,
    SSH,
    GSISSH,
    LOCAL;


    @Override
    public boolean accepts(AuthConfigType type) {
        if (this.equals(GSISSH)) {// || this.equals(GRAM) || this.equals(UNICORE)) {
            return (type.equals(AuthConfigType.X509));
        } else if (this.equals(API)) {
            return (type.equals(AuthConfigType.PASSWORD) ||
                    type.equals(AuthConfigType.TOKEN));
        } else if (this.equals(SSH)) {
            return (type.equals(AuthConfigType.PASSWORD) || type.equals(AuthConfigType.SSHKEYS));
        } else if (this.equals(LOCAL)) {
            return type.equals(AuthConfigType.LOCAL);
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return name();
    }
}
