/**
 * 
 */
package org.iplantc.service.apps.queue.actions;

import org.iplantc.service.common.exceptions.DependencyException;
import org.iplantc.service.common.exceptions.DomainException;
import org.iplantc.service.common.exceptions.PermissionException;
import org.iplantc.service.systems.exceptions.SystemUnavailableException;
import org.iplantc.service.systems.exceptions.SystemUnknownException;
import org.iplantc.service.transfer.URLCopy;

import java.nio.channels.ClosedByInterruptException;

/**
 * @author dooley
 *
 */
public interface WorkerAction<T> {

    /**
     * This method performs the actual task described by
     * this action. That may include staging data, invoking
     * processes, etc.
     * 
     * @param job
     * @return 
     * @throws SystemUnavailableException
     * @throws SystemUnknownException
     * @throws PermissionException 
     * @throws JobException
     * @throws JobDependencyException 
     */
    void run()
    throws SystemUnavailableException, SystemUnknownException, ClosedByInterruptException, DomainException, DependencyException, PermissionException;
    
    boolean isStopped();

    /**
     * @param stopped the stopped to set
     */
    void setStopped(boolean stopped);

    /**
     * @return the entity upon which the worker is acting
     */
    T getEntity();

    /**
     * @param entity the the entity upon which the worker is acting
     */
    void setEntity(T entity);

    /**
     * @return the urlCopy
     */
    URLCopy getUrlCopy();

    /**
     * @param urlCopy the urlCopy to set
     */
    void setUrlCopy(URLCopy urlCopy);

    /**
     * Throws an exception if {@link #isStopped()} returns true
     * @throws ClosedByInterruptException
     */
    void checkStopped() throws ClosedByInterruptException;
    
}
