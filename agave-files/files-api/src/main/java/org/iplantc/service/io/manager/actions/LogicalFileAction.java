package org.iplantc.service.io.manager.actions;

import org.iplantc.service.io.model.LogicalFile;
import org.iplantc.service.transfer.exceptions.RemoteDataException;

import java.io.IOException;

/**
 * Represents a generic action to be performed on a {@link LogicalFile}.
 * 
 * @author dooley
 *
 */
public interface LogicalFileAction {

	/**
	 * Performs the actual work represented by concrete 
	 * implementations of this interface. 
	 * @return
	 * @throws RemoteDataException
	 * @throws IOException
	 */
    LogicalFile doAction() throws RemoteDataException, IOException;
}
