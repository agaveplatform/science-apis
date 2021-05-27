package org.iplantc.service.io.manager.actions;

import org.iplantc.service.io.model.LogicalFile;
import org.iplantc.service.transfer.exceptions.RemoteDataException;

import java.io.IOException;

public class ImportAction extends AbstractAction {
	
	ActionContext data = null;
	public ImportAction(ActionContext context) {
		super(context);
	}

	@Override
	public LogicalFile doAction() throws RemoteDataException, IOException {
		return null;
	}

}
