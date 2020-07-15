package org.iplantc.service.jobs.managers.killers;

import org.iplantc.service.jobs.model.Job;
import org.iplantc.service.systems.model.ExecutionSystem;

public class ProcessKiller extends AbstractJobKiller {

	public ProcessKiller(Job job, ExecutionSystem executionSystem)
	{
		super(job, executionSystem);
	}

    /**
	 * Will always return null as process id are numeric
	 * @see org.iplantc.service.jobs.managers.killers.AbstractJobKiller#getAltCommand()
	 */
	@Override
	protected String getAltCommand() {
		return null;
	}
}
