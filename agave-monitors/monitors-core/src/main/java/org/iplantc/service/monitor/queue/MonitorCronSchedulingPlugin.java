package org.iplantc.service.monitor.queue;

import org.iplantc.service.common.queue.GenericSchedulingPlugin;

/**
 * Class to initialize worker tasks to process monitor events.
 * This class is called by a servlet filter on startup so it will 
 * begin running even if no service is called.
 * 
 * @author dooley
 *
 */
public class MonitorCronSchedulingPlugin extends GenericSchedulingPlugin 
{
	/**
	 * 
	 */
	public MonitorCronSchedulingPlugin() {
		super();
	}

	@Override
	protected Class<?> getJobClass()
	{
		return MonitorCronListener.class;
	}

	@Override
	protected String getPluginGroup()
	{
		return "MonitorCron";
	}

	@Override
	protected int getTaskCount()
	{
        return 1;
	}
}
