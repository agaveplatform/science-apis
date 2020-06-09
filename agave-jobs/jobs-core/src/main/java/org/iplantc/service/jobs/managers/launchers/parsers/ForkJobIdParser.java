package org.iplantc.service.jobs.managers.launchers.parsers;

import org.apache.commons.lang.StringUtils;
import org.iplantc.service.jobs.exceptions.RemoteJobIDParsingException;

public class ForkJobIdParser implements RemoteJobIdParser {

	@Override
	public String getJobId(String output) throws RemoteJobIDParsingException
	{
		String pid = null;
		if (!StringUtils.isEmpty(output))
		{
			String[] lines = output.replaceAll("\r", "\n").split("\n");
			for(String line: lines) {
				if (StringUtils.startsWith(line, "[")) continue;

				if (StringUtils.isNumeric(StringUtils.trimToNull(line))) {
					pid = line.trim();
					break;
				}
			}

			if (pid == null) {
				throw new RemoteJobIDParsingException(output);
			} else {
				return pid;
			}
		}
		throw new RemoteJobIDParsingException("No response from server upon job launch");
	}
}
