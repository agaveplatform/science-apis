package org.iplantc.service.jobs.managers.launchers;

import org.iplantc.service.systems.model.enumerations.ExecutionType;
import org.iplantc.service.systems.model.enumerations.SchedulerType;
import org.testng.annotations.Factory;

import java.util.HashSet;

/**
 * This class is a TestNG factory class which generates and 
 * runs a matrix of tests to test validation and remote job 
 * submission of all {@link JobLauncher} classes.
 *
 * @author dooley
 *
 */
public class JobLauncherTestFactory
{
	@Factory
    public Object[] createInstances() {
    	HashSet<Object> testCases = new HashSet<Object>();

		for (ExecutionType executionType: ExecutionType.values()) {
			if (ExecutionType.ATMOSPHERE == executionType) continue;

			// We have a container for each scheduler.
			for (SchedulerType scheduler : executionType.getCompatibleSchedulerTypes()) {
				// ignore schedulers for which we don't yet have containers.
				if (SchedulerType.LOADLEVELER == scheduler ||
						SchedulerType.UNKNOWN == scheduler ||
						scheduler.name().startsWith("CUSTOM")) {
					continue;
				}

				// uncomment out to run over a subset of schedulers
//				if (List.of(SchedulerType.PBS, SchedulerType.FORK).contains(scheduler)) {

					// create the test case
					testCases.add(new JobLauncherTest(executionType, scheduler));

					// Test containers can frequently pull double duty, so we iterate over
					// each scheduler type, and run a test for every execution type supporting a system running that kind
					// of scheduler. This gives us decent coverage over OS (alpine, centos, ubuntu, fedora, etc), scheduler,
					// and basic unix toolchains.
					if (ExecutionType.CLI != executionType) testCases.add(new JobLauncherTest(ExecutionType.CLI, scheduler));
//				}
			}
		}

        return testCases.toArray();
    }
}