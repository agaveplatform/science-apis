package org.iplantc.service.systems.model.enumerations;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.iplantc.service.systems.exceptions.SystemArgumentException;

/**
 * Execution systems fall into a taxonomy based on the method they use to run jobs. That basis of that taonomy is
 * their {@link ExecutionType}, enumerated here.
 * @author dooley
 * 
 */
public enum ExecutionType
{
	/**
	 * Represents virtual machine execution through an alternative Openstack UI.
	 * @deprecated
	 */
	ATMOSPHERE,
	/**
	 * Supports managed execution through a remote batch scheduler. Supported schedulers are enumerated as
	 * {@link SchedulerType}.
	 */
	HPC,
	/**
	 * Supports execution on a CondorHT pool.
	 */
	CONDOR,
	/**
	 * Supports Forked execution on a remote linux system.
	 */
	CLI;
	
	@Override
	public String toString() {
		return name();
	}

	/**
	 * Some {@link ExecutionType} can be used interchangeably to run tasks in the same way. One example is the
	 * {@link #CLI}'s {@link SchedulerType#FORK} can also be invoked by systems with a {@link ExecutionType#HPC}. This
	 * method provides a convenient way to determine such compatibility.
	 *
	 * @return immutable list of compatibile {@link ExecutionType}.
	 */
	public List<ExecutionType> getCompatibleExecutionTypes()
	{
		List<ExecutionType> types = new ArrayList<ExecutionType>();
		if (this.equals(ATMOSPHERE)) {
			types = List.of(CLI);
		} else if (this.equals(HPC)) {
			types = List.of(HPC, CLI);
		} else if (this.equals(CONDOR)) {
			types = List.of(CONDOR);
		} else if (this.equals(CLI)) {
			types = List.of(CLI);
		}
		return types;
	}

	/**
	 * Returns the list of compatible {@link SchedulerType} for this {@link ExecutionType}.
	 * @return list of {@link SchedulerType}
	 */
	public List<SchedulerType> getCompatibleSchedulerTypes() {
		switch (this) {
			case CLI:
				return List.of(SchedulerType.FORK);
			case CONDOR:
				return List.of(SchedulerType.CONDOR, SchedulerType.CUSTOM_CONDOR);
			case HPC:
				return Arrays.stream(SchedulerType.values())
						.filter( t -> ! List.of(SchedulerType.FORK, SchedulerType.CONDOR, SchedulerType.CUSTOM_CONDOR, SchedulerType.UNKNOWN).contains(t))
						.collect(Collectors.toList());
			default:
				return List.of();
		}
	}
}
