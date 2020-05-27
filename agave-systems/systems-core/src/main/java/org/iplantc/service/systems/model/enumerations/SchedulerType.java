/**
 * 
 */
package org.iplantc.service.systems.model.enumerations;

import org.iplantc.service.systems.model.ExecutionSystem;

/**
 * Agave runs jobs on {@link ExecutionSystem} by interacting with a remote scheduler. This class contains all the
 * schedulers Agave currently supports. When adding a new scheduler, information about how to submit, query, and delete
 * jobs must be included in the respective methods of this class.
 *
 * TODO: Refactor the commands into a Factory and represent each {@link SchedulerType} as a class implemeting a common interface.
 * 
 * @author dooley
 * 
 */
public enum SchedulerType
{
	/**
	 * Supports interaction with Platform LSF, owned by IBM. https://www.ibm.com/us-en/marketplace/hpc-workload-management.
	 * Default scheduling directives are used representing common configurations portable across schedulers.
	 * <p>
	 * Currently integration tested against OpenLava 3.3, IBM Spectrum LSF Suite Community Edition 10.2.0.6
	 */
	LSF,
	/**
	 * Supports interaction with Platform LSF, owned by IBM. https://www.ibm.com/us-en/marketplace/hpc-workload-management.
	 * Custom scheduling directives are supported, allowing for overriding the default agave directives to interact with
	 * advanced scheduler features.
	 * <p>
	 * Currently integration tested against 	OpenLava 3.3, IBM Spectrum LSF Suite Community Edition 10.2.0.6
	 */
	CUSTOM_LSF,
	/**
	 * Supports interaction with <a href="https://www.ibm.com/support/knowledgecenter/SSFJTW/loadl_welcome.html">IBM Tivoli Workload Scheduler LoadLeveler</a>,
	 * owned by IBM. Default scheduling directives are used representing common configurations portable across schedulers.
	 * <p>
	 * Last tested against verion 5.1. Currently looking for a dockerized integration test container or open source license.
	 */
	LOADLEVELER,
	/**
	 * Supports interaction with <a href="https://www.ibm.com/support/knowledgecenter/SSFJTW/loadl_welcome.html">IBM Tivoli Workload Scheduler LoadLeveler</a>,
	 * owned by IBM. Custom scheduling directives are supported, allowing for overriding the default agave directives
	 * to interact with advanced scheduler features.
	 * <p>
	 * Last tested against verion 5.1. Currently looking for a dockerized integration test container or open source license.
	 */
	CUSTOM_LOADLEVELER,
	/**
	 * Supports interaction with PBS Professional and PBS Professional Community Edition, both currently developed by
	 * Adaptive Computing. PBS and OpenPBS are no longer actively developed.
	 *
	 * Last tested against verion 19.1.2 of PBS Profesional Community Edition.
	 */
	PBS,
	/**
	 * Supports interaction with PBS Professional and PBS Professional Community Edition, both currently developed by
	 * Adaptive Computing. PBS and OpenPBS are no longer actively developed.
	 *
	 * Last tested against verion 19.1.2 of PBS Profesional Community Edition.
	 */
	CUSTOM_PBS,
	/**
	 * Supports interaction with the Grid Engine lineage of schedulers. Currently developed commercially as
	 * <a href="http://www.univa.com/products/">Univa Grid Engine</a> and available for download as FOSS as
	 * <a href="https://arc.liv.ac.uk/trac/SGE">Open Grid Scheduler</a>. Default scheduling directives are used
	 * representing common configurations portable across schedulers.
	 * <p>
	 * Last tested against verion 6.2.
	 */
	SGE,
	/**
	 * Supports interaction with the Grid Engine lineage of schedulers. Currently developed commercially as
	 * <a href="http://www.univa.com/products/">Univa Grid Engine</a> and available for download as FOSS as
	 * <a href="https://arc.liv.ac.uk/trac/SGE">Open Grid Scheduler</a>. Custom scheduling directives are
	 * supported, allowing for overriding the default agave directives to interact with advanced scheduler
	 * features.
	 * <p>
	 * Last tested against version 6.2.
	 */
	CUSTOM_GRIDENGINE,
	/**
	 * Supports interaction with <a href="https://research.cs.wisc.edu/htcondor/">HTCondor</a>.
	 * Default scheduling directives are used representing common configurations portable across schedulers.
	 * <p>
	 * Last tested against version 8.4.9.
	 * license.
	 */
	CONDOR,
	/**
	 * Supports interaction with <a href="https://research.cs.wisc.edu/htcondor/">HTCondor</a>. Custom scheduling
	 * supported, allowing for overriding the default agave directives to interact with advanced scheduler features.
	 * <p>
	 * Last tested against verion 8.4.9.
	 */
	CUSTOM_CONDOR,
	/**
	 * Supports direct script execution on a remote host. Scheduler directives are ignored under this scheduler. All
	 * Fork execution will inherit the authenticated user's default shell unless otherwise specified in the Software
	 * definition.
	 * <p>
	 * Last tested against ubuntu 18.04, centos 6.5, and centos 7.2.
	 */
	FORK,
	/**
	 * Supports interaction with <a href="http://adaptivecomputing.com/cherry-services/torque-resource-manager/">Torque Resource Manager</a>.
	 * Torque is closed source as of June 2018 and maintained by Adaptive Computing. Torque was originally a fork of
	 * OpenPBS and retains the basic syntax. Thus, this Torque is currently an alias for {@link #PBS} and {@link #MOAB}.
	 * Default scheduling directives are used representing common configurations portable across schedulers.
	 * <p>
	 * Last tested against verion 5.0.0, 5.1.2, and 6.1.2. Currently looking for a dockerized integration test container or
	 * open source license.
	 */
	TORQUE,
	/**
	 * Supports interaction with <a href="http://adaptivecomputing.com/cherry-services/torque-resource-manager/">Torque Resource Manager</a>.
	 * Torque is closed source as of June 2018 and maintained by Adaptive Computing. Torque was originally a fork of
	 * OpenPBS and retains the basic syntax. Thus, this Torque is currently an alias for {@link #PBS} and {@link #MOAB}.
	 * Custom scheduling directives are supported, allowing for overriding the default agave directives to interact with
	 * advanced scheduler features.
	 * <p>
	 * Last tested against verion 5.0.0, 5.1.2, and 6.1.2. Currently looking for a dockerized integration test container or
	 * open source license.
	 */
	CUSTOM_TORQUE,
	/**
	 * Supports interaction with <a href="http://adaptivecomputing.com/cherry-services/moab-hpc/">Moab</a> by Adaptive
	 * Computing. Moab is a commerically available scheduler developed by Adadptive Computing. It supports integration
	 * with Torque and PBS as well as vendor extensions allowing it to integrate with LSF, Docker, and Kubernetes. We
	 * focus on the Torque and PBS support in our integration and expose it as an alias to {@link #TORQUE}. Default
	 * scheduling directives are used representing common configurations portable across schedulers.
	 * <p>
	 * Last tested against torque verion 5.0.0, 5.1.2, and 6.1.2. Currently looking for a dockerized integration test container or
	 * open source license.
	 */
	MOAB,
	/**
	 * Supports interaction with <a href="http://adaptivecomputing.com/cherry-services/moab-hpc/">Moab</a> by Adaptive
	 * Computing. Moab is a commerically available scheduler developed by Adadptive Computing. It supports integration
	 * with Torque and PBS as well as vendor extensions allowing it to integrate with LSF, Docker, and Kubernetes. We
	 * focus on the Torque and PBS support in our integration and expose it as an alias to {@link #TORQUE}. Custom
	 * scheduling directives are supported, allowing for overriding the default agave directives to interact with
	 * advanced scheduler features.
	 * <p>
	 * Last tested against torque verion 5.0.0, 5.1.2, and 6.1.2. Currently looking for a dockerized integration test container or
	 * open source license.
	 */
	CUSTOM_MOAB,
	/**
	 * Supports interaction with <a href="https://slurm.schedmd.com/documentation.html">Slurm Workload Manager</a>.
	 * Slurm is actively developed as a FOSS project by SchedMD. Default scheduling directives are used representing
	 * common configurations portable across schedulers.
	 * <p>
	 * Last tested against slurm verion 14.03.9.
	 */
	SLURM,
	/**
	 * Supports interaction with <a href="https://slurm.schedmd.com/documentation.html">Slurm Workload Manager</a>.
	 * Slurm is actively developed as a FOSS project by SchedMD. Custom scheduling directives are supported, allowing
	 * for overriding the default agave directives to interact with advanced scheduler features.
	 * <p>
	 * Last tested against torque verion 14.03.9.
	 */
	CUSTOM_SLURM,
	/**
	 * Placeholder representing the lack of a defined {@link SchedulerType}. Ths value will cause all job submissions
	 * and Software definitions to fail.
	 */
	UNKNOWN;

	/**
	 * Provides the default batch submit command to run when submitting a job to this {@link SchedulerType}.
	 * The target of this command will be the batch submit file generated from the job's software wrapper script.
	 *
	 * @return string with the submit command
	 */
	public String getBatchSubmitCommand() 
	{
		switch (this) 
		{
			case LSF:
			case CUSTOM_LSF:
				return "bsub < ";
			case LOADLEVELER:
			case CUSTOM_LOADLEVELER:
				return "llsub";
			case TORQUE:
			case CUSTOM_TORQUE:
			case MOAB:
            case CUSTOM_MOAB:
			case PBS:
			case CUSTOM_PBS:
			case SGE:
			case CUSTOM_GRIDENGINE:
				return "qsub";
			case CONDOR:
            case CUSTOM_CONDOR:
                return "condor_submit";
			case UNKNOWN:
			case FORK:
				return "";
			case SLURM:
			case CUSTOM_SLURM:
				return "sbatch";
			default:
				return "qsub";
				
		}
	}

	/**
	 * Provides the default job kill command for this {@link SchedulerType}. The response will get the remote job or
	 * process id corresponding to the agave job.
	 *
	 * @return the kill command used by the {@link SchedulerType}
	 */
	public String getBatchKillCommand() 
	{
		switch (this) 
		{
			case LSF:
			case CUSTOM_LSF:
				return "bkill ";
			case LOADLEVELER:
			case CUSTOM_LOADLEVELER:
				return "llcancel ";
			case TORQUE:
			case CUSTOM_TORQUE:
			case MOAB:
            case CUSTOM_MOAB:
            case PBS:
            case CUSTOM_PBS:
            case SGE:
			case CUSTOM_GRIDENGINE:
				return "qdel ";
			case CONDOR:
            case CUSTOM_CONDOR:
                return "condor_rm ";
			case UNKNOWN:
			case FORK:
				return "kill -9 ";
			case SLURM:
			case CUSTOM_SLURM:
				return "scancel ";
			default:
				return "qdel ";
		}
	}
	
	@Override
	public String toString() {
		return name();
	}

	/**
	 * Provides the command used by the {@link SchedulerType} to query for job status. When possible, this should be a
	 * query based on the remote job id rather than a parsing of a potentially very large queue listing. This response
	 * of this command must be parsable by Agave, so responses that return single string expressions are highly
	 * preferred.
	 *
	 * @return the query command used to find job status for the {@link SchedulerType}.
	 */
	public String getBatchQueryCommand()
	{
		switch (this) 
		{
			case LSF:
			case CUSTOM_LSF:
				return "bjobs -w -noheader ";
			case LOADLEVELER:
			case CUSTOM_LOADLEVELER:
				return "llq -l ";
			case SGE:
			case CUSTOM_GRIDENGINE:
				return "qstat -ext -urg -xml ";
			case TORQUE:
			case CUSTOM_TORQUE:
			case MOAB:
			case CUSTOM_MOAB:
//				return "qstat -f -1 ";
				return "qstat -a | grep ^";
			case PBS:
			case CUSTOM_PBS:
				return "qstat -x -f ";
			case CONDOR:
			case CUSTOM_CONDOR:
				return "condor_q -format '%d'  JobStatus";
			case UNKNOWN:
			case FORK:
				return "ps -o pid= -o user= -o stat= -o time= -o comm= -p ";
			case SLURM:
			case CUSTOM_SLURM:
				return "sacct -p -o 'JOBID,State,ExitCode' -n -j ";
			default:
				return "qstat";
		}
	}
}