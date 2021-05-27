/**
 *
 */
package org.iplantc.service.jobs.model.scripts;

import org.iplantc.service.apps.model.Software;
import org.iplantc.service.jobs.model.Job;
import org.iplantc.service.systems.model.ExecutionSystem;

import static org.iplantc.service.systems.model.enumerations.SchedulerType.*;

/**
 * Factory for instantiating a {@link SubmitScript} which contains the scheduler directives
 * for submitting a job to a remote {@link org.iplantc.service.systems.model.enumerations.SchedulerType}
 *
 * @author dooley
 *
 */
public class SubmitScriptFactory {

    /**
     * Factory method to get an instance of the appropriate {@link SubmitScript} for the given job.
     *
     * @param job the job for which the submit script is being created
     * @param software the app being run by the job
     * @param executionSystem the system on which the app will be run
     * @return instance of the {@link SubmitScript} for the {@code job}
     */
    public static SubmitScript getInstance(Job job, Software software, ExecutionSystem executionSystem) {
        if (SLURM == job.getSchedulerType()) {
            return new SlurmSubmitScript(job, software, executionSystem);
        } else if (CUSTOM_SLURM == job.getSchedulerType()) {
            return new CustomSlurmSubmitScript(job, software, executionSystem);
        } else if (PBS == job.getSchedulerType()) {
            return new PbsSubmitScript(job, software, executionSystem);
        } else if (CUSTOM_PBS == job.getSchedulerType()) {
            return new CustomPbsSubmitScript(job, software, executionSystem);
        } else if (TORQUE == job.getSchedulerType() || MOAB == job.getSchedulerType()) {
            return new TorqueSubmitScript(job, software, executionSystem);
        } else if (CUSTOM_TORQUE == job.getSchedulerType() || CUSTOM_MOAB == job.getSchedulerType()) {
            return new CustomTorqueSubmitScript(job, software, executionSystem);
        } else if (CONDOR == job.getSchedulerType()) {
            return new CondorSubmitScript(job, software, executionSystem);
        } else if (CUSTOM_CONDOR == job.getSchedulerType()) {
            return new CustomCondorSubmitScript(job, software, executionSystem);
        } else if (LSF == job.getSchedulerType()) {
            return new LsfSubmitScript(job, software, executionSystem);
        } else if (CUSTOM_LSF == job.getSchedulerType()) {
            return new CustomLsfSubmitScript(job, software, executionSystem);
        } else if (LOADLEVELER == job.getSchedulerType()) {
            return new LoadLevelerSubmitScript(job, software, executionSystem);
        } else if (CUSTOM_LOADLEVELER == job.getSchedulerType()) {
            return new CustomLoadLevelerSubmitScript(job, software, executionSystem);
        } else if (SGE == job.getSchedulerType()) {
            return new SgeSubmitScript(job, software, executionSystem);
        } else if (CUSTOM_GRIDENGINE == job.getSchedulerType()) {
            return new CustomGridEngineSubmitScript(job, software, executionSystem);
        }

        return new ForkSubmitScript(job, software, executionSystem);
    }

}
