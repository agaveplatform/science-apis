package org.iplantc.service.jobs.managers.launchers;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.hibernate.cfg.NotYetImplementedException;
import org.iplantc.service.jobs.exceptions.JobMacroResolutionException;
import org.iplantc.service.jobs.managers.JobManager;
import org.iplantc.service.jobs.model.Job;
import org.iplantc.service.jobs.model.enumerations.StartupScriptJobVariableType;
import org.iplantc.service.jobs.util.Slug;
import org.iplantc.service.systems.exceptions.SystemUnavailableException;
import org.iplantc.service.systems.model.BatchQueue;
import org.iplantc.service.systems.model.ExecutionSystem;
import org.iplantc.service.systems.model.enumerations.StartupScriptSystemVariableType;

/**
 * Class to handle resolution of the job macros available in the wrapper template, startup script, job request, etc.
 */
public class StartupScriptJobMacroResolver {

    private static final Logger log = Logger.getLogger(StartupScriptJobMacroResolver.class);

    private final Job job;
    private ExecutionSystem executionSystem;

    public StartupScriptJobMacroResolver(Job job) {
        this.job = job;
    }

    /**
     * The {@code startupScript} is a optional path to a file residing on, and defined by, the {@link ExecutionSystem}
     * of this job. The path may include job macros similar to the app wrapper template. This method resolves any
     * macros in path (not the actual file), and returns it for use building the submit command.
     *
     * @return the {@code startupScript} file path on the remote system, filtered for any job macros
     * @throws JobMacroResolutionException when the execution system or batch queue is not available
     */
    public String resolve() throws JobMacroResolutionException
    {
        try {
            ExecutionSystem executionSystem = getExecutionSystem();

            if (StringUtils.isBlank(executionSystem.getStartupScript())) {
                return null;
            }
            else {
                String resolvedStartupScript = executionSystem.getStartupScript();

                for (StartupScriptSystemVariableType systemMacro: StartupScriptSystemVariableType.values()) {
                    resolvedStartupScript = StringUtils.replace(resolvedStartupScript, "${" + systemMacro.name() + "}", resolveStartupScriptSystemMacros(executionSystem, systemMacro));
                }

                for (StartupScriptJobVariableType jobMacro: StartupScriptJobVariableType.values()) {
                    resolvedStartupScript = StringUtils.replace(resolvedStartupScript, "${" + jobMacro.name() + "}", resolveStartupScriptJobMacro(getJob(), executionSystem, jobMacro));
                }

                return resolvedStartupScript;
            }
        } catch (SystemUnavailableException e) {
            throw new JobMacroResolutionException("Execution system " + job.getSystem() +
                    " is no longer available to resolve batch queue for job " + job.getUuid());
        }
    }

    /**
     * Resolves {@link StartupScriptJobVariableType} variable macros in the {@link ExecutionSystem#getStartupScript()}
     * value for the job's execution system. Tenancy is honored with respect to the system and job.
     *
     * @param job the job for which to resolve execution system
     * @param startupScriptVariable the {@link StartupScriptJobVariableType} to
     * @return resolved value of the variable.
     * @throws JobMacroResolutionException when the macro cannot be resolved. Generally this is due to an unavailable execution system
     */
    protected String resolveStartupScriptJobMacro(Job job, ExecutionSystem executionSystem, StartupScriptJobVariableType startupScriptVariable) throws JobMacroResolutionException {
        switch (startupScriptVariable) {
            case AGAVE_JOB_NAME:
                return Slug.toSlug(job.getName());
            case AGAVE_JOB_ID:
                return StringUtils.isNotEmpty(job.getUuid()) ? job.getUuid() : "";
            case AGAVE_JOB_APP_ID:
                return job.getSoftwareName();
            case AGAVE_JOB_EXECUTION_SYSTEM:
                return StringUtils.isNotEmpty(job.getSystem()) ? job.getSystem() : "";
            case AGAVE_JOB_BATCH_QUEUE:
                return job.getBatchQueue();
            case AGAVE_JOB_BATCH_QUEUE_EFFECTIVE_NAME:
                BatchQueue queue = executionSystem.getQueue(job.getBatchQueue());
                if (queue != null) {
                    return queue.getEffectiveMappedName();
                } else {
                    throw new JobMacroResolutionException("Batch queue " + job.getBatchQueue() +
                            " is no longer available on system " + job.getSystem() +
                            " to resolve effective batch queue name for job " + job.getUuid());
                }
            case AGAVE_JOB_ARCHIVE_PATH:
                return (StringUtils.isNotEmpty(job.getArchivePath()) ? job.getArchivePath() : "");
            case AGAVE_JOB_OWNER:
                return job.getOwner();
            case AGAVE_JOB_TENANT:
                return job.getTenantId();
            case AGAVE_JOB_WORK_PATH:
                return job.getWorkPath();
            default:
                throw new NotYetImplementedException("The startupScript variable " + startupScriptVariable.name() + " is not yet supported.");
        }
    }

    /**
     * Resolves job-specific macros in the {@link ExecutionSystem#getStartupScript()} value for the
     * system. Tenancy is honored with respect to the system and job.
     *
     * @param executionSystem the execution system to use to resolve the template variable
     * @param startupScriptVariable the {@link StartupScriptSystemVariableType} to resolve for the given {@code executionSystem}
     * @return resolved value of the exeuction system variable.
     * @throws JobMacroResolutionException when the macro cannot be resolved. Generally this is due to an unavailable execution system
     */
    protected String resolveStartupScriptSystemMacros(ExecutionSystem executionSystem, StartupScriptSystemVariableType startupScriptVariable) throws JobMacroResolutionException {
        return startupScriptVariable.resolveForSystem(executionSystem);
    }

    /**
     * @return the job whose startup script is being resolved
     */
    public Job getJob() {
        return this.job;
    }

    /**
     * Resolves {@link ExecutionSystem} for job.
     *
     * @return the job {@link ExecutionSystem}
     * @throws SystemUnavailableException if the system is no longer present or available
     * @see JobManager#getJobExecutionSystem(Job)
     */
    protected ExecutionSystem getExecutionSystem() throws SystemUnavailableException {
        if (executionSystem == null) {
            this.executionSystem = new JobManager().getJobExecutionSystem(getJob());
        }

        return executionSystem;
    }
}
