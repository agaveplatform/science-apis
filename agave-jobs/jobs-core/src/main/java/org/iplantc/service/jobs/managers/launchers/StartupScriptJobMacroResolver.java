package org.iplantc.service.jobs.managers.launchers;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.hibernate.cfg.NotYetImplementedException;
import org.iplantc.service.jobs.exceptions.JobMacroResolutionException;
import org.iplantc.service.jobs.model.Job;
import org.iplantc.service.jobs.model.enumerations.StartupScriptJobVariableType;
import org.iplantc.service.jobs.util.Slug;
import org.iplantc.service.systems.model.BatchQueue;
import org.iplantc.service.systems.model.ExecutionSystem;
import org.iplantc.service.systems.model.enumerations.StartupScriptSystemVariableType;

import javax.validation.constraints.NotNull;

/**
 * Class to handle resolution of the job macros available in the wrapper template, startup script, job request, etc.
 */
public class StartupScriptJobMacroResolver {

    private static final Logger log = Logger.getLogger(StartupScriptJobMacroResolver.class);

    private final Job job;
    private final ExecutionSystem executionSystem;

    public StartupScriptJobMacroResolver(@NotNull Job job, ExecutionSystem executionSystem) {
        this.job = job;
        this.executionSystem = executionSystem;
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
//        try {
            ExecutionSystem executionSystem = getExecutionSystem();
            String startupScript = executionSystem.getStartupScript();
            if (StringUtils.isBlank(startupScript)) {
                return null;
            }
            else {
                String resolvedStartupScript = startupScript;

                for (StartupScriptSystemVariableType systemMacro: StartupScriptSystemVariableType.values()) {
                    String escapedSystemMacro = "${" + systemMacro.name() + "}";
                    // checking for existence first so we can skip resolving the value when not necessary
                    if (resolvedStartupScript.contains(escapedSystemMacro)) {
                        resolvedStartupScript = StringUtils.replace(resolvedStartupScript, escapedSystemMacro, resolveStartupScriptSystemMacros(executionSystem, systemMacro));
                    }
                }

                for (StartupScriptJobVariableType jobMacro: StartupScriptJobVariableType.values()) {
                    String escapedJobMacro = "${" + jobMacro.name() + "}";
                    // checking for existence first so we can skip resolving the value when not necessary
                    if (resolvedStartupScript.contains(escapedJobMacro)) {
                        resolvedStartupScript = StringUtils.replace(resolvedStartupScript, escapedJobMacro, resolveStartupScriptJobMacro(getJob(), executionSystem, jobMacro));
                    }
                }

                return resolvedStartupScript;
            }
//        } catch (SystemUnavailableException e) {
//            throw new JobMacroResolutionException("Execution system " + getJob().getSystem() +
//                    " is no longer available to resolve batch queue for job " + getJob().getUuid());
//        }
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
                return Slug.toSlug(getJob().getName());
            case AGAVE_JOB_ID:
                return StringUtils.isNotEmpty(getJob().getUuid()) ? getJob().getUuid() : "";
            case AGAVE_JOB_APP_ID:
                return getJob().getSoftwareName();
            case AGAVE_JOB_EXECUTION_SYSTEM:
                return StringUtils.isNotEmpty(getJob().getSystem()) ? getJob().getSystem() : "";
            case AGAVE_JOB_BATCH_QUEUE:
                return getJob().getBatchQueue();
            case AGAVE_JOB_BATCH_QUEUE_EFFECTIVE_NAME:
                BatchQueue queue = executionSystem.getQueue(getJob().getBatchQueue());
                if (queue != null) {
                    return queue.getEffectiveMappedName();
                } else {
                    throw new JobMacroResolutionException("Batch queue " + getJob().getBatchQueue() +
                            " is no longer available on system " + getJob().getSystem() +
                            " to resolve effective batch queue name for job " + getJob().getUuid());
                }
            case AGAVE_JOB_ARCHIVE_PATH:
                return (StringUtils.isNotEmpty(getJob().getArchivePath()) ? getJob().getArchivePath() : "");
            case AGAVE_JOB_OWNER:
                return getJob().getOwner();
            case AGAVE_JOB_TENANT:
                return getJob().getTenantId();
            case AGAVE_JOB_WORK_PATH:
                return getJob().getWorkPath();
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
     */
    protected ExecutionSystem getExecutionSystem() {
        return executionSystem;
    }
}
