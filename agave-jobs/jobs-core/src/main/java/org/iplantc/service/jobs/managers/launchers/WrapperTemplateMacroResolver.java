package org.iplantc.service.jobs.managers.launchers;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.iplantc.service.apps.model.Software;
import org.iplantc.service.common.persistence.TenancyHelper;
import org.iplantc.service.common.util.TimeUtils;
import org.iplantc.service.jobs.Settings;
import org.iplantc.service.jobs.exceptions.JobMacroResolutionException;
import org.iplantc.service.jobs.model.Job;
import org.iplantc.service.jobs.model.enumerations.JobStatusType;
import org.iplantc.service.jobs.model.enumerations.WrapperTemplateAttributeVariableType;
import org.iplantc.service.jobs.model.enumerations.WrapperTemplateStatusVariableType;
import org.iplantc.service.jobs.util.Slug;
import org.iplantc.service.systems.model.BatchQueue;
import org.iplantc.service.systems.model.ExecutionSystem;
import org.joda.time.DateTime;

import javax.validation.constraints.NotNull;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Class to handle resolution of the job macros available in the wrapper template.
 */
public class WrapperTemplateMacroResolver {

    private static final Logger log = Logger.getLogger(WrapperTemplateMacroResolver.class);
    private static final String EMPTY_CALLBACK_MACRO_REGEX = "(?s).*\\$\\{AGAVE_JOB_CALLBACK_NOTIFICATION\\}.*";
    private static final String DEFAULT_CALLBACK_MACRO_REGEX = "(?s)(?:.*)?(?:(\\$\\{AGAVE_JOB_CALLBACK_NOTIFICATION\\|(?:([a-zA-Z0-9_,\\s]*))\\}))(?:.*)?";
    private static final String CUSTOM_CALLBACK_MACRO_REGEX = "(?s)(?:.*)?(?:(\\$\\{AGAVE_JOB_CALLBACK_NOTIFICATION\\|(?:(\\s*[a-zA-Z0-9_]*\\s*))\\|(?:([a-zA-Z0-9_,\\s]*))\\}))(?:.*)?";

    private final Job job;
    private final ExecutionSystem executionSystem;

    public WrapperTemplateMacroResolver(@NotNull Job job, ExecutionSystem executionSystem) {
        this.job = job;
        this.executionSystem = executionSystem;
    }

    /**
     * The raw {@code wrapperTemplate} generated from the {@link Software} asset to run as the executable on the
     * remote system. This method resolves any macros in path (not the actual file), and returns it for use building
     * the submit command.
     *
     * @param wrapperTemplate the template to resolve
     * @return the resolved {@code wrapperTemplate} filtered for any job macros
     * @throws JobMacroResolutionException when the execution system or batch queue is not available
     */
    public String resolve(String wrapperTemplate) throws JobMacroResolutionException
    {
        String resolvedTemplate = resolveJobAttributeMacros(wrapperTemplate);
        resolvedTemplate = resolveJobStatusMacros(resolvedTemplate);

        return resolvedTemplate == null ? "" : resolvedTemplate;
//
//        try {
//            ExecutionSystem executionSystem = getExecutionSystem();
//
//            if (StringUtils.isBlank(executionSystem.getStartupScript())) {
//                return null;
//            }
//            else {
//                String resolvedStartupScript = executionSystem.getStartupScript();
//
//                for (WrapperTemplateAttributeVariableType jobMacro: WrapperTemplateAttributeVariableType.values()) {
//                    resolvedStartupScript = StringUtils.replace(resolvedStartupScript, "${" + jobMacro.name() + "}", resolveJobAttributeMacros(getJob(), executionSystem, jobMacro));
//                }
//
//                for (WrapperTemplateStatusVariableType macro: WrapperTemplateStatusVariableType.values()) {
//                    wrapperTemplate = StringUtils.replace(wrapperTemplate, "${" + macro.name() + "}", macro.resolveForJob(getJob()));
//                }
//
//                return resolvedStartupScript;
//            }
//        } catch (SystemUnavailableException e) {
//            throw new JobMacroResolutionException("Execution system " + getJob().getSystem() +
//                    " is no longer available to resolve batch queue for job " + getJob().getUuid());
//        }
    }

    /**
     * Replaces all the {@link WrapperTemplateAttributeVariableType} macros in the given {@code wrapperTemplate}.
     *
     * @param wrapperTemplate the template to resolve
     * @return the {@code wrapperTemplate} file path on the remote system, filtered for any job macros
     * @throws JobMacroResolutionException when the execution system or batch queue is not available
     */
    public String resolveJobAttributeMacros(String wrapperTemplate) throws JobMacroResolutionException {
//        try {
            ExecutionSystem executionSystem = getExecutionSystem();

            if (StringUtils.isBlank(wrapperTemplate)) {
                return null;
            }
            else {
                String resolvedTemplate = wrapperTemplate;
                for (WrapperTemplateAttributeVariableType jobAttributeMacro : WrapperTemplateAttributeVariableType.values()) {
                    String escapedJobAttributeMacro = "${" + jobAttributeMacro.name() + "}";
                    // checking for existence first so we can skip resolving the value when not necessary
                    if (resolvedTemplate.contains(escapedJobAttributeMacro)) {
                        resolvedTemplate = StringUtils.replace(resolvedTemplate, escapedJobAttributeMacro, resolveJobAttributeMacro(executionSystem, jobAttributeMacro));
                    }
                }

                return resolvedTemplate;
            }
//        } catch (SystemUnavailableException e) {
//            throw new JobMacroResolutionException("Execution system " + getJob().getSystem() +
//                    " is no longer available to resolve batch queue for job " + getJob().getUuid());
//        }
    }

    /**
     * Replaces all the {@link WrapperTemplateStatusVariableType} macros in the given {@code wrapperTemplate}.
     *
     * @param wrapperTemplate the template to resolve
     * @return the {@code wrapperTemplate} file path on the remote system, filtered for any job macros
     * @throws JobMacroResolutionException when the execution system or batch queue is not available
     */
    public String resolveJobStatusMacros(String wrapperTemplate) throws JobMacroResolutionException {
//        try {
            ExecutionSystem executionSystem = getExecutionSystem();

            if (StringUtils.isBlank(wrapperTemplate)) {
                return null;
            }
            else {
                String resolvedTemplate = wrapperTemplate;
                for (WrapperTemplateStatusVariableType jobStatusCallbackMacro : WrapperTemplateStatusVariableType.values()) {
                    String escapedJobStatusCallbackMacro = "${" + jobStatusCallbackMacro.name() + "}";
                    // checking for existence first so we can skip resolving the value when not necessary
                    if (resolvedTemplate.contains(escapedJobStatusCallbackMacro)) {
                        resolvedTemplate = StringUtils.replace(resolvedTemplate, escapedJobStatusCallbackMacro, resolveJobCallbackMacro(executionSystem, jobStatusCallbackMacro));
                    }
                }

                return resolvedTemplate;
            }
//        } catch (SystemUnavailableException e) {
//            throw new JobMacroResolutionException("Execution system " + getJob().getSystem() +
//                    " is no longer available to resolve batch queue for job " + getJob().getUuid());
//        }
    }

    /**
     * Returns the {@link WrapperTemplateAttributeVariableType} value for the given job.
     *
     * @param executionSystem the job execution.
     * @param jobAttributeMacro the job attribute macro to resolve
     * @return resolved value of the variable.
     * @throws JobMacroResolutionException when the macro cannot be resolved. Generally this is due to an unavailable execution system
     */
    protected String resolveJobAttributeMacro(ExecutionSystem executionSystem, WrapperTemplateAttributeVariableType jobAttributeMacro) throws JobMacroResolutionException {
        switch (jobAttributeMacro) {
            case IPLANT_JOB_NAME:
            case AGAVE_JOB_NAME:
                return Slug.toSlug(getJob().getName());
            case AGAVE_JOB_NAME_RAW:
                return getJob().getName();
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
            case AGAVE_JOB_SUBMIT_TIME:
                return getJob().getSubmitTime() == null ? "" : new DateTime(getJob().getSubmitTime()).toString();
            case AGAVE_JOB_ARCHIVE_SYSTEM:
                return (getJob().getArchiveSystem() == null ? "" : getJob().getArchiveSystem().getSystemId());
            case AGAVE_JOB_ARCHIVE_PATH:
                return (StringUtils.isNotEmpty(getJob().getArchivePath()) ? getJob().getArchivePath() : "");
            case AGAVE_JOB_NODE_COUNT:
                return String.valueOf(getJob().getNodeCount());
            case IPLANT_CORES_REQUESTED:
                return String.valueOf(getJob().getProcessorsPerNode() * getJob().getNodeCount());
            case AGAVE_JOB_PROCESSORS_PER_NODE:
                return String.valueOf(getJob().getProcessorsPerNode());
            case AGAVE_JOB_MEMORY_PER_NODE:
                return String.valueOf(getJob().getMemoryPerNode());
            case AGAVE_JOB_MAX_RUNTIME:
                return String.valueOf(getJob().getMaxRunTime());
            case AGAVE_JOB_MAX_RUNTIME_MILLISECONDS:
                try {
                    return String.valueOf(TimeUtils.getMillisecondsForMaxTimeValue(getJob().getMaxRunTime()));
                } catch (Exception e) {
                    return "0";
                }
            case AGAVE_JOB_MAX_RUNTIME_SECONDS:
                try {
                    return String.valueOf(Math.floor(TimeUtils.getMillisecondsForMaxTimeValue(getJob().getMaxRunTime()) / 1000));
                } catch (Exception e) {
                    return "0";
                }
            case AGAVE_BASE_URL:
                return TenancyHelper.resolveURLToCurrentTenant("https://example.com/", getJob().getTenantId());
            case AGAVE_JOB_OWNER:
                return getJob().getOwner();
            case AGAVE_JOB_TENANT:
                return getJob().getTenantId();
            case AGAVE_JOB_ARCHIVE:
                return getJob().isArchiveOutput() ? "1" : "";
            case AGAVE_JOB_WORK_PATH:
                return getJob().getWorkPath();
            case AGAVE_JOB_ARCHIVE_URL:
                if (getJob().isArchiveOutput()) {
                    return String.format("%smedia/system/%s/%s",
                            TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_IO_SERVICE, getJob().getTenantId()),
                            getJob().getArchiveSystem().getSystemId(),
                            getJob().getArchivePath());
                } else {
                    return "";
                }
            default:
                throw new JobMacroResolutionException("The template variable " + jobAttributeMacro.name() + " is not yet supported.");
        }
    }

    /**
     * Resolves the {@link WrapperTemplateStatusVariableType} for the given job into bash commands that will be run on
     * the remote host within the wrapper script during the life of the job. These represent
     * {@code AGAVE_JOB_CALLBACK_<JOB STATUS>} macros in the wrapper template. Tenancy is honored with respect to the
     * system and job.
     *
     * @param executionSystem the job execution.
     * @param jobStatusMacro the job attribute macro to resolve
     * @return resolved value of the variable.
     * @throws JobMacroResolutionException when the macro cannot be resolved. Generally this is due to an unavailable execution system
     */
    protected String resolveJobCallbackMacro(ExecutionSystem executionSystem, WrapperTemplateStatusVariableType jobStatusMacro) throws JobMacroResolutionException {
        if (jobStatusMacro == WrapperTemplateStatusVariableType.AGAVE_JOB_CALLBACK_NOTIFICATION) {
            return resolveNotificationEventMacro(null, null);
        } else {
            return String.format("agave_log_response $(curl --connect-timeout 10 -sSk \"%strigger/job/%s/token/%s/status/%s?filter=id,status\" 2>&1) \n\n\n",
                    resolveTenantJobUrl(getJob().getTenantId()),
                    getJob().getUuid(),
                    getJob().getUpdateToken(),
                    jobStatusMacro.getStatus().name());
        }
    }

    /**
     * Resolves the runtime notifications defined by the user in their wrapper templates. This includes all notifications
     * that match the following forms:
     * <ul>
     *     <li>{@code JOB_RUNTIME_CALLBACK_EVENT}: an empty callback equal to a {@link JobStatusType#HEARTBEAT}  callback.</li>
     *     <li>{@code JOB_RUNTIME_CALLBACK_EVENT:}</li>
     * </ul>
     * @param wrapperTemplate the resolved wrapper template
     * @return
     */
    public String resolveRuntimeNotificationMacros(String wrapperTemplate) {
        String resolvedTemplate = wrapperTemplate;

        Pattern emptyCallbackPattern = Pattern.compile(EMPTY_CALLBACK_MACRO_REGEX);
        Matcher callbackMatcher = emptyCallbackPattern.matcher(resolvedTemplate);
        while (callbackMatcher.matches()) {
            String callbackSnippet = resolveNotificationEventMacro(
                    "JOB_RUNTIME_CALLBACK_EVENT", new String[]{});

            resolvedTemplate = StringUtils.replace(resolvedTemplate, callbackMatcher.group(0), callbackSnippet);

            callbackMatcher = emptyCallbackPattern.matcher(resolvedTemplate);
        }

        // process the notification template first so there is no confusion or namespace conflict prior to resolution
        Pattern defaultCallbackPattern = Pattern.compile(DEFAULT_CALLBACK_MACRO_REGEX);
        callbackMatcher = defaultCallbackPattern.matcher(resolvedTemplate);
        while (callbackMatcher.matches()) {
            String callbackSnippet = resolveNotificationEventMacro(
                    "JOB_RUNTIME_CALLBACK_EVENT", StringUtils.split(callbackMatcher.group(2), ","));

            resolvedTemplate = StringUtils.replace(resolvedTemplate, callbackMatcher.group(1), callbackSnippet);

            callbackMatcher = defaultCallbackPattern.matcher(resolvedTemplate);
        }

        Pattern customCallbackPattern = Pattern.compile(CUSTOM_CALLBACK_MACRO_REGEX);
        callbackMatcher = customCallbackPattern.matcher(resolvedTemplate);
        while (callbackMatcher.matches()) {
            String callbackSnippet = resolveNotificationEventMacro(
                    callbackMatcher.group(2), StringUtils.split(callbackMatcher.group(3), ","));

            resolvedTemplate = StringUtils.replace(resolvedTemplate, callbackMatcher.group(1), callbackSnippet);

            callbackMatcher = customCallbackPattern.matcher(resolvedTemplate);
        }

        return resolvedTemplate;
    }

    /**
     * Resolves a template variable with optional runtime arguments supplied which will then
     * be passed as a json object in a postback to the trigger service. Syntax is of the form
     * as <pre>${AGAVE_JOB_CALLBACK_NOTIFICATION::FOO,BAR,BAM}</pre>
     *
     * @param eventName the name of the event
     * @param callbackVariableNames the callback variable names to honor
     * @return a bash snippet containing callback functions to run on the remote host within the wrapper script during the life of the job.
     */
    public String resolveNotificationEventMacro(String eventName, String[] callbackVariableNames) {

        if (callbackVariableNames == null) {
            callbackVariableNames = new String[]{};
        }

        eventName = StringUtils.trimToNull(eventName);

        StringBuilder sb = new StringBuilder();

        sb.append("# Building job callback document to send to any notifications\n");
        sb.append("AGAVE_CALLBACK_FILE=\".callback-$(date +%Y-%m-%dT%H:%M:%S%z)\"\n");
        sb.append("echo -e \"{\" > \"$AGAVE_CALLBACK_FILE\"\n");
        if (!ArrayUtils.isEmpty(callbackVariableNames)) {
            for (String varName : new HashSet<String>(Arrays.asList(callbackVariableNames))) {
                varName = StringUtils.trimToNull(varName);
                if (varName != null) {
                    String key = varName;
                    String value = varName;
                    // if there is a colon in the value, it's a custom name for the variable
                    // such as name:JOB_NAME
                    if (varName.contains(":")) {
                        String[] kv = StringUtils.split(varName, ":");
                        if (kv.length >= 2) {
                            key = kv[0];
                            value = kv[1];
                        }
                    }

                    sb.append(String.format("echo '  \"%s\": \"'$(printf %%q \"$%s\")'\",' >> \"$AGAVE_CALLBACK_FILE\"\n", key, value));
                }
            }
        }

        // add the user-defined event if provided
        if (StringUtils.isEmpty(eventName)) {
            eventName = "JOB_RUNTIME_CALLBACK_EVENT";
        }

        sb.append("echo '  \"CUSTOM_USER_JOB_EVENT_NAME\": \"" + eventName + "\"' >> \"$AGAVE_CALLBACK_FILE\"\n");

        sb.append("echo -e \"}\" >> \"$AGAVE_CALLBACK_FILE\"\n\n");
        String callbackUrl = resolveTenantJobUrl(getJob().getTenantId());
        sb.append(String.format("agave_log_response $(cat \"$AGAVE_CALLBACK_FILE\" | sed  -e \"s#: \\\"''\\\"#: \\\"\\\"#g\" | curl --connect-timeout 10 -sS%s -H \"Content-Type: application/json\" -X POST --data-binary @- \"%strigger/job/%s/token/%s/status/HEARTBEAT?pretty=true\" 2>&1) \n\n\n",
                callbackUrl.startsWith("https") ? "k" : "",
                callbackUrl,
                getJob().getUuid(),
                getJob().getUpdateToken()));

        return sb.toString();
    }

    /**
     * Users can include any of the {@link WrapperTemplateStatusVariableType#userAccessibleJobCallbackMacros()} in their
     * wrapper template. All other status callback values will cause issues in the proper execution and monitoring of
     * the job. This method removes all {@link WrapperTemplateStatusVariableType} that are not available to the user
     * from the wrapper template.
     * @param wrapperTemplate the wrapper template to filter
     * @return the wrapper with the reserved {@link WrapperTemplateStatusVariableType} macros removed
     */
    public String removeReservedJobStatusMacros(String wrapperTemplate) {
        List<WrapperTemplateStatusVariableType> userAccessibleStatuCallbacks = WrapperTemplateStatusVariableType.userAccessibleJobCallbackMacros();
        for (WrapperTemplateStatusVariableType jobCallbackMacro: WrapperTemplateStatusVariableType.values()) {
            if (!userAccessibleStatuCallbacks.contains(jobCallbackMacro)) {
                wrapperTemplate = StringUtils.replace(wrapperTemplate, "${" + jobCallbackMacro.name() + "}", "");
            }
        }

        return wrapperTemplate;
    }

    /**
     * Resolves the job url for the job tenant, replacing the generic url with the tenant base url.
     * @param tenantId the id of the tenant against which to evaluate the job url
     * @return the url of the job for the given tenant
     */
    protected String resolveTenantJobUrl(String tenantId) {
        return TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_JOB_SERVICE, tenantId);
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
    protected ExecutionSystem getExecutionSystem() { //throws SystemUnavailableException {
//        if (executionSystem == null) {
//            this.executionSystem = new JobManager().getJobExecutionSystem(getJob());
//        }
        return executionSystem;
    }
}
