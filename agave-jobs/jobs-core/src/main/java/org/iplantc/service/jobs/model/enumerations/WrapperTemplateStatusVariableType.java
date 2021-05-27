package org.iplantc.service.jobs.model.enumerations;

import java.util.List;

public enum WrapperTemplateStatusVariableType implements WrapperTemplateVariableType {
    // status macros
    IPLANT_RUNNING(JobStatusType.RUNNING),
    AGAVE_JOB_CALLBACK_RUNNING(JobStatusType.RUNNING),
    IPLANT_SUCCESS(JobStatusType.FINISHED),
    AGAVE_JOB_CALLBACK_SUCCESS(JobStatusType.FINISHED),
    IPLANT_CLEANING_UP(JobStatusType.CLEANING_UP),
    AGAVE_JOB_CALLBACK_CLEANING_UP(JobStatusType.CLEANING_UP),
    IPLANT_FAILURE(JobStatusType.FAILED),
    AGAVE_JOB_CALLBACK_FAILURE(JobStatusType.FAILED),
    IPLANT_ARCHIVING_START(JobStatusType.ARCHIVING),
    AGAVE_JOB_CALLBACK_ARCHIVING_START(JobStatusType.ARCHIVING),
    IPLANT_ARCHIVING_SUCCESS(JobStatusType.ARCHIVING_FINISHED),
    AGAVE_JOB_CALLBACK_ARCHIVING_SUCCESS(JobStatusType.ARCHIVING_FINISHED),
    IPLANT_ARCHIVING_FAILURE(JobStatusType.ARCHIVING_FAILED),
    AGAVE_JOB_CALLBACK_ARCHIVING_FAILURE(JobStatusType.ARCHIVING_FAILED),

    AGAVE_JOB_CALLBACK_ALIVE(JobStatusType.HEARTBEAT),
    AGAVE_JOB_CALLBACK_NOTIFICATION(JobStatusType.HEARTBEAT);

    private JobStatusType status = null;

    WrapperTemplateStatusVariableType(JobStatusType status) {
        this.status = status;
    }


    /**
     * These are the white listed status callbacks users can add in their wrapper templates
     * to be called by the API at runtime. Anything not in this list will be removed when
     * generating the job ipcexe file.
     *
     * @return List of {@link WrapperTemplateStatusVariableType} approved for use by users.
     */
    public static List<WrapperTemplateStatusVariableType> userAccessibleJobCallbackMacros() {
        return List.of(
                IPLANT_FAILURE,
                AGAVE_JOB_CALLBACK_FAILURE,
                AGAVE_JOB_CALLBACK_ALIVE,
                AGAVE_JOB_CALLBACK_NOTIFICATION);
    }

    /**
     * @return The {@link JobStatusType} corresponding to this macro.
     */
    public JobStatusType getStatus() {
        return status;
    }
}
