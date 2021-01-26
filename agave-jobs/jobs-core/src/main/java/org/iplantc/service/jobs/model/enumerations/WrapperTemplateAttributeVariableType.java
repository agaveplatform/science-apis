package org.iplantc.service.jobs.model.enumerations;

public enum WrapperTemplateAttributeVariableType implements WrapperTemplateVariableType
{
	// job value macros
	IPLANT_JOB_NAME,
	AGAVE_JOB_NAME,
	AGAVE_JOB_NAME_RAW,
	AGAVE_JOB_ID,
	AGAVE_JOB_APP_ID,
	AGAVE_JOB_EXECUTION_SYSTEM,
	AGAVE_JOB_BATCH_QUEUE,
	AGAVE_JOB_BATCH_QUEUE_EFFECTIVE_NAME,
	AGAVE_JOB_SUBMIT_TIME,
	AGAVE_JOB_ARCHIVE_SYSTEM,
	AGAVE_JOB_ARCHIVE_PATH,
	AGAVE_JOB_NODE_COUNT,
	IPLANT_CORES_REQUESTED,
	AGAVE_JOB_PROCESSORS_PER_NODE,
	AGAVE_JOB_MEMORY_PER_NODE,
	AGAVE_JOB_MAX_RUNTIME,
	AGAVE_JOB_MAX_RUNTIME_MILLISECONDS,
	AGAVE_JOB_MAX_RUNTIME_SECONDS,
	AGAVE_JOB_ARCHIVE_URL,
	AGAVE_JOB_WORK_PATH,

	AGAVE_JOB_OWNER,
	AGAVE_JOB_TENANT,
	AGAVE_BASE_URL,
	AGAVE_JOB_ARCHIVE;
}
