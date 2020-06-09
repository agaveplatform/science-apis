package org.iplantc.service.jobs.model.enumerations;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.hibernate.cfg.NotYetImplementedException;
import org.iplantc.service.common.dao.TenantDao;
import org.iplantc.service.common.exceptions.TenantException;
import org.iplantc.service.common.model.Tenant;
import org.iplantc.service.common.persistence.TenancyHelper;
import org.iplantc.service.common.util.TimeUtils;
import org.iplantc.service.jobs.Settings;
import org.iplantc.service.jobs.exceptions.JobMacroResolutionException;
import org.iplantc.service.jobs.model.Job;
import org.iplantc.service.jobs.util.Slug;
import org.iplantc.service.systems.dao.SystemDao;
import org.iplantc.service.systems.exceptions.SystemUnavailableException;
import org.iplantc.service.systems.model.BatchQueue;
import org.iplantc.service.systems.model.ExecutionSystem;
import org.iplantc.service.systems.util.ServiceUtils;
import org.joda.time.DateTime;

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
