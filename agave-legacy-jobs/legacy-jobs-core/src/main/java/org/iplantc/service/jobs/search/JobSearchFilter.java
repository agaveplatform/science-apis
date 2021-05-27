package org.iplantc.service.jobs.search;

import java.util.Date;
import java.util.Map;

import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.iplantc.service.common.search.AgaveResourceSearchFilter;
import org.iplantc.service.common.util.StringToTime;
import org.iplantc.service.jobs.model.enumerations.JobStatusType;
import org.joda.time.DateTime;

/**
 * This class is the basis for search support across the API.
 * Each service shold implement this class to filter query parameters
 * from the url into valid hibernate query.
 * 
 * @author dooley
 *
 */
public class JobSearchFilter extends AgaveResourceSearchFilter
{
	public JobSearchFilter() {}

	/* (non-Javadoc)
	 * @see org.iplantc.service.common.search.AgaveResourceSearchFilter#getSearchTermPrefix()
	 */
	@Override
	protected String getSearchTermPrefix() {
	    return "j.";
	}

    /* (non-Javadoc)
	 * @see org.iplantc.service.common.search.AgaveResourceSearchFilter#getSearchTermMappings()
	 */
	@Override
	public Map<String, String> getSearchTermMappings()
	{
		if (searchTermMappings.isEmpty()) {
			searchTermMappings.put("appid", "%sapp_id");
			searchTermMappings.put("archive", "%sarchive_output");
			searchTermMappings.put("archivepath", "%sarchive_path");
			searchTermMappings.put("archivesystem", "a.system_id ");
			searchTermMappings.put("remotequeue", "%sremote_queue");
			//searchTermMappings.put("charge", "%scharge");
			searchTermMappings.put("accepted", "%saccepted");
			searchTermMappings.put("created", "%screated");
			searchTermMappings.put("ended", "%sended");
			searchTermMappings.put("laststatusmessage", "%slast_message");
			searchTermMappings.put("executionsystem", "%system_id");
			searchTermMappings.put("id", "%suuid");
			searchTermMappings.put("inputs", "%sinputs");
			searchTermMappings.put("lastmodified", "%slast_updated");
			searchTermMappings.put("lastupdated", "%slast_updated");
			searchTermMappings.put("remotejobid", "%sremote_job_id");
			searchTermMappings.put("schedulerjobid", "%sremote_sched_id");
			searchTermMappings.put("maxhours", "time_to_sec(%smax_hours)");
			searchTermMappings.put("memorypernode", "%smemory_gb");
			searchTermMappings.put("name", "%sname");
			searchTermMappings.put("nodecount", "%snode_count");
			searchTermMappings.put("workpath", "%swork_path");
			searchTermMappings.put("owner", "%sowner");
			searchTermMappings.put("parameters", "%sparameters");
			searchTermMappings.put("processorspernode", "%sprocessor_count");
			searchTermMappings.put("submitretries", "%sremote_submit_retries");
			searchTermMappings.put("runtime", "%sremote_started is not null and (abs(unix_timestamp(%sended) - unix_timestamp(%sremote_started)))");
			searchTermMappings.put("remotestarted", "%sremote_started");
			searchTermMappings.put("status", "%sstatus");
			searchTermMappings.put("remotesubmitted", "%sremote_submitted");
			searchTermMappings.put("visible", "%svisible");
			searchTermMappings.put("walltime", "%sended is not null and (abs(unix_timestamp(%sended) - unix_timestamp(%screated)))");
		}
		
		return searchTermMappings;

	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public Map<String, Class> getSearchTypeMappings()
	{
		if (searchTypeMappings.isEmpty()) {
			searchTypeMappings.put("appid", String.class);
			searchTypeMappings.put("archive", Boolean.class);
			searchTypeMappings.put("archivepath", String.class);
			searchTypeMappings.put("archivesystem", String.class);
			searchTypeMappings.put("remotequeue", String.class);
			searchTypeMappings.put("accepted", Date.class);
			searchTypeMappings.put("created", Date.class);
			//searchTypeMappings.put("charge", Double.class);
			searchTypeMappings.put("ended", Date.class);
			searchTypeMappings.put("laststatusmessage", String.class);
			searchTypeMappings.put("executionsystem", String.class);
			searchTypeMappings.put("id", String.class);
			searchTypeMappings.put("inputs", String.class);
			searchTypeMappings.put("lastmodified", Date.class);
			searchTypeMappings.put("lastupdated", Date.class);
			searchTypeMappings.put("remotejobid", String.class);
			searchTypeMappings.put("schedulerjobid", String.class);
			searchTypeMappings.put("maxhours", String.class);
			searchTypeMappings.put("memorypernode", Float.class);
			searchTypeMappings.put("name", String.class);
			searchTypeMappings.put("nodecount", Integer.class);
			searchTypeMappings.put("workpath", String.class);
			searchTypeMappings.put("owner", String.class);
			searchTypeMappings.put("parameters", String.class);
			searchTypeMappings.put("processorspernode", Integer.class);
			searchTypeMappings.put("submitretries", Integer.class);
			searchTypeMappings.put("runtime", Long.class);
			searchTypeMappings.put("remotestarted", Date.class);
			searchTypeMappings.put("status", JobStatusType.class);
			searchTypeMappings.put("remotesubmitted", Date.class);
			searchTypeMappings.put("visible", Boolean.class);
			searchTypeMappings.put("walltime", Long.class);
		}
		
		return searchTypeMappings;

	}
	
	/* (non-Javadoc)
	 * @see org.iplantc.service.common.search.AgaveResourceSearchFilter#strongTypeSearchValue(java.lang.Class, java.lang.String, java.lang.String)
	 */
	@Override
	public Object strongTypeSearchValue(Class searchTermType, String searchField, String searchValue)
    throws IllegalArgumentException {
		if (searchTermType == Date.class) {
            Object time = StringToTime.date(searchValue);
            if (Boolean.FALSE.equals(time)) {
                if (NumberUtils.isDigits(searchValue)) {
                    try {
                        DateTime dateTime = new DateTime(Long.valueOf(searchValue));
                        return dateTime.toDate();
                    } catch (IllegalArgumentException e) {
                        throw new IllegalArgumentException("Illegal date format for " + searchField);
                    }
                } else {
                    try {
                        DateTime dateTime = new DateTime(searchValue);
                        return dateTime.toDate();
                    } catch (IllegalArgumentException e) {
                        throw new IllegalArgumentException("Illegal date format for " + searchField);
                    }
                }
            } else {
                return time;
            }
        } else if (searchTermType == Long.class) {
            if (NumberUtils.isNumber(searchValue)) {
                return NumberUtils.toLong(searchValue);
            } else {
                throw new IllegalArgumentException("Illegal integer value for " + searchField);
            }
        } else if (searchTermType == Integer.class) {
            if (NumberUtils.isNumber(searchValue)) {
                return NumberUtils.toInt(searchValue);
            } else {
                throw new IllegalArgumentException("Illegal integer value for " + searchField);
            }
        } else if (searchTermType == Double.class) {
            if (NumberUtils.isNumber(searchValue)) {
                return NumberUtils.toDouble(searchValue);
            } else {
                throw new IllegalArgumentException("Illegal decimal value for " + searchField);
            }
        } else if (searchTermType == Boolean.class) {
            return BooleanUtils.toBoolean(searchValue);
        } else if (searchTermType == JobStatusType.class) {
            try {
            	if (StringUtils.contains(searchValue, "*")) {
            		return searchValue;
            	}
            	else {
            		return JobStatusType.valueOf(StringUtils.upperCase(searchValue)).name();
            	}
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Unknown job status " + searchValue);
            }
        } else if (StringUtils.startsWithIgnoreCase(searchField, "maxhours")) {
            if (NumberUtils.isNumber(searchValue)) {
                return NumberUtils.toInt(searchValue, 0);
            } else {
                String[] hms = StringUtils.split(searchValue, ":");
                int secs = 0;
                if (hms != null) 
                {
                    if (hms.length > 2) {
                        secs += NumberUtils.toInt(hms[2], 0) * 3600;
                    }
                    
                    if (hms.length > 1) {
                        secs += NumberUtils.toInt(hms[1], 0) * 60;
                    }
                
                    secs += NumberUtils.toInt(hms[0], 0);
                }
                return secs;
            }
        } else {
            return searchValue;
        }
    }
}

