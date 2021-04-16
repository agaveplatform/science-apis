/**
 * 
 */
package org.iplantc.service.jobs.dao;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.hibernate.*;
import org.hibernate.transform.AliasToEntityMapResultTransformer;
import org.hibernate.transform.Transformers;
import org.hibernate.type.StandardBasicTypes;
import org.iplantc.service.apps.util.ServiceUtils;
import org.iplantc.service.common.persistence.HibernateUtil;
import org.iplantc.service.common.persistence.TenancyHelper;
import org.iplantc.service.common.search.AgaveResourceResultOrdering;
import org.iplantc.service.common.search.SearchTerm;
import org.iplantc.service.jobs.Settings;
import org.iplantc.service.jobs.exceptions.JobException;
import org.iplantc.service.jobs.model.Job;
import org.iplantc.service.jobs.model.dto.JobDTO;
import org.iplantc.service.jobs.model.enumerations.JobStatusType;
import org.iplantc.service.jobs.model.enumerations.PermissionType;
import org.iplantc.service.jobs.search.JobSearchFilter;
import org.iplantc.service.systems.model.BatchQueue;
import org.iplantc.service.systems.model.ExecutionSystem;
import org.joda.time.DateTime;

import java.math.BigInteger;
import java.util.*;

/**
 * @author dooley
 * 
 */
public class JobDao
{
    private static final Logger log = Logger.getLogger(JobDao.class);
    
    // The mapping below is used to skip printing sql query statements to the log when
    // the current query text is the same as the last query text.  The map's keys are
    // sql query names and the values are the hashes of the last logged query of each type.
    //
    // The job's sql polling query text is written to the log by setting the environment
    // variable that corresponds to the settings field for each polling query.  Here are
    // how the values are related:
    //
    //  Environment Variable           Settings Field            Polling Quartz Job
    //  --------------------           --------------            ------------------
    // iplant.debug.sql.pending.job    DEBUG_SQL_PENDING_JOB     StagingWatch, SubmissionWatch
    // iplant.debug.sql.executing.job  DEBUG_SQL_EXECUTING_JOB   MonitoringWatch
    // iplant.debug.sql.archiving.job  DEBUG_SQL_ARCHIVING_JOBv  ArchiveWatch
    // 
    // See the org.iplantc.service.jobs.Settings class for details about how environment
    // variable values are assigned to settings fields.  See the selectNextAvailableJob()
    // method on each of the watch classes to see how database polling is initiated.
    //
    // When sql query logging is enabled in this class, the last polling query text in the
    // log applies to all subsequent polling calls up until the next query text appears for 
    // that query type.  The idea is to avoid writing large amounts of redundant text to the
    // logs.
    //
    private enum SqlQueryEnum {PENDING_1, PENDING_2, STAGED_1, STAGED_2, EXECUTING, ARCHIVING_1, ARCHIVING_2}
    private static final HashMap<SqlQueryEnum,Integer> sqlQueryMap = initSqlQueryMap();
	
	protected static Session getSession() {
		Session session = HibernateUtil.getSession();
		HibernateUtil.beginTransaction();
        
		//session.clear();
		session.enableFilter("jobTenantFilter").setParameter("tenantId", TenancyHelper.getCurrentTenantId());
		return session;
	}
	
	protected static Session getSession(String tenantId) {
		Session session = HibernateUtil.getSession();
		HibernateUtil.beginTransaction();
        //session.clear();
		session.enableFilter("jobTenantActivityFilter").setParameter("tenantId", tenantId);
		return session;
	}
	
	/**
	 * Returns {@link Settings#DEFAULT_PAGE_SIZE} jobs for a given username.
	 *
	 * @param username the user making the query
	 * @return list of jobs for the user
	 * @throws JobException if the query cannot be completed
	 */
	public static List<Job> getByUsername(String username) throws JobException
	{
		return JobDao.getByUsername(username, 0, Settings.DEFAULT_PAGE_SIZE);
	}
	
	/**
	 * Returns all jobs for a given username.
	 * @param username the user making the query
	 * @param offset the number of records to skip in the result set
	 * @param limit the max records to return
	 * @return list of jobs for the user
	 * @throws JobException if the query cannot be completed
	 * @deprecated 
	 * @see #getByUsername(String, int, int, AgaveResourceResultOrdering, SearchTerm)
	 */
	public static List<Job> getByUsername(String username, int offset, int limit) 
	throws JobException
	{
		return getByUsername(username, offset, limit, null, null);
	}
	
	/**
	 * Returns all jobs for a given username with optional search result ordering. Permissions will be honored .
	 * @param username the username by which to filter results.
	 * @param offset the number of records to skip in the result set
	 * @param limit the maximum number of records  to return.
	 * @param order the direction to order
	 * @param orderBy the search field by which to order.
	 * @return list of jobs for the given user with pagination
	 * @throws JobException if unable to perform the query
	 */
	@SuppressWarnings("unchecked")
	public static List<Job> getByUsername(String username, int offset, int limit, AgaveResourceResultOrdering order, SearchTerm orderBy) 
	throws JobException
	{
		if (order == null) {
			order = AgaveResourceResultOrdering.DESC;
		}
		
		if (orderBy == null) {
			orderBy = new JobSearchFilter().filterAttributeName("lastupdated");
		}
		
		try
		{
			Session session = getSession();
			session.clear();
			String sql = "select distinct j.* \n"
					+ "from jobs j \n"
					+ "		left join job_permissions p on j.id = p.job_id \n"
					+ "where ( \n"
					+ "			j.owner = :owner \n"
					+ "			or ( \n"
					+ "				p.username = :owner \n" 
					+ "				and p.permission <> :none \n"  
					+ "			) \n" 
					+ "		) \n"
					+ "		and j.visible = :visible \n"
					+ "		and j.tenant_id = :tenantid \n"
					+ "order by " + String.format(orderBy.getMappedField(), orderBy.getPrefix()) + " " +  order.toString() + " \n";
			
			String q = sql;
			q = StringUtils.replace(q, ":owner", String.format("'%s'", username));
			q = StringUtils.replace(q, ":none", String.format("'%s'", PermissionType.NONE.name()));
			q = StringUtils.replace(q, ":visible", String.format("'%d'", 1));
			q = StringUtils.replace(q, ":tenantid", String.format("'%s'", TenancyHelper.getCurrentTenantId()));
			
//			log.debug(q);
			List<Job> jobs = session.createSQLQuery(sql).addEntity(Job.class)
					.setString("owner",username)
					.setString("none",PermissionType.NONE.name())
					.setInteger("visible", 1)
					.setString("tenantid", TenancyHelper.getCurrentTenantId())
					.setFirstResult(offset)
					.setMaxResults(limit)
					.list();
			
			session.flush();
			
			return jobs;

		} catch (ObjectNotFoundException e) {
			// not found is a valid result. return an empty list in that event
			return new ArrayList<Job>();
		} catch (HibernateException ex) {
			throw new JobException(ex);
		} finally {
			try { HibernateUtil.commitTransaction();} catch (Exception ignored) {}
		}
	}

	/**
	 * Gets a job by its database id. This method bypasses the tenant filter and is capable of
	 * querying across tenants.
	 * @param jobId the database id of the job for which to query
	 * @return the {@link Job} with the given {@code jobId} or {@code null} if no matching job found.
	 * @throws JobException if unable to perform the query
	 */
	public static Job getById(long jobId) throws JobException
	{
		try {
			Session session = getSession();
			session.disableFilter("jobTenantFilter");
			return (Job) session.get(Job.class, jobId);
		} catch (ObjectNotFoundException ex) {
            // not found is a valid result. return a null value
            return null;
		} catch (HibernateException ex) {
			throw new JobException(ex);
		} finally {
			try { HibernateUtil.commitTransaction();} catch (Exception ignored) {}
		}
	}
	
	/**
     * Gets a {@link Job} by its uuid, forcing the cache to flush.
     * 
     * * @param uuid agave uuid for the job
     * @return the {@link Job} with the given {@code uuid} or {@code null} if no matching job found.
     * @throws JobException if unable to perform the query.
     * @see #getByUuid(String, boolean)
     */
	public static Job getByUuid(String uuid) throws JobException
    {
	    return getByUuid(uuid, true);
    }
	
	/**
	 * Gets a {@link Job} by its uuid, optionally forcing cache eviction
	 * 
	 * @param uuid agave uuid for the job
	 * @param forceFlush should the cache be flushed on this request?
	 * @return the {@link Job} with the given {@code uuid} or {@code null} if no matching job found.
	 * @throws JobException if unable to perform the query.
	 */
	public static Job getByUuid(String uuid, boolean forceFlush) throws JobException
	{
	    // empty uuid should return null.
		if (StringUtils.isEmpty(uuid)) return null;
		
		try
		{
		    HibernateUtil.beginTransaction();
			Session session = HibernateUtil.getSession();
			session.clear();

			// force a query that bypasses the hibernate cache
			Job job = (Job) session.createSQLQuery("select * from jobs where uuid = :uuid")
			        .addEntity(Job.class)
					.setString("uuid",  uuid)
					.setCacheable(false)
					.setCacheMode(CacheMode.IGNORE)
                    .uniqueResult();
			
			if (forceFlush) session.flush();
			
			return job;
		} catch (ObjectNotFoundException ex) {
		    // if unable to find the object, just return null. this is a valid result.
			return null;
		} catch (HibernateException ex) {
			throw new JobException(ex);
		} finally {
            try { HibernateUtil.commitTransaction();} catch (Exception ignored) {}
		}
	}
	
	/**
	 * Refreshes a stale job. This is helpful to call after a failed concurrent modification update to get the
     * latest revision number for future updates.
	 * @param job the updated job to refresh
	 * @throws JobException if unable to refresh. This is usually due to a subsequent concurrent modification issue
	 */
	public static void refresh(Job job) throws JobException
	{
		try
		{
			Session session = getSession();
			session.clear();
			session.refresh(job);
			session.flush();
		} catch (HibernateException ex) {
		    log.error("Concurrency issue with job " + job.getUuid());
			throw new JobException(ex);
		}
		finally {
			try { HibernateUtil.commitTransaction();} catch (Exception ignored) {}
		}
	}
	
	/**
	 * Merges the current cached job object with the persisted value. The 
	 * current instance is not updated. The merged instance is returned.
	 * 
	 * @param job the updated job to merge with the db version
	 * @return merged job
	 * @throws JobException if unable to merge. This is usually due to a concurrent modification issue
	 */
	public static Job merge(Job job) throws JobException
	{
		try
		{
		    HibernateUtil.beginTransaction();
			Session session = HibernateUtil.getSession();
			return (Job)session.merge(job);
		}
		catch (HibernateException ex) {
		    log.error("Concurrency issue with job " + job.getUuid());
			throw new JobException(ex);
		}
	}
	
	/**
	 * Returns a job owned by the user with the given {@code jobId}. This is essentially
	 * just a security check to make sure the owner matches the job id. ACL are 
	 * not taken into consideration here, just ownership.
	 *
     * TODO: this should return null if no job was found, not throw an exception
	 * @param username the job owner's username for which to filter the job
	 * @param jobId database id of the job for which to lookup
	 * @return the job with the given id or {@code null}
	 * @throws JobException if unable to perform the query, or if the job is null
	 */
	public static Job getByUsernameAndId(String username, long jobId) throws JobException
	{
		try
		{
			Session session = getSession();
			session.clear();
			String hql = "from Job j "
					+ "where j.owner = :owner and "
					+ "		j.id = :jid and "
					+ "		j.visible = :visible ";
			
			Job job = (Job)session.createQuery(hql)
					.setString("owner",username)
					.setLong("jid", jobId)
					.setBoolean("visible", Boolean.TRUE)
					.setMaxResults(1)
					.uniqueResult();
			
			session.flush();
			
			if (job == null) {
				throw new JobException("No job found matching the given owner and id.");
			} else {
				return job;
			}

        } catch (ObjectNotFoundException ex) {
            // if unable to find the object, just return null. this is a valid result.
            return null;
        } catch (HibernateException ex) {
            throw new JobException(ex);
		} finally {
			try { HibernateUtil.commitTransaction();} catch (Exception ignored) {}
		}
	}

	/**
	 * Returns a {@link List} of {@link Job}s belonging to the given user with the given status.
	 * Permissions are observed in this query.
	 * 
	 * @param username the user for whom to query for jobs
	 * @param jobStatus the status of the user jobs for which to query
	 * @return list of jobs for the user with the given status.
	 * @throws JobException if unable to perform the query
	 */
	@SuppressWarnings("unchecked")
	public static List<Job> getByUsernameAndStatus(String username, JobStatusType jobStatus) 
	throws JobException
	{
		try
		{
			Session session = getSession();
			session.clear();
			String sql = "select distinct j.* from jobs j " +
					"left join job_permissions p on j.id = p.job_id " +
					"where ( " + 
					"        j.owner = :owner " +
					"        or 1 = :isadmin " +
					"		 or ( " + 
					"             p.username = :owner " +
					"			  and p.permission <> :none " +
					"		    ) " + 
					"		) and " +
					"		j.visible = :visible and " +
					"		j.status = :status " + 
					"order by j.last_updated desc ";
			
			List<Job> jobs = session.createSQLQuery(sql).addEntity(Job.class)
					.setString("owner",username)
					.setString("none",PermissionType.NONE.name())
					.setInteger("isadmin", BooleanUtils.toInteger(ServiceUtils.isAdmin(username)))
					.setBoolean("visible", true)
					.setString("status", jobStatus.name())
					.list();

			session.flush();
			
			return jobs;

        } catch (ObjectNotFoundException ex) {
            // if unable to find the object, just return empty list. this is a valid result.
            return new ArrayList<Job>();
        } catch (HibernateException ex) {
            throw new JobException(ex);
        } finally {
		  	try { HibernateUtil.commitTransaction();} catch (Exception ignored) {}
		}
	}
	
	
	/**
	 * Returns the number of jobs in an active state for a given user 
	 * on a given system. Active states are defined by 
	 * JobStatusType.getActiveStatuses().
	 * 
	 * @param username the user for whome to count active jobs
     * @param systemId The user-defined name of the system for which to count active jobs.
	 * @return the number of active jobs for {@code username} on the {@code systemId}}
	 * @throws JobException if unable to perform the query.
	 */
	public static long countActiveUserJobsOnSystem(String username, String systemId) throws JobException
	{
		try
		{
			Session session = getSession();
			session.clear();
			String hql = "select count(j.id) "
					+ "from Job j "
					+ "where j.owner = :jobowner "
					+ " 	and j.visible = :visible "
					+ "		and j.system = :jobsystem "
					+ "		and j.status in "
					+ "(" + JobStatusType.getActiveStatusValues() + ") ";
			
			long currentUserJobCount = (Long)session.createQuery(hql)
					.setBoolean("visible", true)
					.setString("jobowner",username)
					.setString("jobsystem", systemId)
					.uniqueResult();
			
			session.flush();
			
			return currentUserJobCount;

		}
		catch (HibernateException ex) {
			throw new JobException(ex);
		}
		finally {
			try { HibernateUtil.commitTransaction();} catch (Exception ignored) {}
		}
	}
	
	/**
	 * Returns the least recently checked active job for monitoring.
	 * 
	 * @return
	 * @throws JobException
	 */
	public static String getNextExecutingJobUuid(String tenantId, String[] owners, String[] systemIds) 
	throws JobException
	{
        // Tracing.
        if (Settings.DEBUG_SQL_EXECUTING_JOB && log.isDebugEnabled()) {
            StringBuilder buf = new StringBuilder(200);
            buf.append("JobDao.getNextExecutingJobUuid() input: tenantId=");
            buf.append(tenantId);
            buf.append(", owners=(");
            for (int i = 0; i < owners.length; i++) {
                if (i != 0) buf.append(", "); 
                buf.append(owners[i]);
            }
            buf.append("), systemIds=(");
            for (int i = 0; i < systemIds.length; i++) {
                if (i != 0) buf.append(", "); 
                buf.append(systemIds[i]);
            }
            buf.append(").");
            log.debug(buf.toString());
        }
        
	    boolean excludeTenant = false;
	    if (StringUtils.isEmpty(tenantId)) {
            tenantId = "%";
        } else if (tenantId.contains("!")) {
            excludeTenant = true;
            tenantId = StringUtils.removeStart(tenantId, "!");
        }
	    
	    boolean excludeOwners = false;
	    if (ArrayUtils.isNotEmpty(owners) && StringUtils.join(owners, ",").contains("!")) {
	        excludeOwners = true;
	        for (int i = 0; i < owners.length; i++)
                owners[i] = StringUtils.removeStart(owners[i], "!");
	    }
	    
	    boolean excludeSystems = false;
        if (ArrayUtils.isNotEmpty(systemIds) && StringUtils.join(systemIds, ",").contains("!")) {
            excludeSystems = true;
            for (int i = 0; i < systemIds.length; i++)
                systemIds[i] = StringUtils.removeStart(systemIds[i], "!");
        }
	    
		try
		{
			Session session = getSession();
			session.disableFilter("jobTenantFilter");
//			session.clear();
			/* we need a step function here to scale back slower...
			 * every 30 seconds for the first 5 minutes
			 * every minute for the next 30 minutes
			 * every 5 minutes for the next hour
			 * every 15 minutes for the next 12 hours
			 * every 30 minutes for the next 24 hours
			 * every hour for the next 14 days
			 */
			String sql =  "select j.uuid \n";
    			        
			if (!ArrayUtils.isEmpty(systemIds)) 
            {   
                sql += "from ( \n"
                     + "    select s.system_id, q.name as queue_name, s.tenant_id \n"
                     + "    from systems s left join batchqueues q on s.id = q.execution_system_id \n"
                     + "    where s.tenant_id :excludetenant like :tenantid and :excludesystems (\n";
                
                for (int i=0;i<systemIds.length;i++) {
                    
                    if (StringUtils.contains(systemIds[i], "#")) {
                        sql += "        (s.system_id = :systemid" + i + " and q.name = :queuename" + i + ") ";
                    } else {
                        sql += "        (s.system_id = :systemid" + i + ") ";
                    }
                    
                    if (systemIds.length > (i+1)) {
                        sql += " or \n ";
                    }
                }
                
                sql    += "\n        ) \n"
                        + "    ) e left join jobs j on j.execution_system = e.system_id \n"
                        + "        and j.queue_request = e.queue_name \n"
                        + "        and j.tenant_id = e.tenant_id ";
            } 
			else {
			    sql    += "from jobs j ";
			}
			
    			sql    += "\nwhere j.visible = 1 and j.created < j.last_updated + INTERVAL 60 DAY \n"
    					+ "    and (\n"
    					+ "        (j.status_checks < 4 and CURRENT_TIMESTAMP >= j.last_updated + INTERVAL 15 SECOND) or  \n"
    					+ "        (j.status_checks < 14 and CURRENT_TIMESTAMP >= j.last_updated + INTERVAL 30 SECOND) or  \n"
    					+ "        (j.status_checks < 44 and CURRENT_TIMESTAMP >= j.last_updated + INTERVAL 60 SECOND) or  \n"
    					+ "        (j.status_checks < 56 and CURRENT_TIMESTAMP >= j.last_updated + INTERVAL 5 MINUTE) or  \n"
    					+ "        (j.status_checks < 104 and CURRENT_TIMESTAMP >= j.last_updated + INTERVAL 15 MINUTE) or  \n"
    					+ "        (j.status_checks < 152 and CURRENT_TIMESTAMP >= j.last_updated + INTERVAL 30 MINUTE) or  \n"
    					+ "        (CURRENT_TIMESTAMP >= j.last_updated + INTERVAL 1 HOUR) \n"
    					+ "    )  \n"
    					+ "    and (\n"
    					+ "      j.status = :queuedstatus \n"
    					+ "      or j.status = :runningstatus \n"
    					+ "      or j.status = :pausedstatus \n"
    					+ "    ) \n"
    					+ "    and j.local_job_id is not null \n"
			            + "    and j.tenant_id :excludetenant like :tenantid \n";
    		
    		if (!ArrayUtils.isEmpty(owners)) {
    		    sql     += "    and j.owner :excludeowners in :owners  \n"; 
    		}
			    sql     += "order by rand()\n";
			
			sql = StringUtils.replace(sql, ":excludeowners", excludeOwners ? "not" : "");
			sql = StringUtils.replace(sql, ":excludetenant", excludeTenant ? "not" : "");
			sql = StringUtils.replace(sql, ":excludesystems", excludeSystems ? "not" : "");
    			
			// This parallel query string is for debugging purposes only.
            String q = StringUtils.replace(sql, ":queuedstatus", "'QUEUED'");
            q = StringUtils.replace(q, ":runningstatus", "'RUNNING'");
            q = StringUtils.replace(q, ":pausedstatus", "'PAUSED'");
            q = StringUtils.replace(q, ":tenantid", "'" + tenantId + "'");        
            
			Query query = session.createSQLQuery(sql)
					.setString("queuedstatus", JobStatusType.QUEUED.name())
					.setString("runningstatus", JobStatusType.RUNNING.name())
					.setString("pausedstatus", JobStatusType.PAUSED.name())
			        .setString("tenantid", tenantId);
			
			if (!ArrayUtils.isEmpty(owners))
			{
			    // remove leading negation if present
                for (int i = 0; i < owners.length; i++)
                    owners[i] = StringUtils.removeStart(owners[i], "!");
                
                query.setParameterList("owners", owners);
                
                q = StringUtils.replace(q, ":owners", "('" + StringUtils.join(owners, "','")+"')");
            }
            
            if (!ArrayUtils.isEmpty(systemIds)) 
            {
                // remove leading negation if present
                for (int i = 0; i < systemIds.length; i++)
                    systemIds[i] = StringUtils.removeStart(systemIds[i], "!");
                
                for (int i=0;i<systemIds.length;i++) {
                    if (StringUtils.contains(systemIds[i], "#")) {
                        String[] tokens = StringUtils.split(systemIds[i], "#");
                        query.setString("systemid" + i, tokens[0]);
                        q = StringUtils.replace(q, ":systemid"+i, "'" + tokens[0] +"'");
                        if (tokens.length > 1) {
                            query.setString("queuename" + i, tokens[1]); 
                            q = StringUtils.replace(q, ":queuename"+i, "'" + tokens[1] +"'");
                        }
                    }
                    else
                    {
                        query.setString("systemid" + i, systemIds[i]);
                        q = StringUtils.replace(q, ":systemid"+i, "'" + systemIds[i] +"'");
                    }
                }
            }
            
            // Print the separately constructed query string after parameter substitution.
            if (Settings.DEBUG_SQL_EXECUTING_JOB && log.isDebugEnabled() && 
                !sameAsLastQuery(SqlQueryEnum.EXECUTING, q))
            {
                log.debug("JobDao.getNextExecutingJobUuid() query: " + q);
                sqlQueryMap.put(SqlQueryEnum.EXECUTING, q.hashCode());
            }
			   
            String uuid = (String)query
			        .setCacheable(false)
                    .setCacheMode(CacheMode.REFRESH)
                    .setMaxResults(1)
                    .uniqueResult();
            
            if (Settings.DEBUG_SQL_EXECUTING_JOB && log.isDebugEnabled())
                log.debug("Next executing job is " + uuid + ".");
            return uuid;
            
		} catch (ObjectNotFoundException|StaleObjectStateException ex) {
            log.error("Unable to select monitoring job on tenant " + tenantId + " due to concurrency issue.", ex);
            return null;
        } catch (HibernateException ex) {
            throw new JobException(ex);
        }
		finally {
			try { HibernateUtil.commitTransaction();} catch (Exception ignored) {}
		}
	}
	
	/**
	 * Returns the total number of jobs in an active state 
	 * on a given system. Active states are defined by 
	 * JobStatusType.getActiveStatuses().
	 *
	 * @param systemId The user-defined name of the system for which to count active jobs.
	 * @return the number of current jobs for the system
	 * @throws JobException if unable to query the db
	 */
	public static long countActiveJobsOnSystem(String systemId) throws JobException
	{
		try
		{
			Session session = getSession();
			session.clear();
			String hql = "select count(*) from Job " +
					"where system = :jobsystem and " +
					"status in (" + JobStatusType.getActiveStatusValues() + ")";
			
			long currentUserJobCount = (Long) session.createQuery(hql)
					.setString("jobsystem", systemId)
					.uniqueResult();
			
			session.flush();
			return currentUserJobCount;
		} catch (HibernateException ex) {
			throw new JobException(ex);
		} finally {
			try { HibernateUtil.commitTransaction();} catch (Exception ignored) {}
		}
	}

    /**
     * Saves or updates a job. This will force the lastUpdated timestamp to update every time.
     *
     * @param job the job to save
     * @throws JobException if unable to save the job
     * @see #persist(Job, boolean)
     */
    public static void persist(Job job)
	throws JobException, UnresolvableObjectException 
	{
		persist(job, true);
	}

    /**
     * Saves or updates a job, optionally forcing the lastUpdated timestamp to update.
     *
     * @param job the job to save
     * @param forceTimestamp true if a timestamp should be set, false otherwise
     * @throws JobException if unable to save the job
     */
	public static void persist(Job job, boolean forceTimestamp) 
	throws JobException, StaleStateException
	{
		if (job == null)
			throw new JobException("Job cannot be null");
		
		Session session = null;
		
		try
		{
			session = getSession();
			
			if (forceTimestamp) {
				job.setLastUpdated(new DateTime().toDate());
			}
			
			session.saveOrUpdate(job);
        } catch (StaleStateException ex) {
            // TODO: swallow this and rethrow a JobException for consistent behavior and easier exception handling upstream
            throw ex;
        } catch (HibernateException ex) {
            log.error("Concurrency issue experienced updating job " + job.getUuid() + " aborting save.");
            try {
                if (session != null && session.isOpen()) {
                    HibernateUtil.rollbackTransaction();
                    session.close();
                }
            } catch (Exception ignored) {
            }

            throw new JobException(ex);
        } finally {
            try {
                HibernateUtil.commitTransaction();
            } catch (Exception ignored) {
            }
        }
	}

	/**
	 * Deletes a job from the db.
	 * @param job the job to delete
	 * @throws JobException if the delete operation fails
	 */
	public static void delete(Job job) throws JobException
	{
		if (job == null)
			throw new JobException("Job cannot be null");

		try
		{
			Session session = getSession();
			session.disableFilter("jobTenantFilter");
			session.disableFilter("jobEventTenantFilter");
			session.disableFilter("jobPermissionTenantFilter");
			session.clear();
			session.delete(job);
//			session.evict(job);
			session.flush();
		}
		catch (HibernateException ex)
		{
			try
			{
				if (HibernateUtil.getSession().isOpen()) {
					HibernateUtil.rollbackTransaction();
				}
			}
			catch (Throwable ignored) {}
				
			throw new JobException(ex);
		}
		finally {
			try { HibernateUtil.commitTransaction();} catch (Exception ignored) {}
		}
	}
	
	
	/**
	 * Selects a job at random from the population of jobs waiting to be archived. The selection 
	 * is made by first getting a list of all users with pending archiving jobs, then selecting 
	 * one user and picking one of their jobs at random. All capacity filtering parameters which 
     * enable restriction of job selection by tenant, owner, system, and batch queue are taken
     * into consideration.
	 * 
	 * @param tenantId tenant to include or exclude from selection.
     * @param owners array of owners to include or exclude from the selection process 
	 * @param systemIds array of systems and queues to include or exclude from the selectino process. 
	 * @return uuid of next job to archive.
	 * @throws JobException if the query fails
	 */
	@SuppressWarnings("unchecked")
	public static String getFairButRandomJobUuidForNextArchivingTask(String tenantId, String[] owners, String[] systemIds)
	throws JobException
	{
        // Tracing.
        if (Settings.DEBUG_SQL_ARCHIVING_JOB && log.isDebugEnabled()) {
            StringBuilder buf = new StringBuilder(200);
            buf.append("JobDao.getFairButRandomJobUuidForNextArchivingTask() input: tenantId=");
            buf.append(tenantId);
            buf.append(", owners=(");
            for (int i = 0; i < owners.length; i++) {
                if (i != 0) buf.append(", "); 
                buf.append(owners[i]);
            }
            buf.append("), systemIds=(");
            for (int i = 0; i < systemIds.length; i++) {
                if (i != 0) buf.append(", "); 
                buf.append(systemIds[i]);
            }
            buf.append(").");
            log.debug(buf.toString());             
        }
        
	    boolean excludeTenant = false;
        if (StringUtils.isEmpty(tenantId)) {
            tenantId = "%";
        } else if (tenantId.contains("!")) {
            excludeTenant = true;
            tenantId = StringUtils.removeStart(tenantId, "!");
        }
        
        boolean excludeOwners = false;
        if (ArrayUtils.isNotEmpty(owners) && StringUtils.join(owners, ",").contains("!")) {
            excludeOwners = true;
            for (int i = 0; i < owners.length; i++)
                owners[i] = StringUtils.removeStart(owners[i], "!");
        }
        
        boolean excludeSystems = false;
        if (ArrayUtils.isNotEmpty(systemIds) && StringUtils.join(systemIds, ",").contains("!")) {
            excludeSystems = true;
            for (int i = 0; i < systemIds.length; i++)
                systemIds[i] = StringUtils.removeStart(systemIds[i], "!");
        }
		
		try
		{
			Session session = getSession();
			session.clear();
			session.disableFilter("jobTenantFilter");
			
			
			String sql = "select j.owner, usg.pending_archiving_tasks, usg.active_archiving_tasks, j.tenant_id  \n";
			
			if (!ArrayUtils.isEmpty(systemIds)) 
            {   
                sql += "from ( \n"
                     + "    select s.system_id, q.name as queue_name, s.tenant_id \n"
                     + "    from systems s left join batchqueues q on s.id = q.execution_system_id \n"
                     + "    where s.tenant_id :excludetenant like :tenantid and :excludesystems (\n";
                
                for (int i=0;i<systemIds.length;i++) {
                    
                    if (StringUtils.contains(systemIds[i], "#")) {
                        sql += "        (s.system_id = :systemid" + i + " and q.name = :queuename" + i + ") ";
                    } else {
                        sql += "        (s.system_id = :systemid" + i + ") ";
                    }
                    
                    if (systemIds.length > (i+1)) {
                        sql += " or \n ";
                    }
                }
                
                sql    += "\n        ) \n"
                        + "    ) e left join jobs j on j.execution_system = e.system_id \n"
                        + "        and j.queue_request = e.queue_name \n"
                        + "        and j.tenant_id = e.tenant_id ";
            } 
            else {
                sql    += "from jobs j ";
            }
			
			sql  += "\n    left join (  \n" + 
					"		select jj.owner,   \n" + 
					"			sum(CASE WHEN jj.status = 'CLEANING_UP' then 1 else 0 end) as pending_archiving_tasks,  \n" + 
					"			sum(CASE WHEN jj.status = 'ARCHIVING' then 1 else 0 end) as active_archiving_tasks,  \n" + 
					"			jj.tenant_id  \n" + 
					"		from jobs jj  where jj.visible = 1 \n" + 
					"		group by jj.owner, jj.tenant_id  \n" + 
					"    ) as usg on j.owner = usg.owner and j.tenant_id = usg.tenant_id  \n" + 
					"where j.status in ('ARCHIVING', 'CLEANING_UP')   \n" + 
					"	and j.visible = 1   \n" +
					"	and usg.pending_archiving_tasks > 0   \n" +
					"	and usg.tenant_id :excludetenant like :tenantid  \n";
			
			if (!ArrayUtils.isEmpty(owners)) {
			    sql += " and usg.owner :excludeowners in :owners  \n";
			}
			
			if (!ArrayUtils.isEmpty(systemIds)) {
			    
			    sql += "    and :excludesystems ( ";
			    
			    for (int i=0;i<systemIds.length;i++) {
			        
			        if (StringUtils.contains(systemIds[i], "#")) {
			            sql += "         (j.execution_system = :systemid" + i + " and j.queue_request = :queuename" + i + ")  \n";
			        } else {
			            sql += "         (j.execution_system = :systemid" + i + "  \n"
                             + "             or j.archive_system in ( \n"
                             + "                                    select s.id  \n"
                             + "                                    from systems s  \n"
                             + "                                    where e.system_id in (:systemid" + i + ")  \n" 
                             + "                                            and s.tenant_id = j.tenant_id  \n"
                             + "                                     ) \n"
                             + "         )  \n";
			        }
			        
			        if (systemIds.length > (i+1)) {
			            sql += " or  \n";
			        }
			    }
			    
			    sql += "        ) \n";
			}
			
			sql +=  "group by j.owner, j.tenant_id, usg.pending_archiving_tasks, usg.active_archiving_tasks  \n" + 
					"order by RAND() \n";
			
			sql = StringUtils.replace(sql, ":excludeowners", excludeOwners ? "not" : "");
			sql = StringUtils.replace(sql, ":excludetenant", excludeTenant ? "not" : "");
			sql = StringUtils.replace(sql, ":excludesystems", excludeSystems ? "not" : "");
           
			// This parallel query string is for debugging purposes only.
			String q = StringUtils.replace(sql, ":tenantid", "'" + tenantId + "'");
			
//			log.debug(sql);
			Query query = session.createSQLQuery(sql)
			        .addScalar("owner", StandardBasicTypes.STRING)
					.addScalar("pending_archiving_tasks", StandardBasicTypes.INTEGER)
					.addScalar("active_archiving_tasks", StandardBasicTypes.INTEGER)
					.addScalar("tenant_id", StandardBasicTypes.STRING)
					.setResultTransformer(AliasToEntityMapResultTransformer.INSTANCE)
					.setString("tenantid", tenantId);
			
			if (!ArrayUtils.isEmpty(owners)) 
			{
			    query.setParameterList("owners", owners);
			    q = StringUtils.replace(q, ":owners", "('" + StringUtils.join(owners, "','")+"')");
            }
            
            if (!ArrayUtils.isEmpty(systemIds)) 
            {
                for (int i=0;i<systemIds.length;i++) {
                    if (StringUtils.contains(systemIds[i], "#")) {
                        String[] tokens = StringUtils.split(systemIds[i], "#");
                        query.setString("systemid" + i, tokens[0]);
                        q = StringUtils.replace(q, ":systemid"+i, "'" + tokens[0] +"'");
                        if (tokens.length > 1) {
                            query.setString("queuename" + i, tokens[1]); 
                            q = StringUtils.replace(q, ":queuename"+i, "'" + tokens[1] +"'");
                        }
                    }
                    else
                    {
                        query.setString("systemid" + i, systemIds[i]);
                        q = StringUtils.replace(q, ":systemid"+i, "'" + systemIds[i] +"'");
                    }
                }
            }
            
            // Print the separately constructed query string after parameter substitution.
            if (Settings.DEBUG_SQL_ARCHIVING_JOB && log.isDebugEnabled() && 
                !sameAsLastQuery(SqlQueryEnum.ARCHIVING_1, q)) 
            {
                log.debug("JobDao.getFairButRandomJobUuidForNextArchivingTask() query 1:\n" + q);
                sqlQueryMap.put(SqlQueryEnum.ARCHIVING_1, q.hashCode());
            }
            
            List<Map<String,Object>> aliasToValueMapList = query.setCacheable(false)
                                                                .setCacheMode(CacheMode.REFRESH)
                                                                .setMaxResults(1).list();
			
			if (aliasToValueMapList.isEmpty()) {
                if (Settings.DEBUG_SQL_ARCHIVING_JOB && log.isDebugEnabled())
                    log.debug("No user/tenant selected for archiving.");
				return null;
			} else {
				String username = (String) aliasToValueMapList.get(0).get("owner");
				String tid = (String) aliasToValueMapList.get(0).get("tenant_id");
				
				if (Settings.DEBUG_SQL_ARCHIVING_JOB && log.isDebugEnabled())
				    log.debug("User " + username + " in tenant " + tid + " selected for archiving.");
				
				sql = "select j.uuid \n";
				if (!ArrayUtils.isEmpty(systemIds)) 
	            {   
	                sql += "from ( \n"
	                     + "    select s.system_id, q.name as queue_name, s.tenant_id \n"
	                     + "    from systems s left join batchqueues q on s.id = q.execution_system_id \n"
	                     + "    where s.tenant_id :excludetenant like :tenantid and :excludesystems (\n";
	                
	                for (int i=0;i<systemIds.length;i++) {
	                    
	                    if (StringUtils.contains(systemIds[i], "#")) {
	                        sql += "        (s.system_id = :systemid" + i + " and q.name = :queuename" + i + ") ";
	                    } else {
	                        sql += "        (s.system_id = :systemid" + i + ") ";
	                    }
	                    
	                    if (systemIds.length > (i+1)) {
	                        sql += " or \n ";
	                    }
	                }
	                
	                sql    += "\n        ) \n"
	                        + "    ) e left join jobs j on j.execution_system = e.system_id \n"
	                        + "        and j.queue_request = e.queue_name \n"
	                        + "        and j.tenant_id = e.tenant_id ";
	            } 
	            else {
	                sql    += "from jobs j \n";
	            }
				
				sql	+= "where j.owner = :owner \n"
					+ "		and j.visible = 1  \n"
					+ "		and j.tenant_id = :tenantid \n"
					+ "		and j.status = 'CLEANING_UP' \n"
					+ "order by rand() ";
				
				
				// This parallel query string is for debugging purposes only.
				String jq = StringUtils.replace(sql, ":excludesystems", excludeSystems ? "not" : "");
				jq = StringUtils.replace(jq, ":tenantid", tid);
				jq = StringUtils.replace(jq, ":owner", username);
				
				sql = StringUtils.replace(sql, ":excludetenant", excludeTenant ? "not" : "");
				sql = StringUtils.replace(sql, ":excludesystems", excludeSystems ? "not" : "");
	           
				Query jobQuery = session.createSQLQuery(sql)
						.setString("owner", username)
						.setString("tenantid", tid);
				
				if (!ArrayUtils.isEmpty(systemIds)) 
	            {
	                for (int i=0;i<systemIds.length;i++) {
	                    if (StringUtils.contains(systemIds[i], "#")) {
	                        String[] tokens = StringUtils.split(systemIds[i], "#");
	                        jobQuery.setString("systemid" + i, tokens[0]);
	                        jq = StringUtils.replace(jq, ":systemid"+i, "'" + tokens[0] +"'");
	                        if (tokens.length > 1) {
	                            jobQuery.setString("queuename" + i, tokens[1]); 
	                            jq = StringUtils.replace(jq, ":queuename"+i, "'" + tokens[1] +"'");
	                        }
	                    }
	                    else
	                    {
	                        jobQuery.setString("systemid" + i, systemIds[i]);
	                        jq = StringUtils.replace(jq, ":systemid"+i, "'" + systemIds[i] +"'");
	                    }
	                }
	            }
				
				// Print the separately constructed query string after parameter substitution.
				if (Settings.DEBUG_SQL_ARCHIVING_JOB && log.isDebugEnabled() && 
				    !sameAsLastQuery(SqlQueryEnum.ARCHIVING_2, jq)) 
				{
				    log.debug("JobDao.getFairButRandomJobUuidForNextArchivingTask() query 2:\n" + jq);
				    sqlQueryMap.put(SqlQueryEnum.ARCHIVING_2, jq.hashCode());
				}
				
				List<String> uuids = (List<String>)jobQuery.setCacheable(false)
						.setCacheMode(CacheMode.REFRESH)
                    	.setMaxResults(1)
						.list();
				
				if (uuids.isEmpty()) {
					// should never happen
				    log.error("Unable to select archiving job for " + username + " on tenant " + tid + ".");
					return null;
				} else {
	                if (Settings.DEBUG_SQL_ARCHIVING_JOB && log.isDebugEnabled())
	                    log.debug("Selected " + uuids.get(0) + " as the next archiving job on tenant " + tid + ".");
					return uuids.get(0);
				}
            }
        } catch (StaleObjectStateException ex) {
            log.error("Unable to select archiving job on tenant " + tenantId + " due to stale object.", ex);
            return null;
        } catch (ObjectNotFoundException ex) {
            log.warn("Unable to select archiving job on tenant " + tenantId + " due to object not found.", ex);
            return null;
        } catch (HibernateException ex) {
            log.error("Unable to select archiving job on tenant " + tenantId + " due to hibernate problem.", ex);
            throw new JobException(ex);
        } finally {
            try {
                HibernateUtil.commitTransaction();
            } catch (Exception ignored) {
            }
        }
		
	}
	
	/**
	 * Selects a user at random from the population of users with jobs of the given {@code status}.
	 * System quotas are taken into consideration as are the capacity filtering parameters which 
	 * enable restriction of job selection by tenant, owner, system, and batch queue.
	 * 
	 * @param status by which to filter the list of active jobs.
	 * @param tenantId tenant to include or exclude from selection.
     * @param owners array of owners to include or exclude from the selection process 
     * @param systemIds array of systems and queues to include or exclude from the selectino process. 
     * @return username of user with pending job
	 * @throws JobException if the query failed
	 */
	@SuppressWarnings("unchecked")
	public static String getRandomUserForNextQueuedJobOfStatus(JobStatusType status, String tenantId, String[] systemIds, String[] owners)
	throws JobException
	{
	    boolean excludeTenant = false;
        if (StringUtils.isEmpty(tenantId)) {
            tenantId = "%";
        } else if (tenantId.contains("!")) {
            excludeTenant = true;
            tenantId = StringUtils.removeStart(tenantId, "!");
        }
        
        boolean excludeOwners = false;
        if (ArrayUtils.isNotEmpty(owners) && StringUtils.join(owners, ",").contains("!")) {
            excludeOwners = true;
            for (int i = 0; i < owners.length; i++)
                owners[i] = StringUtils.removeStart(owners[i], "!");
        }
        
        boolean excludeSystems = false;
        if (ArrayUtils.isNotEmpty(systemIds) && StringUtils.join(systemIds, ",").contains("!")) {
            excludeSystems = true;
            for (int i = 0; i < systemIds.length; i++)
                systemIds[i] = StringUtils.removeStart(systemIds[i], "!");
        }
		
		Session session = null;
		try
		{ 
		    HibernateUtil.beginTransaction();
			session = HibernateUtil.getSession();
			session.clear();
			
			String sql = "select jq.owner, jq.tenant_id   \n" +
    			        "from (    \n" +
    			        "       select sq.system_id, sq.name, sq.max_jobs, sq.max_user_jobs, t.system_backlogged_queue_jobs, t.system_queue_jobs, sq.tenant_id    \n" +
    			        "       from (    \n";
			if (JobStatusType.PENDING == status) {
                sql +=  "               select bqj.execution_system, bqj.queue_request, sum( if(bqj.status not in ('PENDING','PROCESSING_INPUTS'), 1, 0)) as system_queue_jobs, sum( if(bqj.status in ('PENDING','PROCESSING_INPUTS'), 1, 0)) as system_backlogged_queue_jobs, bqj.tenant_id     \n";
			} else {
			    sql +=  "               select bqj.execution_system, bqj.queue_request, sum( if(bqj.status not in ('PENDING','PROCESSING_INPUTS','STAGED','STAGING_INPUTS'), 1, 0)) as system_queue_jobs, sum( if(bqj.status in ('PENDING','PROCESSING_INPUTS','STAGED','STAGING_INPUTS'), 1, 0)) as system_backlogged_queue_jobs, bqj.tenant_id     \n";
			}
    		sql +=      "               from jobs bqj     \n" +
    			        "               where bqj.visible = 1 and bqj.status in ('PENDING','PROCESSING_INPUTS', 'RUNNING', 'PAUSED', 'QUEUED', 'CLEANING_UP', 'SUBMITTING', 'STAGING_INPUTS', 'STAGING_JOB', 'STAGED') \n" +
    			        "                     and bqj.tenant_id :excludetenant like :tenantid \n";
	        
    	    if (!ArrayUtils.isEmpty(owners)) {
    	        sql += "    and bqj.owner :excludeowners in :owners \n";
            }
            
            if (!ArrayUtils.isEmpty(systemIds)) {
                
                sql += "    and :excludesystems ( \n";
                
                for (int i=0;i<systemIds.length;i++) {
                    
                    if (StringUtils.contains(systemIds[i], "#")) {
                        sql += "         (bqj.execution_system = :systemid" + i + " and bqj.queue_request = :queuename" + i + ") ";
                    } else {
                        sql += "         (bqj.execution_system = :systemid" + i + ") ";
                    }
                    
                    if (systemIds.length > (i+1)) {
                        sql += " or \n";
                    }
                }
                
                sql += "        ) \n";
            }
                    
    		sql +=      "               group by bqj.execution_system, bqj.queue_request, bqj.tenant_id    \n" +
    			        "           ) as t  \n" +
    			        "           left join (  \n" +
    			        "               select ss.system_id, q.name, q.max_jobs, q.max_user_jobs, ss.tenant_id   \n" +
    			        "               from batchqueues q  \n" +
    			        "                   left join systems ss on ss.id = q.execution_system_id  \n" +
    			        "                   where ss.type = 'EXECUTION'  \n" +
    			        "           ) as sq on sq.system_id = t.execution_system and sq.name = t.queue_request and t.tenant_id = sq.tenant_id    \n" +
    			        "       where t.system_queue_jobs < sq.max_jobs     \n" +
    			        "           or sq.max_jobs = -1    \n" +
    			        "           or sq.max_jobs is NULL  \n" +
    			        "   ) as sysq   \n" +
    			        "   left join (    \n" +
    			        "       select buqj.owner, buqj.status, buqj.execution_system, buqj.queue_request, auj.user_system_queue_jobs, auj.total_backlogged_user_jobs, buqj.tenant_id     \n" +
    			        "       from jobs buqj  \n" +
    			        "           left join ( \n";
    		if (JobStatusType.PENDING == status) {
                sql +=  "               select aj.owner, aj.execution_system, aj.queue_request, sum( if(aj.status in ('PENDING','PROCESSING_INPUTS'), 0, 1)) as user_system_queue_jobs, sum( if(aj.status in ('PENDING','PROCESSING_INPUTS'), 1, 0)) as total_backlogged_user_jobs, aj.tenant_id \n";
    		} else {
    		    sql +=  "               select aj.owner, aj.execution_system, aj.queue_request, sum( if(aj.status in ('PENDING','PROCESSING_INPUTS','STAGED','STAGING_INPUTS'), 0, 1)) as user_system_queue_jobs, sum( if(aj.status in ('PENDING','PROCESSING_INPUTS','STAGED','STAGING_INPUTS'), 1, 0)) as total_backlogged_user_jobs, aj.tenant_id \n";
    		}
    		sql +=      "               from jobs aj  \n" +
    			        "               where aj.visible = 1 and aj.status in ( 'PENDING','PROCESSING_INPUTS', 'RUNNING', 'PAUSED', 'QUEUED', 'CLEANING_UP', 'SUBMITTING', 'STAGING_INPUTS', 'STAGING_JOB', 'STAGED')   \n" +
    			        "               group by aj.owner, aj.execution_system, aj.queue_request, aj.tenant_id \n" +
    			        "           ) as auj on buqj.owner = auj.owner and buqj.execution_system = auj.execution_system and buqj.queue_request = auj.queue_request and buqj.tenant_id = auj.tenant_id \n" +
    			        "       group by buqj.tenant_id, buqj.owner, buqj.status, buqj.execution_system, buqj.queue_request, auj.user_system_queue_jobs, auj.total_backlogged_user_jobs \n" +
    			        "   ) as jq on jq.execution_system = sysq.system_id and jq.queue_request = sysq.name and jq.tenant_id = sysq.tenant_id    \n" +
    			        "where     \n" +
    			        "   jq.status like :status      \n" +
    			        "   and (    \n" +
    			        "       jq.user_system_queue_jobs < sysq.max_user_jobs    \n" +
    			        "       or sysq.max_user_jobs = -1    \n" +
    			        "       or sysq.max_user_jobs is NULL   \n" +
    			        "   ) \n";
    			        
			if (JobStatusType.PENDING == status) {
				sql +=	"	and jq.total_backlogged_user_jobs > 0 \n";
			}
			if (!ArrayUtils.isEmpty(owners)) {
                sql += "    and jq.owner :excludeowners in :owners \n";
            }
			sql +=		"	and jq.queue_request is not NULL    \n" +
						"	and jq.queue_request <> ''    \n" +

						"	and jq.tenant_id :excludetenant like :tenantid \n" +
						"group by jq.owner, jq.tenant_id   \n" +
						"order by rand() \n";
			
			sql = StringUtils.replace(sql, ":excludeowners", excludeOwners ? "not" : "");
			sql = StringUtils.replace(sql, ":excludetenant", excludeTenant ? "not" : "");
			sql = StringUtils.replace(sql, ":excludesystems", excludeSystems ? "not" : "");
           
            // This parallel query string is for debugging purposes only.
			String q = StringUtils.replace(sql, ":status", "'" + status + "'");
            q = StringUtils.replace(q, ":tenantid", "'" + tenantId + "'");
			
			Query query = session.createSQLQuery(sql)
			        .addScalar("owner", StandardBasicTypes.STRING)
					.addScalar("tenant_id", StandardBasicTypes.STRING) 
					.setResultTransformer(AliasToEntityMapResultTransformer.INSTANCE)
					.setString("status", status.name())
					.setString("tenantid", tenantId);
			
			if (!ArrayUtils.isEmpty(owners)) {
			    query.setParameterList("owners", owners);
                q = StringUtils.replace(q, ":owners", "('" + StringUtils.join(owners, "','")+"')");
            }
			
			if (!ArrayUtils.isEmpty(systemIds)) {
			    for (int i=0;i<systemIds.length;i++) 
                {
                    if (StringUtils.contains(systemIds[i], "#")) {
                        String[] tokens = StringUtils.split(systemIds[i], "#");
                        query.setString("systemid" + i, tokens[0]);
                        q = StringUtils.replace(q, ":systemid"+i, "'" + tokens[0] +"'");
                        if (tokens.length > 1) {
                            query.setString("queuename" + i, tokens[1]); 
                            q = StringUtils.replace(q, ":queuename"+i, "'" + tokens[1] +"'");
                        }
                    }
                    else
                    {
                        query.setString("systemid" + i, systemIds[i]);
                        q = StringUtils.replace(q, ":systemid"+i, "'" + systemIds[i] +"'");
                    }
                }
			}
			
			// Print the separately constructed query string after parameter substitution.
			if (Settings.DEBUG_SQL_PENDING_JOB && log.isDebugEnabled()) 
			{
                // Only the PENDING and STAGED statuses are passed to this method.
                SqlQueryEnum queryName = null;
                if (status == JobStatusType.PENDING) queryName = SqlQueryEnum.PENDING_1;
                 else queryName = SqlQueryEnum.STAGED_1;
                
                // Use the query name to determine if the current query text needs to be logged. 
                if (!sameAsLastQuery(queryName, q)) {
                    log.debug("JobDao.getRandomUserForNextQueuedJobOfStatus() query:\n" + q);
                    sqlQueryMap.put(queryName, q.hashCode()); 
                }
			}
			
//			log.debug(query.getQueryString());
			List<Map<String,Object>> aliasToValueMapList = query
			        .setCacheable(false)
                    .setCacheMode(CacheMode.REFRESH)
                    .setMaxResults(1)
					.list();
			
			if (aliasToValueMapList.isEmpty()) {
			    if (Settings.DEBUG_SQL_PENDING_JOB && log.isDebugEnabled())
			       log.debug("No username selected for the next job of status " + status.name() + ".");
				return null;
			} else {
			    String username = (String) aliasToValueMapList.get(0).get("owner");
                if (Settings.DEBUG_SQL_PENDING_JOB && log.isDebugEnabled()) 
                   log.debug("Selected " + username + " as next " + status.name() + " job.");
				return username;
			}
		}
        catch (StaleObjectStateException ex) {
            log.error("Random user selection failed for " + status.name() + " due to stale object.", ex);
            return null;
        }
        catch (ObjectNotFoundException ex)
        {
            log.warn("Random user selection failed for " + status.name() + " due to object not found.", ex);
            return null;
        }
        catch (HibernateException ex)
        {
            log.error("Random user selection failed for " + status.name() + " due to hibernate problem.", ex);
            throw new JobException(ex);
        }
		finally {
            try { HibernateUtil.commitTransaction();} catch (Exception ignored) {}
        }
	}

	/**
     * Selects a job at random from the population of jobs of the given {@code status}. This method
     * calls out to {@link #getRandomUserForNextQueuedJobOfStatus(JobStatusType, String, String[], String[])} 
     * first to select a user in a fair manner, then selects one of the user's jobs which honors
     * all system quotas and capacity filtering parameters which enable restriction of job 
     * selection by tenant, owner, system, and batch queue.
     * 
     * @param status by which to filter the list of active jobs.
     * @param tenantId tenant to include or exclude from selection.
     * @param owners array of owners to include or exclude from the selection process 
     * @param systemIds array of systems and queues to include or exclude from the selectino process. 
     * @return username of user with pending job
     * @see JobDao#getRandomUserForNextQueuedJobOfStatus(JobStatusType, String, String[], String[])
     * @throws JobException when the query fails to complete
     */
    @SuppressWarnings("unchecked")
    public static String getNextQueuedJobUuid(JobStatusType status, String tenantId, String[] owners, String[] systemIds)
	throws JobException
	{
        // Tracing.
        if (Settings.DEBUG_SQL_PENDING_JOB && log.isDebugEnabled()) {
            StringBuilder buf = new StringBuilder(200);
            buf.append("JobDao.getNextQueuedJobUuid() input: status=");
            buf.append(status.name()); 
            buf.append(", tenantId=");
            buf.append(tenantId);
            buf.append(", owners=(");
            for (int i = 0; i < owners.length; i++) {
                if (i != 0) buf.append(", "); 
                buf.append(owners[i]);
            }
            buf.append("), systemIds=(");
            for (int i = 0; i < systemIds.length; i++) {
                if (i != 0) buf.append(", "); 
                buf.append(systemIds[i]);
            }
            buf.append(").");
            log.debug(buf.toString());             
        }
        
    	// we don't need to clone these arrays, they should be safe, but I'm tired and this is safe, so we'll do this
		// until i come back and add formal whitelist/blacklist support.
		String nextUser = getRandomUserForNextQueuedJobOfStatus(status, tenantId, (String[])ArrayUtils.clone(systemIds), (String[])ArrayUtils.clone(owners));

		// Is any user ready to have a job run?
		if (StringUtils.isBlank(nextUser)){
		    if (log.isTraceEnabled())
		        log.trace("Skipping " + status.name() + " job selection because no qualified user.");
		    return null;
		}

		boolean excludeTenant = false;
	    if (StringUtils.isEmpty(tenantId)) {
            tenantId = "%";
        } else if (tenantId.contains("!")) {
            excludeTenant = true;
            tenantId = StringUtils.removeStart(tenantId, "!");
        }
	    
		try
		{
		    
		    HibernateUtil.beginTransaction();
			    
		    Session session = HibernateUtil.getSession();
    			
            String sql = "select jq.owner, jq.tenant_id, jq.uuid   \n" +
                         "from (    \n" + 
                         "       select sq.system_id, sq.name, sq.max_jobs, sq.max_user_jobs, t.system_backlogged_queue_jobs, t.system_queue_jobs, sq.tenant_id    \n" + 
                         "       from (    \n";
            if (JobStatusType.PENDING == status) {
                sql +=  "               select bqj.execution_system, bqj.queue_request, sum( if(bqj.status not in ('PENDING','PROCESSING_INPUTS'), 1, 0)) as system_queue_jobs, sum( if(bqj.status in ('PENDING','PROCESSING_INPUTS'), 1, 0)) as system_backlogged_queue_jobs, bqj.tenant_id     \n";
            } else {
                sql +=  "               select bqj.execution_system, bqj.queue_request, sum( if(bqj.status not in ('PENDING','PROCESSING_INPUTS','STAGED','STAGING_INPUTS'), 1, 0)) as system_queue_jobs, sum( if(bqj.status in ('PENDING','PROCESSING_INPUTS','STAGED','STAGING_INPUTS'), 1, 0)) as system_backlogged_queue_jobs, bqj.tenant_id     \n";
            }
            sql +=      "               from jobs bqj     \n" + 
                        "               where bqj.visible = 1 and bqj.status in ('PENDING','PROCESSING_INPUTS', 'RUNNING', 'PAUSED', 'QUEUED', 'CLEANING_UP', 'SUBMITTING', 'STAGING_INPUTS', 'STAGING_JOB', 'STAGED') \n" +
                        "                     and bqj.tenant_id :excludetenant like :tenantid \n" + 
                        "                     and bqj.owner = :owner \n";
                
            if (!ArrayUtils.isEmpty(systemIds)) 
            {
                boolean exclusive = false;
                if (StringUtils.join(systemIds, ",").contains("!")) {
                    exclusive = true;
                }
                    
                sql += "    and ( \n";
                    
                for (int i=0;i<systemIds.length;i++) {
                        
                    if (StringUtils.contains(systemIds[i], "#")) {
                        sql += "         (bqj.execution_system " + (exclusive ? " <> " : " = ") + " :systemid" + i + " and bqj.queue_request " + (exclusive ? " <> " : " = ") + " :queuename" + i + ") ";
                    } else {
                        sql += "         (bqj.execution_system " + (exclusive ? " <> " : " = ") + " :systemid" + i + ") ";
                    }
                        
                    if (systemIds.length > (i+1)) {
                        sql += " or \n";
                    }
                }
                    
                sql += "        ) \n";
            }
                        
            sql +=      "               group by bqj.execution_system, bqj.queue_request, bqj.tenant_id    \n" + 
                        "           ) as t  \n" + 
                        "           left join (  \n" + 
                        "               select ss.system_id, q.name, q.max_jobs, q.max_user_jobs, ss.tenant_id   \n" + 
                        "               from batchqueues q  \n" + 
                        "                   left join systems ss on ss.id = q.execution_system_id  \n" + 
                        "                   where ss.type = 'EXECUTION'  \n" + 
                        "           ) as sq on sq.system_id = t.execution_system and sq.name = t.queue_request and t.tenant_id = sq.tenant_id    \n" + 
                        "       where t.system_queue_jobs < sq.max_jobs     \n" + 
                        "           or sq.max_jobs = -1    \n" + 
                        "           or sq.max_jobs is NULL  \n" + 
                        "   ) as sysq   \n" + 
                        "   left join (    \n" + 
                        "       select buqj.owner, buqj.status, buqj.execution_system, buqj.queue_request, auj.user_system_queue_jobs, auj.total_backlogged_user_jobs, buqj.tenant_id, buqj.uuid     \n" + 
                        "       from jobs buqj  \n" + 
                        "           left join ( \n";
            if (JobStatusType.PENDING == status) {
                sql +=  "               select aj.owner, aj.execution_system, aj.queue_request, sum( if(aj.status in ('PENDING','PROCESSING_INPUTS'), 0, 1)) as user_system_queue_jobs, sum( if(aj.status in ('PENDING','PROCESSING_INPUTS'), 1, 0)) as total_backlogged_user_jobs, aj.tenant_id \n";
            } else {
                sql +=  "               select aj.owner, aj.execution_system, aj.queue_request, sum( if(aj.status in ('PENDING','PROCESSING_INPUTS','STAGED','STAGING_INPUTS'), 0, 1)) as user_system_queue_jobs, sum( if(aj.status in ('PENDING','PROCESSING_INPUTS','STAGED','STAGING_INPUTS'), 1, 0)) as total_backlogged_user_jobs, aj.tenant_id \n";
            }
            sql +=      "               from jobs aj  \n" + 
                        "               where aj.visible = 1 and aj.status in ( 'PENDING','PROCESSING_INPUTS', 'RUNNING', 'PAUSED', 'QUEUED', 'CLEANING_UP', 'SUBMITTING', 'STAGING_INPUTS', 'STAGING_JOB', 'STAGED')   \n" + 
                        "               group by aj.owner, aj.execution_system, aj.queue_request, aj.tenant_id \n" + 
                        "           ) as auj on buqj.owner = auj.owner and buqj.execution_system = auj.execution_system and buqj.queue_request = auj.queue_request and buqj.tenant_id = auj.tenant_id \n" + 
                        "       group by buqj.tenant_id, buqj.owner, buqj.status, buqj.execution_system, buqj.queue_request, auj.user_system_queue_jobs, auj.total_backlogged_user_jobs, buqj.uuid \n" + 
                        "   ) as jq on jq.execution_system = sysq.system_id and jq.queue_request = sysq.name and jq.tenant_id = sysq.tenant_id    \n" + 
                        "where     \n" + 
                        "   jq.status like :status      \n" + 
                        "   and (    \n" + 
                        "       jq.user_system_queue_jobs < sysq.max_user_jobs    \n" + 
                        "       or sysq.max_user_jobs = -1    \n" + 
                        "       or sysq.max_user_jobs is NULL   \n" + 
                        "   ) \n"; 
                            
            if (JobStatusType.PENDING == status) {
                sql +=  "   and jq.total_backlogged_user_jobs > 0 \n";    
            }
                
            sql +=      "   and jq.queue_request is not NULL    \n" + 
                        "   and jq.queue_request <> ''    \n" + 
                        "   and jq.tenant_id :excludetenant like :tenantid \n" +
                        "group by jq.owner, jq.execution_system, jq.queue_request, jq.status, jq.tenant_id, jq.uuid    \n" +
                        "order by rand() \n";
                
            sql = StringUtils.replace(sql, ":excludetenant", excludeTenant ? "not" : "");
    			
            Query query = session.createSQLQuery(sql)
                        .addScalar("owner", StandardBasicTypes.STRING)
                        .addScalar("tenant_id", StandardBasicTypes.STRING)
                        .addScalar("uuid", StandardBasicTypes.STRING)
                        .setResultTransformer(AliasToEntityMapResultTransformer.INSTANCE)
                        .setString("status", status.name())
                        .setString("tenantid", tenantId)
                        .setString("owner", nextUser);
                
            // This parallel query string is for debugging purposes only.
            String q = StringUtils.replace(sql, ":status", "'" + status + "'");
            q = StringUtils.replace(q, ":tenantid", "'" + tenantId + "'");
            q = StringUtils.replace(q, ":owner", nextUser);
                
            if (!ArrayUtils.isEmpty(systemIds)) 
            {
                // remove leading negation if present
                for (int i = 0; i < systemIds.length; i++)
                    systemIds[i] = StringUtils.removeStart(systemIds[i], "!");
                    
                    
                for (int i=0;i<systemIds.length;i++) 
                {
                    if (StringUtils.contains(systemIds[i], "#")) {
                        String[] tokens = StringUtils.split(systemIds[i], "#");
                        query.setString("systemid" + i, tokens[0]);
                        q = StringUtils.replace(q, ":systemid"+i, "'" + tokens[0] +"'");
                        if (tokens.length > 1) {
                            query.setString("queuename" + i, tokens[1]); 
                            q = StringUtils.replace(q, ":queuename"+i, "'" + tokens[1] +"'");
                        }
                    }
                    else
                    {
                        query.setString("systemid" + i, systemIds[i]);
                        q = StringUtils.replace(q, ":systemid"+i, "'" + systemIds[i] +"'");
                    }
                }
            }
                
            // Print the separately constructed query string after parameter substitution.
            if (Settings.DEBUG_SQL_PENDING_JOB && log.isDebugEnabled()) 
            {
                // Only the PENDING and STAGED statuses are passed to this method.
                SqlQueryEnum queryName = null;
                if (status == JobStatusType.PENDING) queryName = SqlQueryEnum.PENDING_2;
                 else queryName = SqlQueryEnum.STAGED_2;
                
                // Use the query name to determine if the current query text needs to be logged. 
                if (!sameAsLastQuery(queryName, q)) {
                    log.debug("JobDao.getNextQueuedJobUuid() query:\n" + q);
                    sqlQueryMap.put(queryName, q.hashCode());
                }
                
            }
            List<Map<String,Object>> aliasToValueMapList = query
                        .setCacheable(false)
                        .setCacheMode(CacheMode.REFRESH)
                        .setMaxResults(1)
                        .list();
                
            if (aliasToValueMapList.isEmpty()) {
                if (Settings.DEBUG_SQL_PENDING_JOB && log.isDebugEnabled())
                    log.debug("No job uuid selected for the next job with status " + status.name() + ".");
                return null;
            } else {
                String uuid = (String) aliasToValueMapList.get(0).get("uuid");
                if (Settings.DEBUG_SQL_PENDING_JOB && log.isDebugEnabled())
                    log.debug("Selected " + uuid + " as the next " + status.name() + " job.");
                return uuid;
            }
			
		}
		catch (StaleObjectStateException ex) {
		    log.error("Database polling failed for " + status.name() + " due to stale object.", ex);
			return null;
		}
		catch (ObjectNotFoundException ex)
		{
		    log.warn("Database polling failed for " + status.name() + " due to object not found.", ex);
			return null;
		}
		catch (HibernateException ex)
		{
		    log.error("Database polling failed for " + status.name() + " due to hibernate problem.", ex);
			throw new JobException(ex);
		}
		finally {
            try { HibernateUtil.commitTransaction();} catch (Exception ignored) {}
        }
	}
	
	/**
	 * Performs a string replacement and update on the job with the 
	 * given id. This is only needed because job inputs are not a 
	 * separate table.
	 * 
	 * @param jobId the db id of the job to update
	 * @param source the value within the inputs column to be replaced
	 * @param dest the value with which to replace {@code source} in the inputs column
	 * @throws JobException when the query fails to complete
	 */
	public static void updateInputs(long jobId, String source, String dest)
	throws JobException
	{
		try
		{
			Session session = getSession();
			session.clear();
			String sql = "update jobs set inputs = replace(inputs, :source, :dest)";
			session.createQuery(sql).setString("source", source).setString(
					"dest", dest).executeUpdate();
			
			session.flush();
		} catch (HibernateException ex) {
			throw new JobException(ex);
		} finally {
			try {
				HibernateUtil.commitTransaction();
			} catch (Exception ignored) {
			}
		}

	}

	/**
	 * Searches for jobs by the given user who matches the given set of 
	 * parameters. Permissions are honored in this query.
	 * 
	 * @param username the user for whom to search for user jobs
	 * @param searchCriteria Map of key value pairs by which to query.
	 * @return list of matching jobs, marshalled to {@link JobDTO}
	 * @throws JobException if unable to perform the query
	 */
	public static List<JobDTO> findMatching(String username,
			Map<SearchTerm, Object> searchCriteria) throws JobException
	{
		return JobDao.findMatching(username, searchCriteria, 0, Settings.DEFAULT_PAGE_SIZE);
	}
	
	/**
	 * Searches for jobs by the given user who matches the given set of 
	 * parameters. Permissions are honored in this query.
	 *
	 * @param username the user for whom to search for user jobs
	 * @param searchCriteria Map of key value pairs by which to query.
	 * @param offset the number of records to skip in the result set
	 * @param limit the maximum number of records  to return.
	 * @return list of matching jobs, marshalled to {@link JobDTO}
	 * @throws JobException if unable to perform the query
	 */
	public static List<JobDTO> findMatching(String username,
			Map<SearchTerm, Object> searchCriteria,
			int offset, int limit) throws JobException
	{
		return findMatching(username, searchCriteria, offset, limit, null, null);
	}
	
	/**
	 * Searches for jobs by the given user who matches the given map of {@link SearchTerm}s parameters.
	 * Permissions are honored in this query.
	 *
	 * @param username the user for whom to search for user jobs
	 * @param searchCriteria Map of key value pairs by which to query.
	 * @param offset the number of records to skip in the result set
	 * @param limit the maximum number of records  to return.
	 * @param order the direction to order
	 * @param orderBy the search field by which to order.
     * @return list of matching jobs, marshalled to {@link JobDTO}
	 * @throws JobException if unable to perform the query
	 */
	@SuppressWarnings("unchecked")
	public static List<JobDTO> findMatching(String username,
			Map<SearchTerm, Object> searchCriteria,
			int offset, int limit, AgaveResourceResultOrdering order, SearchTerm orderBy) throws JobException
	{
		if (order == null) {
			order = AgaveResourceResultOrdering.ASCENDING;
		}
		
		if (orderBy == null) {
			orderBy = new JobSearchFilter().filterAttributeName("lastupdated");
		}
		
		try
		{
			Map<String, Class> searchTypeMappings = new JobSearchFilter().getSearchTypeMappings();
			Session session = getSession();
			session.clear();
			String sql = "SELECT j.archive_output, \n" + 
					"       j.archive_output, \n" + 
					"       j.archive_path, \n" + 
					"       a.system_id as archive_system, \n" + 
					"       j.charge, \n" + 
					"       j.created, \n" + 
					"       j.end_time, \n" + 
					"       j.error_message, \n" + 
					"       j.execution_system, \n" +
					"       j.execution_type, \n" +
					"       j.id, \n" + 
					"       j.inputs, \n" + 
					"       j.internal_username, \n" + 
					"       j.last_updated, \n" + 
					"       j.local_job_id, \n" + 
					"       j.memory_request, \n" + 
					"       j.name, \n" + 
					"       j.node_count, \n" + 
					"       j.owner, \n" + 
					"       j.parameters, \n" + 
					"       j.processor_count, \n" + 
					"       j.queue_request, \n" + 
					"       j.requested_time, \n" + 
					"       j.retries, \n" +
					"       j.scheduler_type, \n" +
					"       j.scheduler_job_id, \n" +
					"       j.software_name, \n" + 
					"       j.start_time, \n" + 
					"       j.status, \n" + 
					"       j.status_checks, \n" + 
					"       j.submit_time, \n" + 
					"       j.tenant_id, \n" + 
					"       j.update_token, \n" + 
					"       j.uuid, \n" + 
					"       j.visible, \n" + 
					"       j.work_path \n" + 
					" FROM jobs j \n" +
					" LEFT OUTER JOIN systems a ON j.archive_system = a.id \n";
			if (!ServiceUtils.isAdmin(username)) {
				
				sql += " WHERE ( \n" +
				    "       j.owner = :jobowner OR \n" +
					"       j.id in ( \n" + 
				    "               SELECT pm.job_id FROM job_permissions as pm \n" +
					"               WHERE pm.username = :jobowner AND pm.permission <> :none \n" +
					"              ) \n" +
					"      ) AND \n";
			} else {
				sql += " WHERE ";
			}
			
			sql +=  "        j.tenant_id = :tenantid "; 
			
			for (SearchTerm searchTerm: searchCriteria.keySet()) 
			{
				sql += "\n       AND       " + searchTerm.getExpression();
			}
			
			if (!sql.contains("j.visible")) {
				sql +=  "\n       AND j.visible = :visiblebydefault ";
			}
			
			sql +=	"\n ORDER BY " + String.format(orderBy.getMappedField(), orderBy.getPrefix()) + " " + order.toString() + " \n";
			
			String q = sql;
//			log.debug(q);
			SQLQuery query = session.createSQLQuery(sql);
			query.addScalar("id", StandardBasicTypes.LONG)
				.addScalar("charge", StandardBasicTypes.DOUBLE)
				.addScalar("memory_request", StandardBasicTypes.DOUBLE)
				.addScalar("node_count", StandardBasicTypes.INTEGER)
				.addScalar("processor_count", StandardBasicTypes.INTEGER)
				.addScalar("retries", StandardBasicTypes.INTEGER)
				.addScalar("status_checks", StandardBasicTypes.INTEGER)
				.addScalar("archive_output", StandardBasicTypes.BOOLEAN)
				.addScalar("visible", StandardBasicTypes.BOOLEAN)
				.addScalar("archive_path", StandardBasicTypes.STRING)
				.addScalar("archive_system", StandardBasicTypes.STRING)
				.addScalar("created", StandardBasicTypes.TIMESTAMP)
				.addScalar("end_time", StandardBasicTypes.TIMESTAMP)
				.addScalar("error_message", StandardBasicTypes.STRING)
				.addScalar("execution_system", StandardBasicTypes.STRING)
				.addScalar("execution_type", StandardBasicTypes.STRING)
				.addScalar("inputs", StandardBasicTypes.STRING)
				.addScalar("internal_username", StandardBasicTypes.STRING)
				.addScalar("last_updated", StandardBasicTypes.TIMESTAMP)
				.addScalar("local_job_id", StandardBasicTypes.STRING)
				.addScalar("name", StandardBasicTypes.STRING)
				.addScalar("owner", StandardBasicTypes.STRING)
				.addScalar("parameters", StandardBasicTypes.STRING)
				.addScalar("queue_request", StandardBasicTypes.STRING)
				.addScalar("requested_time", StandardBasicTypes.STRING)
				.addScalar("scheduler_type", StandardBasicTypes.STRING)
				.addScalar("scheduler_job_id", StandardBasicTypes.STRING)
				.addScalar("software_name", StandardBasicTypes.STRING)
				.addScalar("start_time", StandardBasicTypes.TIMESTAMP)
				.addScalar("status", StandardBasicTypes.STRING)
				.addScalar("submit_time", StandardBasicTypes.TIMESTAMP)
				.addScalar("tenant_id", StandardBasicTypes.STRING)
				.addScalar("update_token", StandardBasicTypes.STRING)
				.addScalar("uuid", StandardBasicTypes.STRING)
				.addScalar("work_path", StandardBasicTypes.STRING)
				.setResultTransformer(Transformers.aliasToBean(JobDTO.class));
			
            query.setString("tenantid", TenancyHelper.getCurrentTenantId());
			
			q = StringUtils.replace(q, ":tenantid", "'" + TenancyHelper.getCurrentTenantId() + "'");
			
			if (sql.contains(":visiblebydefault") ) {
				query.setBoolean("visiblebydefault", Boolean.TRUE);
				
				q = StringUtils.replace(q, ":visiblebydefault", "1");
			}
			
		 	if (!ServiceUtils.isAdmin(username)) {
		 		query.setString("jobowner",username)
		 			.setString("none",PermissionType.NONE.name());
		 		q = StringUtils.replace(q, ":jobowner", "'" + username + "'");
		 		q = StringUtils.replace(q, ":none", "'NONE'");
		 	}
		 	
		 	for (SearchTerm searchTerm: searchCriteria.keySet()) 
			{
			    if (searchTerm.getOperator() == SearchTerm.Operator.BETWEEN || searchTerm.getOperator() == SearchTerm.Operator.ON) {
			        List<String> formattedDates = (List<String>)searchTerm.getOperator().applyWildcards(searchCriteria.get(searchTerm));
			        for(int i=0;i<formattedDates.size(); i++) {
			            query.setString(searchTerm.getSearchField()+i, formattedDates.get(i));
			            q = StringUtils.replace(q, ":" + searchTerm.getSearchField() + i, "'" + formattedDates.get(i) + "'");
			        }
			    }
			    else if (searchTerm.getOperator().isSetOperator()) 
				{
					query.setParameterList(searchTerm.getSearchField(), (List<Object>)searchCriteria.get(searchTerm));
					q = StringUtils.replace(q, ":" + searchTerm.getSearchField(), "('" + StringUtils.join((List<String>)searchTerm.getOperator().applyWildcards(searchCriteria.get(searchTerm)), "','") + "')" );
				}
				else if (searchTypeMappings.get(searchTerm.getSafeSearchField()) == Date.class ) {
					query.setDate(searchTerm.getSafeSearchField(), (java.util.Date)searchCriteria.get(searchTerm));

					q = q.replaceAll(":" + searchTerm.getSafeSearchField(),
							"'" + String.valueOf(searchTerm.getOperator().applyWildcards(searchCriteria.get(searchTerm))) + "'");
				}
				else if (searchTypeMappings.get(searchTerm.getSafeSearchField()) == Integer.class) {
					query.setInteger(searchTerm.getSafeSearchField(), (Integer)searchCriteria.get(searchTerm));

					q = q.replaceAll(":" + searchTerm.getSafeSearchField(),
							"'" + String.valueOf(searchTerm.getOperator().applyWildcards(searchCriteria.get(searchTerm))) + "'");
				}
				else 
				{
					query.setParameter(searchTerm.getSearchField(), 
							searchTerm.getOperator().applyWildcards(searchCriteria.get(searchTerm)));
					q = StringUtils.replace(q, ":" + searchTerm.getSearchField(), "'" + String.valueOf(searchTerm.getOperator().applyWildcards(searchCriteria.get(searchTerm))) + "'");
				}
			    
			}
			
//			log.debug(q);
			
			List<JobDTO> jobs = query
					.setFirstResult(offset)
					.setMaxResults(limit)
					.setCacheable(false)
					.setCacheMode(CacheMode.IGNORE)
					.list();

			session.flush();
			
			return jobs;

		} catch (Throwable ex) {
			// general catchall here since several things could go wrong
			throw new JobException(ex);
		} finally {
			try {
				HibernateUtil.commitTransaction();
			} catch (Exception ignored) {
			}
		}
	}

	/**
	 * Returns the wall clock time for this job as computed from the {@link Job#getEndTime()} and
	 * {@link Job#getCreated()}. If the job has not yet finished, the time since creation will be used.
	 *
	 * @param uuid the agave uuid of the job for which the wall time will be calculated
	 * @return the wall time in seconds
	 * @throws JobException if unable to perform the calculation
	 */
	public static int getJobWallTime(String uuid) throws JobException {
		try
		{
			Session session = getSession();
			session.clear();
			String sql = "SELECT abs(unix_timestamp(j.end_time) - unix_timestamp(j.created)) as walltime from jobs j where j.uuid = :uuid";
			
			return ((BigInteger)session.createSQLQuery(sql)
					.addScalar("walltime")
					.setString("uuid", uuid)
					.uniqueResult()).intValue();

		} catch (Throwable ex) {
			throw new JobException(ex);
		} finally {
			try {
				HibernateUtil.commitTransaction();
			} catch (Exception ignored) {
			}
		}
	}

	/**
	 * Returns the job's remote execution time, independent of queue wait and data staging, etc. The job's
	 * {@link Job#getEndTime()} and {@link Job#getStartTime()} are used for this calculation. If the job has
	 * not yet finished, the current run time will be used. If the job has not yet started running, 0 will
	 * be returned.
	 *
	 * @param uuid the agave uuid of the job for which the wall time will be calculated
	 * @return the wall time in seconds or 0 if it has not yet started to run.
	 * @throws JobException if unable to perform the calculation
	 */
	public static int getJobRunTime(String uuid) throws JobException {
		try
		{
			Session session = getSession();
			session.clear();
			String sql = "SELECT abs(unix_timestamp(j.end_time) - unix_timestamp(j.start_time)) as runtime from jobs j where j.uuid = :uuid";
			
			return ((BigInteger)session.createSQLQuery(sql)
					.addScalar("runtime")
					.setString("uuid", uuid)
					.uniqueResult()).intValue();

		} catch (Throwable ex) {
			throw new JobException(ex);
		} finally {
			try {
				HibernateUtil.commitTransaction();
			} catch (Exception ignored) {
			}
		}
	}

	/**
	 * Fetches all jobs from the current tenant. No pagination is performed here, so beware this result set.
	 * @return list of all jobs
	 * @throws JobException if the query cannot be performed.
	 */
	@SuppressWarnings("unchecked")
	public static List<Job> getAll() throws JobException
	{
		try
		{
			Session session = getSession();
			session.clear();
			List<Job> jobs = (List<Job>)session.createQuery("FROM Job").list();
			
			session.flush();
			
			return jobs;
		} catch (HibernateException ex) {
			throw new JobException(ex);
		} finally {
			try {
				HibernateUtil.commitTransaction();
			} catch (Exception ignored) {
			}
		}
	}

	/**
	 * Counts the number of active jobs in a tenant.
	 * @return number of currently active jobs.
	 * @throws JobException if the query cannot be performed.
	 */
	public static Integer countTotalActiveJobs() throws JobException
	{
		try
		{
			Session session = getSession();
			session.clear();
			String hql = "select count(*) "
					+ "from Job j "
					+ "where j.status in (" + JobStatusType.getActiveStatusValues() + ") "
					+ "		and j.visible = :visible";
			
			int currentJobCount = ((Long)session.createQuery(hql)
					.setBoolean("visible", true)
					.uniqueResult()).intValue();
			
			session.flush();
			
			return currentJobCount;

		}
		catch (Throwable ex)
		{
			throw new JobException(ex);
		}
		finally {
			try { HibernateUtil.commitTransaction();} catch (Exception ignored) {}
		}
	}

	/**
	 * Counts the number of active jobs on a {@link BatchQueue}. The {@code systemId} and {@code queueName} are given
	 * instead of domain objects to avoid redundant validation within this method.
	 *
	 * @param systemId the system id {@link ExecutionSystem} with the named {@link BatchQueue}
	 * @param queueName the name of the {@link BatchQueue} for which to count active jobs
	 * @return the number of active jobs associated with a {@link BatchQueue}.
	 * @throws JobException if the query cannot be performed
	 */
	public static Long countActiveJobsOnSystemQueue(String systemId, String queueName)
	throws JobException
	{
		try
		{
			Session session = getSession();
			session.clear();
			String hql = "select count(*) from Job where "
					+ "		queue_request = :queuerequest and "
					+ "		system = :jobsystem and "
					+ "		status in " + "(" + JobStatusType.getActiveStatusValues() + ") and "
					+ "		visible = :visible ";
			
			long currentUserJobCount = (Long) session.createQuery(hql)
					.setBoolean("visible", true)
					.setString("queuerequest", queueName)
					.setString("jobsystem", systemId)
					.uniqueResult();
			
			session.flush();
			
			return currentUserJobCount;

		} catch (HibernateException ex) {
			throw new JobException(ex);
		} finally {
			try {
				HibernateUtil.commitTransaction();
			} catch (Exception ignored) {
			}
		}
	}

	/**
	 * Counts the number of active jobs on a {@link BatchQueue} for a given {@code username}. The {@code systemId}
	 * and {@code queueName} are given instead of domain objects to avoid redundant validation within this method.
	 *
	 * @param owner the user for whom to count active jobs
	 * @param systemId the system id {@link ExecutionSystem} with the named {@link BatchQueue}
	 * @param queueName the name of the {@link BatchQueue} for which to count active jobs
	 * @return the number of active jobs associated with a {@link BatchQueue}.
	 * @throws JobException if the query cannot be performed
	 */
	public static Long countActiveUserJobsOnSystemQueue(String owner, String systemId, String queueName)
	throws JobException
	{
		try
		{
			Session session = getSession();
			session.clear();
			String hql = "select count(*) "
					+ "from Job where "
					+ "		queue_request = :queuerequest and "
					+ "		owner = :jobowner and "
					+ "		system = :jobsystem and "
					+ "		status in " + "(" + JobStatusType.getActiveStatusValues() + ") and "
					+ "		visible = :visible ";
			
			long jobCount = (Long) session.createQuery(hql)
					.setBoolean("visible", true)
					.setString("jobowner", owner)
					.setString("queuerequest", queueName)
					.setString("jobsystem", systemId)
					.uniqueResult();
			
			session.flush();
			
			return jobCount;
		} catch (HibernateException ex) {
			throw new JobException(ex);
		} finally {
			try {
				HibernateUtil.commitTransaction();
			} catch (Exception ignored) {
			}
		}
	}

	/**
	 * Returns all zombie jobs which have active transfers that have not been 
	 * updated in the last 15 minutes or which have been in intermediate 
	 * statuses without transfers for over an hour.
	 *
	 * @param tenantId the tenant for which to search.
	 * @return {@link List<Long>} of zombie {@link Job} ids
	 */
	@SuppressWarnings("unchecked")
	public static List<BigInteger> findZombieJobs(String tenantId) 
	throws JobException
	{
		boolean excludeTenant = false;
	    if (StringUtils.isEmpty(tenantId)) {
            tenantId = "%";
        } else if (tenantId.contains("!")) {
            excludeTenant = true;
            tenantId = StringUtils.removeStart(tenantId, "!");
        }
	    
		Session session = null;
		try
		{
			HibernateUtil.beginTransaction();
			session = getSession();
			session.clear();
			session.disableFilter("jobTenantFilter");
			
			String sql ="SELECT j.id " + 
						"FROM jobs j " +
						"WHERE j.status in ('PROCESSING_INPUTS', 'STAGING_INPUTS', 'STAGING_JOB', 'SUBMITTING_JOB', 'SUBMITTING', 'ARCHIVING')  " + 
						"	   AND NOW() > DATE_ADD(j.last_updated, INTERVAL 45 minute) " +  
						"	   AND j.visible = 1 " + 
						"	   AND j.tenant_id :excludetenant like :tenantId " + 
						"ORDER BY j.last_updated ASC ";
			
			sql = StringUtils.replace(sql, ":excludetenant", excludeTenant ? "not" : "");
			
			List<BigInteger> jobIds = session.createSQLQuery(sql)
					.setCacheable(false)
					.setCacheMode(CacheMode.IGNORE)
					.setString("tenantId", tenantId)
					.list();
			
			return jobIds;
		} catch (StaleObjectStateException e) {
			// unable to perform the query due to stale data. return null and try again later
			return null;
		} catch (Throwable e) {
			throw new JobException("Failed to retrieve zombie archiving jobs", e);
		} finally {
			try {
				HibernateUtil.commitTransaction();
			} catch (Exception ignored) {
			}
		}
	}

	/** Compare the current query text to the last query text for the same named query.
	 * 
	 * @param queryName the name of the sql query to be checked
	 * @param query the current query that will be logged only if it's different 
	 *     than the preceding query of the same name
	 * @return true if the current query and the last query text is the same; false otherwise
	 */
	private static boolean sameAsLastQuery(SqlQueryEnum queryName, String query)
	{
	    Integer hashCode = sqlQueryMap.get(queryName);
	    if (hashCode == null) return false;
	    return hashCode == query.hashCode();
	}
		
	/** Initialize the static mapping of sql query names to query string hashcodes.
	 * The mapping is used to skip printing sql query statements to the log when
	 * the current query text is the same as the last query text. 
	 * 
	 * @return the map initialized with all possible keys.
	 */
	private static HashMap<SqlQueryEnum,Integer> initSqlQueryMap()
	{
	    // Create and initialize the map with all possible keys.
	    HashMap<SqlQueryEnum,Integer> map = new HashMap<>();
	    map.put(SqlQueryEnum.PENDING_1, null);
	    map.put(SqlQueryEnum.PENDING_2, null);
	    map.put(SqlQueryEnum.STAGED_1, null);
	    map.put(SqlQueryEnum.STAGED_2, null);
	    map.put(SqlQueryEnum.EXECUTING, null);
	    map.put(SqlQueryEnum.ARCHIVING_1, null);
	    map.put(SqlQueryEnum.ARCHIVING_2, null);
	    
	    return map;
	}
}
