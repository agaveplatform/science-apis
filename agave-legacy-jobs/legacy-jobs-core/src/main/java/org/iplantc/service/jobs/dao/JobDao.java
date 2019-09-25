/**
 * 
 */
package org.iplantc.service.jobs.dao;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.hibernate.CacheMode;
import org.hibernate.HibernateException;
import org.hibernate.ObjectNotFoundException;
import org.hibernate.Query;
import org.hibernate.SQLQuery;
import org.hibernate.Session;
import org.hibernate.StaleObjectStateException;
import org.hibernate.StaleStateException;
import org.hibernate.UnresolvableObjectException;
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
import org.joda.time.DateTime;

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
	 * Returns all jobs for a given username.
	 * 
	 * @param username
	 * @return
	 * @throws JobException
	 */
	public static List<Job> getByUsername(String username) throws JobException
	{
		return JobDao.getByUsername(username, 0, Settings.DEFAULT_PAGE_SIZE);
	}
	
	/**
	 * Returns all jobs for a given username.
	 * @param username
	 * @param offset
	 * @param limit
	 * @return
	 * @throws JobException
	 * @deprecated 
	 * @see {@link #getByUsername(String, int, int, AgaveResourceSearchResultOrdering, SearchTerm)
	 */
	public static List<Job> getByUsername(String username, int offset, int limit) 
	throws JobException
	{
		return getByUsername(username, offset, limit, null, null);
	}
	
	/**
	 * Returns all jobs for a given username with optional search result ordering
	 * @param username
	 * @param offset
	 * @param limit
	 * @return
	 * @throws JobException
	 */
	@SuppressWarnings("unchecked")
	public static List<Job> getByUsername(String username, int offset, int limit, AgaveResourceResultOrdering order, SearchTerm orderBy) 
	throws JobException
	{
		if (order == null) {
			order = AgaveResourceResultOrdering.DESC;
		}
		
		String sortField = "";
		if (orderBy == null) {
			// This is a fix.  new JobSearchFilter().filterAttributeName("lastupdated"); returns a null
			// in some unpredictable sequence of API calls to history and summary end-points. So, commented
			// out the statement
			//orderBy = new JobSearchFilter().filterAttributeName("lastupdated");
			sortField = "j.last_updated";
		} else {
			sortField = String.format(orderBy.getMappedField(), orderBy.getPrefix())  ;
		}
		
		try
		{
			Session session = getSession();
			session.clear();
			String sql = "select distinct j.* \n"
					+ "from aloe_jobs j \n"
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
					+ "order by " + sortField +" " +  order.toString() + " \n";
			
			String q = sql;
			q = StringUtils.replace(q, ":owner", String.format("'%s'", username));
			q = StringUtils.replace(q, ":none", String.format("'%s'", PermissionType.NONE.name()));
			q = StringUtils.replace(q, ":visible", String.format("'%d'", 1));
			q = StringUtils.replace(q, ":tenantid", String.format("'%s'", TenancyHelper.getCurrentTenantId()));
			
//			log.debug(q);
			List<Job> jobs = session.createSQLQuery(sql).addEntity(Job.class)
					.setString("owner",username)
					.setString("none",PermissionType.NONE.name())
					.setInteger("visible", new Integer(1))
					.setString("tenantid", TenancyHelper.getCurrentTenantId())
					.setFirstResult(offset)
					.setMaxResults(limit)
					.list();
			
			session.flush();
			
			return jobs;

		}
		catch (ObjectNotFoundException e) {
			return new ArrayList<Job>();
		}
		catch (HibernateException ex)
		{
			throw new JobException(ex);
		}
		finally {
			try { HibernateUtil.commitTransaction();} catch (Exception e) {}
		}
	}

	/**
	 * Gets a job by its unique id.
	 * @param jobId
	 * @return
	 * @throws JobException
	 */
	public static Job getById(long jobId) throws JobException
	{
		try
		{
			Session session = getSession();
//			session.clear();
			session.disableFilter("jobTenantFilter");
			Job job = (Job) session.get(Job.class, jobId);
			
//			session.flush();
			
			return job;

		}
		catch (ObjectNotFoundException ex)
		{
			return null;
		}
		catch (HibernateException ex)
		{
			throw new JobException(ex);
		}
		finally {
			try { HibernateUtil.commitTransaction();} catch (Exception e) {}
		}
	}
	
	/**
     * Gets a {@link Job} by its uuid.
     * 
     * @param uuid identifier for the job
     * @return {@link Job} object
     * @throws JobException
     */
	public static Job getByUuid(String uuid) throws JobException
    {
	    return getByUuid(uuid, true);
    }
	
	
	/**
	 * Gets a {@link Job} by its uuid, optionally forcing cache eviction
	 * 
	 * @param uuid identifier for the job
	 * @param forceFlush should the cache be flushed on this request?
	 * @return {@link Job} object
	 * @throws JobException
	 */
	public static Job getByUuid(String uuid, boolean forceFlush) throws JobException
	{
		if (StringUtils.isEmpty(uuid)) return null;
		
		try
		{
		    HibernateUtil.beginTransaction();
			Session session = HibernateUtil.getSession();
									
			Job job = (Job) session.createSQLQuery("select * from aloe_jobs where uuid = :uuid")
			        .addEntity(Job.class)
					.setString("uuid",uuid)
					.setCacheable(false)
					.setCacheMode(CacheMode.IGNORE)
                    .uniqueResult();
			
			if (forceFlush) session.flush();
			
			return job;

		}
		catch (ObjectNotFoundException ex)
		{
			return null;
		}
		catch (HibernateException ex)
		{
			throw new JobException(ex);
		}
		finally {
//		    if (forceFlush)
		        try { HibernateUtil.commitTransaction();} catch (Exception e) {}
		}
	}
	
	
	/**
	 * Refreshes a stale job
	 * @param job
	 * @return
	 * @throws JobException
	 */
	public static void refresh(Job job) throws JobException
	{
		try
		{
			Session session = getSession();
			session.clear();
			session.refresh(job);
			session.flush();
		}
		catch (HibernateException ex)
		{
		    log.error("Concurrency issue with job " + job.getUuid());
			throw new JobException(ex);
		}
		finally {
			try { HibernateUtil.commitTransaction();} catch (Exception e) {}
		}
	}
	
	/**
	 * Merges the current cached job object with the persisted value. The 
	 * current instance is not updated. The merged instance is returned.
	 * 
	 * @param job
	 * @return merged job.
	 * @throws JobException
	 */
	public static Job merge(Job job) throws JobException
	{
		try
		{
		    HibernateUtil.beginTransaction();
			Session session = HibernateUtil.getSession();
			return (Job)session.merge(job);
		}
		catch (HibernateException ex)
		{
		    log.error("Concurrency issue with job " + job.getUuid());
			throw new JobException(ex);
		}
	}
	
	/**
	 * Returns a job owned by the user with teh given job id. This is essentially
	 * just a security check to make sure the owner matches the job id. ACL are 
	 * not taken into consideration here, just ownership.
	 * 
	 * @param username
	 * @param jobId
	 * @return
	 * @throws JobException
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

		}
		catch (ObjectNotFoundException ex)
		{
			return null;
		}
		catch (HibernateException ex)
		{
			throw new JobException(ex);
		}
		finally {
			try { HibernateUtil.commitTransaction();} catch (Exception e) {}
		}
	}

	/**
	 * Returns a {@link List} of {@link Job}s belonging to the given user with the given status.
	 * Permissions are observed in this query.
	 * 
	 * @param username
	 * @param jobStatus
	 * @return
	 * @throws JobException
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
					.setInteger("isadmin", new Integer(BooleanUtils.toInteger(ServiceUtils.isAdmin(username))) )
					.setBoolean("visible", true)
					.setString("status", jobStatus.name())
					.list();

			session.flush();
			
			return jobs;

		}
		catch (ObjectNotFoundException e) {
			return new ArrayList<Job>();
		}
		catch (HibernateException ex)
		{
			throw new JobException(ex);
		}
		finally {
			try { HibernateUtil.commitTransaction();} catch (Exception e) {}
		}
	}
	
	
	/**
	 * Returns the number of jobs in an active state for a given user 
	 * on a given system. Active states are defined by 
	 * JobStatusType.getActiveStatuses().
	 * 
	 * @param username
	 * @param system
	 * @return
	 * @throws JobException
	 */
	public static long countActiveUserJobsOnSystem(String username, String system) throws JobException
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
			
			long currentUserJobCount = ((Long)session.createQuery(hql)
					.setBoolean("visible", true)
					.setString("jobowner",username)
					.setString("jobsystem", system)
					.uniqueResult()).longValue();
			
			session.flush();
			
			return currentUserJobCount;

		}
		catch (ObjectNotFoundException ex)
		{
			throw new JobException(ex);
		}
		catch (HibernateException ex)
		{
			throw new JobException(ex);
		}
		finally {
			try { HibernateUtil.commitTransaction();} catch (Exception e) {}
		}
	}
	
	
	
	/**
	 * Returns the total number of jobs in an active state 
	 * on a given system. Active states are defined by 
	 * JobStatusType.getActiveStatuses().
	 * 
	 * @param username
	 * @param system
	 * @return jobCount 
	 * @throws JobException
	 */
	public static long countActiveJobsOnSystem(String system) throws JobException
	{
		try
		{
			Session session = getSession();
			session.clear();
			String hql = "select count(*) from Job " +
					"where system = :jobsystem and " +
					"status in (" + JobStatusType.getActiveStatusValues() + ")";
			
			long currentUserJobCount = ((Long)session.createQuery(hql)
					.setString("jobsystem", system)
					.uniqueResult()).longValue();
			
			session.flush();
			
			return currentUserJobCount;

		}
		catch (ObjectNotFoundException ex)
		{
			throw new JobException(ex);
		}
		catch (HibernateException ex)
		{
			throw new JobException(ex);
		}
		finally {
			try { HibernateUtil.commitTransaction();} catch (Exception e) {}
		}
	}
	
	public static void persist(Job job) 
	throws JobException, UnresolvableObjectException 
	{
		persist(job, true);
	}
	
	public static void persist(Job job, boolean forceTimestamp) 
	throws JobException, UnresolvableObjectException
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
		}
		catch (UnresolvableObjectException ex) {
//		    throw ex;
		}
		catch (StaleStateException ex) {
		    throw ex;
		}
		catch (HibernateException ex)
		{
			try
			{
				if (session != null && session.isOpen())
				{
					HibernateUtil.rollbackTransaction();
					session.close();
				}
			}
			catch (Exception e) {}
			log.error("Concurrency issue with job " + job.getUuid());
			
			throw new JobException(ex);
		}
		finally {
		    try {HibernateUtil.commitTransaction(); } catch (Exception e) {}
		}
	}

	/**
	 * Deletes a job from the db.
	 * @param job
	 * @throws JobException
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
			catch (Throwable e) {}
				
			throw new JobException(ex);
		}
		finally {
			try { HibernateUtil.commitTransaction();} catch (Exception e) {}
		}
	}
	
	
	/**
	 * Selects a job at random from the population of jobs waiting to be archived. The selection 
	 * is made by first getting a list of all users with pending archiving jobs, then selecting 
	 * one user and picking one of their jobs at random. All capacity filtering parameters which 
     * enable restriction of job selection by tenant, owner, system, and batch queue are taken
     * into consideration.
	 * 
	 * @param tenantid tenant to include or exclude from selection.
     * @param owners array of owners to include or exclude from the selection process 
	 * @param systemIds array of systems and queues to include or exclude from the selectino process. 
	 * @return uuid of next job to archive.
	 * @throws JobException
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
		}
        catch (StaleObjectStateException ex) {
            log.error("Unable to select archiving job on tenant " + tenantId + " due to stale object.", ex);
            return null;
        }
        catch (ObjectNotFoundException ex)
        {
            log.warn("Unable to select archiving job on tenant " + tenantId + " due to object not found.", ex);
            return null;
        }
        catch (HibernateException ex)
        {
            log.error("Unable to select archiving job on tenant " + tenantId + " due to hibernate problem.", ex);
            throw new JobException(ex);
        }
		finally {
			try { HibernateUtil.commitTransaction();} catch (Exception e) {}
		}
		
	}
	
	/**
	 * Selects a user at random from the population of users with jobs of the given {@code status}.
	 * System quotas are taken into consideration as are the capacity filtering parameters which 
	 * enable restriction of job selection by tenant, owner, system, and batch queue.
	 * 
	 * @param status by which to filter the list of active jobs.
	 * @param tenantid tenant to include or exclude from selection.
     * @param owners array of owners to include or exclude from the selection process 
     * @param systemIds array of systems and queues to include or exclude from the selectino process. 
     * @return username of user with pending job
	 * @throws JobException
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
            try { HibernateUtil.commitTransaction();} catch (Exception e) {}
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
     * @param tenantid tenant to include or exclude from selection.
     * @param owners array of owners to include or exclude from the selection process 
     * @param systemIds array of systems and queues to include or exclude from the selectino process. 
     * @return username of user with pending job
     * @see {@link #getRandomUserForNextQueuedJobOfStatus(JobStatusType, String, String[], String[])}
     * @throws JobException
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
            try { HibernateUtil.commitTransaction();} catch (Exception e) {}
        }
	}
	
	/**
	 * Performs a string replacement and update on the job with the 
	 * given id. This is only needed because job inputs are not a 
	 * separate table.
	 * 
	 * @param jobId
	 * @param source
	 * @param dest
	 * @throws JobException
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
		}
		catch (HibernateException ex)
		{
			throw new JobException(ex);
		}
		finally {
			try { HibernateUtil.commitTransaction();} catch (Exception e) {}
		}

	}

	/**
	 * Searches for jobs by the given user who matches the given set of 
	 * parameters. Permissions are honored in this query.
	 * 
	 * @param username
	 * @param searchCriteria Map of key value pairs by which to query.
	 * @return
	 * @throws JobException
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
	 * @param username
	 * @param searchCriteria
	 * @param offset
	 * @param limit
	 * @return
	 * @throws JobException
	 */
	public static List<JobDTO> findMatching(String username,
			Map<SearchTerm, Object> searchCriteria,
			int offset, int limit) throws JobException
	{
		return findMatching(username, searchCriteria, offset, limit, null, null);
	}
	
	/**
	 * Searches for jobs by the given user who matches the given set of 
	 * parameters. Permissions are honored in this query.
	 *
	 * @param username
	 * @param searchCriteria
	 * @param offset
	 * @param limit
	 * @return
	 * @throws JobException
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
//			if (orderBy == null) {
//				orderBy = j.last_updated
//			}
		}
		
		try
		{
			Session session = getSession();
			session.clear();
			String sql = "SELECT j.archive_path, \n" + 
						"       a.system_id as archive_system, \n" + 
						"       j.id, \n" + 
						"       j.name, \n" +
						"       j.tenant_id, \n" + 
						"		j.tenant_queue, \n" +
						"       j.owner, \n" +
						" 		j.app_id, \n" +
						" 		j.app_uuid, \n" +
						"       j.status, \n" + 
						" 		j.last_message, \n" +
						"		j.accepted, \n" +
						"       j.created, \n" + 
						"		j.ended, \n" +
						"       j.last_updated, \n" + 
						"       j.uuid, \n" + 		
						"       j.work_path, \n" + 
						" 		j.archive, \n" +
						"		j.archive_system_id, \n" +
					    "       j.node_count, \n" + 
					    "       j.processor_count, \n" + 
					    " 		j.memory_gb, \n" +
						" 		j.max_hours, \n" +
						"       j.inputs, \n" + 
					    "       j.parameters, \n" + 
					    " 		j.remote_job_id, \n" +
						" 		j.remote_sched_id, \n" +
						"       j.remote_queue, \n" +
						" 		j.remote_submitted, \n" +
						"       j.remote_started, \n" +
						" 		j.remote_ended, \n" +
						" 		j.remote_outcome, \n" +
						" 		j.remote_submit_retries, \n" +
						" 		j.remote_status_checks, \n" +
						" 		j.failed_status_checks, \n" +
						"       j.last_status_check, \n" +
						"       j.update_token, \n" + 
					    "       j.visible, \n" + 					
					    " 		j.blocked_count \n" +
					" FROM aloe_jobs j \n" +
					" LEFT OUTER JOIN systems a ON j.archive_system_id = a.id \n";
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
			
			SQLQuery query = session.createSQLQuery(sql);
			query.addScalar("id", StandardBasicTypes.LONG)
			     .addScalar("name", StandardBasicTypes.STRING)
			     .addScalar("tenant_id", StandardBasicTypes.STRING)
			     .addScalar("tenant_queue", StandardBasicTypes.STRING)
			     .addScalar("owner", StandardBasicTypes.STRING)
			     .addScalar("archive_system", StandardBasicTypes.STRING)
			     .addScalar("app_id", StandardBasicTypes.STRING)
			     .addScalar("app_uuid", StandardBasicTypes.STRING)
				 .addScalar("status", StandardBasicTypes.STRING)
				 .addScalar("last_message", StandardBasicTypes.STRING)
				 .addScalar("accepted", StandardBasicTypes.TIMESTAMP)
				 .addScalar("created", StandardBasicTypes.TIMESTAMP)
				 .addScalar("ended", StandardBasicTypes.TIMESTAMP)
				 .addScalar("last_updated", StandardBasicTypes.TIMESTAMP)
				 .addScalar("uuid", StandardBasicTypes.STRING)
				 .addScalar("work_path", StandardBasicTypes.STRING)
				 .addScalar("archive", StandardBasicTypes.BOOLEAN)
				 .addScalar("archive_path", StandardBasicTypes.STRING)
				 .addScalar("archive_system_id", StandardBasicTypes.STRING)
				 .addScalar("node_count", StandardBasicTypes.INTEGER)
				 .addScalar("processor_count", StandardBasicTypes.INTEGER)
				 .addScalar("memory_gb", StandardBasicTypes.FLOAT)
				 .addScalar("max_hours", StandardBasicTypes.FLOAT)
				 .addScalar("inputs", StandardBasicTypes.STRING)
				 .addScalar("parameters", StandardBasicTypes.STRING)
				 .addScalar("remote_job_id", StandardBasicTypes.STRING)
				 .addScalar("remote_sched_id", StandardBasicTypes.STRING)
				 .addScalar("remote_queue", StandardBasicTypes.STRING)
				 .addScalar("remote_submitted", StandardBasicTypes.TIMESTAMP)
				 .addScalar("remote_started", StandardBasicTypes.TIMESTAMP)
				 .addScalar("remote_ended", StandardBasicTypes.TIMESTAMP)
				 .addScalar("remote_outcome", StandardBasicTypes.STRING)
				 .addScalar("remote_submit_retries", StandardBasicTypes.INTEGER)
				 .addScalar("remote_status_checks", StandardBasicTypes.INTEGER)
				 .addScalar("failed_status_checks", StandardBasicTypes.INTEGER)
				 .addScalar("last_status_check", StandardBasicTypes.TIMESTAMP)
				 .addScalar("blocked_count", StandardBasicTypes.INTEGER)
				 .addScalar("visible", StandardBasicTypes.BOOLEAN)
				 .addScalar("update_token", StandardBasicTypes.STRING)
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
				else 
				{
					query.setParameter(searchTerm.getSearchField(), 
							searchTerm.getOperator().applyWildcards(searchCriteria.get(searchTerm)));
					q = StringUtils.replace(q, ":" + searchTerm.getSearchField(), "'" + String.valueOf(searchTerm.getOperator().applyWildcards(searchCriteria.get(searchTerm))) + "'");
				}
			    
			}
			

			
			List<JobDTO> jobs = query
					.setFirstResult(offset)
					.setMaxResults(limit)
					.setCacheable(false)
					.setCacheMode(CacheMode.IGNORE)
					.list();

			session.flush();
			
			return jobs;

		}
		catch (Throwable ex)
		{
			throw new JobException(ex);
		}
		finally {
			try { HibernateUtil.commitTransaction();} catch (Exception e) {}
		}
	}
	
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
			
		}
		catch (Throwable ex)
		{
			throw new JobException(ex);
		}
		finally {
			try { HibernateUtil.commitTransaction();} catch (Exception e) {}
		}
	}
	
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
			
		}
		catch (Throwable ex)
		{
			throw new JobException(ex);
		}
		finally {
			try { HibernateUtil.commitTransaction();} catch (Exception e) {}
		}
	}

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
		}
		catch (HibernateException ex)
		{
			throw new JobException(ex);
		}
		finally {
			try { HibernateUtil.commitTransaction();} catch (Exception e) {}
		}
	}

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
			try { HibernateUtil.commitTransaction();} catch (Exception e) {}
		}
	}
	
//	/**
//	 * Collects current active job activity stats for a single tenant using 
//	 * formula driven attributes.
//	 *  
//	 * @param tenantCode The unique code of the tenant
//	 * @return {@link TenantJobActivity} containing stats on active jobs in the tenant
//	 * @throws JobException
//	 */
//	public static SummaryTenantJobActivity getSummaryTenantJobActivity(String tenantCode)
//	throws JobException
//	{
//		try
//		{
//			HibernateUtil.beginTransaction();
//			Session session = HibernateUtil.getSession();
//			String hql = "from SummaryTenantJobActivity where id.tenantId = :tenantCode";
//			SummaryTenantJobActivity tenantJobActivity = (SummaryTenantJobActivity)(session.createQuery(hql)
//					.setString("tenantCode", tenantCode)
//					.uniqueResult());
//			
//			session.flush();
//			
//			return tenantJobActivity;
//
//		}
//		catch (Throwable ex)
//		{
//			throw new JobException(ex);
//		}
//		finally {
//			try { HibernateUtil.commitTransaction();} catch (Exception e) {}
//		}
//	}
//	
//	/**
//	 * Collects current active job activity stats for all tenants using 
//	 * formula driven attributes.
//	 *  
//	 * @return {@link List} of {@link TenantJobActivity} containing stats on active jobs for each tenant
//	 * @throws JobException
//	 */
//	@SuppressWarnings("unchecked")
//	public static List<SummaryTenantJobActivity> getSummaryTenantJobActivityForAllTenants()
//	throws JobException
//	{
//		try
//		{
//			HibernateUtil.beginTransaction();
//			Session session = HibernateUtil.getSession();
//			String hql = "from SummaryTenantJobActivity";
//			List<SummaryTenantJobActivity> tenantJobActivities = (List<SummaryTenantJobActivity>)session
//					.createQuery(hql)
//					.list();
//			
//			session.flush();
//			
//			return tenantJobActivities;
//
//		}
//		catch (Throwable ex)
//		{
//			throw new JobException(ex);
//		}
//		finally {
//			try { HibernateUtil.commitTransaction();} catch (Exception e) {}
//		}
//	}

	public static Long countActiveJobsOnSystemQueue(String system, String queueName) 
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
			
			long currentUserJobCount = ((Long)session.createQuery(hql)
					.setBoolean("visible", true)
					.setString("queuerequest", queueName)
					.setString("jobsystem", system)
					.uniqueResult()).longValue();
			
			session.flush();
			
			return currentUserJobCount;

		}
		catch (ObjectNotFoundException ex)
		{
			throw new JobException(ex);
		}
		catch (HibernateException ex)
		{
			throw new JobException(ex);
		}
		finally {
			try { HibernateUtil.commitTransaction();} catch (Exception e) {}
		}
	}

	public static Long countActiveUserJobsOnSystemQueue(String owner, String system, String queueName) 
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
			
			long jobCount = ((Long)session.createQuery(hql)
					.setBoolean("visible", true)
					.setString("jobowner",owner)
					.setString("queuerequest", queueName)
					.setString("jobsystem", system)
					.uniqueResult()).longValue();
			
			session.flush();
			
			return jobCount;

		}
		catch (ObjectNotFoundException ex)
		{
			throw new JobException(ex);
		}
		catch (HibernateException ex)
		{
			throw new JobException(ex);
		}
		finally {
			try { HibernateUtil.commitTransaction();} catch (Exception e) {}
		}
	}

	/**
	 * Returns all zombie jobs which have active transfers that have not been 
	 * updated in the last 15 minutes or which have been in intermediate 
	 * statuses without transfers for over an hour.
	 * 
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
		}
		catch (StaleObjectStateException e) {
			return null;
		}
		catch (Throwable e)
		{
			throw new JobException("Failed to retrieve zombie archiving jobs", e);
		}
		finally {
			try { HibernateUtil.commitTransaction();} catch (Exception e) {}
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
