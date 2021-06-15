/**
 *
 */
package org.iplantc.service.io.dao;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.hibernate.*;
import org.iplantc.service.common.auth.JWTClient;
import org.iplantc.service.common.exceptions.TenantException;
import org.iplantc.service.common.model.Tenant;
import org.iplantc.service.common.persistence.HibernateUtil;
import org.iplantc.service.io.Settings;
import org.iplantc.service.io.exceptions.TaskException;
import org.iplantc.service.io.model.LogicalFile;
import org.iplantc.service.io.model.QueueTask;
import org.iplantc.service.io.model.StagingTask;
import org.iplantc.service.io.model.enumerations.StagingTaskStatus;
import org.iplantc.service.io.queue.TransferTaskScheduler;
import org.iplantc.service.notification.exceptions.NotificationException;
import org.iplantc.service.transfer.exceptions.TransferException;
import org.iplantc.service.transfer.model.TransferTask;
import org.quartz.SchedulerException;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Date;

/**
 * DAO for {@link StagingTask} and {@link TransferTask}. This is deprecated in favor of the new transfers api.
 *
 * @author dooley
 * @deprecated
 * @see TransferTaskScheduler
 */
@Deprecated
public class QueueTaskDao
{
    private static final Logger log = Logger.getLogger(QueueTaskDao.class);
	private static final ObjectMapper objectMapper = new ObjectMapper();

	// Used internally to distinguish between task types.
    private enum TaskType {STAGING}
	public static Long getNextStagingTask(String[] tenantIds)
	{
	    return getNextTask(tenantIds, TaskType.STAGING);
	}

	/**
	 *
	 * @param id
	 * @return
	 * @deprecated
	 */
	public static StagingTask getStagingTaskById(Long id)
	{
		try
		{
			HibernateUtil.beginTransaction();
			Session session = HibernateUtil.getSession();
			StagingTask task = (StagingTask)session.get(StagingTask.class, id);

			HibernateUtil.commitTransaction();

			if (task == null) log.warn("Staging task with id " + id + " not found.");
			return task;
		}
		catch (HibernateException e)
		{
		    String msg = "Failed to retrieve staging task with id = " + id + ".";
		    log.error(msg, e);

			try {
				if (HibernateUtil.getSession().isOpen()) {
					HibernateUtil.rollbackTransaction();
				}
			} catch (Exception e1) {
			    log.warn("Hibernate unable to rollback failed transaction.", e1);
			}
			throw e;
		}
	}

	/**
	 * @param task
	 * @throws TaskException
	 * @throws StaleObjectStateException
	 * @deprecated
	 */
	public static void persist(QueueTask task)
	throws TaskException, StaleObjectStateException
	{
		try {
			HibernateUtil.beginTransaction();
			Session session = HibernateUtil.getSession();
			task.setLastUpdated(new Date());
			session.saveOrUpdate(task);
			HibernateUtil.commitTransaction();
		}
		catch (StaleObjectStateException e) {
		    String msg = "Unable to persist stale queue task with id " + task.getId() + ".";
		    log.error(msg, e);
			throw e;
		}
		catch (HibernateException e)
		{
		    String msg = "Unable to persist queue task with id " + task.getId() + ".";
            log.error(msg, e);

			try {
				if (HibernateUtil.getSession().isOpen()) {
					HibernateUtil.rollbackTransaction();
				}
			} catch (Exception e1) {
			    log.warn("Hibernate unable to rollback failed transaction.", e1);
			}
			throw new TaskException("msg", e);
		}
	}

	/**
	 *
	 * @param task
	 * @throws TaskException
	 * @throws StaleObjectStateException
	 * @deprecated
	 */
	public static void remove(QueueTask task)
	throws TaskException, StaleObjectStateException
	{
		if (task == null) {
			throw new HibernateException("Null task cannot be deleted.");
		}

		try {
			HibernateUtil.beginTransaction();
			Session session = HibernateUtil.getSession();
			session.delete(task);
			HibernateUtil.commitTransaction();
		}
		catch (StaleObjectStateException e) {
            String msg = "Unable to remove stale queue task with id " + task.getId() + ".";
            log.error(msg, e);
			throw e;
		}
		catch (HibernateException e)
		{
            String msg = "Unable to remove queue task with id " + task.getId() + ".";
            log.error(msg, e);

			try {
				if (HibernateUtil.getSession().isOpen()) {
					HibernateUtil.rollbackTransaction();
				}
			} catch (Exception e1) {
			    log.warn("Hibernate unable to rollback failed transaction.", e1);
			}
			throw new TaskException("Failed to delete task", e);
		}
	}

	/**
	 *
	 * @param task
	 * @return
	 * @throws TransferException
	 * @deprecated
	 */
	public static QueueTask merge(QueueTask task) throws TransferException
	{
		if (task == null)
			throw new TaskException("QueueTask cannot be null");

		try
		{
			HibernateUtil.beginTransaction();
			Session session = HibernateUtil.getSession();
			task.setLastUpdated(new Date());
			QueueTask mergedTask = (QueueTask)session.merge(task);
			HibernateUtil.commitTransaction();
			return mergedTask;
		}
		catch (HibernateException ex)
		{
            String msg = "Unable to merge queue task with id " + task.getId() + ".";
            log.error(msg, ex);

			try
			{
				if (HibernateUtil.getSession().isOpen()) {
					HibernateUtil.rollbackTransaction();
				}
			}
			catch (Exception e) {
			    log.warn("Hibernate unable to rollback failed transaction.", e);
			}

			throw new TransferException(msg, ex);
		}
	}


	/**
	 * Submits a {@link TransferTask} to the transfers api via HTTP.
	 *
	 * @param file the logical file to transfer. This represents the destination of the transfer
	 * @param username the user who requested the transfer
	 * @throws SchedulerException if unable to submit the request
	 * @deprecated 
	 * @see TransferTaskScheduler#enqueueStagingTask(LogicalFile, String)
	 */
	public void enqueueStagingTask(LogicalFile file, String username) throws SchedulerException, NotificationException {
		TransferTaskScheduler scheduler = new TransferTaskScheduler();
		scheduler.enqueueStagingTask(file, username);
	}

	/**
	 * Mockable method to udpate a logical file without relying on the static {@link LogicalFileDao#persist(LogicalFile)}
	 * method.
	 * @param logicalFile the logical file to update.
	 */
	protected void updateLogicalFileAndSwallowException(LogicalFile logicalFile) {
		try {
			LogicalFileDao.persist(logicalFile);
		} catch (Throwable t) {
			log.error("Failure to update logical file " + logicalFile.getUuid() + " while submitting transfer request: "
					+ t.getMessage());
		}
	}

	/**
	 * Generates the JWT header expected for this tenant. {@code x-jwt-assertion-<tenant_code>} where the tenant code
	 * is noncified.
	 * @param tenantId the current {@link Tenant#getTenantCode()}
	 * @return the expected internal JWT for the given {@code tenantId}
	 */
	protected String getHttpAuthHeader(String tenantId) {
		return String.format("x-jwt-assertion-%s", StringUtils.replace(tenantId, ".", "-")).toLowerCase();
	}

	/**
	 * The jwt auth token string to get
	 * @return the serialized jwt auth token for a given user
	 * @throws TenantException if the tenantId is no good
	 */
	public String getHttpAuthToken(String username, String tenantId) throws TenantException {
		return JWTClient.createJwtForTenantUser(username, tenantId, false);
	}

	/**
	 * Generate keys to use in the request to the transfers-api
	 * @param username
	 * @param roles
	 * @return
	 * @throws IOException
	 */
//	private static String getJWTToken(String username, String roles) throws IOException {
////		// generate keys to use in the test. We persist them to temp files so we can pass them to the api via the
////		// config settings.
//		CryptoHelper cryptoHelper = new CryptoHelper();
//		Path privateKey = Files.write(Files.createTempFile("private", "pem"), cryptoHelper.getPrivateKey().getBytes());
//		Path publicKey = Files.write(Files.createTempFile("public", "pem"), cryptoHelper.getPublicKey().getBytes());
//
//
//		JWTAuthOptions jwtAuthOptions = new JWTAuthOptions()
//				.setJWTOptions(new JWTOptions()
//						.setLeeway(30)
//						.setAlgorithm("RS256"))
//				.setPermissionsClaimKey("http://wso2.org/claims/role")
//				.addPubSecKey(new PubSecKeyOptions()
//						.setAlgorithm("RS256")
//						.setPublicKey(CryptoHelper.publicKey(publicKey.toAbsolutePath().toString()))
//						.setSecretKey(CryptoHelper.privateKey(privateKey.toAbsolutePath().toString())));
//
//		AgaveJWTAuthProviderImpl jwtAuth = new AgaveJWTAuthProviderImpl(jwtAuthOptions);
//
//		// Add wso2 claims set
//		String givenName = username.replace("user", "");
//		JsonObject claims = new JsonObject()
//				.put("http://wso2.org/claims/subscriber", username)
//				.put("http://wso2.org/claims/applicationid", "-9999")
//				.put("http://wso2.org/claims/applicationname", "agaveops")
//				.put("http://wso2.org/claims/applicationtier", "Unlimited")
//				.put("http://wso2.org/claims/apicontext", "/internal")
//				.put("http://wso2.org/claims/version", org.iplantc.service.common.Settings.SERVICE_VERSION)
//				.put("http://wso2.org/claims/tier", "Unlimited")
//				.put("http://wso2.org/claims/keytype", "PRODUCTION")
//				.put("http://wso2.org/claims/usertype", "APPLICATION_USER")
//				.put("http://wso2.org/claims/enduser", username)
//				.put("http://wso2.org/claims/enduserTenantId", "-9999")
//				.put("http://wso2.org/claims/emailaddress", username + "@example.com")
//				.put("http://wso2.org/claims/fullname", org.apache.commons.lang3.StringUtils.capitalize(givenName) + " User")
//				.put("http://wso2.org/claims/givenname", givenName)
//				.put("http://wso2.org/claims/lastname", "User")
//				.put("http://wso2.org/claims/primaryChallengeQuestion", "N/A")
//				.put("http://wso2.org/claims/role", ServiceUtils.explode(",", List.of("Internal/everyone,Internal/subscriber", roles)))
//				.put("http://wso2.org/claims/title", "N/A");
//
//		JWTOptions jwtOptions = (JWTOptions) new JWTOptions()
//				.setAlgorithm("RS256")
//				.setExpiresInMinutes(10_080) // 7 days
//				.setIssuer("transfers-api-integration-tests")
//				.setSubject(username);
//
//		return jwtAuth.generateToken(claims, jwtOptions);
//
//	}

	/** Do the real work of querying for the next task.  This method
	 * throws some type of runtime execution on error and may modify
	 * the tenandIds array.
	 *
	 * @param tenantIds non-null array of tenant ids
	 * @param taskType one of the two supported task types
	 * @return a task id
	 * @throws RuntimeException on error
	 */
    private static Long getNextTask(String[] tenantIds, TaskType taskType)
    {
        // ---------------------- Input validation ----------------------
        // This shouldn't happen (see Settings.getQueueTaskTenantIds()).
        if (tenantIds == null) {
            String msg = "Unable to query next " + taskType.name().toLowerCase() +
                         " task because of null tenant id input.";
            log.error(msg);
            throw new TaskException(msg);
        }

        // Check for the invalid tenant id.  An exception is thrown when 
        // initialization errors caused by invalid input and hard to recover 
        // from when first detected are encountered here.
        if ((tenantIds.length > 0) &&
            Settings.INVALID_QUEUE_TASK_TENANT_ID.equals(tenantIds[0]))
        {
            String msg = "Unable to query next " + taskType.name().toLowerCase() +
                         " task because of invalid tenant id input.";
            log.error(msg);
            throw new TaskException(msg);
        }

        // ---------------------- String Assignment ---------------------
        // Assign task type dependent variables.
        String tableName =  "staging_tasks" ;
        String status    =  StagingTaskStatus.STAGING_QUEUED.name();

        // Tracing.
        if (log.isTraceEnabled()) {
            String msg = "Retrieving next queued task of type " + taskType.name();
            if (tenantIds.length > 0) msg += " for tenant(s) " + StringUtils.join(tenantIds, ", ") + ".";
              else msg += " for all tenants.";
            log.trace(msg);
        }

        // Construct the tenant id part of the sql where clause.
        String  tenantClause  = "";
        boolean excludeTenant = false;
        if (tenantIds.length > 0) {

            // We can detect negation by inspecting just the first element
            // since input validation guarantees that the elements are either
            // all assertions or all negations.  We remove the leading ! on
            // all negations.
            if (tenantIds[0].startsWith("!")) {
                excludeTenant = true;
                for (int i = 0; i < tenantIds.length; i++) {
                    tenantIds[i] = StringUtils.removeStart(tenantIds[i], "!");
                }
            }

            // Build clause with a placeholder for each tenant id.
            tenantClause = "and f.tenant_id " + (excludeTenant ? "not" : "") + " in (";
            for (int i = 0; i < tenantIds.length; i++) {
                if (i == 0) tenantClause += ":tid" + i;
                 else tenantClause += ", :tid" + i;
            }
            tenantClause += ") "; // trailing space
        }

        // ---------------------- Task Query ----------------------------
        try
        {
            Session session = HibernateUtil.getSession();
            session.clear();

            // Initialize the query text with placeholders.
            String hql = "select t.id "
                    + "from " + tableName + " t left join logical_files f on t.logical_file_id = f.id "
                    + "where t.status = :status " + tenantClause + "order by rand()";

            // Construct the query object and begin filling in placeholders.
            Query qry = session.createSQLQuery(hql)
                               .setCacheable(false)
                               .setCacheMode(CacheMode.REFRESH)
                               .setString("status", status);

            // Fill in all tenantId placeholder fields that we 
            // constructed in the above tenant clause.
            for (int i = 0; i < tenantIds.length; i++) {
                qry.setString("tid" + i, tenantIds[i]);
            }

            // Execute the query.
            BigInteger taskId = (BigInteger) qry.setMaxResults(1).uniqueResult();

            // Success.
            HibernateUtil.commitTransaction();

            // Get the result as a long if one was selected.
            Long result = null;
            if (taskId != null) result = taskId.longValue();
            if (log.isTraceEnabled()) {
                String msg = "Next selected " + taskType.name() + " task: " + result;
                log.trace(msg);
            }
            return result;
        }
        catch (HibernateException e)
        {
            // Construct error message and log.
            String tenantInfo;
            if (tenantIds.length == 0) tenantInfo = "any tenant.";
            else if (tenantIds.length == 1) tenantInfo = "tenant " + tenantIds[0] + ".";
            else tenantInfo = "tenants [" + StringUtils.join(tenantIds, ", ") + "].";

            String msg = "Failure to retrieve next " + taskType.name().toLowerCase() +
                         " task for " + tenantInfo;
            log.error(msg, e);

            // Explicitly rollback if possible.
            try
            {
                if (HibernateUtil.getSession().isOpen()) {
                    HibernateUtil.rollbackTransaction();
                }
            }
            catch (Exception e1) {
                log.warn("Hibernate unable to rollback failed transaction.", e1);
            }

            // Throw the original exception.
            throw new HibernateException(msg, e);
        }
    }
}
