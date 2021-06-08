/**
 *
 */
package org.iplantc.service.io.dao;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.hibernate.*;
import org.iplantc.service.common.persistence.HibernateUtil;
import org.iplantc.service.io.Settings;
import org.iplantc.service.io.exceptions.TaskException;
import org.iplantc.service.io.model.LogicalFile;
import org.iplantc.service.io.model.QueueTask;
import org.iplantc.service.io.model.StagingTask;
import org.iplantc.service.io.model.enumerations.StagingTaskStatus;
import org.iplantc.service.transfer.exceptions.TransferException;
import org.quartz.SchedulerException;

import java.io.*;
import java.math.BigInteger;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Date;

/**
 * @author dooley
 *
 */
public class QueueTaskDao
{
    private static final Logger log = Logger.getLogger(QueueTaskDao.class);
//	protected ObjectMapper mapper = new ObjectMapper();
	private String strJwtToken;

	public String getStrJwtToken() {
		return strJwtToken;
	}

	public void setStrJwtToken(String strJwtToken) {
		this.strJwtToken = strJwtToken;
	}

	// Used internally to distinguish between task types.
    private enum TaskType {STAGING}

	public static Long getNextStagingTask(String[] tenantIds)
	{
	    return getNextTask(tenantIds, TaskType.STAGING);
	}


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
	 * Pushes a job into the quartz queue by adding a StagingTask record to the db.
	 * The task will be pulled by the existing triggers and run.
	 *
	 * @param file the file to transfer
	 * @param createdBy the user to whom the staging task belongs
	 * @throws SchedulerException
	 */
	public void enqueueStagingTask(LogicalFile file, String createdBy)
	throws SchedulerException
	{
        DataOutputStream wr = null;
        HttpURLConnection connection = null;
        ObjectMapper mapper = new ObjectMapper();
		try
		{
			StagingTask task = new StagingTask(file, createdBy);
//			TransferTask tt = FileTransferVerticle.getInstance().submit(task);
//			return tt;

//            String strUrl = TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_TRANSFER_SERVICE) +
//                    "api/transfers/";
            String strUrl = "http://localhost:8085/api/transfers";
            URL url = new URL(strUrl);

            log.info("Calling transfers service " + strUrl + " with src uri " + file.getSourceUri() + " and dest: " + file.getPath());

            // Construct data
            ObjectNode transfer = mapper.createObjectNode();
            transfer.put("source", file.getSourceUri());
            transfer.put("dest", file.getPath());

            // Send data
            connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Content-Type", "application/json; utf-8");
            connection.setRequestProperty("Accept", "application/json");
            connection.setRequestProperty("Content-Length", "" + transfer.asInt());
            connection.setRequestProperty("Content-Language", "en-US");
           	connection.setRequestProperty("x-jwt-assertion-dev-staging", getStrJwtToken());
            connection.setUseCaches(false);
            connection.setDoOutput(true);
            connection.setDoInput(true);

            wr = new DataOutputStream(connection.getOutputStream());
            wr.writeBytes(transfer.toString());
            wr.flush();
            wr.close();

            //Get Response
            InputStream is = connection.getInputStream();

            if (connection.getResponseCode() == 201){
                BufferedReader rd = new BufferedReader(new InputStreamReader(is));
                String line;
                StringBuffer response = new StringBuffer();
                while ((line = rd.readLine()) != null) {
                    response.append(line);
                    response.append('\r');
                }
                rd.close();

                //Add transfer uuid to track in the transfers service
				JsonNode jsonResponse = mapper.readTree(response.toString());
                file.setTransferUuid(jsonResponse.get("result").get("uuid").textValue());

                LogicalFileDao.updateTransferStatus(file, StagingTaskStatus.STAGING_QUEUED, createdBy);

            } else {
                //failed to queue staging task
                file.setStatus(StagingTaskStatus.STAGING_FAILED.name());
                try {
                    LogicalFileDao.persist(file);
                } catch (Exception e1) {
                    String msg2 = "Failure to save logical file " + file.getName() + " while enqueuing staging task.";
                    log.error(msg2, e1);
                }
            }		}
		catch (Exception e)
		{
		    String msg = "Failure to enqueue staging task with logical file " + file.getName() + ".";
		    log.error(msg, e);

			file.setStatus(StagingTaskStatus.STAGING_FAILED.name());
			try {
				LogicalFileDao.persist(file);
			} catch (Exception e1) {
			    String msg2 = "Failure to save logical file " + file.getName() + " while enqueuing staging task.";
			    log.error(msg2, e1);
			}

			throw new SchedulerException(msg, e);
		}
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
