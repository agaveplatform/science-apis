package org.iplantc.service.io.queue;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.iplantc.service.common.auth.JWTClient;
import org.iplantc.service.common.exceptions.TenantException;
import org.iplantc.service.common.model.Tenant;
import org.iplantc.service.io.clients.APIResponse;
import org.iplantc.service.io.clients.TransferService;
import org.iplantc.service.io.dao.LogicalFileDao;
import org.iplantc.service.io.exceptions.TaskException;
import org.iplantc.service.io.model.LogicalFile;
import org.iplantc.service.io.model.enumerations.StagingTaskStatus;
import org.iplantc.service.transfer.model.TransferTask;
import org.quartz.SchedulerException;

public class TransferTaskScheduler {
    private static final Logger log = Logger.getLogger(TransferTaskScheduler.class);

    /**
     * Submits a {@link TransferTask} to the transfers api via HTTP.
     *
     * @param file     the logical file to transfer. This represents the destination of the transfer
     * @param username the user who requested the transfer
     * @throws SchedulerException if unable to submit the request
     */
    public void enqueueStagingTask(LogicalFile file, String username) throws SchedulerException {
        try {
            JsonNode responseBody = callTransferClient(file, username);

            if (responseBody.isNull()) {
                //failed
                file.setStatus(StagingTaskStatus.STAGING_FAILED.name());
                throw new SchedulerException("Error response received when submitting a new transfer request.");
            } else {
                // this is the value of the "result" field in the response
                String uuid = parseTransferResponse(responseBody);

                //Add transfer uuid to track in the transfers service
                file.setTransferUuid(uuid);
            }
        } catch (SchedulerException e) {
            throw e;
        } catch (Exception e) {

            file.setStatus(StagingTaskStatus.STAGING_FAILED.name());

            throw new SchedulerException("Unexpected error encountered submitting transfer " +
                    "request to the transfers api.", e);
        } finally {
            // save the uuid with the logical file
            updateLogicalFileAndSwallowException(file);
        }
    }


    /**
     * Extract transfer task uuid from the json response object
     * @param apiResultObject {@link JsonNode} response from transfer service
     * @return the uuid of the TransferTask
     * @throws SchedulerException if no uuid is returned
     */
    protected String parseTransferResponse(JsonNode apiResultObject) throws SchedulerException {
        // the apiResultObject is the value of the result field in the response. We can query for uuid as a top
        // level field. The text value will be null, so we're safe to request it directly and do a single null
        // check on the value.
        String uuid = apiResultObject.get("/uuid").asText();
        if (uuid == null || uuid.isEmpty()) {
            throw new SchedulerException("No uuid returned from transfers api: " + apiResultObject);
        }

        return uuid;
    }

    /**
     * Make POST request to the transfer service client to submit transfer request
     * @param file {@link LogicalFile} to transfer
     * @param username user requesting the transfer
     * @return the result object from the json response
     * @throws SchedulerException if unable to make the request
     * @throws TaskException if unable to make the request
     */
    protected JsonNode callTransferClient(LogicalFile file, String username) throws SchedulerException, TaskException {
        TransferService transferService = new TransferService(username, file.getTenantId());
        APIResponse response = transferService.post(file.getSourceUri(), file.getPath());

        if (response.isSuccess()) {
            return response.getResult();
        } else {
            throw new SchedulerException("Failed to connect to transfer service to initiate transfer request");
        }
    }


    /**
     * Mockable method to udpate a logical file without relying on the static {@link LogicalFileDao#persist(LogicalFile)}
     * method.
     *
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
     *
     * @param tenantId the current {@link Tenant#getTenantCode()}
     * @return the expected internal JWT for the given {@code tenantId}
     */
    protected String getHttpAuthHeader(String tenantId) {
        return String.format("x-jwt-assertion-%s", StringUtils.replace(tenantId, ".", "-")).toLowerCase();
    }

    /**
     * The jwt auth token string to get
     *
     * @return the serialized jwt auth token for a given user
     * @throws TenantException if the tenantId is no good
     */
    public String getHttpAuthToken(String username, String tenantId) throws TenantException {
        return JWTClient.createJwtForTenantUser(username, tenantId, false);
    }

}
