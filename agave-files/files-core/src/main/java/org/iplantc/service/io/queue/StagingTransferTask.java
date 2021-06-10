package org.iplantc.service.io.queue;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.lang.StringUtils;
import org.apache.http.NameValuePair;
import org.apache.http.message.BasicNameValuePair;
import org.apache.log4j.Logger;
import org.iplantc.service.common.auth.JWTClient;
import org.iplantc.service.common.exceptions.TenantException;
import org.iplantc.service.common.model.Tenant;
import org.iplantc.service.io.clients.APIResponse;
import org.iplantc.service.io.clients.TransferService;
import org.iplantc.service.io.dao.LogicalFileDao;
import org.iplantc.service.io.exceptions.FileProcessingException;
import org.iplantc.service.io.model.LogicalFile;
import org.iplantc.service.io.model.enumerations.StagingTaskStatus;
import org.iplantc.service.transfer.model.TransferTask;
import org.quartz.SchedulerException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class StagingTransferTask {
    private static final Logger log = Logger.getLogger(StagingTransferTask.class);

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
                String uuid = parseTransferResponse(responseBody);

                //Add transfer uuid to track in the transfers service
                file.setTransferUuid(uuid);
            }
        } catch (SchedulerException e) {
            throw e;
        } catch (IOException e) {
            throw new SchedulerException("Failed to parse the response from the transfers api: " + e.getMessage());
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
     * Extract transfer task uuid
     * @param jsonResponse {@link JsonNode} response from transfer service
     * @return
     * @throws SchedulerException
     */
    protected String parseTransferResponse(JsonNode jsonResponse) throws SchedulerException {
        JsonNode uuidNode = jsonResponse.at("/response/uuid");
        if (uuidNode == null || uuidNode.isNull()) {
            throw new SchedulerException("No uuid returned from transfers api: " + jsonResponse);
        }
        //Add transfer uuid to track in the transfers service
        return uuidNode.asText();
    }


    /**
     * Make POST request to the transfer service client to submit transfer request
     * @param file {@link LogicalFile} to transfer
     * @param username user requesting the transfer
     * @return
     * @throws SchedulerException
     * @throws IOException
     * @throws TenantException
     * @throws FileProcessingException
     */
    protected JsonNode callTransferClient(LogicalFile file, String username) throws SchedulerException, IOException, TenantException, FileProcessingException {
        List<NameValuePair> nvps = new ArrayList<>();
        nvps.add(new BasicNameValuePair("source", file.getSourceUri()));
        nvps.add(new BasicNameValuePair("dest", file.getPath()));
        APIResponse response = new TransferService().post(getHttpAuthToken(username, file.getTenantId()), nvps);

        if (response.isSuccess()) {
            return response.getResult();
        } else {
            throw new SchedulerException("Failed to connect to transfer service to initiate transfer request");
        }


        //        String transfersApiAddress = "http://transfers/api/transfers";
//        HttpURLConnection connection = null;
//        URL transfersApiUrl = null;
//
//        try {
//            transfersApiUrl = new URL(transfersApiAddress);
//        } catch (MalformedURLException e) {
//            throw new SchedulerException("Unable to connect to transfers api. " + e.getMessage());
//        }
//
//        log.info("Calling transfers service " + transfersApiAddress + " with src uri " +
//                file.getSourceUri() + " and dest: " + file.getPath());
//
//        // Construct transfer request body
//        String transferRequestBody = objectMapper.createObjectNode()
//                .put("source", file.getSourceUri())
//                .put("dest", file.getPath())
//                .toString();
//
//        // create post request
//        connection = (HttpURLConnection) transfersApiUrl.openConnection();
//        connection.setRequestMethod("POST");
//        connection.setRequestProperty("Content-Type", "application/json; utf-8");
//        connection.setRequestProperty("Accept", "application/json");
//        connection.setRequestProperty("Content-Length", "" + transferRequestBody.length());
//        connection.setRequestProperty("Content-Language", "en-US");
//        connection.setRequestProperty(getHttpAuthHeader(file.getTenantId()), getHttpAuthToken(username, file.getTenantId()));
//        connection.setUseCaches(false);
//        connection.setDoOutput(true);
//        connection.setDoInput(true);
//
//        // send request
//        try (OutputStream connectionOutputStream = connection.getOutputStream()) {
//            connectionOutputStream.write(transferRequestBody.getBytes(StandardCharsets.UTF_8));
//            connectionOutputStream.flush();
//        } catch (IOException e) {
//            throw new SchedulerException("Failed to submit transfer request to api: " + e.getMessage());
//        }
//
//        String responseBody = null;
//        int responseCode = 0;
//        String responseMessage = null;
//        try (InputStream is = connection.getInputStream()) {
//            responseBody = Streams.asString(is);
//            responseCode = connection.getResponseCode();
//            responseMessage = connection.getResponseMessage();
//        } catch (IOException e) {
//            throw new SchedulerException("Failed to read response from transfers api: " + e.getMessage());
//        }
//
//        // Handle response from transfer api request
//        if (responseCode == 201) {
//            // this is the only success response
//            JsonNode jsonResponse = objectMapper.readTree(responseBody);
//            if (jsonResponse == null) {
//                throw new SchedulerException("Unable to parse response from transfers api: " + responseBody);
//            } else {
//                JsonNode uuidNode = jsonResponse.at("/response/uuid");
//                if (uuidNode == null || uuidNode.isNull()) {
//                    throw new SchedulerException("No uuid returned from transfers api: " + responseBody);
//                }
//                //Add transfer uuid to track in the transfers service
//                file.setTransferUuid(uuidNode.asText());
//            }
//
//            // this will be handled by the transfers api created event notification listener
//            // LogicalFileDao.updateTransferStatus(file, StagingTaskStatus.STAGING_QUEUED, createdBy);
//        } else {
//            // request failed
//            file.setStatus(StagingTaskStatus.STAGING_FAILED.name());
//
//            throw new SchedulerException("Error response received while submitting a new transfer request: " +
//                    "code: " + responseCode + ", message: " + responseMessage);
//
//        }
//        return responseBody;
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
