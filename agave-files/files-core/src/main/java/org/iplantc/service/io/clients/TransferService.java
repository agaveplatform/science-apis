/**
 * 
 */
package org.iplantc.service.io.clients;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.iplantc.service.common.Settings;
import org.iplantc.service.common.auth.JWTClient;
import org.iplantc.service.common.exceptions.TenantException;
import org.iplantc.service.common.model.Tenant;
import org.iplantc.service.io.exceptions.TaskException;

import javax.validation.constraints.NotNull;

/**
 * HTTP client to send transfer requests to the transfers-api
 *
 * @author dooley
 *
 */
public class TransferService {
	
	private static final Logger log = Logger.getLogger(TransferService.class);
	private static final ObjectMapper objectMapper = new ObjectMapper();

	protected String baseUrl = Settings.IPLANT_TRANSFER_SERVICE;
	protected String username;
	protected String tenantId;


	/**
	 * Base constructor using the {@link Settings#IPLANT_TRANSFER_SERVICE} as the base url
	 * @param username the user making the call
	 * @param tenantId the tenant making the call
	 */
	public TransferService(@NotNull String username, @NotNull String tenantId) {
		this.username = username;
		this.tenantId = tenantId;
	}

	/**
	 * Full constructor including the address of the transfer api.
	 * @param username the user making the call
	 * @param tenantId the tenant making the call
	 * @param baseUrl the base url of the transfer api used to construct all request URL
	 */
	public TransferService(@NotNull String username, @NotNull String tenantId, @NotNull String baseUrl) {
		this(username, tenantId);
		this.baseUrl = baseUrl;
	}

	/**
	 * Perform an authenticated GET on the endpoint returning a structured
	 * APIResponse of a transfer task matching the transfer uuid.
	 * 
	 * @param transferId  uuid of the transfer task to retrieve
	 * @return the api response
	 * @throws TaskException if the request failed
	 */
	public APIResponse get(String transferId)
	throws TaskException {
		
		if (StringUtils.isEmpty(getBaseUrl())) {
			throw new TaskException("No endpoint provided");
		}
		
		String endpoint = getBaseUrl() + "/" + transferId;

		try (CloseableHttpClient httpClient = HttpClientBuilder.create().build()){
        	HttpGet get = new HttpGet(endpoint);
			get.setHeader("Content-Type", "application/json; utf-8");
			get.setHeader("Accept", "application/json");
			get.setHeader("Content-Language", "en-US");
			get.setHeader(getHttpAuthHeader(getTenantId()), getHttpAuthToken(getUsername(), getTenantId()));

            HttpResponse response = httpClient.execute(get);

            try {
                log.debug("Response from transfers api: " + response.getStatusLine());
                HttpEntity entity = response.getEntity();
                String body = EntityUtils.toString(entity, "UTF-8");
                EntityUtils.consume(entity);
                return new APIResponse(body);
            } finally {
                get.releaseConnection();
            }
        } 
        catch (TaskException e) {
        	throw e;
        }
        catch (Exception e) {
        	throw new TaskException("Failed to perform GET on " + endpoint, e);
        }
    }

	/**
	 * Perform an authenticated POST on the endpoint returning a successful structured
	 * APIResponse containing the added transfer task
	 *
	 * @param source the source URI
	 * @param dest the destination uri
	 * @return the response from the API
	 * @throws TaskException if unable to make the request
	 */
	public APIResponse post(String source, String dest)
    throws TaskException {

		try (CloseableHttpClient httpClient = HttpClientBuilder.create().build()){
        	String body = objectMapper.createObjectNode()
					.put("source", source)
					.put("dest", dest).toString();


            HttpPost post = new HttpPost(getBaseUrl());
			post.setHeader("Content-Type", "application/json; utf-8");
			post.setHeader("Accept", "application/json");
			post.setHeader("Content-Language", "en-US");
			post.setHeader(getHttpAuthHeader(getTenantId()), getHttpAuthToken(getUsername(), getTenantId()));

            post.setEntity(new StringEntity(body, ContentType.APPLICATION_JSON));
            
            HttpResponse response = httpClient.execute(post);

            try {
				int responseCode = response.getStatusLine().getStatusCode();
				log.debug("Response from transfers api: " + response.getStatusLine());
                HttpEntity entity = response.getEntity();
                String entityContent = EntityUtils.toString(entity, "UTF-8");
				if ( responseCode == 201 || responseCode == 200 ) {
                return new APIResponse(entityContent);
				} else {
					throw new TaskException("Failed to perform POST on " + baseUrl + ": " + entityContent);
				}
            } finally {
                post.releaseConnection();
            }
        } 
        catch (TaskException e) {
        	throw e;
        }
        catch (Exception e) {
        	throw new TaskException("Failed to perform POST on " + baseUrl, e);
        }
    }
	
	protected boolean isEmpty(String val) {
		return (val == null || val.isEmpty());
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

	/**
	 * The base url for the transfers api.
	 * @return the base url to request
	 */
	public String getBaseUrl() {
		return baseUrl;
	}

	/**
	 * @param baseUrl The base url to set
	 */
	public void setBaseUrl(String baseUrl) {
		this.baseUrl = baseUrl;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getTenantId() {
		return tenantId;
	}

	public void setTenantId(String tenantId) {
		this.tenantId = tenantId;
	}
}
