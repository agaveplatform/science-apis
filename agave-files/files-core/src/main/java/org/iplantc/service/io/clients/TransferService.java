/**
 * 
 */
package org.iplantc.service.io.clients;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.iplantc.service.common.auth.JWTClient;
import org.iplantc.service.common.exceptions.TenantException;
import org.iplantc.service.common.model.Tenant;
import org.iplantc.service.io.Settings;
import org.iplantc.service.io.exceptions.TaskException;

/**
 * Abstract parent class for all API Service clients
 * @author dooley
 *
 */
public class TransferService {
	
	private static Logger log = Logger.getLogger(TransferService.class);
	protected String baseUrl = Settings.TRANSFER_SERVICE_URL;

	/**
	 * Perform an authenticated GET on the endpoint returning a structured
	 * APIResponse of a transfer task matching the transfer uuid.
	 * 
	 * @param token serialized jwt used for the request
	 * @param transferId  uuid of the transfer task to retrieve
	 * @return
	 * @throws TaskException
	 */
	public APIResponse get(String token, String transferId)
	throws TaskException {
		
		if (StringUtils.isEmpty(baseUrl)) {
			throw new TaskException("No endpoint provided");
		}
		
		if (token == null) {
			throw new TaskException("No authenticatino token provided");
		}
		
		HttpClient httpClient = null;
		String endpoint = baseUrl + "/" + transferId;

		try {
        	httpClient = getHTTPClient(endpoint, JWTClient.getCurrentEndUser(), token);
            HttpGet get = new HttpGet(endpoint);

            HttpResponse response = httpClient.execute(get);

            try {
                log.debug(response.getStatusLine());
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
        finally {
            try { httpClient.getConnectionManager().shutdown(); } catch (Exception e) {}
        }
    }

	/**
	 * Perform an authenticated POST on the endpoint returning a successful structured
	 * APIResponse containing the added transfer task
	 *
	 * @param token AuthenticationToken used for the request
	 * @param nvps List of form variables.
	 * @return
	 * @throws TaskException
	 */
	public APIResponse post(String token, List <NameValuePair> nvps)
    throws TaskException {

		if (token == null) {
			throw new TaskException("No authenticatino token provided");
		}
		
		HttpClient httpClient = null;

		HttpURLConnection connection = null;
		String tenantId;
		String username;
        try {
        	tenantId = JWTClient.getCurrentTenant();
        	username = JWTClient.getCurrentEndUser();

			httpClient = getHTTPClient(baseUrl, username, token);
            HttpPost post = new HttpPost(baseUrl);
			post.setHeader("Content-Type", "application/json; utf-8");
			post.setHeader("Accept", "application/json");
			post.setHeader("Content-Length", "application/json; utf-8");
			post.setHeader("Content-Langage", "en-US");
			post.setHeader(getHttpAuthHeader(tenantId), getHttpAuthToken(username, tenantId));
            post.setEntity(new UrlEncodedFormEntity(nvps));
            
            HttpResponse response = httpClient.execute(post);

            try {
                log.debug(response.getStatusLine());
                HttpEntity entity = response.getEntity();
                String body = EntityUtils.toString(entity, "UTF-8");
                EntityUtils.consume(entity);
                return new APIResponse(body);
            } finally {
                post.releaseConnection();
            }
        } 
        catch (TaskException e) {
        	throw e;
        }
        catch (Exception e) {
        	throw new TaskException("Failed to perform PUT on " + baseUrl, e);
        } 
        finally {
            try { httpClient.getConnectionManager().shutdown(); } catch (Exception e) {}
        }
    }
	
	protected boolean isEmpty(String val) {
		return (val == null || val.isEmpty());
	}
	
	/**
	 * Returns a pre-authenticated client. Client must be closed manually.
	 * 
	 * @param endpoint
	 * @param apiUsername
	 * @param apiPassword
	 * @return
	 * @throws Exception
	 */
	private HttpClient getHTTPClient(String endpoint, String apiUsername, String apiPassword)
	throws Exception
	{

		URL url = new URL(endpoint);
		HttpClient httpClient = HttpClientBuilder.create().build();

        return httpClient;
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
