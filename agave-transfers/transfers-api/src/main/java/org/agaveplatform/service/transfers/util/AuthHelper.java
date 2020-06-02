package org.agaveplatform.service.transfers.util;

import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonObject;
import net.minidev.json.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.iplantc.service.common.auth.JWTClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class AuthHelper {
    private static Logger log = LoggerFactory.getLogger(AuthHelper.class);

    private static final String JWT_HEADER_PREFIX = "x-jwt-assertion";
    private static final String INTERNALUSER_HEADER_NAME = "x-agave-internaluser";

    /**
     * Returns the first header starting with the {@link #JWT_HEADER_PREFIX}.
     * @param headers list of requet headers
     * @return the name of the header or null if none match
     */
    public static String getAuthHeader(List<Map.Entry<String,String>> headers) {
        for (Map.Entry<String, String> header: headers) {
            String key = header.getKey().toLowerCase();
            // find the first matching header with the JWT_HEADER_PREFIX
            if (key.startsWith(JWT_HEADER_PREFIX)) {
                return key;
            }
        }
        return null;
    }

    /**
     * Parses out the tenant id from the tenant auth header. Expected format is <pre>JWT_HEADER_PREFIX-TENANT_ID</pre>
     * @param authHeader the header name starting with the {@link #JWT_HEADER_PREFIX} and ending in the tenant id
     * @return the tenant id or null if no id is present in the header
     */
    public static String getTenantIdFromAuthHeader(String authHeader) {
        String tenantId = null;
        if (StringUtils.startsWith(authHeader, JWT_HEADER_PREFIX)) {
            if (! StringUtils.equalsIgnoreCase(authHeader, JWT_HEADER_PREFIX)) {
                tenantId = authHeader.toLowerCase().substring(JWT_HEADER_PREFIX.length() + 1);
                tenantId = StringUtils.replaceChars(tenantId, '_', '.');
                tenantId = StringUtils.replaceChars(tenantId, '-', '.');
            }
        }

        return tenantId;
    }

    public static boolean authenticate(HttpServerRequest req) {
        List<Map.Entry<String,String>> headers = req.headers().entries();
        String tenantId;
        String username;

        for (Map.Entry<String, String> header: headers) {
            String key = header.getKey().toLowerCase();
            // find the first matching header with the JWT_HEADER_PREFIX
            if (key.startsWith(JWT_HEADER_PREFIX)) {

                // debug headers if requested
                if (StringUtils.isNotEmpty(req.getParam("debugjwt"))) {
                    log.debug(key + " : " + header.getValue());
                }

                if (StringUtils.equalsIgnoreCase(key, JWT_HEADER_PREFIX)) {
                    tenantId= null;
                } else {
                    tenantId = key.toLowerCase().substring(JWT_HEADER_PREFIX.length() + 1);
                }

                try {
                    // if we find the header, parse it and add the bearer token to the context
                    // for later use
                    if (JWTClient.parse(header.getValue(), tenantId)) {

                        String bearerToken = req.getHeader("Authorization");
                        if (StringUtils.isNotEmpty(bearerToken)) {
                            bearerToken = StringUtils.removeStart(bearerToken, "Bearer");
                            bearerToken = StringUtils.strip(bearerToken);

                            JWTClient.setCurrentBearerToken(bearerToken);
                        }
                        return true;
                    } else {
                        return false;
                    }
                } catch (Exception e) {
                    log.error("Invalid JWT header presented for tenant " + tenantId);
                }
            }
        }

        log.error("No " + JWT_HEADER_PREFIX + " header found in request. Authentication failed.");
        return false;
    }

    public static boolean isTenantAdmin(JsonObject claims) {
        try {
            String roles = (String)claims.getString("http://wso2.org/claims/role");
            if (!StringUtils.isEmpty(roles)) {
                for(String role: Arrays.asList(StringUtils.split(roles, ","))) {
                    if (StringUtils.endsWith(role, "-services-admin") || StringUtils.endsWith(role, "-super-admin")) {
                        if (role.contains("/")) {
                            role = role.substring(role.lastIndexOf("/") + 1);
                        }
                        if (StringUtils.startsWith(role, claims.getString("rawTenantId"))) {
                            return true;
                        }
                    }
                }
                return false;
            } else {
                return false;
            }
        } catch (Exception e) {
            return false;
        }
    }
}
