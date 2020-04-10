package org.agaveplatform.service.transfers.auth;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.util.*;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.AuthProvider;
import org.agaveplatform.service.transfers.exception.JWTVerifyException;

public class Wso2TokenVerifier implements AuthProvider {

    private final String audience;
    private final byte[] secret;
    private final String appName;

    /**
     * Create a verifier to check JWT tokens from WSO2 AM.
     *
     * @param appName  The client name.
     * @param audience The client ID.
     * @param secret   The client secret.
     */
    public Wso2TokenVerifier(String appName, String audience, String secret) {
        Objects.requireNonNull(appName);
        requireNotEmpty("appName", appName);
        Objects.requireNonNull(audience);
        requireNotEmpty("audience", audience);
        Objects.requireNonNull(secret);
        requireNotEmpty("secret", secret);
        this.appName = appName;
        this.audience = audience;
        this.secret = Base64.getUrlDecoder().decode(secret.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void authenticate(JsonObject credentials, Handler<AsyncResult<io.vertx.ext.auth.User>> resultHandler) {
        String token = credentials.getString("jwt");
        String tenantId = credentials.getString("tenantId");
        try {
            JsonObject tokenInfo = verifyToken(token, tenantId);
            String name = tokenInfo.getString("username", "guest");

            List<String> permissions = Arrays.asList(tokenInfo.getString("roles", "").split(","));
            User user = new User(name, permissions);
            resultHandler.handle(Future.succeededFuture(user));
        } catch (Exception e) {
            resultHandler.handle(Future.failedFuture(e));
        }
    }

    private JsonObject verifyToken(String token, String tenantId) throws NoSuchAlgorithmException, InvalidKeyException, IOException, SignatureException, JWTVerifyException {
        JwtClient client = new JwtClient(token, tenantId);
        return client.parse(true);
    }

    private static void requireNotEmpty(String name, String value) {
        Objects.requireNonNull(name);
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException(name + " should not be empty");
        }
    }
}