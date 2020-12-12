package io.vertx.ext.web.handler.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.file.FileSystemException;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.spi.json.JsonCodec;
import io.vertx.ext.auth.PubSecKeyOptions;
import io.vertx.ext.auth.User;
import io.vertx.ext.auth.jwt.JWTAuth;
import io.vertx.ext.auth.jwt.JWTAuthOptions;
import io.vertx.ext.jwt.JWK;
import io.vertx.ext.jwt.JWT;
import io.vertx.ext.jwt.JWTOptions;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.List;

/**
 * Handles JWT tenant resolution and parsing for authentication coming from Agave's WSO2 APIM.
 */
public class AgaveJWTAuthProviderImpl implements JWTAuth {

    private static final JsonArray EMPTY_ARRAY = new JsonArray();

    private final JWT jwt;
    private static final Charset UTF8 = StandardCharsets.UTF_8;
    private static final Base64.Decoder decoder = Base64.getUrlDecoder();
    private final String permissionsClaimKey;
    private final JWTOptions jwtOptions;

    @SuppressWarnings( "deprecation") // because the upstream JWTAuthOption class is in flux right now.
    public AgaveJWTAuthProviderImpl(JWTAuthOptions config) {
        this.permissionsClaimKey = config.getPermissionsClaimKey();
        this.jwtOptions = config.getJWTOptions();


        try {
            // attempt to load pem keys
            this.jwt = new JWT();

            final List<PubSecKeyOptions> keys = config.getPubSecKeys();

            if (keys != null) {
                for (PubSecKeyOptions pubSecKey : config.getPubSecKeys()) {
                    jwt.addJWK(new JWK(pubSecKey.getAlgorithm(), pubSecKey.isCertificate(), pubSecKey.getPublicKey(), pubSecKey.getSecretKey()));
                }
            }
        } catch (FileSystemException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void authenticate(JsonObject authInfo, Handler<AsyncResult<User>> resultHandler) {
        try {
            JsonObject payload = null;
            if (authInfo.getJsonObject("options").isEmpty()) {
                payload = new JsonObject(new String(decoder.decode(authInfo.getString("jwt").split("\\.")[1]), UTF8));
            } else {
                payload = jwt.decode(authInfo.getString("jwt"));

                try {
                    jwt.isExpired(payload, jwtOptions);
                } catch (RuntimeException e) {
                    resultHandler.handle(Future.failedFuture("Expired JWT token."));
                    return;
                }

                if (jwtOptions.getAudience() != null) {
                    JsonArray target;
                    if (payload.getValue("aud") instanceof String) {
                        target = new JsonArray().add(payload.getValue("aud", ""));
                    } else {
                        target = payload.getJsonArray("aud", EMPTY_ARRAY);
                    }

                    if (Collections.disjoint(jwtOptions.getAudience(), target.getList())) {
                        resultHandler.handle(Future.failedFuture("Invalid JWT audient. expected: " + JsonCodec.INSTANCE.toString(jwtOptions.getAudience())));
                        return;
                    }
                }

                if (jwtOptions.getIssuer() != null) {
                    if (!jwtOptions.getIssuer().equals(payload.getString("iss"))) {
                        resultHandler.handle(Future.failedFuture("Invalid JWT issuer"));
                        return;
                    }
                }
            }

            // add the tenant info from the auth headers to the payload so the user object will have it handy.
            payload.put("tenantId", authInfo.getString("tenantId"));
            payload.put("rawTenantId", authInfo.getString("rawTenantId"));

            resultHandler.handle(Future.succeededFuture(new Wso2JwtUser(payload, permissionsClaimKey)));

        } catch (RuntimeException e) {
            resultHandler.handle(Future.failedFuture(e));
        }
    }

    @Override
    public String generateToken(JsonObject claims, final JWTOptions options) {
        final JsonObject _claims = claims.copy();

        // we do some "enhancement" of the claims to support roles and permissions
        if (options.getPermissions() != null && !_claims.containsKey(permissionsClaimKey)) {
            _claims.put(permissionsClaimKey, new JsonArray(options.getPermissions()));
        }

        return jwt.sign(_claims, options);
    }
}
