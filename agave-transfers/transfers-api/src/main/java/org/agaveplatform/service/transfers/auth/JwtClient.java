package org.agaveplatform.service.transfers.auth;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSVerifier;
import com.nimbusds.jose.Requirement;
import com.nimbusds.jose.crypto.RSASSAVerifier;
import com.nimbusds.jwt.ReadOnlyJWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import io.vertx.core.json.JsonObject;
import net.minidev.json.JSONObject;
import org.agaveplatform.service.transfers.exception.JWTVerifyException;
import org.apache.commons.lang.StringUtils;
import org.iplantc.service.common.Settings;
import org.iplantc.service.common.auth.SHA256withRSAVerifier;
import org.iplantc.service.common.clients.HTTPSClient;
import org.iplantc.service.common.dao.TenantDao;
import org.iplantc.service.common.exceptions.TenantException;
import org.iplantc.service.common.model.Tenant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.AuthenticationException;
import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.NoSuchProviderException;
import java.security.Security;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.interfaces.RSAPublicKey;
import java.text.ParseException;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;

public class JwtClient {

    private static final Logger log = LoggerFactory.getLogger(JwtClient.class);
    private static final ConcurrentHashMap<String, RSAPublicKey> tenantPublicKeys = new ConcurrentHashMap<String, RSAPublicKey>();

    String token;
    String tenantId;
    JsonObject claims;

//    Map<String, RSAPublicKey> publicKeys = new Map<String, RSAPublicKey>();

    public JwtClient(String token, String tenantId) {
        this.token = token;
        this.tenantId = tenantId;
    }

    /**
     * Parses the JWT and optionally validates the signature against the known public key of the tenant auth server.
     *
     * @param verify should the signature be verified during parsing
     * @return the JWT claim set as a JsonObject
     * @throws JWTVerifyException if verify was requested and the JWT was invalid
     */
    public JsonObject parse(boolean verify) throws JWTVerifyException
    {
        if (StringUtils.isEmpty(getTenantId())) {
            throw new JWTVerifyException("No tenant specified for the given JWT");
        }

        if (StringUtils.isEmpty(getToken())) {
            throw new JWTVerifyException("No token provided to parse");
        }

        try
        {
            // Create HMAC signer
            SignedJWT signedJWT = SignedJWT.parse(getToken());

            // if verifying the signature, do so here
            if (verify) {

                RSASSAVerifier.SUPPORTED_ALGORITHMS.add(new JWSAlgorithm("SHA256withRSA", Requirement.OPTIONAL));

                JWSVerifier verifier = new SHA256withRSAVerifier(getPublicKeyForTenant(tenantId));

                if (!signedJWT.verify(verifier)) {
                    throw new JWTVerifyException("Invalid JWT signature.");
                }
            }

            // now verify the expiration date to be safe not verifying the signature,
            ReadOnlyJWTClaimsSet claimSet = signedJWT.getJWTClaimsSet();
            Date expirationDate = claimSet.getExpirationTime();

            if (expirationDate == null)
                throw new AuthenticationException(
                        "No expiration date in the JWT header. Authentication failed.");

            if (expirationDate.before(new Date()))
                throw new AuthenticationException(
                        "JWT has expired. Authentication failed.");

            String tenantId = StringUtils.lowerCase(getTenantId());
            tenantId = StringUtils.replaceChars(tenantId, '_', '.');
            tenantId = StringUtils.replaceChars(tenantId, '-', '.');

            claims = new JsonObject(claimSet.getAllClaims());
            claims.put("rawTenantId", getTenantId()); // unmodified tenant id
            claims.put("tenantId", tenantId);

            String username = getUsernameFromSubject(claims.getString("http://wso2.org/claims/enduser"), tenantId);
            if (StringUtils.isBlank(username)) {
                throw new AuthenticationException(
                        "No end user specified in the JWT header. Authentication failed.");
            } else {
                claims.put("username", username);
            }

            return claims;
        }
        catch (TenantException e) {
            throw new JWTVerifyException("Failed to validate JWT object.", e);
        }
        catch (ParseException e) {
            throw new JWTVerifyException("Failed to parse JWT object. Authentication failed.", e);
        }
        catch (Throwable e) {
            throw new JWTVerifyException("Error processing JWT header. Authentication failed.", e);
        }
    }

    /**
     * Fetches the {@link RSAPublicKey} for the given tenant for use in
     * verifying the JWT signature.
     *
     * @param tenantId the tenant for which to fetch the public key
     * @return RSAPublicKey suitable for use verifying the JWT signature
     * @throws TenantException when the tenant key cannot be fetched
     */
    public RSAPublicKey getPublicKeyForTenant(String tenantId) throws TenantException
    {
        RSAPublicKey tenantPublicKey = tenantPublicKeys.get(tenantId);
        if (tenantPublicKey == null)
        {
            log.debug("Public key for tenant " + tenantId + " not found in the "
                    + "service cache. Fetching now...");

            try (InputStream is = getPublicKeyInputStream(tenantId))
            {
                Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
                CertificateFactory cf = CertificateFactory.getInstance("X509", "BC");

                X509Certificate certificate = (X509Certificate) cf.generateCertificate(is);
                tenantPublicKey = (RSAPublicKey)certificate.getPublicKey();
                tenantPublicKeys.put(tenantId,  tenantPublicKey);
            }
            catch (NoSuchProviderException e) {
                throw new TenantException("Unable to load public key for tenant " + tenantId
                        + ". No security provider found to handle the key type.", e);
            }
            catch (FileNotFoundException e) {
                throw new TenantException("Unable to locate public key for tenant " + tenantId
                        + " at " + getPublicKeyUrlForTenant(tenantId), e);
            }
            catch (IOException e) {
                throw new TenantException("Unable to fetch public key for tenant " + tenantId
                        +  " from " + getPublicKeyUrlForTenant(tenantId), e);
            }
            catch (CertificateException e) {
                throw new TenantException("Unable to parse public key for tenant " + tenantId
                        +  " from " + getPublicKeyUrlForTenant(tenantId), e);
            }
            catch (Exception e) {
                throw new TenantException("Unable to load public key for tenant " + tenantId
                        + ". Unexpected error occurred.", e);
            }
        }

        return tenantPublicKey;
    }

    private String getPublicKeyUrlForTenant(String tenantId) throws TenantException {
        Tenant tenant = new TenantDao().findByTenantId(tenantId);
        if (tenant != null) {
            return StringUtils.removeEnd(tenant.getBaseUrl(), "/") + "/apim/v2/publickey";
        }
        else {
            throw new TenantException("No tenant found for id " + tenantId);
        }
    }

    private InputStream getPublicKeyInputStream(String tenantId)
            throws IOException, FileNotFoundException
    {
        HTTPSClient client = null;
        try {
            String publicKeyUrl = getPublicKeyUrlForTenant(tenantId);
            client = new HTTPSClient(publicKeyUrl);
            log.debug("Fetching public key for tenant " + tenantId + " from " + publicKeyUrl + "...");
            String sPublicKey = client.getText();
            if (!StringUtils.isEmpty(sPublicKey)) {
                return new ByteArrayInputStream(sPublicKey.getBytes());
            }
            else {
                throw new FileNotFoundException("No public key found for tenant " + tenantId);
            }
        }
        catch (Exception e) {
            throw new IOException("Failed to fetch the public key for tenant " + tenantId, e);
        }
    }

    public JSONObject decodeJwt(String serializedJWT, RSAPublicKey pubKey) throws Exception
    {
        // register the SHA256withRSA algorithm with nimbus and Java security so
        // it will be recognized
        RSASSAVerifier.SUPPORTED_ALGORITHMS.add(
                new JWSAlgorithm("SHA256withRSA", Requirement.OPTIONAL));
        Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());

        // Parse the JWT into its constituent parts
        SignedJWT jwt = SignedJWT.parse(serializedJWT);

        // verify the signature
        JWSVerifier verifier = new SHA256withRSAVerifier(pubKey);
        if (jwt.verify(verifier)) {
            return jwt.getJWTClaimsSet().toJSONObject();
        }
        else {
            throw new Exception("Bad signature on jwt");
        }
    }

    public String getApplicationId()
    {
        try {
            return (String)claims.getString("http://wso2.org/claims/applicationid");
        } catch (Exception e) {
            return null;
        }
    }

    public String[] getRoles()
    {
        try {
            return claims.getString("http://wso2.org/claims/roles", "").split(",");
        } catch (Exception e) {
            return new String[]{};
        }
    }

    public String getSubscriber()
    {
        try {
            return claims.getString("http://wso2.org/claims/subscriber");
        } catch (Exception e) {
            return null;
        }
    }

    public String getUsernameFromSubject(String subject, String tenantId)
    {
        try {
            String endUser = subject;
            endUser = StringUtils.replace(endUser, "@carbon.super", "");
            if (StringUtils.endsWith(endUser, tenantId)) {
                return StringUtils.substring(endUser, 0, (-1 * tenantId.length() - 1));
            } else if (endUser.contains("@")){
                return StringUtils.substringBefore(endUser, "@");
            } else if (endUser.contains("/")){
                return StringUtils.substringAfter(endUser, "/");
            } else {
                return endUser;
            }
        } catch (Exception e) {
            return null;
        }
    }


    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }
}
