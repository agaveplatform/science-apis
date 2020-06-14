package io.vertx.ext.web.handler.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.auth.AbstractUser;
import io.vertx.ext.auth.AuthProvider;
import org.apache.commons.lang.StringUtils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

@SuppressWarnings("deprecation") // Currently no replacement until vertx 4 release
public class Wso2JwtUser extends AbstractUser {

    private static final Logger log = LoggerFactory.getLogger(io.vertx.ext.auth.jwt.impl.JWTUser.class);

    enum AdminRole { SUPER_ADMIN, WORLD_ADMIN, TENANT_ADMIN, SERVICES_ADMIN; }
    enum ImplicitUserRole { IMPERSONATOR; }
    enum UserAccountManagerRole { USER_ACCOUNT_MANAGER; }

    private final String WSO2_ACCOUNT_MANAGER_REGEX = "^(?<userStore>[\\w]+/)(?<tenantId>[\\w\\-]+)-(?<roleName>user-account-manager)$";
    private final String WSO2_IMPERSONATOR_REGEX = "^(?<userStore>[\\w]+/)(?<tenantId>[\\w\\-]+)-(?<roleName>impersonator)$";
    private final String WSO2_ADMIN_REGEX = "^(?<userStore>[\\w]+/)(?<tenantId>[\\w\\-]+)-(?<roleName>[\\w\\-]{1,}-admin)$";

    private JsonObject jwtToken;
    private JsonArray roles = new JsonArray();
    private JsonArray permissions = new JsonArray();

    boolean adminRoleExists = false;

    private String tenantId;
    private String username;
    private String email;
    private String firstName;
    private String lastName;
    private String fullName;

    /**
     * Sole constructor this this principal. Extracts a full user profile and role information from the JWT.
     *
     * @param jwtToken the full claim set from the JWT.
     * @param roleClaimKey the claim key for the WSO2 roles assigned to the user.
     */
    public Wso2JwtUser(JsonObject jwtToken, String roleClaimKey) {
        this.jwtToken = jwtToken;
        this.tenantId = jwtToken.getString("tenantId");
        this.fullName = jwtToken.getString("http://wso2.org/claims/fullname");
        this.firstName = jwtToken.getString("http://wso2.org/claims/givenname");
        this.lastName = jwtToken.getString("http://wso2.org/claims/lastname");
        this.username = getUsernameFromSubject(jwtToken.getString("http://wso2.org/claims/enduser"), tenantId);
        this.email = jwtToken.getString("http://wso2.org/claims/emailaddress");

        String commaSeparatedRoleList = jwtToken.getString(roleClaimKey, "");
        String rawTenantId = jwtToken.getString("rawTenantId", tenantId);
        this.permissions = parseJwtRoles(commaSeparatedRoleList, rawTenantId);
    }

    /**
     * Parses all the roles provided by WSO2 as a comma-separated list in the {@code http://wso2.org/claims/role} claim.
     * Roles generally come in the form {@code <UserStore>/<TenantId>-<RoleName>}. Several roles are predefined with
     * every tenant and have internal entitlements throughout the platform. Those are returned as entries under both
     * their original and short-name.
     *
     * @param commaSeparatedRoleList the comma-separated list of roles from the role claim in the JWT
     * @param tenantRolePrefix the value prefixing the contextualized role. This is usually the raw tenant id from the JWT header
     * @return a {@link JsonArray} of roles from the JWT.
     */
    private JsonArray parseJwtRoles(String commaSeparatedRoleList, String tenantRolePrefix) {
        JsonArray roles = new JsonArray();

        //  For known roles matching predefined, tenant-level entitlements,
        // we add them to the list via their short names for easier lookup and consumption within the service.
        for (String role: StringUtils.split(commaSeparatedRoleList, ",")) {

            // We always add the original role so that it can still be looked up by fully qualified value if need be.
            roles.add(role);

            // Avoid adding duplicate admin entry entry
            if ( role.equalsIgnoreCase("admin")) {
                // this is a reserve role in WSO2, so we grant it implicit permissions by default.
                this.adminRoleExists = true;
            } else {
                String shortRoleName = resolveRoleShortName(role, tenantRolePrefix);
                // If a short name is returned, it is a trusted role, so we add it to the array for easier lookup.
                // Otherwise, we leave only the original in the list, forgoing any shortname. Note that
                // roles
                if (shortRoleName != null){
                    roles.add(shortRoleName);
                }
            }
        }

        return roles;
    }

    /**
     * @return true if any kind of admin role was present in the jwt. false otherwise
     */
    public boolean isAdminRoleExists() {
        return adminRoleExists;
    }

    @Override
    protected void doIsPermitted(String permission, Handler<AsyncResult<Boolean>> resultHandler) {
        resultHandler.handle(Future.succeededFuture(permissions.contains(permission)));
    }

    @Override
    public JsonObject principal() {
        return new JsonObject()
                .put("username", username)
                .put("email", email)
                .put("firstName", firstName)
                .put("lastName", lastName)
                .put("fullName", fullName)
                .put("permissions", permissions)
                .put("roles", roles)
                .put("tenantId", tenantId);
    }

    @Override
    public void setAuthProvider(AuthProvider authProvider) {
    }

    /**
     * Checks the WSO2 role value against a predefined set of roles using the regex defined in this class. A match
     * will return the resulting short name of the role.
     *
     * @param wso2Role the raw role name as provided in the JWT
     * @param rolePrefix the value prefixing the contextualized role. This is usually the raw tenant id from the JWT header
     * @return the admin role short name if it matches any of the known regex and the {@code rolePrefix}, null otherwise
     */
    protected String resolveRoleShortName(String wso2Role, String rolePrefix) {
        String roleName = parseWso2RoleRegex(WSO2_ADMIN_REGEX, wso2Role, rolePrefix);
        if (roleName != null) {
            // set a flag denoting that an admin role was found so we don't have to parse for multiple values down
            // the road when making implicit permission checks.
            this.adminRoleExists = true;
            return roleName;
        }

        roleName = parseWso2RoleRegex(WSO2_ACCOUNT_MANAGER_REGEX, wso2Role, rolePrefix);
        if (roleName != null) return roleName;

        roleName = parseWso2RoleRegex(WSO2_IMPERSONATOR_REGEX, wso2Role, rolePrefix);

        // value will be null if not found, which is what we would return anyway upon no matching value
        return roleName;
    }

    /**
     * Parses a WSO2 Role value in the form {@code <UserStore>/<TenantId>-<RoleName>} and
     * checks for the {@code <TenantId>} matching {@code rolePrefix}. If found, just the last component is returned.
     *
     * @param wso2Role the raw role coming from WSO2.
     * @param rolePrefix the value prefixing the contextualized role. usually the raw tenant id from the jwt
     * @return the admin role short name if it matches the regex and the {@code rolePrefix}, null otherwise
     */
    protected String parseWso2RoleRegex(String regex, String wso2Role, String rolePrefix) {
        Matcher matcher = Pattern.compile(regex).matcher(wso2Role);
        if (matcher.find()) {
            // wso2 roles should be prefixed with the tenant id. ensure that matches
            if (StringUtils.equalsIgnoreCase(rolePrefix, matcher.group("tenantId"))) {
                // if it matches, return the "*-admin" value
                return matcher.group("roleName");
            }  else {
                // ignore roles from another tenant as they can't apply to authorization decisions in
                // a multitenant-aware service.
            }
        }

        return null;
    }

    /**
     * Parses the username of the user out of the JWT subject. Stripping out any tenant-specific info that may have
     * been added by WSO2. This does create the possibility of namespace conflicts within a tenant if the tenant does
     * not provide unique handles for each user, or keys off email addresses, however we currently do not allow that.
     * @param subject the raw value of the WSO2 subject claim
     * @param tenantId the tenant id
     * @return the simple username of the subject in the JWT
     */
    protected String getUsernameFromSubject(String subject, String tenantId)
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



}