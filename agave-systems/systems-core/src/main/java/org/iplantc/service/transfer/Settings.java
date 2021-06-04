/**
 * 
 */
package org.iplantc.service.transfer;

import org.apache.log4j.Logger;
import org.ietf.jgss.GSSCredential;

import java.util.*;

/**
 * @author dooley
 * 
 */
public class Settings {

    private static final Logger log = Logger.getLogger(Settings.class);
    
	private static Properties					props			= new Properties();

	private static final Map<String, GSSCredential>	userProxies		= Collections
																		.synchronizedMap(new HashMap<String, GSSCredential>());

	/* Trusted user settings */
	public static List<String> 					TRUSTED_USERS = new ArrayList<String>();
	
	public static String						HOSTNAME;
	public static String						AUTH_SOURCE;
	public static String						API_VERSION;
	public static String						SERVICE_VERSION;

	/* Community user credentials */
	public static String						COMMUNITY_USERNAME;
	public static String						COMMUNITY_PASSWORD;

	/* Authentication service settings */
	public static String 						IPLANT_AUTH_SERVICE;
	public static String						IPLANT_MYPROXY_SERVER;
	public static int							IPLANT_MYPROXY_PORT;
	public static String						IPLANT_LDAP_URL;
	public static String						IPLANT_LDAP_BASE_DN;
	public static String						KEYSTORE_PATH;
	public static String						TRUSTSTORE_PATH;
	public static String						TRUSTED_CA_CERTS_DIRECTORY;
	public static String						MAIL_SERVER;
	public static String 						MAILSMTPSPROTOCOL;
	public static String 						MAILLOGIN;    
    public static String 						MAILPASSWORD;
    
	/* Data service settings */
	public static String						TEMP_DIRECTORY;
	
	/* Iplant API service endpoints */
	public static String						IPLANT_IO_SERVICE;
	public static String						IPLANT_JOB_SERVICE;
	public static String						IPLANT_PROFILE_SERVICE;
	public static String						IPLANT_ATMOSPHERE_SERVICE;
	public static String						IPLANT_LOG_SERVICE;
	public static String 						IPLANT_TRANSFER_SERVICE;
	public static String                        IPLANT_NOTIFICATION_SERVICE;
	
	/* Job service settings */
	public static boolean						DEBUG;
	public static String						DEBUG_USERNAME;
	
//	public static int							REFRESH_RATE	= 0;
    public static int                           MAX_STAGING_TASKS;
    public static int                           STAGING_TIMEOUT_SECS;
    public static int                           MAX_STAGING_RETRIES;
	public static int							MAX_ARCHIVE_TASKS;
	public static int 							MAX_USER_JOBS_PER_SYSTEM;
	public static int 							MAX_USER_CONCURRENT_TRANSFERS;
	
	public static boolean						SLAVE_MODE;
	public static boolean 						CONDOR_GATEWAY;

	public static String						PUBLIC_USER_USERNAME;
	public static String						WORLD_USER_USERNAME;
	
//	public static int 							MAX_SUBMISSION_RETRIES;	
    public static Integer 						DEFAULT_PAGE_SIZE;

	public static String 						IPLANT_DOCS;

	public static boolean						ALLOW_RELAY_TRANSFERS;
	public static int 							MAX_RELAY_TRANSFER_SIZE;

	public static String						TRANSFER_NOTIFICATION_SUBJECT;
	public static String						TRANSFER_NOTIFICATION_QUEUE;
	
	// Set the logging level for the Maverick SSH library.
	// The acceptable values are ERROR, INFO, DEBUG.
	// See MaverickSFTPLogger for details.
	public static String                        MAVERICK_LOG_LEVEL = "ERROR";
	
	static
	{
		props = org.iplantc.service.common.Settings.loadRuntimeProperties();
		
		HOSTNAME = org.iplantc.service.common.Settings.getLocalHostname();

		AUTH_SOURCE = (String) props.get("iplant.auth.source");

		COMMUNITY_USERNAME = (String) props.get("iplant.community.username");

		COMMUNITY_PASSWORD = (String) props.get("iplant.community.password");

		String trustedUsers = (String)props.get("iplant.trusted.users");
		if (trustedUsers != null && !trustedUsers.equals("")) {
			for (String user: trustedUsers.split(",")) {
				TRUSTED_USERS.add(user);
			}
		}
		
		API_VERSION = props.getProperty("iplant.api.version");
		
		SERVICE_VERSION = props.getProperty("iplant.service.version");
		
		IPLANT_MYPROXY_SERVER = (String) props.get("iplant.myproxy.server");

		try {IPLANT_MYPROXY_PORT = Integer.valueOf(props.getProperty("iplant.myproxy.port"));}
        catch (Exception e) {
            log.error("Failure loading setting iplant.myproxy.port.", e);
            IPLANT_MYPROXY_PORT = 0;
        }

		IPLANT_LDAP_URL = (String) props.get("iplant.ldap.url");

		IPLANT_LDAP_BASE_DN = (String) props.get("iplant.ldap.base.dn");

		TEMP_DIRECTORY = (String) props.get("iplant.server.temp.dir");
		
		IPLANT_AUTH_SERVICE = (String)props.get("iplant.auth.service");
		if (!IPLANT_AUTH_SERVICE.endsWith("/")) IPLANT_AUTH_SERVICE += "/";
		
		IPLANT_IO_SERVICE = (String) props.get("iplant.io.service");
		if (!IPLANT_IO_SERVICE.endsWith("/")) IPLANT_IO_SERVICE += "/";
		
		IPLANT_JOB_SERVICE = (String) props.get("iplant.job.service");
		if (!IPLANT_JOB_SERVICE.endsWith("/")) IPLANT_JOB_SERVICE += "/";

		IPLANT_PROFILE_SERVICE = (String) props.get("iplant.profile.service");
		if (!IPLANT_PROFILE_SERVICE.endsWith("/")) IPLANT_PROFILE_SERVICE += "/";

		IPLANT_LOG_SERVICE = (String) props.get("iplant.log.service");
		if (!IPLANT_LOG_SERVICE.endsWith("/")) IPLANT_LOG_SERVICE += "/";
		
		IPLANT_TRANSFER_SERVICE = (String) props.get("iplant.transfer.service");
		if (!IPLANT_TRANSFER_SERVICE.endsWith("/")) IPLANT_TRANSFER_SERVICE += "/";
		
		IPLANT_DOCS = (String) props.get("iplant.service.documentation");
		if (!IPLANT_DOCS.endsWith("/")) IPLANT_DOCS += "/";
		
		IPLANT_NOTIFICATION_SERVICE = (String) props.get("iplant.notification.service");
        if (!IPLANT_NOTIFICATION_SERVICE.endsWith("/")) IPLANT_NOTIFICATION_SERVICE += "/";
		
		try {DEBUG = Boolean.valueOf(props.getProperty("iplant.debug.mode", "true"));}
        catch (Exception e) {
            log.error("Failure loading setting iplant.debug.mode.", e);
            DEBUG = true;
        }

		DEBUG_USERNAME = (String) props.get("iplant.debug.username");

		KEYSTORE_PATH = (String) props.get("system.keystore.path");

		TRUSTSTORE_PATH = (String) props.get("system.truststore.path");
		
		TRUSTED_CA_CERTS_DIRECTORY = (String) props.get("system.ca.certs.path");
		
		MAIL_SERVER = props.getProperty("mail.smtps.host");
		
		MAILSMTPSPROTOCOL = props.getProperty("mail.smtps.auth");
		
		MAILLOGIN = props.getProperty("mail.smtps.user");
		
		MAILPASSWORD = props.getProperty("mail.smtps.passwd");

		try {SLAVE_MODE = Boolean.valueOf(props.getProperty("iplant.slave.mode", "false"));}
        catch (Exception e) {
            log.error("Failure loading setting iplant.slave.mode.", e);
            SLAVE_MODE = false; 
        }
		
		try {CONDOR_GATEWAY = Boolean.valueOf(props.getProperty("iplant.condor.gateway.node", "false"));}
        catch (Exception e) {
            log.error("Failure loading setting iplant.slave.mode.", e);
            CONDOR_GATEWAY = false; 
        }
		
		try {MAX_STAGING_TASKS = Integer.valueOf(props.getProperty("iplant.max.staging.tasks", "1"));}
		catch (Exception e) {
		    log.error("Failure loading setting iplant.max.staging.tasks.", e);
		    MAX_STAGING_TASKS = 1;
		}

        try {STAGING_TIMEOUT_SECS = Integer.valueOf(props.getProperty("iplant.staging.same.server.timeout.secs", "240"));}
        catch (Exception e) {
            log.error("Failure loading setting iplant.staging.same.server.timeout.secs.", e);
            STAGING_TIMEOUT_SECS = 240;
        }
        
        try {MAX_STAGING_RETRIES = Integer.valueOf(props.getProperty("iplant.max.staging.retries", "3"));}
        catch (Exception e) {
            log.error("Failure loading setting iplant.max.staging.retries.", e);
            MAX_STAGING_RETRIES = 3;
        }
        
        try {MAX_ARCHIVE_TASKS = Integer.valueOf(props.getProperty("iplant.max.archive.tasks", "1"));}
        catch (Exception e) {
            log.error("Failure loading setting iplant.max.archive.tasks.", e);
            MAX_ARCHIVE_TASKS = 1;
        }
		
		try {MAX_RELAY_TRANSFER_SIZE = Integer.valueOf(props.getProperty("iplant.max.relay.transfer.size", "2"));}
        catch (Exception e) {
            log.error("Failure loading setting iplant.max.relay.transfer.size.", e);
            MAX_RELAY_TRANSFER_SIZE = 2;
        }
		
		try {ALLOW_RELAY_TRANSFERS = Boolean.valueOf(props.getProperty("iplant.allow.relay.transfers", "false"));}
        catch (Exception e) {
            log.error("Failure loading setting iplant.allow.relay.transfers.", e);
            ALLOW_RELAY_TRANSFERS = false;
        }
		
		String maxUserJobs = (String) props.get("iplant.max.user.jobs.per.system");
		try {
			MAX_USER_JOBS_PER_SYSTEM = Integer.parseInt(maxUserJobs);
		} catch (Exception e) {
			MAX_USER_JOBS_PER_SYSTEM = Integer.MAX_VALUE;
		}
		
		String maxUserTransfers = (String) props.get("iplant.max.user.concurrent.transfers");
		try {
			MAX_USER_CONCURRENT_TRANSFERS = Integer.parseInt(maxUserTransfers);
		} catch (Exception e) {
			MAX_USER_CONCURRENT_TRANSFERS = Integer.MAX_VALUE;
		}
		
		PUBLIC_USER_USERNAME = (String) props.get("iplant.public.user");

		WORLD_USER_USERNAME = (String) props.get("iplant.world.user");

		try {DEFAULT_PAGE_SIZE = Integer.parseInt(props.getProperty("iplant.default.page.size", "25"));}
        catch (Exception e) {
            log.error("Failure loading setting iplant.default.page.size.", e);
            DEFAULT_PAGE_SIZE = 25;
        }
		
		// The default value is set above so we just swallow any exceptions.
		// The acceptable values are: ERROR, INFO, DEBUG
		try {
		    String s = (String) props.get("iplant.maverick.log.level");
		    if ("ERROR".equals(s) || "INFO".equals(s) || "DEBUG".equals(s))
		        MAVERICK_LOG_LEVEL = s;
		} 
		catch (Exception e){}
		
	}

	public static GSSCredential getProxyForUser(String username)
	{
		return userProxies.get(username);
	}

	public static void setProxyForUser(String username, GSSCredential proxy)
	{
		userProxies.put(username, proxy);
	}

}
