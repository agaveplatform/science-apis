package org.iplantc.service.common.uuid;

import com.mongodb.*;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.codehaus.plexus.util.FileUtils;
import org.hibernate.CacheMode;
import org.hibernate.Criteria;
import org.hibernate.Session;
import org.iplantc.service.common.Settings;
import org.iplantc.service.common.exceptions.UUIDException;
import org.iplantc.service.common.persistence.HibernateUtil;

import java.io.File;
import java.util.List;
import java.util.Map;

import static com.mongodb.client.model.Filters.eq;

public class UUIDEntityLookup {
    
    private static final Logger log = Logger.getLogger(UUIDEntityLookup.class);

	public static String getResourceUrl(UUIDType entityType, String uuid) throws UUIDException
	{
		if (entityType == null) { 
		    String msg = "Null resource type received for uuid: " + uuid;
		    log.error(msg);
			throw new UUIDException(msg);
		} else if (entityType.equals(UUIDType.FILE)) {
			return resolveLogicalFileURLFromUUID(uuid);
		} else if (entityType.equals(UUIDType.PROFILE)) {
			String[] uuidTokens = StringUtils.split(uuid, "-");
			String username = null;
			if (uuidTokens.length == 4) {
				username = uuidTokens[1];
			} 
			return Settings.IPLANT_PROFILE_SERVICE + "profiles/" + username;
		} else if (entityType.equals(UUIDType.INTERNALUSER)) {
			Object internalUserUsername = getEntityFieldByUuid(entityType.name(), "username", uuid);
			Object profileUsername = getEntityFieldByUuid(entityType.name(), "created_by", uuid);
			return Settings.IPLANT_PROFILE_SERVICE + "profiles/" + 
				profileUsername.toString() + "/" + internalUserUsername.toString();
		} else if (entityType.equals(UUIDType.JOB)) {
			return Settings.IPLANT_JOB_SERVICE + uuid;
		} else if (entityType.equals(UUIDType.JOB_EVENT)) {
			Object jobUuid = getEntityFieldByUuid("jobevent", "entity_uuid", uuid);
			return Settings.IPLANT_JOB_SERVICE + jobUuid + "/history/" + uuid;
		} else if (entityType.equals(UUIDType.SYSTEM)) {
			Object systemId = getEntityFieldByUuid(entityType.name(), "system_id", uuid);
			return Settings.IPLANT_SYSTEM_SERVICE + systemId.toString();
		} else if (entityType.equals(UUIDType.SYSTEM_EVENT)) {
			Object systemUuid = getEntityFieldByUuid("systemevent", "entity_uuid", uuid);
			return Settings.IPLANT_SYSTEM_SERVICE + systemUuid + "/history/" + uuid;
		} else if (entityType.equals(UUIDType.APP)) {
			Map<String,Object> map = getEntityFieldByUuid("select `name`, `version`, `publicly_available`, `revision_count` from softwares where uuid = '" + uuid + "' and `available` = 1");
			if (map.isEmpty()) 
			{
			    String msg = "Resource id cannot be null for uuid: " + uuid;
			    log.error(msg);
				throw new UUIDException(msg);
			} 
			else 
			{
				String softwareUniqueName = buildSoftwareUrl(map);
				return Settings.IPLANT_APP_SERVICE + softwareUniqueName;
			}
		} else if (entityType.equals(UUIDType.APP_EVENT)) {
			Object softwareUuid = getEntityFieldByUuid("softwareevent", "software_uuid", uuid);
			Map<String,Object> map = getEntityFieldByUuid("select `name`, `version`, `publicly_available`, `revision_count` from softwares where uuid = '" + softwareUuid + "' and `available` = 1");
			if (map.isEmpty())
			{
				String msg = "Resource id cannot be null for uuid: " + uuid;
				log.error(msg);
				throw new UUIDException(msg);
			}
			else {
				String softwareUniqueName = buildSoftwareUrl(map);
				return Settings.IPLANT_APP_SERVICE + softwareUniqueName + "/history/" + uuid;
			}
		} else if (entityType.equals(UUIDType.POSTIT)) {
			Object postIt = getEntityFieldByUuid(entityType.name(), "postit_key", uuid);
			return Settings.IPLANT_POSTIT_SERVICE + postIt.toString();
		} else if (entityType.equals(UUIDType.TRANSFER)) {
			return Settings.IPLANT_TRANSFER_SERVICE + uuid;
		} else if (entityType.equals(UUIDType.SCHEMA)) {
			return Settings.IPLANT_METADATA_SERVICE + "schemas/" + uuid;
		} else if (entityType.equals(UUIDType.METADATA)) {
			return Settings.IPLANT_METADATA_SERVICE + "data/" + uuid;
		} else if (entityType.equals(UUIDType.NOTIFICATION)) {
			return Settings.IPLANT_NOTIFICATION_SERVICE + uuid;
		} else if (entityType.equals(UUIDType.NOTIFICATION_DELIVERY)) {
			String notificationUuid = getNotificationUuidForDeliveryAttempt(uuid);
			return Settings.IPLANT_NOTIFICATION_SERVICE + notificationUuid + "/attempts/" + uuid;
		} else if (entityType.equals(UUIDType.MONITOR)) {
			return Settings.IPLANT_MONITOR_SERVICE + uuid;
		} else if (entityType.equals(UUIDType.MONITORCHECK)) {
			Object monitorId = getEntityFieldByUuid("monitor_check", "monitor", uuid);
			Map<String, Object> map = getEntityFieldByUuid("select `uuid` from monitors where id = '" + monitorId + "'");
			if (map.isEmpty()) {
				String msg = "Resource id cannot be null for uuid: " + uuid;
				log.error(msg);
				throw new UUIDException(msg);
			} else {
				Object monitorUuid = map.get("uuid");
				return Settings.IPLANT_MONITOR_SERVICE + monitorUuid + "/checks/" + uuid;
			}
		} else if (entityType.equals(UUIDType.MONITOR_EVENT)) {
			Object monitorUuid = getEntityFieldByUuid("monitorevent", "entity_uuid", uuid);
			return Settings.IPLANT_MONITOR_SERVICE + monitorUuid + "/history/" + uuid;
//		} else if (entityType.equals(UUIDType.MONITORCHECK_EVENT)) {
//			Object monitorId = getEntityFieldByUuid("monitor_check", "monitor", uuid);
//			Map<String,Object> map = getEntityFieldByUuid("select `uuid` from monitors where id = '" + monitorId + "'");
//			if (map.isEmpty())
//			{
//				String msg = "Resource id cannot be null for uuid: " + uuid;
//				log.error(msg);
//				throw new UUIDException(msg);
//			}
//			else {
//				Object monitorUuid = map.get("uuid");
//				return Settings.IPLANT_MONITOR_SERVICE + monitorUuid + "/checks/" + uuid;
//			}
//		} else if (entityType.equals(UUIDType.TRANSFER_UPDATE)) {
//			Object transferUuid = getEntityFieldByUuid("transferupdate", "entity_uuid", uuid);
//			return Settings.IPLANT_TRANSFER_SERVICE + transferUuid.toString() + "/updates/" + uuid;
		} else if (entityType.equals(UUIDType.TAG)) {
			return Settings.IPLANT_TAGS_SERVICE + uuid;
		} else if (entityType.equals(UUIDType.REALTIME_CHANNEL)) {
			return Settings.IPLANT_REALTIME_SERVICE + uuid;
		} else if (entityType.equals(UUIDType.GROUP)) {
			return Settings.IPLANT_GROUPS_SERVICE + uuid;
		} else if (entityType.equals(UUIDType.CLIENTS)) {
			return Settings.IPLANT_CLIENTS_SERVICE + uuid;
//		} else if (entityType.equals(UUIDType.CLIENTS)) {
//			return Settings.IPLANT_CLIENTS_SERVICE + uuid;
		} else if (entityType.equals(UUIDType.ROLE)) {
			return Settings.IPLANT_ROLES_SERVICE + uuid;
		} else if (entityType.equals(UUIDType.TENANT)) {
			return Settings.IPLANT_TENANTS_SERVICE + uuid;
		} else if (entityType.equals(UUIDType.REACTOR)) {
			return Settings.IPLANT_REACTOR_SERVICE + uuid;
		} else if (entityType.equals(UUIDType.ABACO_AGENT)) {
			return Settings.IPLANT_ABACO_SERVICE + uuid;
		} else if (entityType.equals(UUIDType.REPOSITORY)) {
			return Settings.IPLANT_REPOSITORY_SERVICE + uuid;
		} else if (entityType.equals(UUIDType.TAG_EVENT)) {
			Object tagUuid = getEntityFieldByUuid("tagevent", "entity_uuid", uuid);
			return Settings.IPLANT_TAGS_SERVICE + tagUuid.toString() + "/history/" + uuid;
		} else {
		    String msg = "Unable to resolve " + entityType.name().toLowerCase() +
                         " identifier to a known resource for uuid: " + uuid;
		    log.error(msg);
			throw new UUIDException(msg);
		}
	}

	/**
	 * Constructs the software unique name from the query result map passed in.
	 * @param queryResult the map of column names and values returned from the db query
	 * @return the software unique name regardless of public or private app
	 */
	private static String buildSoftwareUrl(Map<String, Object> queryResult) {
		String softwareUniqueName = queryResult.get("name") + "-" + queryResult.get("version");
		Object available = queryResult.get("publicly_available");
		if (available instanceof Byte) {
			if ((Byte) queryResult.get("publicly_available") == 1) {
				softwareUniqueName += "u" + ((Integer) queryResult.get("revision_count")).toString();
			}
		} else if (available instanceof Boolean) {
			if ((Boolean) available) {
				softwareUniqueName += "u" + ((Integer) queryResult.get("revision_count")).toString();
			}
		} else if (available instanceof Integer) {
			if ((Integer) queryResult.get("publicly_available") == 1) {
				softwareUniqueName += "u" + ((Integer) queryResult.get("revision_count")).toString();
			}
		}

		return softwareUniqueName;
	}

	/**
	 * Looks up the value of <code>fieldName</code> in the table referenced for <code>entityType</code>
	 * and keyed off the <code>uuid</code>.
	 * @param entityType the entity type to look up. This will be pluralized to build the query.
	 * @param fieldName the column whose value will be returned.
	 * @param uuid the uuid of the entity to lookup in the <code>entityType</code> table
	 * @return the value in the <code>fieldName</code> column
	 * @throws UUIDException if the query is null or cannot be run.
	 */
	@SuppressWarnings("unchecked")
    private static Object getEntityFieldByUuid(String entityType, String fieldName, String uuid)
    throws UUIDException
    {
        // ObjectType should be an enum value and prevent injection attacks.
		if (StringUtils.isEmpty(entityType)) {
		    String msg = "EntityType cannot be null for uuid: " + uuid;
		    log.error(msg);
            throw new UUIDException(msg);
		}
        
        if (StringUtils.isEmpty(uuid)) {
            String msg = "UUID cannot be null.";
            log.error(msg);
            throw new UUIDException(msg);
        }
        
        String tableName = entityType.toLowerCase() + "s";

        List<Object> fieldValues;
        try 
        {
        	Session session = HibernateUtil.getSession();
            
            fieldValues = session
            		.createSQLQuery("select " + fieldName + " from " + tableName + " where uuid = :uuid")
            		.setString("uuid", uuid)
            		.setCacheable(false)
            		.setCacheMode(CacheMode.IGNORE)
            		.list();
            
            HibernateUtil.commitTransaction();
        }
        catch(Throwable e) 
        {
            String msg = "Unable to select " + fieldName + " from " + tableName + " for uuid " + uuid + ".";
            log.error(msg, e);
            throw new UUIDException(msg, e);
        }
        
        // Make sure we got something.
        if (fieldValues == null || fieldValues.isEmpty()) {
            String msg = "Field " + fieldName + " not present for uuid " + uuid + ".";
            log.error(msg);
            throw new UUIDException(msg);
        }
        
        return fieldValues.get(0);
    }

	/**
	 * Runs the given <code>sql</code> query over agave's relational db.
	 * @param sql the query to run
	 * @return a map of the columns and values returned from the query
	 * @throws UUIDException if the query is null or cannot be run.
	 */
	@SuppressWarnings("unchecked")
	private static Map<String, Object> getEntityFieldByUuid(String sql) 
    throws UUIDException
    {
        // ObjectType should be an enum value and prevent injection attacks.
		if (StringUtils.isEmpty(sql)) {
		    String msg = "SQL query cannot be null";
		    log.error(msg);
            throw new UUIDException(msg);
		}
        
		Map<String, Object> row;
        try 
        {
			Session session = HibernateUtil.getSession();
            
            row = (Map<String, Object>)session
            		.createSQLQuery(sql)
            		.setResultTransformer(Criteria.ALIAS_TO_ENTITY_MAP)
            		.uniqueResult();
            
            HibernateUtil.commitTransaction();
        }
        catch(Throwable e) 
        {
            String msg = "Query failed: " + sql;
            log.error(msg, e);
            throw new UUIDException(msg, e);
        } 
        
        if (row == null) {
            String msg = "No row return for query: " + sql;
            log.error(msg);
            throw new UUIDException(msg);
        }
        
        return row;
    }

	/**
	 * Returns the url for a logical file by querying the db by uuid and constructing a valid url
	 * @param uuid the uuid of the <code>LogicalFile</code>
	 * @return the url for the given logical file
	 * @throws UUIDException if the uuid is invalid or query cannot be run.
	 */
	protected static String resolveLogicalFileURLFromUUID(String uuid) throws UUIDException
	{
		String sql = "SELECT s.system_id as fileitem_systemid, st.home_dir, st.root_dir, f.path as absolutepath, f.tenant_id"
		        + " FROM logical_files f" 
		        + "   LEFT JOIN systems s ON f.system_id = s.id" 
		        + "   LEFT JOIN storageconfigs st ON s.storage_config = st.id"
		        + " WHERE f.uuid = '" + uuid + "'";
		
		Map<String,Object> map = getEntityFieldByUuid(sql);
		if (map.isEmpty()) {
		    String msg = "No such UUID present: " + uuid;
		    log.error(msg);
			throw new UUIDException(msg);
		}
		else 
		{
			String resolvedPath = getAgaveRelativePathFromAbsolutePath((String)map.get("absolutepath"), 
																	(String)map.get("root_dir"), 
																	(String)map.get("home_dir"));
			
			return Settings.IPLANT_FILE_SERVICE + 
						"media/system/" + 
						map.get("fileitem_systemid") +
						File.separator + resolvedPath;
		}
	}

	/**
	 * Resolves an <code>absolutePath</code> relative to the <code>rootDir</code> and <code>homeDir</code>.
	 *
	 * @param absolutePath the absolute path on a <code>RemoteSystem</code>
	 * @param rootDir the path by which to resolve the <code>absolutePath</code>
	 * @param homeDir the home directory relative to the <code>rootDir</code>
	 * @return the relative path as it would be referenced by a user on a <code>RemoteSystem
	 */
	protected static String getAgaveRelativePathFromAbsolutePath(String absolutePath, String rootDir, String homeDir)
	{	
		rootDir = FilenameUtils.normalize(rootDir);
		if (!StringUtils.isEmpty(rootDir)) {
			if (!rootDir.endsWith("/")) {
				rootDir += "/";
			}
		} else {
			rootDir = "/";
		}

		homeDir = FilenameUtils.normalize(homeDir);
        if (!StringUtils.isEmpty(homeDir)) {
            homeDir = rootDir +  homeDir;
            if (!homeDir.endsWith("/")) {
                homeDir += "/";
            }
        } else {
            homeDir = rootDir;
        }

        homeDir = homeDir.replaceAll("/+", "/");
        rootDir = rootDir.replaceAll("/+", "/");
        
		if (StringUtils.isEmpty(absolutePath)) {
			return homeDir;
		}
		
		String adjustedPath = absolutePath;
		if (adjustedPath.endsWith("/..") || adjustedPath.endsWith("/.")) {
			adjustedPath += File.separator;
		}
		
		if (adjustedPath.startsWith("/")) {
			absolutePath = FileUtils.normalize(adjustedPath);
		} else {
			absolutePath = FilenameUtils.normalize(adjustedPath);
		}
		
		absolutePath = absolutePath.replaceAll("/+", "/");
		
		return "/" + StringUtils.substringAfter(absolutePath, rootDir);
	}

	/**
	 * Fetches or creates a MongoDB capped collection with the given name
	 *
	 * @return An instance of the mongo client connection
	 * @throws UUIDException if the db connection cannot be initialized
	 */
	private static MongoClient getMongoClient() throws UUIDException
	{
		// Set up MongoDB connection
		try
		{
			MongoCredential credential = MongoCredential.createScramSha1Credential(
					Settings.FAILED_NOTIFICATION_DB_USER,
					Settings.FAILED_NOTIFICATION_DB_SCHEME,
					Settings.FAILED_NOTIFICATION_DB_PWD.toCharArray());

			return new MongoClient(
					new ServerAddress(Settings.FAILED_NOTIFICATION_DB_HOST, Settings.FAILED_NOTIFICATION_DB_PORT),
					credential,
					MongoClientOptions.builder().build());
		}
		catch (Exception e) {
			throw new UUIDException("Failed to get mongodb database connection", e);
		}
	}

	/**
	 * Fetches or creates a MongoDB capped collection with the given name
	 * @param mongoClient a mongodb client connection
	 * @return An instance of the mongo feailed notification db
	 * @throws UUIDException when the database cannot be accessed on a valid connection
	 */
	private static MongoDatabase getMongoDB(MongoClient mongoClient) throws UUIDException
	{
		// Set up MongoDB connection
		try
		{
			return mongoClient.getDatabase(Settings.FAILED_NOTIFICATION_DB_SCHEME);
		}
		catch (Exception e) {
			throw new UUIDException("Failed to get mongodb database connection", e);
		}
	}

	/**
	 * Returns the <code>Notification</code> uuid corresponding to the given <code>NotificationAttempt</code>.
	 * @param uuid the <code>NotificationAttempt</code> to look up
	 * @return the uuid of the <code>Notification</code> corresponding to the <code>NotificationAttempt</code>
	 * @throws UUIDException when the notification attempt cannot be looked up or found.
	 */
	private static String getNotificationUuidForDeliveryAttempt(String uuid) throws UUIDException {
		String notificationId = null;
		MongoDatabase db;
		try (MongoClient mongoClient = getMongoClient()) {
			db = getMongoDB(mongoClient);
			for (String collectionName : db.listCollectionNames()) {
				FindIterable<Document> result = db.getCollection(collectionName).find(eq("id", uuid)).limit(1);

				if (result != null) {
					Document attemptDoc = result.first();
					if (attemptDoc != null) {
						return attemptDoc.getString("notificationId");
					}
				}
			}

			return notificationId;
		} catch (MongoException e) {
			throw new UUIDException("Failed to fetch notification attempt for " + uuid, e);
		} catch (Exception e) {
			throw new UUIDException("Unexpected server error while fetching notification attempt for " + uuid, e);
		}
	}
}
