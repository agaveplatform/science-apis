package org.iplantc.service.common.uuid;

import java.io.File;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.codehaus.plexus.util.FileUtils;
import org.hibernate.CacheMode;
import org.hibernate.Criteria;
import org.hibernate.Session;
import org.iplantc.service.common.Settings;
import org.iplantc.service.common.exceptions.UUIDException;
import org.iplantc.service.common.persistence.HibernateUtil;

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
		} else if (entityType.equals(UUIDType.SYSTEM)) {
			Object systemId = getEntityFieldByUuid(entityType.name(), "system_id", uuid);
			return Settings.IPLANT_SYSTEM_SERVICE + systemId.toString();
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
				String softwareUniqueName = (String)map.get("name") + "-" + (String)map.get("version");
				Object available = map.get("publicly_available");
				if (available instanceof Byte) {
				    if ((Byte)map.get("publicly_available") == 1) {
				        softwareUniqueName += "u" + ((Integer)map.get("revision_count")).toString();
				    }
				}
				else if (available instanceof Boolean) {
					if ((Boolean)available) {
						softwareUniqueName += "u" + ((Integer)map.get("revision_count")).toString();
					}
				}
				else if (available instanceof Integer) {
					if ((Integer)map.get("publicly_available") == 1) {
						softwareUniqueName += "u" + ((Integer)map.get("revision_count")).toString();
					}
				}
				
				return Settings.IPLANT_APP_SERVICE + softwareUniqueName;
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
		} else if (entityType.equals(UUIDType.MONITOR)) {
			return Settings.IPLANT_MONITOR_SERVICE + uuid;
		} else if (entityType.equals(UUIDType.TRANSFER)) {
			return Settings.IPLANT_TRANSFER_SERVICE + uuid;
		} else if (entityType.equals(UUIDType.TAG)) {
			return Settings.IPLANT_TAGS_SERVICE + uuid;
		} else if (entityType.equals(UUIDType.REALTIME_CHANNEL)) {
			return Settings.IPLANT_REALTIME_SERVICE + uuid;
		} else if (entityType.equals(UUIDType.GROUP)) {
			return Settings.IPLANT_GROUPS_SERVICE + uuid;
		} else if (entityType.equals(UUIDType.CLIENTS)) {
			return Settings.IPLANT_CLIENTS_SERVICE + uuid;
		} else if (entityType.equals(UUIDType.CLIENTS)) {
			return Settings.IPLANT_CLIENTS_SERVICE + uuid;
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

        List<Object> fieldValues = null;
        try 
        {
        	Session session = HibernateUtil.getSession();
            
            fieldValues = session
            		.createSQLQuery("select " + fieldName + " from " + tableName + " where uuid = :uuid")
            		.setString("uuid", uuid.toString())
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
        
		Map<String, Object> row = null;
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
						(String)map.get("fileitem_systemid") + 
						File.separator + resolvedPath;
		}
	}
	
	protected static String getAgaveRelativePathFromAbsolutePath(String absolutepath, String rootDir, String homeDir) 
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
        
		if (StringUtils.isEmpty(absolutepath)) {
			return homeDir;
		}
		
		String adjustedPath = absolutepath;
		if (adjustedPath.endsWith("/..") || adjustedPath.endsWith("/.")) {
			adjustedPath += File.separator;
		}
		
		if (adjustedPath.startsWith("/")) {
			absolutepath = FileUtils.normalize(adjustedPath);
		} else {
			absolutepath = FilenameUtils.normalize(adjustedPath);
		}
		
		absolutepath = absolutepath.replaceAll("/+", "/");
		
		return "/" + StringUtils.substringAfter(absolutepath, rootDir);
	}

}
