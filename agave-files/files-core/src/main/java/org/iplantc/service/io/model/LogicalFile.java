/**
 * 
 */
package org.iplantc.service.io.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.codehaus.plexus.util.FileUtils;
import org.hibernate.annotations.Filter;
import org.hibernate.annotations.FilterDef;
import org.hibernate.annotations.Filters;
import org.hibernate.annotations.ParamDef;
import org.iplantc.service.common.persistence.TenancyHelper;
import org.iplantc.service.common.uri.UrlPathEscaper;
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.common.uuid.UUIDType;
import org.iplantc.service.io.Settings;
import org.iplantc.service.io.exceptions.FileEventProcessingException;
import org.iplantc.service.io.manager.FileEventProcessor;
import org.iplantc.service.io.model.enumerations.FileEventType;
import org.iplantc.service.io.model.enumerations.StagingTaskStatus;
import org.iplantc.service.io.util.ServiceUtils;
import org.iplantc.service.notification.model.Notification;
import org.iplantc.service.systems.model.RemoteSystem;
import org.iplantc.service.systems.model.StorageConfig;
import org.iplantc.service.transfer.model.RemoteFilePermission;
import org.joda.time.DateTime;
import org.json.JSONException;

import javax.persistence.*;
import javax.xml.bind.annotation.XmlElement;
import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.file.InvalidPathException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;


/**
 * @author dooley
 * 
 */
@Entity
@Table(name = "logical_files")
@FilterDef(name="logicalFileTenantFilter", parameters=@ParamDef(name="tenantId", type="string"))
@Filters(@Filter(name="logicalFileTenantFilter", condition="tenant_id=:tenantId"))
public class LogicalFile {
    
    private static final Logger log = Logger.getLogger(LogicalFile.class);
    
	public static final String DIRECTORY = "dir";
	public static final String FILE = "file";
	public static final String RAW = "raw";
	
	private Long id;
	private String name;
	private String uuid;
	private String owner;
	private String internalUsername;
	private Date lastUpdated;
	private String sourceUri;
	private String path;
	private long pathHash;
	private String status = StagingTaskStatus.STAGING_COMPLETED.name();
	private RemoteSystem system;
	private String nativeFormat = RAW;
	private String tenantId;		// current api tenant
	private Date created = new Date();
	
	private List<FileEvent> events = new ArrayList<FileEvent>();
	
	public LogicalFile() {
		setTenantId(TenancyHelper.getCurrentTenantId());
		setUuid(new AgaveUUID(UUIDType.FILE).toString());
		setLastUpdated(created);
	}
	
	public LogicalFile(String owner, RemoteSystem system, String absoluteDestinationPath)
	{
		this();
		setSystem(system);
		setOwner(owner);
		setPath(StringUtils.trim(absoluteDestinationPath));
		setName(FilenameUtils.getName(getPath()));
	}
	
	public LogicalFile(String owner, RemoteSystem system, URI sourceUrl, String absoluteDestinationPath)
	{
		this(owner, system, absoluteDestinationPath);
		this.sourceUri = sourceUrl == null ? null : sourceUrl.toString();
	}
	
	public LogicalFile(String owner, RemoteSystem system, File sourceFile, String absoluteDestinationPath) 
	{
		this(owner, system, absoluteDestinationPath);
		this.sourceUri = sourceFile == null ? null : sourceFile.toURI().toString();
	}
	
	public LogicalFile(String owner, RemoteSystem system, String sourceUri, String absoluteDestinationPath, String name)
	{
		this(owner, system, absoluteDestinationPath);
		setSourceUri(sourceUri);
		setName(name);
		
	}

	public LogicalFile(String owner, RemoteSystem system, String sourceUri, String absoluteDestinationPath, String name, String status, String nativeFormat)
	{
		this(owner, system, sourceUri, absoluteDestinationPath, name);
		setStatus(status);
		setNativeFormat(nativeFormat);
	}
	
	/**
	 * @return the id
	 */
	@Id
	@GeneratedValue
	@Column(name = "id", unique = true, nullable = false)
	@XmlElement
	public Long getId() {
		return id;
	}

	/**
	 * @param id
	 *            the id to set
	 */
	public void setId(Long id) {
		this.id = id;
	}

	/**
	 * @return the name
	 */
	@Column(name = "name", nullable = false, length = 64)
	public String getName() {
		return name;
	}

	/**
	 * @param name
	 *            the name to set
	 */
	public void setName(String name) 
	{
		if (StringUtils.isEmpty(name)) {
			this.name = "/";
		} else {
			this.name = name;
		}
	}

	/**
	 * @return the uuid
	 */
	@Column(name = "uuid", nullable = false, length = 64, unique=true)
	public String getUuid() {
		return uuid;
	}

	/**
	 * @param uuid
	 *            the uuid to set
	 */
	public void setUuid(String uuid) {
		this.uuid = uuid;
	}

	/**
	 * @return the owner
	 */
	@Column(name = "owner", nullable = false, length = 32)
	public String getOwner() {
		return owner;
	}

	/**
	 * @param owner
	 *            the owner to set
	 */
	public void setOwner(String owner) {
		this.owner = owner;
	}

	/**
	 * @return the internalUsername
	 */
	@Column(name = "internalUsername", nullable = true, length = 32)
	public String getInternalUsername()
	{
		return internalUsername;
	}

	/**
	 * @param internalUsername the internalUsername to set
	 */
	public void setInternalUsername(String internalUsername)
	{
		this.internalUsername = internalUsername;
	}

	/**
	 * @return the system
	 */
	@ManyToOne(fetch = FetchType.EAGER)
    @JoinColumn(name = "systemId", referencedColumnName = "id")
    public RemoteSystem getSystem()
	{
		return system;
	}

	/**
	 * @param system the system to set
	 */
	public void setSystem(RemoteSystem system)
	{
		this.system = system;
	}

	/**
	 * @return the lastUpdated
	 */
	@Temporal(TemporalType.TIMESTAMP)
	@Column(name = "lastUpdated", nullable = false, length = 19)
	public Date getLastUpdated() {
		return lastUpdated;
	}

	/**
	 * @param lastUpdated
	 *            the lastUpdated to set
	 */
	public void setLastUpdated(Date lastUpdated) {
		this.lastUpdated = lastUpdated;
	}

	/**
	 * @param sourceUri the sourceUri to set
	 */
	public void setSourceUri(String sourceUri) {
		this.sourceUri = sourceUri;
	}

	/**
	 * @return the source
	 */
	@Column(name = "source", nullable = true, length = 255)
	public String getSourceUri() {
		return sourceUri;
	}

	/**
	 * @return the url
	 */
	@Column(name = "path", nullable = false, length = 255)
	public String getPath() {
		return path;
	}
	
	
	@Column(name = "path_hash", nullable = false)
	public long getPathHash() {
		return pathHash;
	}
	
	/**
	 * Paths are stored internally as absolute paths. Agave URLs abstract
	 * these by resolving them relative to the system.storageConfig.homeDir and 
	 * system.storageConfig.rootDir. This method resolves the absolute path
	 * back to a path suitable for URL presentation.
	 * @return the path relative to the {@link org.iplantc.service.systems.model.StorageSystem} root path.
	 * @throws InvalidPathException if the {@link LogicalFile#getPath()} is not within the
	 * 	{@link RemoteSystem} {@link StorageConfig#getRootDir()} rootDir
	 */
	@Transient
	public String getAgaveRelativePathFromAbsolutePath() 
	{
		String absoluteSystemRootDir = FilenameUtils.normalize(system.getStorageConfig().getRootDir() + "/");
		if (absoluteSystemRootDir == null) {
			absoluteSystemRootDir = "/";
		}

		// homeDir is always relative to rootDir, so we can
		// effectively treat homeDir as an absolute path. We can prepend the / here because the
		// normalize method will remove a double slash if present.
		String relativeSystemHomeDir = FilenameUtils.normalize(system.getStorageConfig().getHomeDir() + "/");
		if (relativeSystemHomeDir == null) {
			relativeSystemHomeDir = "/";
		}
		String absoluteSystemHomeDir = absoluteSystemRootDir +  relativeSystemHomeDir + "/";

		// handle double slashes here to make things easier to compare
		absoluteSystemHomeDir = absoluteSystemHomeDir.replaceAll("/+", "/");
        absoluteSystemRootDir = absoluteSystemRootDir.replaceAll("/+", "/");


		String absoluteFileItemPath = getPath();

		// any trailing relative paths get slashes appended to ensure they resolve as directories
		if (absoluteFileItemPath.endsWith("/..") || absoluteFileItemPath.endsWith("/.")) {
			absoluteFileItemPath += "/";
		}

		// just in case the file item path was not an absolute path, we resolve it cleanly either way.
		// we will clean it up below when we check for existence relative to the root dir.
		if (absoluteFileItemPath.startsWith("/")) {
			absoluteFileItemPath = FileUtils.normalize(absoluteFileItemPath);
		} else {
			// this really should never happen as the #getPath() is supposed to always be an absolute path
			// on the remote system to avoid issues with the user changing the system root and homedir. We
			// still want to be able to access the original data, so we need the original absolute path.
			absoluteFileItemPath = FilenameUtils.normalize(absoluteFileItemPath);
		}

		// if the file item path is within the system homedir, we can return the path relative to that.
		// This becomes problematic, however, because the home directory concept changes for public systems,
		// thus we would have to pull in more domain info here to make that decision. Such a choice could
		// be expensive to determine, so we instead use the agave absolute path for all resolutions.
//		if (StringUtils.startsWith(absoluteFileItemPath, absoluteSystemHomeDir)) {
//			absoluteFileItemPath = StringUtils.substringAfter(absoluteFileItemPath, relativeSystemHomeDir);
//		}

		if (StringUtils.isEmpty(absoluteFileItemPath)) {
			absoluteFileItemPath = "/";
		}

		// We need to ensure the file item path is within the system defined rootDir. Once we determine that, we
		// can strip the absolute system root dir from the file item path and return the result as an absolute path.
		if (StringUtils.startsWith(absoluteFileItemPath, absoluteSystemRootDir)) {
			return  "/" + StringUtils.substringAfter(absoluteFileItemPath, absoluteSystemRootDir);
		} else if (StringUtils.equals(absoluteFileItemPath + "/", absoluteSystemRootDir)) {
			return "/";
		} else {
			// invalid path. do we map to root, or throw an exception? Throwing an exception changes behavior.
			throw new InvalidPathException(path, "Path does not reside within the rootDir defined for the system.");
		}
	}

	/**
	 * @param path the path to set
	 */
	public void setPath(String path) 
	{
		if (StringUtils.isEmpty(path) || StringUtils.equals(path, "/")) {
			this.path = "/";
		} else if (StringUtils.endsWith(path, "/")) {
			this.path = StringUtils.substring(path, 0, -1);
		} else {
			this.path = path;
		}
		
		this.path = StringUtils.replace(this.path, "//", "/");
		
		try {
			this.setPathHash(ServiceUtils.getMD5LongHash(this.path));
		} catch (Exception e) {
			throw new RuntimeException("MD5 algorithm is not available in the environment while hashing path column", e);
		}
	}
	
	/**
	 * @param pathHash the pathHash to set
	 */
	public void setPathHash(long pathHash) 
	{
		this.pathHash = pathHash;
	}

	/**
	 * @param status the status to set
	 */
	public void setStatus(String status) {
		this.status = status;
	}
		
	@Transient
	public void setStatus(StagingTaskStatus status) {
		this.status = status.name();
	}

	/**
	 * @return the status
	 */
	@Column(name = "status", nullable = true, length = 32)
	public String getStatus() {
		return status;
	}

	/**
	 * @param nativeFormat the nativeFormat to set
	 */
	public void setNativeFormat(String nativeFormat) {
		if (StringUtils.isEmpty(nativeFormat)) {
			this.nativeFormat = RAW;
		} else {
			this.nativeFormat = nativeFormat;
		}
	}
	
	/**
	 * Whether this LogicalFile was recorded as a file or directory
	 * @return true if getNativeFormat() == LogicalFile.DIRECTORY
	 */
	@Transient
	public boolean isDirectory() {
		return StringUtils.equals(getNativeFormat(), DIRECTORY);
	}

	/**
	 * @return the nativeFormat
	 */
	@Column(name = "nativeFormat", nullable = true, length = 32)
	public String getNativeFormat() {
		return nativeFormat;
	}

	/**
	 * @return the tenantId
	 */
	@Column(name = "tenant_id", nullable = false, length = 64)
	public String getTenantId()
	{
		return tenantId;
	}

	/**
	 * @param tenantId the tenantId to set
	 */
	public void setTenantId(String tenantId)
	{
		this.tenantId = tenantId;
	}

	/**
	 * @return the transferTask
	 */
	@OneToMany(cascade = {CascadeType.ALL}, mappedBy = "logicalFile", fetch=FetchType.EAGER, orphanRemoval=true)
	public List<FileEvent> getEvents()
	{
		return events;
	}

	/**
	 * Override the existing list of {@link FileEvent} for this logical file.
	 * @param events the transferTask events to set.
	 */
	public void setEvents(List<FileEvent> events)
	{
		this.events = events;
	}
	
	/**
	 * Adds an event to the history of this job. This will automatically
	 * be saved with the logicalFile when the logicalFile is persisted.
	 * 
	 * @param event the content event to add
	 */
	public void addContentEvent(FileEvent event) {
		FileEventProcessor eventProcessor = new FileEventProcessor(); 
		try {
			this.events.add(eventProcessor.processContentEvent(this, event));
		} catch (FileEventProcessingException e) {
			log.error("Failed to add logical file content change event. " + e.getMessage());
		}
	}
	
	/**
	 * Adds an event to the history of this job. This will automatically
	 * be saved with the logicalFile when the logicalFile is persisted.
	 * 
	 * @param eventType the type of event created
	 * @param createdBy the user who created the content event
	 */
	public void addContentEvent(FileEventType eventType, String createdBy) {
		FileEvent event = new FileEvent(
					eventType,
					eventType.getDescription(),
					createdBy,
					getTenantId());
			
		addContentEvent(event);
	}
	
	/**
	 * Adds an event to the history of this job. This will automatically
	 * be saved with the logicalFile when the logicalFile is persisted.
	 * 
	 * @param event the actual permission file event to add
	 * @param permission the permission applied to the {@link LogicalFile} that raised this {@code event}
	 */
	public void addPermissionEvent(FileEvent event, RemoteFilePermission permission) {
		FileEventProcessor eventProcessor = new FileEventProcessor();
		try {
			this.events.add(eventProcessor.processPermissionEvent(this, permission, event));
		} catch (FileEventProcessingException e) {
			log.error("Failed to add logical file permission event. " + e.getMessage());
		}
	}
	
	/**
	 * @param created
	 *            the created to set
	 */
	public void setCreated(Date created) {
		this.created = created;
	}
	
	/**
	 * @return the created
	 */
	@Temporal(TemporalType.TIMESTAMP)
	@Column(name = "created", nullable = false, length = 19)
	@XmlElement
	public Date getCreated() {
		return created;
	}
	
	public boolean equals(Object o) {
		if (o instanceof LogicalFile) {
			return (path.equals(((LogicalFile)o).path) && 
					uuid.equals(((LogicalFile)o).uuid) &&
					owner.equals(((LogicalFile)o).owner));
		} else {
			return false;
		}
	}
	
	@Transient
	@JsonProperty("_links")
	public ObjectNode getHypermedia() {
		ObjectMapper mapper = new ObjectMapper();
		ObjectNode links = mapper.createObjectNode();
		
		links.set("self", mapper.createObjectNode()
	    		.put("href", getPublicLink()));
	    links.set("system", mapper.createObjectNode()
				.put("href", getSystem().getPublicLink()));
		links.put("profile", mapper.createObjectNode()
				.put("href", getOwnerLink()));
		links.put("history", mapper.createObjectNode()
				.put("href", getEventLink()));
		return links;
	}
	
	
	@JsonValue
	public String toJSON() throws JSONException 
	{
		ObjectMapper mapper = new ObjectMapper();
		ObjectNode json = (ObjectNode)mapper.createObjectNode()
			.put("name", getName())
			.put("uuid", getUuid())
			.put("owner", getOwner())
			.put("internalUsername", getInternalUsername())
			.put("lastModified", new DateTime(getLastUpdated()).toString())
			.put("source", getSourceUri())
			.put("path", getAgaveRelativePathFromAbsolutePath())
			.put("status", getStatus())
			.put("systemId", getSystem().getSystemId())
			.put("nativeFormat", getNativeFormat())
			.set("_links", getHypermedia());
	
		return json.toString();
	}
	
	public LogicalFile clone() {
		LogicalFile clone = new LogicalFile();
		clone.setCreated(new Date());
		clone.setLastUpdated(new Date());
		clone.setName(name);
		clone.setNativeFormat(nativeFormat);
		clone.setOwner(owner);
		clone.setSourceUri(sourceUri);
		clone.setStatus(status);
		clone.setPath(path);
		
		return clone;
	}
	
	public String toString() {
		return getPublicLink();
	}
	
	@Transient
	public String getPublicLink() {
		String resolvedPath = StringUtils.removeStart(getPath(), getSystem().getStorageConfig().getRootDir());
		resolvedPath = StringUtils.removeEnd(resolvedPath, "/"); 
        if ((resolvedPath != null) && !resolvedPath.startsWith("/"))  // Avoid multiple leading slashes.
        	resolvedPath = "/" + resolvedPath;

        return TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_IO_SERVICE) +
				"media/system/" + getSystem().getSystemId() + "/" + 
				UrlPathEscaper.escape(resolvedPath);
		
	}
	
	@Transient
	public String getEventLink() {
		String resolvedPath = StringUtils.removeStart(getPath(), getSystem().getStorageConfig().getRootDir());
		resolvedPath = StringUtils.removeEnd(resolvedPath, "/"); 
        if ((resolvedPath != null) && !resolvedPath.startsWith("/"))  // Avoid multiple leading slashes.
        	resolvedPath = "/" + resolvedPath;
		return TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_IO_SERVICE) + 
				"history/system/" + getSystem().getSystemId() + "/" + 
				UrlPathEscaper.escape(resolvedPath);
	}
	
	@Transient
	public String getMetadataLink() {
		String query = "{\"associationIds\":\"" + getUuid() + "\"}";
		try {
			query = URLEncoder.encode(query, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			query = URLEncoder.encode(query);
		}
		return TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_METADATA_SERVICE) + "data?q=" + query;
	}
	
	@Transient
	public String getOwnerLink() {
		return TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_PROFILE_SERVICE) + getOwner();
	}

	/**
	 * Serializes to json with notifications embedded in hypermedia response.
	 * @param notifications the list of notifications to add to the json representation of this node
	 * @return {@link LogicalFile} marshalled to a JSON string with notifications included
	 */
	public String toJsonWithNotifications(List<Notification> notifications) {
		ObjectMapper mapper = new ObjectMapper();
		ArrayNode notifArray = mapper.createArrayNode();
		
		if (notifications != null) {
			String baseNotificationUrl = TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_NOTIFICATION_SERVICE);
			for(Notification n: notifications) {
				notifArray.add(mapper.createObjectNode()
						.put("href", baseNotificationUrl + n.getUuid())
						.put("title", n.getEvent()));
			}
		}
			
		ObjectNode hypermedia = getHypermedia();
		hypermedia.set("notification", notifArray);
		
		ObjectNode json = (ObjectNode)mapper.createObjectNode()
			.put("name", getName())
			.put("uuid", getUuid())
			.put("owner", getOwner())
			.put("internalUsername", getInternalUsername())
			.put("lastModified", new DateTime(getLastUpdated()).toString())
			.put("source", getSourceUri())
			.put("path", getAgaveRelativePathFromAbsolutePath())
			.put("status", getStatus())
			.put("systemId", getSystem().getSystemId())
			.put("nativeFormat", getNativeFormat())
			.set("_links", hypermedia);
		
		
		return json.toString();
	}

}
