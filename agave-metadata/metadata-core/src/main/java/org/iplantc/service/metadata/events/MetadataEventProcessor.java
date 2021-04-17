package org.iplantc.service.metadata.events;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.iplantc.service.common.exceptions.EntityEventProcessingException;
import org.iplantc.service.metadata.model.MetadataItem;
import org.iplantc.service.metadata.model.MetadataPermission;
import org.iplantc.service.metadata.model.enumerations.MetadataEventType;
import org.iplantc.service.notification.managers.NotificationManager;

public class MetadataEventProcessor {

	private static final Logger log = Logger.getLogger(MetadataEventProcessor.class);
	private static final ObjectMapper mapper = new ObjectMapper();
		
	public MetadataEventProcessor() {}

	/**
	 * Generates notification events for content-related changes to a 
	 * {@link MetadataItem}. This could be a CRUD operation or a change
	 * to the credentials.
	 * 
	 * @param uuid the uuid of the metadata item
	 * @param eventType the event being triggered
	 * @param createdBy the user who caused this event
	 * @param serializedMetadataItem the serialized metadata object to include in the message body.
	 */
	public void processContentEvent(String uuid, MetadataEventType eventType, String createdBy, String serializedMetadataItem) {
		
		try {
			if (StringUtils.isEmpty(uuid)) {
				throw new EntityEventProcessingException("Valid metadata uuid must be provided to process event.");
			}
			
			if (eventType == null) {
				throw new EntityEventProcessingException("Valid metadata event type must be provided to process event.");
			}
			
			if (StringUtils.isEmpty(createdBy)) {
				throw new EntityEventProcessingException("Valid metadata creator username must be provided to process event.");
			}
			
			if (StringUtils.isEmpty(serializedMetadataItem)) {
				throw new EntityEventProcessingException("Unable to serialize metadata item for processing.");
			}
			
			String sJson = "{\"metadata\":" + serializedMetadataItem + "}";
			
			// fire event on monitor
			processNotification(uuid, eventType.name(), createdBy, sJson);
			
//			// We fire delegated monitor chec events on the system associated with this metadata item.
			// disabling until we get benchmarks.
//			for (String associatedUuid: metadata.getAssociations().getRawUuid()) {
//				processNotification(associatedUuid, "METADATA_" + eventType.name(), createdBy, sJson);
//			}
		}
		catch (EntityEventProcessingException e) {
			log.error(e.getMessage());
		}
	}
	
	
	
	/**
	 * Generates notification events for content-related changes to a 
	 * {@link MetadataItem}. This could be a CRUD operation or a change
	 * to the credentials.
	 * 
	 * @param metadata the metadata item on which the even is triggered
	 * @param eventType the event being triggered
	 * @param createdBy the user who caused this event
	 * @return the {@link MetadataItem} updated with the association to the {@link MetadataItem}
	 */
	public MetadataItem processContentEvent(MetadataItem metadata, MetadataEventType eventType, String createdBy) {
		
		try {
			if (metadata == null) {
				throw new EntityEventProcessingException("Valid metadata must be provided to process event.");
			}
			
			if (eventType == null) {
				throw new EntityEventProcessingException("Valid metadata event type must be provided to process event.");
			}
			
			if (StringUtils.isEmpty(createdBy)) {
				throw new EntityEventProcessingException("Valid metadata creator username must be provided to process event.");
			}
			
			String sJson = null;
			try {
				ObjectNode json = getMapper().createObjectNode();
			    json.set("metadata", getMapper().readTree(getMapper().writeValueAsString(metadata)));
			    sJson = json.toString();
			} catch (Throwable e) {
			    log.error(String.format("Failed to serialize metadata "
			            + "%s to json for %s event notification", 
			            metadata.getUuid(), eventType.name()), e);
			}
			
			// fire event on monitor
			processNotification(metadata.getUuid(), eventType.name(), createdBy, sJson);
			
			// We fire delegated monitor chec events on the system being monitored.
			for (String associatedUuid: metadata.getAssociations().getRawUuid()) {
				processNotification(associatedUuid, "METADATA_" + eventType.name(), createdBy, sJson);
			}
		}
		catch (EntityEventProcessingException e) {
			log.error(e.getMessage());
		}
		
		return metadata;
	}
	
	/**
	 * Throws events for the given {@link MetadataItem} uuid. This will result in a 
	 * notification event being thrown onto the the deleted item and with only the uuid
	 * in the body. Generally this is only called from a bulk operation where speed of
	 * operation trumps verboseness of messages.
	 *
	 * @param uuid the uuid of the impacted uuid
	 * @param eventType the metadata event type
	 * @param createdBy the user responsible for this event
	 * @return the uuid of the target metadata item
	 * @deprecated batch events not yet supported
	 */
	public String processBulkContentEvent(String uuid, MetadataEventType eventType, String createdBy) {
		try {
			if (StringUtils.isEmpty(uuid)) {
				throw new EntityEventProcessingException("Valid metadata uuid must be provided to process event.");
			}
			
			if (eventType == null) {
				throw new EntityEventProcessingException("Valid metadata event type must be provided to process event.");
			}
			
			if (StringUtils.isEmpty(createdBy)) {
				throw new EntityEventProcessingException("Valid metadata creator username must be provided to process event.");
			}
			
			String sJson = null;
			try {
				ObjectNode json = getMapper().createObjectNode();
				json.putArray("metadata")
						.addObject()
							.put("uuid",  uuid);
			    sJson = json.toString();
			} catch (Throwable e) {
			    log.error(String.format("Failed to serialize metadata "
			            + "%s to json for %s event notification", 
			            uuid, eventType.name()), e);
			}
			
			// fire event on monitor
			processNotification(uuid, eventType.name(), createdBy, sJson);
		}
		catch (EntityEventProcessingException e) {
			log.error("Failed to process batch metadata event: " + e.getMessage());
		}
		
		return uuid;
		
	}
	
	/**
	 * Generates notification events for permission-related changes to a {@link MetadataItem}. This could be a CRUD
	 * operation or a change to the permissions.
	 * 
	 * @param uuid the uuid of the {@link MetadataItem} on which the permission event is triggered
	 * @param permission the {@link MetadataPermission} triggering this event
	 * @param eventType the {@link MetadataEventType} being triggered
	 * @param createdBy the user who caused this event
	 */
	public void processPermissionEvent(String uuid, MetadataPermission permission, MetadataEventType eventType, String createdBy, String serializedMetadataItem) {
		
		try {
			if (StringUtils.isEmpty(uuid)) {
				throw new EntityEventProcessingException("Valid metadata uuid must be provided to process event.");
			}
			
			if (permission == null) {
				throw new EntityEventProcessingException("Valid permission grant must be provided to process event.");
			}
			
			if (eventType == null) {
				throw new EntityEventProcessingException("Valid metadata event type must be provided to process event.");
			}
			
			if (StringUtils.isEmpty(createdBy)) {
				throw new EntityEventProcessingException("Valid metadata creator username must be provided to process event.");
			}
			
			if (StringUtils.isEmpty(serializedMetadataItem)) {
				throw new EntityEventProcessingException("Unable to serialize metadata item for processing.");
			}
			
			String sJson = null;
			try {
				sJson = "{\"metadata\":" + serializedMetadataItem + ", \"permission\":" + permission.toJSON(uuid) + "}";
			} catch (Throwable e) {
			    log.error(String.format("Failed to serialize metadata "
			            + "%s to json for %s event notification", 
			            uuid, eventType.name()), e);
			}
			
			// fire event on monitor
			processNotification(uuid, eventType.name(), createdBy, sJson);
			
//			// We fire delegated monitor chec events on the system being monitored.
			// hold on propagation events until they're useful.
//			for (String associatedUuid: metadata.getAssociations().getRawUuid()) {
//				processNotification(associatedUuid, "METADATA_" + eventType.name(), createdBy, sJson);
//			}
		}
		catch (EntityEventProcessingException e) {
			log.error(e.getMessage());
		}
	}
	
	/**
	 * Publishes an event message for the {@code associatedUuid}.
	 * 
	 * @param associatedUuid the uuid of the related resource
	 * @param event the name of the event
	 * @param createdBy the user responsible for this event
	 * @param sJson the messageBody
	 * @return the number of messages published
	 */
	private int processNotification(String associatedUuid, String event, String createdBy, String sJson) {
		try {
            return NotificationManager.process(associatedUuid, event, createdBy, sJson);
        }
        catch (Throwable e) {
            log.error(String.format("Failed to send delegated event notification "
                    + "to %s to on a %s event", 
                    associatedUuid, event), e);
            return 0;
        }
	}


	protected ObjectMapper getMapper() {
		return mapper;
	}
}
