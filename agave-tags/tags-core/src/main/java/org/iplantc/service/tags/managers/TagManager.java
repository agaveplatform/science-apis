package org.iplantc.service.tags.managers;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.iplantc.service.tags.dao.TagDao;
import org.iplantc.service.tags.dao.TagEventDao;
import org.iplantc.service.tags.events.TagEventProcessor;
import org.iplantc.service.tags.exceptions.*;
import org.iplantc.service.tags.model.Tag;
import org.iplantc.service.tags.model.TagEvent;
import org.iplantc.service.tags.model.TaggedResource;
import org.iplantc.service.tags.model.enumerations.TagEventType;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class TagManager {
	
	private static final Logger log = Logger.getLogger(TagManager.class);
	
	private TagDao dao = new TagDao();
	private TagEventDao eventDao = new TagEventDao();
	private TagEventProcessor eventProcessor = new TagEventProcessor();
	
	public TagManager() {}

	/**
	 * Inserts a tag and sends the appropriate notifications. Permissions
	 * are verified prior to deletion.
	 * 
	 * @param json the json representation of the tag to add
	 * @param username the new tag owner
	 * @throws TagException if unable to add the tag
	 * @throws TagPermissionException if user does not have permission to add a tag
	 */
	public Tag addTagForUser(JsonNode json, String username) 
	throws TagValidationException, TagException 
	{	
		Tag tag = Tag.fromJSON(json);
		tag.setOwner(username);
    	
		// check for uniqueness. We could just catch the uniqueness 
		// exception from hibernate, but we'll defensively check against 
		// db/hibernate stupidity
    	if (getDao().doesTagNameExistForUser(tag.getName(), username)) {
    		throw new TagValidationException("Tag with the name " + 
    				tag.getName() + " already exists. " +
    				"Tag names must be unique per user.");
    	}
    	else {
    		getDao().persist(tag);
    		
    		// alert any subscribers
    		try {
				getEventProcessor().processContentEvent(tag, 
						new TagEvent(tag.getUuid(),
								TagEventType.CREATED,
								"Tag was created by " + username,
								username));
			} catch (TagEventProcessingException e) {
				log.error("Failed to send tag creation event for " + tag.getUuid(), e);
			}
    	}
    	
    	return tag;
	}
	

	public Tag updateTag(Tag existingTag, JsonNode json, String authenticatedUsername) 
	throws TagValidationException, TagException 
	{
		if (json.has("associationIds")) {
			if (json.get("associationIds").isNull() || 
					(json.get("associationIds").isArray() && json.get("associationIds").size() == 0)) {
				
				TaggedResourceManager trManager = new TaggedResourceManager(existingTag);
				trManager.clearAllFromTag(authenticatedUsername);
				return trManager.getTag();
			}
			else if (json.get("associationIds").isArray()) {
				
				List<TaggedResource> newTaggedResources = new ArrayList<TaggedResource>();
				for(Iterator<JsonNode> iter = json.get("associationIds").iterator(); iter.hasNext();) {
					TaggedResource tr = new TaggedResource(iter.next().textValue(), existingTag);
					if (StringUtils.isNotBlank(tr.getUuid())) {
						newTaggedResources.add(tr);
					}
				}
				
				ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
		        Validator validator = factory.getValidator();
		        
		        Set<ConstraintViolation<List<TaggedResource>>> violations = validator.validate(newTaggedResources);
				if (violations.isEmpty()) {
					List<TaggedResource> oldTaggedResources = existingTag.getTaggedResources();
					existingTag.getTaggedResources().clear();
					for (TaggedResource taggedResource: newTaggedResources) {
						existingTag.addTaggedResource(taggedResource);
					}
    				getDao().persist(existingTag);
    				
    				try {
						eventProcessor.processAssociatedUuidUpdateEvent(existingTag, oldTaggedResources,  
								new TagEvent(existingTag.getUuid(),
										TagEventType.UPDATED,
										"The existing resources were replaced with the following resources: " + 
												StringUtils.join(existingTag.getTaggedResourcesAsArray()),
										authenticatedUsername));
    				} catch (TagEventProcessingException e) {
						log.error("Failed to send tag resource update event for " + existingTag.getUuid(), e);
					}
	        	} 
				else {
	        		throw new TagValidationException(violations.iterator().next().getMessage()); 
	        	}
			}
			else {
				throw new TagValidationException("Invalid associationIds. "
						+ "Please provide an array of uuid for the associationIds field.");
			}
		}
		else {
			throw new TagValidationException("Invalid associationIds. "
					+ "Please provide an array of uuid for the associationIds field.");
		}
	
		return existingTag;
		
	}
	
	public Tag updateTagAssociationId(Tag existingTag, JsonNode json, String username) 
	throws TagValidationException, TagException 
	{
//				Tag tag = Tag.fromJSON(json);
		
		if (getDao().doesTagNameExistForUser(existingTag.getName(), username)) {
    		throw new TagValidationException("Tag with the name " + existingTag.getName() + " already exists. " +
    				"Tag names must be unique per user.");
    	}
    	else {
    		if (json.has("associationIds")) {
    			if (json.get("associationIds").isNull() || 
    					(json.get("associationIds").isArray() && json.get("associationIds").size() == 0)) {
    				List<TaggedResource> oldTaggedResources = existingTag.getTaggedResources();
    				existingTag.getTaggedResources().clear();
    				getDao().persist(existingTag);
    				
    				try {
						eventProcessor.processAssociatedUuidUpdateEvent(existingTag, oldTaggedResources,  
								new TagEvent(existingTag.getUuid(),
										TagEventType.UPDATED,
										"The following resources were untagged: " + 
												StringUtils.join(existingTag.getTaggedResourcesAsArray()),
										username));
					} catch (TagEventProcessingException e) {
						log.error("Failed to send tag resource removal event for " + existingTag.getUuid(), e);
					}
    			}
    			else if (json.get("associationIds").isArray()) {
    				
    				List<TaggedResource> newTaggedResources = new ArrayList<TaggedResource>();
    				for(Iterator<JsonNode> iter = json.get("associationIds").iterator(); iter.hasNext();) {
    					TaggedResource tr = new TaggedResource(iter.next().textValue(), existingTag);
    					if (StringUtils.isNotBlank(tr.getUuid())) {
    						newTaggedResources.add(tr);
    					}
    				}
    				
    				ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
    		        Validator validator = factory.getValidator();
    		        
    		        Set<ConstraintViolation<List<TaggedResource>>> violations = validator.validate(newTaggedResources);
    				if (violations.isEmpty()) {
    					List<TaggedResource> oldTaggedResources = existingTag.getTaggedResources();
    					existingTag.getTaggedResources().clear();
    					for (TaggedResource taggedResource: newTaggedResources) {
    						if (StringUtils.isNotBlank(taggedResource.getUuid())) {
    							existingTag.addTaggedResource(taggedResource);
    						}
    					}
        				getDao().persist(existingTag);
//		        				existingTag.setTaggedResources(newTaggedResources);
//		        				dao.persist(tag);
        				
        				try {
							eventProcessor.processAssociatedUuidUpdateEvent(existingTag, oldTaggedResources,  
									new TagEvent(existingTag.getUuid(),
											TagEventType.UPDATED,
											"The following resources were tagged: " + 
													StringUtils.join(existingTag.getTaggedResourcesAsArray()),
											username));
        				} catch (TagEventProcessingException e) {
    						log.error("Failed to send tag resource update event for " + existingTag.getUuid(), e);
    					}
    	        	} 
    				else {
    	        		throw new TagValidationException(violations.iterator().next().getMessage()); 
    	        	}
    			}
    			else {
    				throw new TagValidationException("Invalid associationIds. "
    						+ "Please provide an array of uuid for the associationIds field.");
    			}
    		}
    		else if (json.isArray()) {
    			if(json.size() == 0) {
    				List<TaggedResource> oldTaggedResources = existingTag.getTaggedResources();
    				existingTag.getTaggedResources().clear();
    				getDao().persist(existingTag);
    				
    				
    				try {
						eventProcessor.processAssociatedUuidUpdateEvent(existingTag, oldTaggedResources,  
								new TagEvent(existingTag.getUuid(),
										TagEventType.UPDATED,
										"The following resources were untagged: " + 
												StringUtils.join(existingTag.getTaggedResourcesAsArray()),
										username));
    				} catch (TagEventProcessingException e) {
						log.error("Failed to send tag resource removal event for " + existingTag.getUuid(), e);
					}
    			}
    			else {
    				List<TaggedResource> newTaggedResources = new ArrayList<TaggedResource>();
    				for(Iterator<JsonNode> iter = json.iterator(); iter.hasNext();) {
    					TaggedResource tr = new TaggedResource(iter.next().textValue(), existingTag);
    					if (StringUtils.isNotBlank(tr.getUuid())) {
    						newTaggedResources.add(tr);
    					}
    				}
    				
    				ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
    		        Validator validator = factory.getValidator();
    		        
    		        Set<ConstraintViolation<List<TaggedResource>>> violations = validator.validate(newTaggedResources);
    				if (violations.isEmpty()) {
    					List<TaggedResource> oldTaggedResources = existingTag.getTaggedResources();
        				existingTag.getTaggedResources().clear();
        				for (TaggedResource taggedResource: newTaggedResources) {
    						if (StringUtils.isNotBlank(taggedResource.getUuid())) {
    							existingTag.addTaggedResource(taggedResource);
    						}
    					}
        				getDao().persist(existingTag);
        				
        				try {
							eventProcessor.processAssociatedUuidUpdateEvent(existingTag, oldTaggedResources,  
									new TagEvent(existingTag.getUuid(),
											TagEventType.UPDATED,
											"The following resources were tagged: " + 
													StringUtils.join(existingTag.getTaggedResourcesAsArray()),
											username));
        				} catch (TagEventProcessingException e) {
    						log.error("Failed to send tag resource update event for " + existingTag.getUuid(), e);
    					}
    	        	} 
    				else {
    	        		throw new TagValidationException(violations.iterator().next().getMessage()); 
    	        	}
    			}
    		}
    		else {
				throw new TagValidationException("Invalid associationIds. "
						+ "Please provide an array of uuid for the associationIds field.");
			}
    	}
		
		return existingTag;
	}
	

	/**
	 * Deletes a tag and sends the appropriate notifications. Permissions
	 * are verified prior to deletion.
	 * 
	 * @param tag the tag to delete
	 * @param username the user requesting the detetion
	 * @throws TagException if deletion fails
	 * @throws TagPermissionException if the user does not have permission to delete
	 */
	public void deleteUserTag(Tag tag, String username) 
	throws TagException, TagPermissionException 
	{	
		if (tag == null) {
			throw new TagException("No tag specifed for deletion");
		}
		
		TagPermissionManager pm = new TagPermissionManager(tag);
		
		if (!pm.canWrite(username)) {
    		throw new TagPermissionException("User does not have permission to view this tag");
    	}
    	else {
    		getDao().delete(tag);
    		
    		try {
				eventProcessor.processContentEvent(tag, 
						new TagEvent(tag.getUuid(),
								TagEventType.DELETED,
								"Tag was deleted by " + username,
								username));
			} catch (TagEventProcessingException e) {
				log.error("Failed to send tag deletion event for " + tag.getUuid(), e);
			}

			try {
				getEventDao().deleteByTagId(tag.getUuid());
			} catch (TagEventPersistenceException e) {
				// tag is already gone. log and swallow this exception
				log.error("Failed to delete history of tag " + tag.getUuid(), e);
			}
		}
	}
	

	/**
	 * @return the dao
	 */
	public TagDao getDao() {
		return dao;
	}

	/**
	 * @param dao the dao to set
	 */
	public void setDao(TagDao dao) {
		this.dao = dao;
	}

	/**
	 * @return the eventProcessor
	 */
	public TagEventProcessor getEventProcessor() {
		return eventProcessor;
	}

	/**
	 * @param eventProcessor the eventProcessor to set
	 */
	public void setEventProcessor(TagEventProcessor eventProcessor) {
		this.eventProcessor = eventProcessor;
	}

	/**
	 * @return the {@link TagEventDao}
	 */
	public TagEventDao getEventDao() {
		return eventDao;
	}

	/**
	 * @param eventDao the {@link TagEventDao} to get
	 */
	public void setEventDao(TagEventDao eventDao) {
		this.eventDao = eventDao;
	}
}
