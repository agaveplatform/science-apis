package org.iplantc.service.tags.dao;

import org.apache.commons.lang3.StringUtils;
import org.hibernate.CacheMode;
import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.iplantc.service.common.Settings;
import org.iplantc.service.common.persistence.HibernateUtil;
import org.iplantc.service.common.persistence.TenancyHelper;
import org.iplantc.service.tags.exceptions.TagEventPersistenceException;
import org.iplantc.service.tags.model.Tag;
import org.iplantc.service.tags.model.TagEvent;
import org.iplantc.service.tags.model.enumerations.TagEventType;

import java.util.List;

/**
 * Model class for interacting with a {@link Tag}'s history. {@link TagEvent}s are
 * not persisted as mapped entities in the Tag class due to the
 * potentially large number.
 * 
 * @author dooley
 * 
 */
public class TagEventDao {

	protected Session getSession() {
		Session session = HibernateUtil.getSession();
		HibernateUtil.beginTransaction();
        session.enableFilter("tagEventTenantFilter").setParameter("tenantId", TenancyHelper.getCurrentTenantId());
		session.clear();
		return session;
	}
	
	/**
	 * Returns the tag event with the given id.
	 * 
	 * @param eventId the database id of the {@link TagEvent}
	 * @return the {@link TagEvent} with id {@code eventId}
	 * @throws TagEventPersistenceException if unable to run the query
	 */
	public TagEvent getById(Long eventId)
	throws TagEventPersistenceException
	{
		if (eventId == null)
			throw new TagEventPersistenceException("Event id cannot be null");

		try
		{
			Session session = getSession();
			
			TagEvent event = (TagEvent)session.get(TagEvent.class, eventId);
			
			session.flush();
			
			return event;
		}
		catch (HibernateException ex)
		{
			throw new TagEventPersistenceException(ex);
		}
		finally {
			try { HibernateUtil.commitTransaction(); } catch (Exception ignored) {}
		}
	}
	
	/**
	 * Returns all tag tag events for the tag with the given id. At most, {@link Settings#DEFAULT_PAGE_SIZE} results
	 * will be returned.
	 *
	 * @param tagUuid the uuid of the tag whose events we will query
	 * @return list of {@link TagEvent} for the {@link Tag} with uuid {@code tagUuid}
	 * @throws TagEventPersistenceException if unable to run the query
	 * @see #getByTagUuid(String, int, int)
	 */
	public List<TagEvent> getByTagUuid(String tagUuid)
	throws TagEventPersistenceException
	{
		return getByTagUuid(tagUuid, Settings.DEFAULT_PAGE_SIZE, 0);
	}
	
	/**
	 * Returns all tag tag events for the tag with the given id. At most, {@code limit} results will be returned after
	 * skipping the first {@code offset} results.
	 *
	 * @param tagUuid the uuid of the tag whose events we will query
	 * @param limit the max results
	 * @param offset the number of events to skip
	 * @return list of {@link TagEvent} for the {@link Tag} with uuid {@code tagUuid}
	 * @throws TagEventPersistenceException if unable to run the query
	 */
	@SuppressWarnings("unchecked")
	public List<TagEvent> getByTagUuid(String tagUuid, int limit, int offset)
	throws TagEventPersistenceException
	{

		if (StringUtils.isEmpty(tagUuid))
			throw new TagEventPersistenceException("Tag uuid cannot be null");

		try
		{
			Session session = getSession();
			
			String hql = "FROM TagEvent t where t.entity = :uuid order by t.id asc";
			List<TagEvent> events = session.createQuery(hql)
			        .setString("uuid", tagUuid)
					.setCacheMode(CacheMode.IGNORE)
					.setCacheable(false)
					.setFirstResult(offset)
					.setMaxResults(limit)
					.list();
			
			session.flush();
			
			return events;
		}
		catch (HibernateException ex)
		{
			throw new TagEventPersistenceException(ex);
		}
		finally {
			try { HibernateUtil.commitTransaction(); } catch (Exception ignored) {}
		}
	}
	
	/**
     * Fetches {@link TagEvent} by {@code tagUuid} and {@code uuid}.
	 *
	 * @param tagUuid the uuid of the tag whose events we will query
	 * @param eventUuid the uuid of the tag whose events we will query
     * @return the {@link TagEvent} for the given {@code tagUuid} with uuid {@code uuid}
     * @throws TagEventPersistenceException if unable to query
     */
    public TagEvent getByTagUuidAndUuid(String tagUuid, String eventUuid)
    throws TagEventPersistenceException
    {

        if (tagUuid == null)
            throw new TagEventPersistenceException("Tag id cannot be null");
        
        if (StringUtils.isEmpty(eventUuid))
            throw new TagEventPersistenceException("Tag event uuid cannot be null");

        try
        {
            Session session = getSession();
            
            String hql = "FROM TagEvent t where t.entity = :tagUuid and t.uuid = :uuid";
            TagEvent event = (TagEvent)session.createQuery(hql)
                    .setString("tagUuid", tagUuid)
                    .setString("uuid", eventUuid)
					.setCacheMode(CacheMode.IGNORE)
					.setCacheable(false)
                    .uniqueResult();
            
            session.flush();
            
            return event;
        }
        catch (HibernateException ex)
        {
            throw new TagEventPersistenceException(ex);
        }
        finally {
            try { HibernateUtil.commitTransaction(); } catch (Exception ignored) {}
        }
    }

	/**
	 * Gets the tag events for the specified tag id and tag status
	 * 
	 * @param tagUuid the uuid of the tag whose events we will query
	 * @param eventType the tag event type for which to query
	 * @return list of {@link TagEvent}  for the given tag id and event type
	 * @throws TagEventPersistenceException if unable to query
	 */
	@SuppressWarnings("unchecked")
	public List<TagEvent> getByTagUuidAndStatus(String tagUuid, TagEventType eventType) 
	throws TagEventPersistenceException
	{
		if (eventType == null)
			throw new TagEventPersistenceException("eventType cannot be null");
		
		if (StringUtils.isEmpty(tagUuid))
			throw new TagEventPersistenceException("tag uuid cannot be null");

		try
		{
			Session session = getSession();
			
			String hql = "FROM TagEvent t where t.entity = :tagUuid and t.status = :status order by t.id asc";
			List<TagEvent> events = session.createQuery(hql)
					.setString("status", eventType.name())
					.setString("tagUuid", tagUuid)
					.setCacheMode(CacheMode.IGNORE)
					.setCacheable(false)
					.list();
			
			session.flush();
			
			return events;
		}
		catch (HibernateException ex)
		{
			throw new TagEventPersistenceException(ex);
		}
		finally {
			try { HibernateUtil.commitTransaction(); } catch (Exception ignored) {}
		}
	}

	/**
	 * Saves a new {@link TagEvent}. Updates existing ones.
	 * @param event the tag event to save
	 * @throws TagEventPersistenceException if unable to save the event
	 */
	public void persist(TagEvent event) throws TagEventPersistenceException
	{
		if (event == null)
			throw new TagEventPersistenceException("TagEvent cannot be null");

		try
		{
			Session session = getSession();
			session.saveOrUpdate(event);
			session.flush();
		}
		catch (HibernateException ex)
		{
			try
			{
				if (HibernateUtil.getSession().isOpen()) {
					HibernateUtil.rollbackTransaction();
				}
			}
			catch (Exception ignored) {}
			
			throw new TagEventPersistenceException("Failed to save tag event.", ex);
		}
		finally {
			try { HibernateUtil.commitTransaction(); } catch (Exception ignored) {}
		}
	}

	/**
	 * Deletes the give tag permission.
	 * 
	 * @param event the event to delete
	 * @throws TagEventPersistenceException if unable to delete the event
	 */
	public void delete(TagEvent event) throws TagEventPersistenceException
	{
		if (event == null)
			throw new TagEventPersistenceException("TagEvent cannot be null");

		try
		{
			Session session = getSession();
			session.delete(event);
			session.flush();
		}
		catch (HibernateException ex)
		{
			try
			{
				if (HibernateUtil.getSession().isOpen()) {
					HibernateUtil.rollbackTransaction();
				}
			}
			catch (Exception ignored) {}
			
			throw new TagEventPersistenceException("Failed to delete tag event.", ex);
		}
		finally {
			try { HibernateUtil.commitTransaction(); } catch (Exception ignored) {}
		}
	}

	/**
	 * Deletes all {@link TagEvent} for the tag with given {@code tagUuid}.
	 *
	 * @param tagUuid the uuid of the tag whose events we will query
	 * @throws TagEventPersistenceException if unable to delete events for the given tag
	 */
	public void deleteByTagId(String tagUuid) throws TagEventPersistenceException
	{
		if (StringUtils.isEmpty(tagUuid)) {
			return;
		}

		try
		{
			Session session = getSession();

			String hql = "DELETE from TagEvent t where t.entity = :uuid";
			session.createQuery(hql)
					.setString("uuid", tagUuid)
					.setCacheMode(CacheMode.IGNORE)
					.setCacheable(false)
					.executeUpdate();
			
			session.flush();
		}
		catch (HibernateException ex)
		{
			try
			{
				if (HibernateUtil.getSession().isOpen()) {
					HibernateUtil.rollbackTransaction();
				}
			}
			catch (Exception ignored) {}
			
			throw new TagEventPersistenceException("Failed to delete events for tag " + tagUuid, ex);
		}
		finally {
			try { HibernateUtil.commitTransaction(); } catch (Exception ignored) {}
		}
	}

}
