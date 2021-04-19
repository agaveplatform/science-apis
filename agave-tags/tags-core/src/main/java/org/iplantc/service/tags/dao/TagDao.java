/**
 * 
 */
package org.iplantc.service.tags.dao;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.hibernate.*;
import org.iplantc.service.common.auth.AuthorizationHelper;
import org.iplantc.service.common.dao.AbstractDao;
import org.iplantc.service.common.persistence.HibernateUtil;
import org.iplantc.service.common.persistence.TenancyHelper;
import org.iplantc.service.common.search.SearchTerm;
import org.iplantc.service.tags.exceptions.TagException;
import org.iplantc.service.tags.model.Tag;
import org.iplantc.service.tags.model.enumerations.PermissionType;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Data access class for internal users.
 * 
 * @author dooley
 */
public class TagDao extends AbstractDao
{
	
	private static final Logger log = Logger.getLogger(TagDao.class);
	
	/* (non-Javadoc)
	 * @see org.iplantc.service.common.persistence.AbstractDao#getSession()
	 */
	@Override
	protected Session getSession() {
		HibernateUtil.beginTransaction();
		Session session = HibernateUtil.getSession();
		session.clear();
		session.enableFilter("tagTenantFilter").setParameter("tenantId", TenancyHelper.getCurrentTenantId());
		return session;
	}
	
	
	/**
	 *  Gets all tags for a given user filtering by active status and system
	 *
	 * @param username the user for which the search runs
	 * @param limit the max results
	 * @param offset the results to skip
	 * @return a list of {@link Tag} matching the search
	 * @throws TagException if unable to query
	 */
	@SuppressWarnings("unchecked")
	public List<Tag> getUserTags(String username, int limit, int offset)
	throws TagException
	{
		try
		{
			Session session = getSession();
			
			Query query = session.createQuery("from Tag where owner = :owner order by created desc")
								 .setString("owner", username)
								 .setMaxResults(limit)
								 .setFirstResult(offset);
			
			List<Tag> tags = (List<Tag>)query.list();
			
			session.flush();
			return tags;
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
			
			throw new TagException(ex);
		}
		finally {
			try { HibernateUtil.commitTransaction(); } catch (Exception ignored) {}
		}
	}
	
	/**
	 * Find {@link Tag}s with the given uuid regardless of tenant.
	 * 
	 * @param uuid the uuid of the tag to search
	 * @return a {@link Tag} with the given uuid
	 * @throws TagException if unable to query
	 */
	public Tag findByUuid(String uuid) throws TagException
	{
		try
		{
			HibernateUtil.beginTransaction();
			Session session = HibernateUtil.getSession();
			session.clear();
			
			String hql = "from Tag t where t.uuid = :uuid";
			
			Tag tag = (Tag)session.createQuery(hql)
					.setString("uuid",uuid)
					.uniqueResult();
			
			session.flush();
			
			return tag;
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
			
			throw new TagException(ex);
		}
		finally {
			try { HibernateUtil.commitTransaction(); } catch (Exception ignored) {}
		}
	}
	
	/**
	 * Finds a {@link Tag} for the given owner by name or uuid. UUID search  
	 * results will always be unique. Name searches will not. In the event of a  
	 * namespace conflict where the owner created a tag with the name of a tag
	 * that was also shared with them, their personal tag is always returned.
	 * 
	 * @param uuidOrName the name or uuid of the tag for which to search
	 * @param username the user whose permission must match the tag with uuid or name
	 * @return a {@link Tag} with the given uuid or name
	 * @throws TagException if unable to query
	 */
	public Tag findByNameOrUuid(String uuidOrName, String username) throws TagException
	{
		if (StringUtils.isEmpty(uuidOrName)) {
			return null;
		}
		else {
			try
			{
				HibernateUtil.beginTransaction();
				Session session = HibernateUtil.getSession();
				session.clear();

				String hql = "from Tag t where (t.name = :uuidORName OR t.uuid = :uuidORName) and " +
						"		( " +
						"			t.owner = :owner OR \n" +
						"	        t.id in ( \n" +
						"               SELECT tp.entityId FROM TagPermission tp \n" +
						"               WHERE tp.username = :owner AND tp.permission <> :none \n" +
						"           ) \n" +
						"       )";

				List<Tag> tags = (List<Tag>)session.createQuery(hql)
						.setString("uuidORName",uuidOrName)
						.setString("owner", username)
						.setString("none", PermissionType.NONE.name())
						.setCacheMode(CacheMode.IGNORE)
						.setCacheable(false)
						.list();

				session.flush();

				// return the first value the user owns if it exists
				// otherwise return the first result
				Optional<Tag> tag = tags.stream().filter(t -> t.getOwner().equals(username)).findFirst();
				return tag.orElseGet(() -> tags.get(0));
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

				throw new TagException(ex);
			}
			finally {
				try { HibernateUtil.commitTransaction(); } catch (Exception ignored) {}
			}
		}
	}
		
	/**
	 * Finds a {@link Tag} for the given owner by name. In the event of a namespace 
	 * conflict where the owner created a tag with the name of a tag that was
	 * also shared with them, their personal tag is always returned.
	 *  
	 * @param name the name of the tag to search
	 * @param owner the owner of the tag
	 * @return a {@link Tag} matching the given {@code name} with the given {@code owner} or null if no match
	 * @throws TagException if unable to query
	 */
	@SuppressWarnings("unchecked")
	public Tag findByNameAndOwner(String name, String owner) 
	throws TagException 
	{
		try
		{	
			HibernateUtil.beginTransaction();
			Session session = HibernateUtil.getSession();
			session.clear();
			
			String hql = "from Tag t where t.name = :name and " +
					"		( " +
					"			t.owner = :owner OR \n" +
                    "	        t.id in ( \n" + 
                    "               SELECT tp.entityId FROM TagPermission tp \n" +
                    "               WHERE tp.username = :owner AND tp.permission <> :none \n" +
                    "           ) \n" +
                    "       )";
			
			List<Tag> tags = (List<Tag>)session.createQuery(hql)
					.setString("name",name)
					.setString("owner", owner)
					.setString("none", PermissionType.NONE.name())
					.list();
			
			session.flush();
			
			if (tags.size() > 0) {
				for (Tag tag: tags) {
					if (StringUtils.equals(tag.getOwner(), owner)) {
						return tag;
					}
				}
			}
			
			return null;
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
			
			throw new TagException(ex);
		}
		finally {
			try { HibernateUtil.commitTransaction(); } catch (Exception ignored) {}
		}
	}
	
	/**
	 * Returns the {@link Tag} matching the given uuid within the current tenant id
	 * 
	 * @param uuid the tag uuid
	 * @return {@link Tag} with the matching uuid
	 * @throws TagException if unable to query
	 */
	public Tag findByUuidWithinSessionTenant(String uuid) throws TagException
	{
		try
		{
			Session session = getSession();
			
			String hql = "from Tag t where t.uuid = :uuid";
			
			Tag tag = (Tag)session.createQuery(hql)
					.setString("uuid",uuid)
					.setCacheMode(CacheMode.IGNORE)
					.setCacheable(false)
					.uniqueResult();
			
			session.flush();
			
			return tag;
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
			
			throw new TagException(ex);
		}
		finally {
			try { HibernateUtil.commitTransaction(); } catch (Exception ignored) {}
		}
	}
	
	/**
	 * Saves or updates the {@link Tag}
	 * @param tag the tag to save or update
	 * @throws TagException if unable to persist the tag
	 */
	public void persist(Tag tag) throws TagException
	{
		if (tag == null)
			throw new TagException("Tag cannot be null");

		try
		{	
			HibernateUtil.beginTransaction();
			Session session = HibernateUtil.getSession();
			session.clear();
			
			session.saveOrUpdate(tag);
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
			
			throw new TagException("Failed to save tag", ex);
		}
		finally {
			try { HibernateUtil.commitTransaction(); } catch (Exception e) {e.printStackTrace();}
		}
	}

	/**
	 * Deletes a {@link Tag}
	 * 
	 * @param tag the tag to delete
	 * @throws TagException if unable to delete the tag
	 */
	public void delete(Tag tag) throws TagException
	{

		if (tag == null)
			throw new TagException("Tag cannot be null");

		try
		{
			HibernateUtil.beginTransaction();
			Session session = HibernateUtil.getSession();
			session.clear();
			session.delete(tag);
			session.getTransaction().commit();
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
			
			throw new TagException("Failed to delete tag", ex);
		}
		finally {
			try { HibernateUtil.commitTransaction(); } catch (Exception ignored) {}
		}
	}

	/**
	 * Merges the active {@link Tag} back with the saved instance. Associations are
	 * not updated.
	 * TODO: this should not be needed. We can refresh instead.
	 *
	 * @param tag the tag to merge
	 * @return the updated tag.
	 * @throws TagException if unable to merge the tag object
	 */
	public Tag merge(Tag tag) throws TagException
	{
		try
		{
			HibernateUtil.beginTransaction();
			Session session = HibernateUtil.getSession();
			session.clear();
			
			Tag mergedTag = (Tag)session.merge(tag);
			
			//session.flush();
			
			return mergedTag;
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
			throw new TagException("Failed to merge tag", ex);
		}
		finally
		{
			try { HibernateUtil.commitTransaction(); } catch (Throwable ignored) {}
		}
	}
	
	/**
	 * Resyncs the {@link Tag} with the stored version replacing
	 * any local changes.
	 * 
	 * @param tag the tag to refresh
	 * @throws TagException if unable to refresh the tag
	 */
	public void refresh(Tag tag) throws TagException
	{
		try
		{
			HibernateUtil.beginTransaction();
			Session session = HibernateUtil.getSession();
			session.clear();
			
			session.refresh(tag);
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
			throw new TagException("Failed to merge tag", ex);
		}
		finally
		{
			try { HibernateUtil.commitTransaction(); } catch (Throwable ignored) {}
		}
	}
	
	/**
	 * Fetch all {@link Tag}s.
	 * 
	 * @return a list of all {@link Tag}s
	 * @throws TagException if unable to fet tags
	 */
	@SuppressWarnings("unchecked")
	public List<Tag> getAll() throws TagException
	{
		try
		{
			Session session = getSession();
			
			List<Tag> users = (List<Tag>) session.createQuery("FROM Tag").list();
			
			session.flush();
			
			return users;
		}
		catch (ObjectNotFoundException ex)
		{
			return null;
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
			
			throw new TagException(ex);
		}
		finally {
			try { HibernateUtil.commitTransaction(); } catch (Exception ignored) {}
		}
	}

	/**
	 * Returns true if a {@link Tag} by the same name already exists for the given user.
	 * @param name name of tag
	 * @param username owner of tag
	 * @return true of a match exists within the current tenant. false otherwise
	 * @throws TagException if unable to query tags
	 */
	public boolean doesTagNameExistForUser(String name, String username) throws TagException 
	{
		try
		{
			Session session = getSession();
			
			String sql = "SELECT t.uuid FROM Tag t where t.name = :name and t.owner = :username";
			
			String match = (String)session.createQuery(sql)
					.setString("name", name)
					.setString("username", username)
					.setCacheMode(CacheMode.IGNORE)
					.setCacheable(false)
					.setMaxResults(1)
					.uniqueResult();
			
			session.flush();

			return match != null;
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
			
			throw new TagException(ex);
		}
		finally {
			try { HibernateUtil.commitTransaction(); } catch (Exception ignored) {}
		}
	}	

	/**
     * Searches for software by the given user who matches the given set of 
     * parameters. Permissions are honored in this query as in pagination
     *
     * @param username the user for which the search runs
     * @param searchCriteria the criteria of the search
     * @param offset the results to skip
     * @param limit the max results
     * @return a list of {@link Tag} matching the search
     * @throws TagException if unable to query
     */
    @SuppressWarnings("unchecked")
    public List<Tag> findMatching(String username, Map<SearchTerm, Object> searchCriteria, int offset, int limit) 
    throws TagException
    {
        try
        {
            Session session = getSession();
            session.clear();
            String hql = "SELECT t FROM Tag t left join t.taggedResources taggedResource \n";
            
            SearchTerm publicSearchTerm = null;
            for (SearchTerm searchTerm: searchCriteria.keySet()) {
                if (searchTerm.getMappedField().startsWith("public")) {
                    publicSearchTerm = searchTerm;
                }
            }
            if (!AuthorizationHelper.isTenantAdmin(username)) {
                // no public search term specified. return public and private
				// otherwise, all public apps will be included in the general where clause
                if (publicSearchTerm == null) {
                    hql +=  " WHERE ( \n" +
                            "       t.owner = :owner OR \n" +
                            "       t.id in ( \n" + 
                            "               SELECT tp.entityId FROM TagPermission tp \n" +
                            "               WHERE tp.username = :owner AND tp.permission <> :none \n" +
                            "              ) \n" +
                            "      ) AND \n";
                } 
                // public = false || public.eq = false || public.neq = true, return only private
                else if ((publicSearchTerm.getOperator() == SearchTerm.Operator.EQ && 
                            !(Boolean)searchCriteria.get(publicSearchTerm)) || 
                        (publicSearchTerm.getOperator() == SearchTerm.Operator.NEQ && 
                            (Boolean)searchCriteria.get(publicSearchTerm))) {
                    hql +=  " WHERE ( \n" +
                            "       t.owner = :owner OR \n" +
                            "       t.id in ( \n" + 
                            "               SELECT tp.entityId FROM TagPermission tp \n" +
                            "               WHERE tp.username = :owner AND tp.permission <> :none \n" +
                            "              ) \n" +
                            "      ) AND \n";
                }
            }
            else {
                hql += " WHERE ";
            }
            
            hql +=  "        t.tenantId = :tenantid "; 
            
            for (SearchTerm searchTerm: searchCriteria.keySet()) 
            {
                if (searchCriteria.get(searchTerm) == null 
                        || StringUtils.equalsIgnoreCase(searchCriteria.get(searchTerm).toString(), "null")) 
                {
                    if (searchTerm.getOperator() == SearchTerm.Operator.NEQ ) {
                        hql += "\n       AND       " + String.format(searchTerm.getMappedField(), searchTerm.getPrefix()) + " is not null ";
                    } else if (searchTerm.getOperator() == SearchTerm.Operator.EQ ) {
                        hql += "\n       AND       " + String.format(searchTerm.getMappedField(), searchTerm.getPrefix()) + " is null ";
                    } else {
                        hql += "\n       AND       " + searchTerm.getExpression();
                    }
                } else {
                    hql += "\n       AND       " + searchTerm.getExpression();
                }
            }
            
            hql +=  " ORDER BY t.name ASC\n";
            
            String q = hql;
            
            Query query = session.createQuery(hql)
                                 .setString("tenantid", TenancyHelper.getCurrentTenantId());
            
            q = q.replaceAll(":tenantid", "'" + TenancyHelper.getCurrentTenantId() + "'");
            
            if (!AuthorizationHelper.isTenantAdmin(username)) {
                query.setString("owner",username)
                    .setString("none",PermissionType.NONE.name());
                q = q.replaceAll(":owner", "'" + username + "'")
                    .replaceAll(":none", "'NONE'");
            }
            
            for (SearchTerm searchTerm: searchCriteria.keySet()) 
            {
                if (searchTerm.getOperator() == SearchTerm.Operator.BETWEEN || searchTerm.getOperator() == SearchTerm.Operator.ON) {
                    List<String> formattedDates = (List<String>)searchTerm.getOperator().applyWildcards(searchCriteria.get(searchTerm));
                    for(int i=0;i<formattedDates.size(); i++) {
                        query.setString(searchTerm.getSafeSearchField()+i, formattedDates.get(i));
                        q = q.replaceAll(":" + searchTerm.getSafeSearchField(), "'" + formattedDates.get(i) + "'");
                    }
                }
                else if (searchTerm.getOperator().isSetOperator()) 
                {
                    query.setParameterList(searchTerm.getSafeSearchField(), (List<Object>)searchCriteria.get(searchTerm));
                    q = q.replaceAll(":" + searchTerm.getSafeSearchField(), "('" + StringUtils.join((List<String>)searchTerm.getOperator().applyWildcards(searchCriteria.get(searchTerm)), "','") + "')" );
                }
                else if (searchCriteria.get(searchTerm) == null 
                        || StringUtils.equalsIgnoreCase(searchCriteria.get(searchTerm).toString(), "null")
                        && (searchTerm.getOperator() == SearchTerm.Operator.NEQ || searchTerm.getOperator() == SearchTerm.Operator.EQ )) {
                    // this was explicitly set to 'is null' or 'is not null'
                }
                else 
                {
                    query.setParameter(searchTerm.getSafeSearchField(), 
                            searchTerm.getOperator().applyWildcards(searchCriteria.get(searchTerm)));
                    q = q.replaceAll(":" + searchTerm.getSafeSearchField(), "'" + searchTerm.getOperator().applyWildcards(searchCriteria.get(searchTerm)) + "'");
                }
                
            }
            
            log.debug(q);
            
            List<Tag> tags = query
                    .setFirstResult(offset)
                    .setMaxResults(limit)
                    .list();
            
            session.flush();
            
            return tags;

        }
        catch (Throwable ex)
        {
            throw new TagException(ex);
        }
        finally {
            try { HibernateUtil.commitTransaction();} catch (Exception ignored) {}
        }
    }
}
