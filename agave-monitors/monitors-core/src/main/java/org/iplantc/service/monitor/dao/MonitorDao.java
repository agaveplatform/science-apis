/**
 * 
 */
package org.iplantc.service.monitor.dao;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.hibernate.HibernateException;
import org.hibernate.ObjectNotFoundException;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.exception.ConstraintViolationException;
import org.hibernate.transform.Transformers;
import org.iplantc.service.common.Settings;
import org.iplantc.service.common.auth.AuthorizationHelper;
import org.iplantc.service.common.dao.AbstractDao;
import org.iplantc.service.common.persistence.HibernateUtil;
import org.iplantc.service.common.persistence.TenancyHelper;
import org.iplantc.service.common.search.SearchTerm;
import org.iplantc.service.monitor.exceptions.MonitorException;
import org.iplantc.service.monitor.model.Monitor;
import org.iplantc.service.monitor.search.MonitorSearchFilter;
import org.iplantc.service.systems.exceptions.SystemException;
import org.iplantc.service.systems.manager.SystemManager;
import org.iplantc.service.systems.model.RemoteSystem;
import org.iplantc.service.systems.model.enumerations.RemoteSystemType;
import org.iplantc.service.systems.search.SystemSearchFilter;
import org.iplantc.service.systems.search.SystemSearchResult;
import org.iplantc.service.transfer.model.enumerations.PermissionType;

/**
 * Data access class for internal users.
 * 
 * @author dooley
 */
@SuppressWarnings("ALL")
public class MonitorDao extends AbstractDao
{
	private static final Logger log = Logger.getLogger(MonitorDao.class);
	/* (non-Javadoc)
	 * @see org.iplantc.service.common.persistence.AbstractDao#getSession()
	 */
	@Override
	protected Session getSession() {
		HibernateUtil.beginTransaction();
		Session session = HibernateUtil.getSession();
		session.clear();
		session.enableFilter("monitorTenantFilter").setParameter("tenantId", TenancyHelper.getCurrentTenantId());
		return session;
	}
	
	/**
	 * Gets all active monitors for a given user.
	 * 
	 * @param username the username for whom the monitors will be retrieved
	 * @return
	 * @throws MonitorException
	 */
	@SuppressWarnings("unchecked")
	public List<Monitor> getActiveUserMonitors(String username) 
	throws MonitorException
	{
		try
		{
			Session session = getSession();
			
			String hql = "from Monitor m " +
					"where m.owner = :owner " +
					"and m.active = :active " +
					"order by m.created DESC";
			
			List<Monitor> monitors = (List<Monitor>)session.createQuery(hql)
					.setBoolean("active", Boolean.TRUE)
					.setString("owner",username)
					.list();
			
			session.flush();
			return monitors;
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
			
			throw new MonitorException(ex);
		}
		finally {
			try { HibernateUtil.commitTransaction(); } catch (Exception ignored) {}
		}
	}
	
	/**
	 *  Gets all monitors for a given user filtering by active status and system
	 *  
	 * @param username
	 * @param includeActive
	 * @param includeInactive
	 * @param systemId
	 * @return
	 * @throws MonitorException
	 */
	@SuppressWarnings("unchecked")
	public List<Monitor> getUserMonitors(String username, boolean includeActive, boolean includeInactive, String systemId) 
	throws MonitorException
	{
		try
		{
			Session session = getSession();
			
			String hql = "from Monitor m " +
					"where m.owner = :owner ";
			
			if (!includeActive || !includeInactive)
			{
				if (includeActive || includeInactive) {
					hql += "and m.active = :active ";
				}
			}
			
			if (systemId != null) {
				hql += "and m.system.systemId = :systemId ";
			}
			
			hql += "order by m.created DESC";
			
			Query query = session.createQuery(hql)
					.setString("owner",username);
			
			if (!includeActive || !includeInactive)
			{
				if (includeActive) {
					query.setBoolean("active", Boolean.TRUE);
				} else if (includeInactive) {
					query.setBoolean("active", Boolean.FALSE);
				}
			}
			
			if (systemId != null) {
				query.setString("systemId", systemId);
			}
					
			List<Monitor> monitors = (List<Monitor>)query.list();
			
			session.flush();
			return monitors;
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
			
			throw new MonitorException(ex);
		}
		finally {
			try { HibernateUtil.commitTransaction(); } catch (Exception ignored) {}
		}
	}
	
	/**
	 * Gets all internal users created by the given apiUsername. This will return both
	 * active and inactive users.
	 * 
	 * @param uuid the uuid of the monitor to retrieve
	 * @return
	 * @throws MonitorException
	 */
	public Monitor findByUuid(String uuid) throws MonitorException
	{
		try
		{
			HibernateUtil.beginTransaction();
			Session session = HibernateUtil.getSession();
			session.clear();
			
			String hql = "from Monitor t where t.uuid = :uuid";
			
			Monitor monitor = (Monitor)session.createQuery(hql)
					.setString("uuid",uuid)
					.uniqueResult();
			
			session.flush();
			
			return monitor;
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
			
			throw new MonitorException(ex);
		}
		finally {
			try { HibernateUtil.commitTransaction(); } catch (Exception ignored) {}
		}
	}
	
	/**
	 * Returns the monitor matching the given uuid withing the current tenant id
	 * 
	 * @param uuid the uuid of the monitor to retrieve
	 * @return
	 * @throws MonitorException
	 */
	public Monitor findByUuidWithinSessionTenant(String uuid) throws MonitorException
	{
		try
		{
			Session session = getSession();

			String hql = "from Monitor t where t.uuid = :uuid";
			
			Monitor monitor = (Monitor)session.createQuery(hql)
					.setString("uuid",uuid)
					.uniqueResult();
			
			session.flush();
			
			return monitor;
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
			
			throw new MonitorException(ex);
		}
		finally {
			try { HibernateUtil.commitTransaction(); } catch (Exception ignored) {}
		}
	}
	
	/**
	 * Saves or updates the InternalUser
	 * @param monitor the monitor to persist
	 * @throws MonitorException
	 */
	public void persist(Monitor monitor) throws MonitorException
	{
		if (monitor == null)
			throw new MonitorException("Monitor cannot be null");
		boolean exceptionThrown = false;
		try
		{	
			HibernateUtil.beginTransaction();
			Session session = HibernateUtil.getSession();
			session.clear();
			
			session.saveOrUpdate(monitor);
			session.flush();
		}
		catch (ConstraintViolationException ex) {
			exceptionThrown = true;
			throw new MonitorException("A monitor already exists for this user on system " + 
					monitor.getSystem().getSystemId(), ex);
		}
		catch (HibernateException ex)
		{
			exceptionThrown = true;
			try
			{
				if (HibernateUtil.getSession().isOpen()) {
					HibernateUtil.rollbackTransaction();
				}
			}
			catch (Exception ignore) {}
			
			throw new MonitorException("Failed to save monitor", ex);
		}
		finally {
			try {
				if (!exceptionThrown) {
					HibernateUtil.commitTransaction();
				}
			} catch (Exception ignore) {}
		}
	}

	/**
	 * Deletes a monitor
	 * 
	 * @param monitor
	 * @throws MonitorException
	 */
	public void delete(Monitor monitor) throws MonitorException
	{

		if (monitor == null)
			throw new MonitorException("Monitor cannot be null");

		try
		{
			HibernateUtil.beginTransaction();
			Session session = HibernateUtil.getSession();
			session.clear();
			session.delete(monitor);
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
			
			throw new MonitorException("Failed to delete monitor", ex);
		}
		finally {
			try { HibernateUtil.commitTransaction(); } catch (Exception ignored) {}
		}
	}

	public Monitor merge(Monitor monitor) throws MonitorException
	{
		try
		{
			HibernateUtil.beginTransaction();
			Session session = HibernateUtil.getSession();
			session.clear();
			
			Monitor mergedMonitor = (Monitor)session.merge(monitor);
			
			//session.flush();
			
			return mergedMonitor;
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
			throw new MonitorException("Failed to merge monitor", ex);
		}
		finally
		{
			try { HibernateUtil.commitTransaction(); } catch (Throwable ignored) {}
		}
	}
	
	public void refresh(Monitor monitor) throws MonitorException
	{
		try
		{
			HibernateUtil.beginTransaction();
			Session session = HibernateUtil.getSession();
			session.clear();
			
			session.refresh(monitor);
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
			throw new MonitorException("Failed to merge monitor", ex);
		}
		finally
		{
			try { HibernateUtil.commitTransaction(); } catch (Throwable ignored) {}
		}
	}
	
	@SuppressWarnings("unchecked")
	public List<Monitor> getAll() throws MonitorException
	{
		try
		{
			Session session = getSession();
			
			List<Monitor> users = (List<Monitor>) session.createQuery("FROM Monitor").list();
			
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
			
			throw new MonitorException(ex);
		}
		finally {
			try { HibernateUtil.commitTransaction(); } catch (Exception ignored) {}
		}
	}

	@SuppressWarnings("unchecked")
	public Monitor getNextPendingActiveMonitor() throws MonitorException
	{
		try
		{
			HibernateUtil.beginTransaction();
			Session session = HibernateUtil.getSession();
			session.clear();
			
			String hql = "from Monitor m " +
					"where m.nextUpdateTime < :rightnow " +
					"and m.active = :active " +
					"order by m.nextUpdateTime ASC";
			
			List<Monitor> monitors = (List<Monitor>)session.createQuery(hql)
					.setBoolean("active", Boolean.TRUE)
					.setParameter("rightnow", new Date())
					.setMaxResults(1)
					.list();
			
			session.flush();
			
			return monitors.isEmpty() ? null : monitors.get(0);
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
			
			throw new MonitorException(ex);
		}
		finally {
			try { HibernateUtil.commitTransaction(); } catch (Exception ignored) {}
		}
		
	}
	
	@SuppressWarnings("unchecked")
	public List<Monitor> getAllPendingActiveMonitor() throws MonitorException
	{
		try
		{
			HibernateUtil.beginTransaction();
			Session session = HibernateUtil.getSession();
			session.clear();
			
			String hql = "from Monitor m " +
					"where m.nextUpdateTime < :rightnow " +
					"and m.active = :active " +
					"order by m.nextUpdateTime ASC";
			
			List<Monitor> monitors = (List<Monitor>)session.createQuery(hql)
					.setBoolean("active", Boolean.TRUE)
					.setDate("rightnow", new Date())
					.list();
			
			session.flush();
			
			return monitors;
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
			
			throw new MonitorException(ex);
		}
		finally {
			try { HibernateUtil.commitTransaction(); } catch (Exception ignored) {}
		}
		
	}

	/**
	 * Returns all user monitors on a given system
	 * 
	 * @param username
	 * @param system
	 * @return
	 * @throws MonitorException
	 */
	@SuppressWarnings("unchecked")
	public List<Monitor> getActiveUserMonitorsOnSystem(String username, RemoteSystem system) throws MonitorException 
	{
		try
		{
			Session session = getSession();
			
			String hql = "from Monitor m " +
					"where m.owner = :owner " +
					"and m.active = :active " +
					"and m.system.id = :systemId " +
					"order by m.created DESC";
			
			List<Monitor> monitors = (List<Monitor>)session.createQuery(hql)
					.setBoolean("active", Boolean.TRUE)
					.setString("owner",username)
					.setLong("systemId", system.getId())
					.list();
			
			session.flush();
			return monitors;
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
			
			throw new MonitorException(ex);
		}
		finally {
			try { HibernateUtil.commitTransaction(); } catch (Exception ignored) {}
		}
	}

	public boolean doesExist(String username, RemoteSystem system) throws MonitorException 
	{
		try
		{
			Session session = getSession();
			
			String hql = "from Monitor m " +
					"where m.owner = :owner " +
					"and m.system.id = :systemId " +
					"order by m.created DESC";
			
			Monitor monitor = (Monitor)session.createQuery(hql)
					.setString("owner",username)
					.setLong("systemId", system.getId())
					.setMaxResults(1)
					.uniqueResult();
			
			session.flush();
			return monitor != null;
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
			
			throw new MonitorException(ex);
		}
		finally {
			try { HibernateUtil.commitTransaction(); } catch (Exception ignored) {}
		}
	}

	/**
	 * Searches for {@link Monitor}s by the given user who matches the
	 * given set of parameters. Permissions are honored in this query. Results
	 * are limited to at most {@link Settings#DEFAULT_PAGE_SIZE}.
	 *
	 * @param username
	 * @param searchCriteria
	 * @return
	 * @throws MonitorException
	 */
	public List<Monitor> findMatching(String username, Map<SearchTerm, Object> searchCriteria)
			throws MonitorException {
		return findMatching(username, searchCriteria, Settings.DEFAULT_PAGE_SIZE, 0, false);
	}

	/**
	 * Searches for {@link Monitor}s by the given user who matches the
	 * given set of parameters. Permissions are honored in this query.
	 *
	 * @param username username of the user making the query
	 * @param searchCriteria map of the search criteria to match
	 * @param limit the max results returned
	 * @param offset the number of results to skip
	 * @param fullResponse should the full entity object be returned?
	 * @return a list of the Monitors
	 * @throws MonitorException
	 */
	@SuppressWarnings("unchecked")
	public List<Monitor> findMatching(String username, Map<SearchTerm, Object> searchCriteria, int limit, int offset, boolean fullResponse)
			throws MonitorException {

		try {
			Class<?> transformClass = Monitor.class;
			Map<String, Class> searchTypeMappings = new MonitorSearchFilter().getSearchTypeMappings();
			Session session = getSession();
			session.clear();
			String hql =  "SELECT distinct m.id as id, \n"
					+ "     m.active as active, \n"
					+ "     m.created as created, \n"
					+ "     m.frequency as frequency, \n"
					+ "     m.system as system, \n"
					+ "     m.internalUsername as internalUsername, \n"
					+ "     m.lastSuccess as lastSuccess, \n"
					+ "     m.lastUpdated as lastUpdated, \n"
					+ "     m.nextUpdateTime as nextUpdateTime, \n"
					+ "     m.owner as owner, \n"
					+ "     m.created as created, \n"
					+ "     m.lastUpdated as lastUpdated, \n"
					+ "     m.updateSystemStatus as updateSystemStatus, \n"
					+ "     m.uuid as uuid \n"
					+ "FROM Monitor m left join m.system \n"
					+ "WHERE m.tenantId = :tenantid ";

			if (!AuthorizationHelper.isTenantAdmin(username)) {
				hql += "\n     AND  m.owner = :owner ";
			}

			for (SearchTerm searchTerm : searchCriteria.keySet()) {
				if (searchTerm.getSearchField().startsWith("owner") && !AuthorizationHelper.isTenantAdmin(username)) {
					continue;
				}
				if (searchCriteria.get(searchTerm) == null
						|| StringUtils.equalsIgnoreCase(searchCriteria.get(searchTerm).toString(), "null"))
				{
					if (searchTerm.getOperator() == SearchTerm.Operator.NEQ ) {
						hql += "\n     AND  " + String.format(searchTerm.getMappedField(), searchTerm.getPrefix()) + " is not null ";
					} else if (searchTerm.getOperator() == SearchTerm.Operator.EQ ) {
						hql += "\n     AND  " + String.format(searchTerm.getMappedField(), searchTerm.getPrefix()) + " is null ";
					} else {
						hql += "\n     AND  " + searchTerm.getExpression();
					}
				} else {
					hql += "\n     AND  " + searchTerm.getExpression();
				}
			}

			hql += " ORDER BY m.lastUpdated DESC\n";

			String q = hql;

			Query query = session.createQuery(hql)
					.setResultTransformer(Transformers.aliasToBean(transformClass))
					.setString("tenantid", TenancyHelper.getCurrentTenantId());

			q = q.replaceAll(":tenantid", "'" + TenancyHelper.getCurrentTenantId() + "'");

			if (hql.contains(":owner")) {
				query.setString("owner", username);
				q = q.replaceAll(":owner", "'" + username + "'");
			}

			for (SearchTerm searchTerm : searchCriteria.keySet()) {
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
				else if (searchTypeMappings.get(searchTerm.getSafeSearchField()) == Date.class ) {
					query.setDate(searchTerm.getSafeSearchField(), (java.util.Date)searchCriteria.get(searchTerm));

					q = q.replaceAll(":" + searchTerm.getSafeSearchField(),
							"'" + String.valueOf(searchTerm.getOperator().applyWildcards(searchCriteria.get(searchTerm))) + "'");
				}
				else if (searchTypeMappings.get(searchTerm.getSafeSearchField()) == Integer.class) {
					query.setInteger(searchTerm.getSafeSearchField(), (Integer)searchCriteria.get(searchTerm));

					q = q.replaceAll(":" + searchTerm.getSafeSearchField(),
							"'" + String.valueOf(searchTerm.getOperator().applyWildcards(searchCriteria.get(searchTerm))) + "'");
				}
				else
				{
					query.setParameter(searchTerm.getSafeSearchField(),
							searchTerm.getOperator().applyWildcards(searchCriteria.get(searchTerm)));
					q = StringUtils.replace(q, ":" + searchTerm.getSafeSearchField(),
							"'" + String.valueOf(searchTerm.getOperator().applyWildcards(searchCriteria.get(searchTerm))) + "'");
				}
			}

//			log.debug(q);

			List<Monitor> monitors = query.setFirstResult(offset).setMaxResults(limit).list();

			session.flush();

			return monitors;

		} catch (Throwable ex) {
			throw new MonitorException(ex);
		} finally {
			try {
				HibernateUtil.commitTransaction();
			} catch (Exception ignored) {
			}
		}
	}
}
