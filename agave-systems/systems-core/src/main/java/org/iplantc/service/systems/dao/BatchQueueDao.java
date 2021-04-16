package org.iplantc.service.systems.dao;

// Generated Feb 1, 2013 6:15:53 PM by Hibernate Tools 3.4.0.CR1

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.hibernate.HibernateException;
import org.hibernate.ObjectNotFoundException;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.criterion.Restrictions;
import org.hibernate.transform.Transformers;
import org.hibernate.type.StandardBasicTypes;
import org.iplantc.service.common.dao.AbstractDao;
import org.iplantc.service.common.persistence.HibernateUtil;
import org.iplantc.service.common.persistence.TenancyHelper;
import org.iplantc.service.common.search.SearchTerm;
import org.iplantc.service.systems.Settings;
import org.iplantc.service.systems.exceptions.SystemException;
import org.iplantc.service.systems.model.BatchQueue;
import org.iplantc.service.systems.model.BatchQueueLoad;
import org.iplantc.service.systems.model.ExecutionSystem;
import org.iplantc.service.systems.search.BatchQueueSearchFilter;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Home object for domain model class BatchQueue.
 * @see org.iplantc.service.systems.model.ExecutionSystem
 * @author Hibernate Tools
 */
public class BatchQueueDao extends AbstractDao {

	private static final Logger log = Logger.getLogger(BatchQueueDao.class);

	/* (non-Javadoc)
	 * @see org.iplantc.service.common.persistence.AbstractDao#getSession()
	 */
	@Override
	protected Session getSession() {
		HibernateUtil.beginTransaction();
		Session session = HibernateUtil.getSession();
		//session.clear();
		session.enableFilter("systemTenantFilter").setParameter("tenantId", TenancyHelper.getCurrentTenantId());
		return session;
	}
	
	/**
     * Returns all {@link BatchQueue} across all systems and tenants
     * @return list of all batch queues
     */
    @SuppressWarnings("unchecked")
    public List<BatchQueue> getAll()
    {
        try
        {
            Session session = getSession();
            session.clear();
            return session.createQuery("from org.iplantc.service.systems.model.BatchQueue").list();
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
            throw ex;
        }
    }
	
	/**
	 * Saves or updates the given {@code queue} to the database.
	 * 
	 * @param queue
	 */
	public void persist(BatchQueue queue)
	{
		try
		{
			Session session = getSession();
			//session.clear();
			session.saveOrUpdate(queue);
			//session.getTransaction().commit();
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
			throw ex;
		}
		finally
		{
			try { HibernateUtil.commitTransaction(); } catch (Throwable ignored) {}
		}
	}

	/**
	 * Deletes the {@code queue} from the database.
	 * 
	 * @param queue
	 * @throws SystemException
	 */
	public void delete(BatchQueue queue) throws SystemException
	{

		if (queue == null)
			throw new SystemException("BatchQueue cannot be null");

		try
		{
			Session session = getSession();
			
			session.delete(queue);
			
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
		}
		finally
		{
			try { HibernateUtil.commitTransaction(); } catch (Throwable ignored) {}
		}
	}

	/**
	 * Fetches the {@link BatchQueue} with the given database {@code id}.
	 * 
	 * @param id database identifier of a {@link BatchQueue}
	 * @return {@link BatchQueue} with the given id or null if no match.
	 */
	public BatchQueue findById(Long id)
	{
		try
		{
		    HibernateUtil.beginTransaction();
			Session session = HibernateUtil.getSession();
			session.clear();
			
			BatchQueue queue = (BatchQueue)session
					.createCriteria(BatchQueue.class)
					.add(Restrictions.idEq(id))
					.uniqueResult();
			
			return queue;
		}
		catch (HibernateException ex)
		{
			throw ex;
		}
	}
	
	/**
     * Fetches the {@link BatchQueue} with the given {@code uuid}.
     * 
     * @param uuid of a {@link BatchQueue}
     * @return {@link BatchQueue} with the given uuid or null if no match.
     */
    public BatchQueue findByUuid(String uuid)
    {
        try
        {
            HibernateUtil.beginTransaction();
            Session session = HibernateUtil.getSession();
            session.clear();
            
            BatchQueue queue = (BatchQueue)session
                    .createCriteria(BatchQueue.class)
                    .add(Restrictions.eq("uuid", uuid))
                    .uniqueResult();
            
            return queue;
        }
        catch (HibernateException ex)
        {
            throw ex;
        }
    }
	
	/**
	 * Fetches the {@link BatchQueue} based on the {@code systemId} of the
	 * {@link ExecutionSystem} and the {@code name} of the {@link BatchQueue}.
	 * 
	 * Tenancy is honored on this query, so make sure you load the tenant of
	 * the corresponding {@link ExecutionSystem} if calling this without a
	 * Reqest session.
	 *  
	 * @param systemId id of the owning {@link ExecutionSystem}
	 * @param queueNameOrUuid name or uuid of the queue
	 * @return
	 */
	public BatchQueue findBySystemIdAndName(String systemId, String queueNameOrUuid)
	{
		try
		{
			Session session = getSession();

			String hql =  "from BatchQueue b "
						+ "where (b.name = :name or uuid = :name) "
						+ "     and b.executionSystem.systemId = :systemId "
						+ "		and b.executionSystem.tenantId = :tenantId "
						+ "		and b.executionSystem.available = :available";

			BatchQueue queue = (BatchQueue)session.createQuery(hql)
			        .setString("name", queueNameOrUuid)
			        .setString("systemId", systemId)
					.setString("tenantId", TenancyHelper.getCurrentTenantId())
					.setBoolean("available", Boolean.TRUE)
					.setMaxResults(1)
					.uniqueResult();
			
			return queue;
		}
		catch (ObjectNotFoundException ex)
		{
			return null;
		}
		catch (HibernateException ex)
		{
			throw ex;
		}
	}
	
	/**
     * Fetches the {@link BatchQueue} based on the {@code systemId} of the
     * {@link ExecutionSystem}.
     * 
     * Tenancy is honored on this query, so make sure you load the tenant of
     * the corresponding {@link ExecutionSystem} if calling this without a
     * Reqest session.
     *  
     * @param systemId id of the owning {@link ExecutionSystem}
     * @return
     */
    @SuppressWarnings("unchecked")
    public List<BatchQueue> findBySystemId(String systemId, int offset, int limit)
    {
        try
        {
            Session session = getSession();

            String hql =  "from org.iplantc.service.systems.model.BatchQueue b "
                        + "where b.executionSystem.systemId = :systemId "
                        + "     and b.executionSystem.tenantId = :tenantId "
                        + "     and b.executionSystem.available = :available";
            return (List<BatchQueue>)session.createQuery(hql)
                    .setString("systemId", systemId)
                    .setString("tenantId", TenancyHelper.getCurrentTenantId())
                    .setBoolean("available", Boolean.TRUE)
                    .setFirstResult(offset)
                    .setMaxResults(limit)
                    .list();
            
        }
        catch (ObjectNotFoundException ex)
        {
            return null;
        }
        catch (HibernateException ex)
        {
            throw ex;
        }
    }

    /**
     * Generates a summary of all activity on the named {@code queueName} by 
     * querying the jobs table.
     * 
     * @param systemId id of the owning {@link ExecutionSystem}
     * @param queueName name or uuid of the queue
     * @return
     */
    public BatchQueueLoad getCurrentLoad(String systemId, String queueName) {
        try
        {
            Session session = getSession();

            String sql =  "select queue_request as name, " + 
                    "       MAX(j.last_updated) as created, " + 
                    "       COUNT(CASE WHEN j.status in ('PENDING','QUEUED','PAUSED','CLEANING_UP','RUNNING','STAGED','PROCESSING_INPUTS','STAGING_INPUTS','STAGING_JOB','SUBMITTING','ARCHIVING') THEN j.status ELSE NULL END) active, " + 
                    "       COUNT(CASE WHEN j.status in ('PENDING','PAUSED','CLEANING_UP','STAGED','PROCESSING_INPUTS') THEN j.status ELSE NULL END) backlogged, " + 
                    "       COUNT(CASE WHEN j.status = 'PENDING' THEN j.id ELSE NULL END) pending, " + 
                    "       COUNT(CASE WHEN j.status in ('PAUSED') THEN j.id ELSE NULL END) paused, " + 
                    "       COUNT(CASE WHEN j.status in ('PROCESSING_INPUTS') THEN j.id ELSE NULL END) processingInputs, " + 
                    "       COUNT(CASE WHEN j.status in ('STAGING_INPUTS') THEN j.id ELSE NULL END) stagingInputs, " + 
                    "       COUNT(CASE WHEN j.status in ('STAGED') THEN j.id ELSE NULL END) staged, " + 
                    "       COUNT(CASE WHEN j.status in ('STAGING_JOB', 'SUBMITTING') THEN j.id ELSE NULL END) submitting, " + 
                    "       COUNT(CASE WHEN j.status = 'QUEUED' THEN j.id ELSE NULL END) queued, " + 
                    "       COUNT(CASE WHEN j.status in ('RUNNING') THEN j.id ELSE NULL END) running, " + 
                    "       COUNT(CASE WHEN j.status in ('CLEANING_UP') THEN j.id ELSE NULL END) cleaningUp, " + 
                    "       COUNT(CASE WHEN j.status = 'ARCHIVING' THEN j.id ELSE NULL END) archiving " + 
                    "from jobs j " + 
                    "where j.execution_system = :systemId  " + 
                    "     and j.queue_request = :queueName " + 
                    "     and j.tenant_id = :tenantId " + 
                    "group by j.queue_request ";
            BatchQueueLoad batchQueueLoad = (BatchQueueLoad)session.createSQLQuery(sql)
                    .addScalar("name",StandardBasicTypes.STRING)
                    .addScalar("created",StandardBasicTypes.DATE)
                    .addScalar("active",StandardBasicTypes.LONG)
                    .addScalar("backlogged",StandardBasicTypes.LONG)
                    .addScalar("pending",StandardBasicTypes.LONG)
                    .addScalar("paused",StandardBasicTypes.LONG)
                    .addScalar("processingInputs",StandardBasicTypes.LONG)
                    .addScalar("stagingInputs",StandardBasicTypes.LONG)
                    .addScalar("submitting",StandardBasicTypes.LONG)
                    .addScalar("queued",StandardBasicTypes.LONG)
                    .addScalar("running",StandardBasicTypes.LONG)
                    .addScalar("cleaningUp",StandardBasicTypes.LONG)
                    .addScalar("archiving",StandardBasicTypes.LONG)
                    .setResultTransformer(Transformers.aliasToBean(BatchQueueLoad.class)) 
                    .setString("queueName", queueName)
                    .setString("systemId", systemId)
                    .setString("tenantId", TenancyHelper.getCurrentTenantId())
                    .uniqueResult();

            if (batchQueueLoad == null) {
				batchQueueLoad = new BatchQueueLoad(queueName);
			}

            return batchQueueLoad;
        }
        catch (ObjectNotFoundException ex)
        {
            return null;
        }
        catch (HibernateException ex)
        {
            throw ex;
        }
    }

	/**
	 * Generates a summary of all activity on the named {@code queueName} for the given {@code username} by querying the jobs table.
	 *
	 * @param systemId the {@link ExecutionSystem} containing the {@code queueName}
	 * @param queueName name of the queue
	 * @param username the user for whom the activity summary will be generated
	 * @return summary info on the jobs currently running for the given {@code username} in the given {@code queueName}
	 */
	public BatchQueueLoad getCurrentLoadForUser(String systemId, String queueName, String username) {
		try
		{
			Session session = getSession();

			String sql =  "select queue_request as name, " +
					"       MAX(j.last_updated) as created, " +
					"       COUNT(CASE WHEN j.status in ('PENDING','QUEUED','PAUSED','CLEANING_UP','RUNNING','STAGED','PROCESSING_INPUTS','STAGING_INPUTS','STAGING_JOB','SUBMITTING','ARCHIVING') THEN j.status ELSE NULL END) active, " +
					"       COUNT(CASE WHEN j.status in ('PENDING','PAUSED','CLEANING_UP','STAGED','PROCESSING_INPUTS') THEN j.status ELSE NULL END) backlogged, " +
					"       COUNT(CASE WHEN j.status = 'PENDING' THEN j.id ELSE NULL END) pending, " +
					"       COUNT(CASE WHEN j.status in ('PAUSED') THEN j.id ELSE NULL END) paused, " +
					"       COUNT(CASE WHEN j.status in ('PROCESSING_INPUTS') THEN j.id ELSE NULL END) processingInputs, " +
					"       COUNT(CASE WHEN j.status in ('STAGING_INPUTS') THEN j.id ELSE NULL END) stagingInputs, " +
					"       COUNT(CASE WHEN j.status in ('STAGED') THEN j.id ELSE NULL END) staged, " +
					"       COUNT(CASE WHEN j.status in ('STAGING_JOB', 'SUBMITTING') THEN j.id ELSE NULL END) submitting, " +
					"       COUNT(CASE WHEN j.status = 'QUEUED' THEN j.id ELSE NULL END) queued, " +
					"       COUNT(CASE WHEN j.status in ('RUNNING') THEN j.id ELSE NULL END) running, " +
					"       COUNT(CASE WHEN j.status in ('CLEANING_UP') THEN j.id ELSE NULL END) cleaningUp, " +
					"       COUNT(CASE WHEN j.status = 'ARCHIVING' THEN j.id ELSE NULL END) archiving " +
					"from jobs j " +
					"where j.execution_system = :systemId  " +
					"     and j.queue_request = :queueName " +
					"     and j.tenant_id = :tenantId " +
					"     and j.owner = :username " +
					"group by j.queue_request ";
			BatchQueueLoad batchQueueLoad = (BatchQueueLoad)session.createSQLQuery(sql)
					.addScalar("name",StandardBasicTypes.STRING)
					.addScalar("created",StandardBasicTypes.DATE)
					.addScalar("active",StandardBasicTypes.LONG)
					.setString("queueName", queueName)
					.setString("systemId", systemId)
					.setString("username", username)
					.setString("tenantId", TenancyHelper.getCurrentTenantId())
					.uniqueResult();

			if (batchQueueLoad == null) {
				batchQueueLoad = new BatchQueueLoad(queueName);
			}

			return batchQueueLoad;
		}
		catch (ObjectNotFoundException ex)
		{
			return null;
		}
		catch (HibernateException ex)
		{
			throw ex;
		}
	}

    /**
     * Searches for {@link BatchQueue}s by the given system who matches the
     * given set of parameters. Permissions are honored in this query.
     *
     * @param systemId
     * @param searchCriteria
     * @return
     * @throws SystemException
     */
    public List<BatchQueue> findMatching(Long systemId, Map<SearchTerm, Object> searchCriteria)
    throws SystemException 
    {
        return findMatching(systemId, searchCriteria, 0, Settings.DEFAULT_PAGE_SIZE);
    }
    
    /**
     * Searches for {@link BatchQueue}s by the given system who matches the
     * given set of parameters. Permissions are honored in this query.
     *
     * @param systemId
     * @param searchCriteria
     * @param offset
     * @param limit
     * @return
     * @throws SystemException
     */
    @SuppressWarnings("unchecked")
    public List<BatchQueue> findMatching(Long systemId, Map<SearchTerm, Object> searchCriteria, int limit, int offset)
    throws SystemException {
        
        try 
        {
			Map<String, Class> searchTypeMappings = new BatchQueueSearchFilter().getSearchTypeMappings();
			Session session = getSession();
            session.clear();
            
            String hql = "SELECT q \n"
                       + "FROM BatchQueue q \n"
                       + "    left join q.executionSystem executionSystem \n"
                       + "WHERE executionSystem.id = :systemId \n";
                       
            for (SearchTerm searchTerm : searchCriteria.keySet()) {
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

            hql += " ORDER BY q.lastUpdated DESC\n";

            String q = hql;

            Query query = session.createQuery(hql)
                                 .setLong("systemId", systemId);
            
            q = q.replace(":systemId", systemId.toString());

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
                            Pattern.quote("'" + String.valueOf(searchTerm.getOperator().applyWildcards(searchCriteria.get(searchTerm))) + "'"));
                }
            }

            log.debug(q);

            List<BatchQueue> queues = query.setFirstResult(offset).setMaxResults(limit).list();

            session.flush();

            return queues;

        } catch (Throwable ex) {
            throw new SystemException(ex);
        } finally {
            try {
                HibernateUtil.commitTransaction();
            } catch (Exception ignored) {
            }
        }
    }

}
