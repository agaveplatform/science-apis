package org.iplantc.service.common.dao;


import org.hibernate.Session;
import org.iplantc.service.common.exceptions.TenantException;
import org.iplantc.service.common.model.Tenant;
import org.iplantc.service.common.persistence.HibernateUtil;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.UUID;

@Test(groups={"integration"})
public class TenantDaoIT {

	private TenantDao dao;

	@BeforeClass
	public void beforeClass()
	{
		dao = new TenantDao();
	}

	@AfterMethod
	public void afterMethod()
	{
		Session session = HibernateUtil.getSession();
		session.clear();
		HibernateUtil.disableAllFilters();
		session.createQuery("delete Tenant").executeUpdate();
		session.flush();
		HibernateUtil.commitTransaction();

	}

	private Tenant createTenant(String tenantCode)
	{
		Tenant tenant = new Tenant();
		tenant.setBaseUrl("https://api.example.com");
		tenant.setTenantCode(tenantCode);
		tenant.setContactEmail("foo@example.com");
		tenant.setContactName("Foo Bar");
		tenant.setStatus("ACTIVE");
		return tenant;
	}

	@Test
	public void save() throws TenantException
	{
		String tenantCode = UUID.randomUUID().toString();
		Tenant tenant = createTenant(tenantCode);

		dao.persist(tenant);
		Assert.assertNotNull(tenant.getId(), "Tenant failed to save.");
	}

	@Test(dependsOnMethods={"save"})
	public void findByTenantId() throws TenantException
	{
		String tenantCode = UUID.randomUUID().toString();
		Tenant tenant = createTenant(tenantCode);

		dao.persist(tenant);
		Assert.assertNotNull(tenant.getId(), "Tenant failed to save.");

		Tenant savedTenant = dao.findByTenantId(tenant.getTenantCode());

		Assert.assertNotNull(savedTenant, "Failed to find a matching tenant by id");

		Assert.assertEquals(savedTenant.getId(), tenant.getId(), "Tenant ids were not equal. Wrong tenant returned from findByTenantId");
	}

	@Test(dependsOnMethods={"findByTenantId"})
	public void update() throws TenantException
	{
		String tenantCode = UUID.randomUUID().toString();
		Tenant tenant = createTenant(tenantCode);
		dao.persist(tenant);
		Assert.assertNotNull(tenant.getId(), "Tenant failed to save.");

		String newContactEmail = UUID.randomUUID() + "@example.com";
		tenant.setContactEmail(newContactEmail);
		dao.persist(tenant);

		Tenant savedTenant = dao.findByTenantId(tenant.getTenantCode());

		Assert.assertNotNull(savedTenant, "Failed to find a matching tenant by id");
		Assert.assertEquals(savedTenant.getContactEmail(), newContactEmail, "Tenant failed to update.");
	}

	@Test(dependsOnMethods={"update"})
	public void remove() throws TenantException
	{
		String tenantCode = UUID.randomUUID().toString();
		Tenant tenant = createTenant(tenantCode);

		dao.persist(tenant);
		Assert.assertNotNull(tenant.getId(), "Tenant failed to save.");

		dao.delete(tenant);

		Tenant savedTenant = dao.findByTenantId(tenant.getTenantCode());

		Assert.assertNull(savedTenant, "Tenant was not removed after delete.");
	}

	@Test(dependsOnMethods={"remove"})
	public void exists() throws TenantException
	{
		String tenantCode = UUID.randomUUID().toString();
		Tenant tenant = createTenant(tenantCode);

		dao.persist(tenant);
		Assert.assertNotNull(tenant.getId(), "Tenant failed to save.");

		Assert.assertTrue(dao.exists(tenantCode), "Valid tenant code should return true");

		Assert.assertFalse(dao.exists(tenantCode + "foo"),
				"Invalid tenant code should return false on existence check.");

		Assert.assertFalse(dao.exists(""),
				"Empty tenant code should return false on existence check.");
	}
}
