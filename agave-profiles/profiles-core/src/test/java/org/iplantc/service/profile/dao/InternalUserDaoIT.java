package org.iplantc.service.profile.dao;

import java.io.IOException;
import java.util.List;

import org.hibernate.HibernateException;
import org.hibernate.ObjectNotFoundException;
import org.hibernate.Session;
import org.iplantc.service.common.persistence.HibernateUtil;
import org.iplantc.service.profile.TestDataHelper;
import org.iplantc.service.profile.exceptions.ProfileArgumentException;
import org.iplantc.service.profile.exceptions.ProfileException;
import org.iplantc.service.profile.model.InternalUser;
import org.json.JSONException;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * @author dooley
 *
 */
@SuppressWarnings("unused")
@Test(groups={"integration"})
public class InternalUserDaoIT {

	private static final String INTERNAL_USER_CREATOR = "testinternaluser";
	private static final String INTERNAL_USER_STRANGER = "testinternalotheruser";
	private static final String SYSTEM_PUBLIC_USER = "public";
	private static final String SYSTEM_UNSHARED_USER = "testotheruser";

	private InternalUserDao dao;
	private TestDataHelper dataHelper;

	/**
	 * Initalizes the test db and adds the test app
	 */
	@BeforeClass
	public void beforeClass()
	{
		dataHelper = TestDataHelper.getInstance();
		dao = new InternalUserDao();
	}

	@BeforeMethod
	private void beforeMethod() throws ProfileException {
		clearInternalUsers();
	}

	@AfterMethod
	private void afterMethod() throws ProfileException {
		clearInternalUsers();
	}

	private void clearInternalUsers() throws ProfileException {
		try
		{
			HibernateUtil.beginTransaction();
			Session session = HibernateUtil.getSession();
			session.clear();
			HibernateUtil.disableAllFilters();
			session.createQuery("DELETE InternalUser").executeUpdate();
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

			throw new ProfileException(ex);
		}
		finally {
			try { HibernateUtil.commitTransaction(); } catch (Exception ignored) {}
		}
	}

	private InternalUser createInternalUser() throws IOException, JSONException, ProfileArgumentException, ProfileException {

		InternalUser internalUser = InternalUser.fromJSON(dataHelper.getTestDataObject(TestDataHelper.TEST_INTERNAL_USER_FILE));
		internalUser.setCreatedBy(INTERNAL_USER_CREATOR);

		return internalUser;
	}

	@DataProvider(name="persistenceInternalUserProvider")
	public Object[][] persistenceInternalUserProvider() throws Exception
	{
		return new Object[][] {
			{ createInternalUser(), "Internal user will persist", false }
		};
	}

	@Test (dataProvider="persistenceInternalUserProvider")
	public void persistInternalUser(InternalUser internalUser, String message, boolean shouldThrowException) {
		InternalUserDao dao = new InternalUserDao();
		boolean actuallyThrewException = false;
		String exceptionMsg = "";
		try
		{
			dao.persist(internalUser);
			Assert.assertTrue(internalUser.getId() > 0, "Failed to generate a user ID.");
		}
		catch(Exception e)
		{
			actuallyThrewException = true;
            exceptionMsg = "Error persisting user " + internalUser.getUsername() + ": " + message;
            if (actuallyThrewException != shouldThrowException) e.printStackTrace();
		}

		Assert.assertEquals(shouldThrowException, actuallyThrewException, exceptionMsg);
	}

	@DataProvider(name="updateInternalUserProvider")
	public Object[][] updateInternalUserProvider() throws Exception
	{
		return new Object[][] {
			{ createInternalUser(), "InternalUser will update", false }
		};
	}

	@Test (dataProvider="updateInternalUserProvider")
	public void updateInternalUserProviderTest(InternalUser internalUser, String message, boolean shouldThrowException) {
		InternalUserDao dao = new InternalUserDao();
		boolean actuallyThrewException = false;
		String exceptionMsg = "";
		try
		{
			dao.persist(internalUser);

			Assert.assertTrue(internalUser.getId() > 0, "InternalUser got an id after persisting.");

			InternalUser savedInternalUser = dao.getInternalUserById(internalUser.getId());

			Assert.assertNotNull(savedInternalUser, "InternalUser was found in db.");

			String name = "testname-" + System.currentTimeMillis();

			savedInternalUser.setFirstName(name);

			dao.persist(savedInternalUser);

			InternalUser updatedInternalUser = dao.getInternalUserById(internalUser.getId());

			Assert.assertNotNull(updatedInternalUser, "InternalUser was found in db.");

			Assert.assertEquals(name, updatedInternalUser.getFirstName(), "Failed to generate a user ID.");
		}
		catch(Exception e)
		{
			actuallyThrewException = true;
            exceptionMsg = "Error persisting user " + internalUser.getUsername() + ": " + message;
            if (actuallyThrewException != shouldThrowException) e.printStackTrace();
		}

        //System.out.println(" exception thrown?  expected " + shouldThrowException + " actual " + actuallyThrewException);
		Assert.assertEquals(actuallyThrewException, shouldThrowException, exceptionMsg);
	}

	@DataProvider(name="deleteInternalUserProvider")
	public Object[][] deleteInternalUserProvider() throws Exception
	{
		return new Object[][] {
			{ createInternalUser(), "InternalUser will delete", false }
		};
	}

	@Test (dataProvider="deleteInternalUserProvider")
	public void delete(InternalUser internalUser, String message, boolean shouldThrowException)
	{
		InternalUserDao dao = new InternalUserDao();
		boolean actuallyThrewException = false;
		String exceptionMsg = "";

		try
		{
			dao.persist(internalUser);

			Assert.assertTrue(internalUser.getId() > 0, "InternalUser got an id after persisting.");

			long id = internalUser.getId();

			internalUser = dao.getInternalUserById(id);

			Assert.assertNotNull(internalUser, "InternalUser was found in db.");

			dao.delete(internalUser);

			internalUser = dao.getInternalUserById(id);

			Assert.assertNull(internalUser, "InternalUser was not found in db.");
		}
		catch(ProfileException e)
		{
			actuallyThrewException = true;
			if (internalUser != null) {
				exceptionMsg = "Error persisting user " + internalUser.getUsername() + ": " + message;
			}
            if (actuallyThrewException != shouldThrowException) e.printStackTrace();
		}

        //System.out.println(" exception thrown?  expected " + shouldThrowException + " actual " + actuallyThrewException);
		Assert.assertEquals(actuallyThrewException, shouldThrowException, exceptionMsg);
	}

	@DataProvider(name="findByExampleInternalUserProvider")
	public Object[][] findByExampleInternalUserProvider() throws Exception
	{
		InternalUser activeUser = createInternalUser();
		activeUser.setEmail("alpha@example.com");
		activeUser.setUsername("alpha");
		activeUser.setFirstName("Alpha");
		activeUser.setLastName("Beta");

		InternalUser inactiveUser = createInternalUser();
		inactiveUser.setEmail("gamma@demo.net");
		inactiveUser.setUsername("gamma");
		inactiveUser.setFirstName("Gamma");
		inactiveUser.setLastName("Delta");
		inactiveUser.setActive(false);

		return new Object[][] {
			{ activeUser.clone(), "email", "alpha@example.com", "active InternalUser was found by exact email", true },
			{ activeUser.clone(), "email", "ALPHA@EXAMPLE.COM", "active InternalUser was found by case insensitive email", true },
			{ activeUser.clone(), "email", "alpha", "active InternalUser was found by email handle", true },
			{ activeUser.clone(), "email", "example.com", "active InternalUser was found by email hostname", true },
			{ activeUser.clone(), "email", "alp", "active InternalUser was found by email prefix", true },
			{ activeUser.clone(), "email", "example", "active InternalUser was found by email host", true },
			{ activeUser.clone(), "email", "beta@example.com", "inactive InternalUser was found by exact email", false },

			{ activeUser.clone(), "username", "alpha", "active InternalUser was found by exact username", true },
			{ activeUser.clone(), "username", "ALPHA", "active InternalUser was found by exact username", true },
			{ activeUser.clone(), "username", "al", "active InternalUser was found by username prefix", true },
			{ activeUser.clone(), "username", "ha", "active InternalUser was found by username suffix", true },
			{ activeUser.clone(), "username", "gamma", "inactive InternalUser was found by exact username", false },

			{ activeUser.clone(), "name", "Alpha Beta", "active InternalUser was not found by exact full name", true },
			{ activeUser.clone(), "name", "ALPHA BETA", "active InternalUser was not found by case insensitive full name", true },
			{ activeUser.clone(), "name", "alpha beta", "active InternalUser was not found by case insensitive full name", true },
			{ activeUser.clone(), "name", "alpha", "active InternalUser was found by first name", true },
			{ activeUser.clone(), "name", "beta", "active InternalUser was found by last name", true },
			{ activeUser.clone(), "name", "alp", "active InternalUser was found by partial first name", true },
			{ activeUser.clone(), "name", "bet", "active InternalUser was found by partial last name", true },
			{ activeUser.clone(), "name", "alpha b", "active InternalUser was found by first name and partial last name", true },
			{ activeUser.clone(), "name", "alp bet", "active InternalUser was found by partial first and last name", false },
			{ activeUser.clone(), "name", "gamma delta", "active InternalUser was found by wrong name", false },

			{ activeUser.clone(), "active", Boolean.TRUE, "active InternalUser was found by username suffix", true },
			{ activeUser.clone(), "active", Boolean.FALSE, "active InternalUser was found by username suffix", false },


			{ inactiveUser.clone(), "email", "gamma@demo.net", "active InternalUser was not found by exact email", true },
			{ inactiveUser.clone(), "email", "GAMMA@DEMO.NET", "active InternalUser was not found by case insensitive email", true },
			{ inactiveUser.clone(), "email", "gamma", "active InternalUser was not found by email handle", true },
			{ inactiveUser.clone(), "email", "demo.net", "active InternalUser was not found by email hostname", true },
			{ inactiveUser.clone(), "email", "gam", "active InternalUser was not found by email prefix", true },
			{ inactiveUser.clone(), "email", "demo", "active InternalUser was not found by email host", true },
			{ inactiveUser.clone(), "email", "alpha@example.com", "active InternalUser was found by exact email", false },

			{ inactiveUser.clone(), "username", "gamma", "active InternalUser was not found by exact username", true },
			{ inactiveUser.clone(), "username", "GAMMA", "active InternalUser was not found by exact username", true },
			{ inactiveUser.clone(), "username", "ga", "active InternalUser was not found by username prefix", true },
			{ inactiveUser.clone(), "username", "ma", "active InternalUser was not found by username suffix", true },
			{ inactiveUser.clone(), "username", "alpha", "inactive InternalUser was found by wrong username", false },

			{ inactiveUser.clone(), "name", "Gamma Delta", "inactive InternalUser was not found by exact full name", true },
			{ inactiveUser.clone(), "name", "GAMMA DELTA", "inactive InternalUser was not found by case insensitive full name", true },
			{ inactiveUser.clone(), "name", "gamma delta", "inactive InternalUser was not found by case insensitive full name", true },
			{ inactiveUser.clone(), "name", "gamma", "inactive InternalUser was found by first name", true },
			{ inactiveUser.clone(), "name", "delta", "inactive InternalUser was found by last name", true },
			{ inactiveUser.clone(), "name", "gam", "inactive InternalUser was found by partial first name", true },
			{ inactiveUser.clone(), "name", "del", "inactive InternalUser was found by partial last name", true },
			{ inactiveUser.clone(), "name", "gamma d", "inactive InternalUser was found by first name and partial last name", true },
			{ inactiveUser.clone(), "name", "gam del", "inactive InternalUser was found by partial first and last name", false },
			{ inactiveUser.clone(), "name", "alpha beta", "inactive InternalUser was found by wrong name", false },

			{ inactiveUser.clone(), "active", Boolean.FALSE, "inactive InternalUser was found by username suffix", true },
			{ inactiveUser.clone(), "active", Boolean.TRUE, "inactive InternalUser was found by username suffix", false },

		};
	}

	@Test (dataProvider="findByExampleInternalUserProvider")
	public void findByExampleInternalUserProviderTest(InternalUser user, String searchTerm, Object searchValue, String message, Boolean shouldFind) {
		boolean actuallyThrewException = false;
		String exceptionMsg = "";
		try
		{
			dao.persist(user);

			Assert.assertTrue(user.getId() > 0, "InternalUser got an id after persisting.");

			List<InternalUser> users = dao.findByExample(INTERNAL_USER_CREATOR, searchTerm, searchValue);

			if (shouldFind)
			{
				Assert.assertFalse(users.isEmpty(), message);
				Assert.assertEquals(users.size(), 1, "Wrong number of results returned form findbyexample");
			}
			else
			{
				Assert.assertTrue(users.isEmpty(), message);
			}
		}
		catch(Exception e)
		{
			Assert.fail("Failed to search for internal user by example", e);
		}
	}

	@Test
	public void getActiveInternalUsersCreatedByAPIUser()
	{
		InternalUserDao dao = new InternalUserDao();
		boolean actuallyThrewException = false;
		String exceptionMsg = "";
		InternalUser user1, user2, user3;

		try
		{
			user1 = createInternalUser();
			user1.setUsername("user1");

			user2 = createInternalUser();
			user2.setUsername("user2");

			user3 = createInternalUser();
			user3.setUsername("user3");
			user3.setActive(false);

			dao.persist(user1);
			dao.persist(user2);
			dao.persist(user3);

			Assert.assertTrue(user1.getId() > 0, "First user failed to persist.");
			Assert.assertTrue(user2.getId() > 0, "Second user failed to persist.");
			Assert.assertTrue(user3.getId() > 0, "Third user failed to persist.");

			List<InternalUser> users = dao.getActiveInternalUsersCreatedByAPIUser(INTERNAL_USER_CREATOR);

			Assert.assertNotNull(users, "No internal users were found.");

			Assert.assertEquals(users.size(), 2, "No internal users were found.");

			Assert.assertTrue(users.contains(user1), "Results did not contain first user.");

			Assert.assertTrue(users.contains(user2), "Results did not contain second user");

			Assert.assertFalse(users.contains(user3), "Results contained inactive user");

		}
		catch(Exception e)
		{
			actuallyThrewException = true;
            exceptionMsg = "Failed to retrieve active users for " + INTERNAL_USER_CREATOR;
            e.printStackTrace();
		}

        //System.out.println(" exception thrown?  expected false actual " + actuallyThrewException);
		Assert.assertFalse(actuallyThrewException, exceptionMsg);
	}

	@Test
	public void getAllInternalUsersCreatedByAPIUser()
	{
		InternalUserDao dao = new InternalUserDao();
		boolean actuallyThrewException = false;
		String exceptionMsg = "";
		InternalUser user1, user2, user3 ;

		try
		{
			user1 = createInternalUser();
			user1.setUsername("user1");

			user2 = createInternalUser();
			user2.setUsername("user2");

			user3 = createInternalUser();
			user3.setUsername("user3");
			user3.setActive(false);

			dao.persist(user1);
			dao.persist(user2);
			dao.persist(user3);

			Assert.assertTrue(user1.getId() > 0, "First user failed to persist.");
			Assert.assertTrue(user2.getId() > 0, "Second user failed to persist.");
			Assert.assertTrue(user3.getId() > 0, "Third user failed to persist.");

			List<InternalUser> users = dao.getAllInternalUsersCreatedByAPIUser(INTERNAL_USER_CREATOR);

			Assert.assertNotNull(users, "No internal users were found.");

			Assert.assertEquals(users.size(), 3, "No internal users were found.");

			Assert.assertTrue(users.contains(user1), "Results did not contain first user.");

			Assert.assertTrue(users.contains(user2), "Results did not contain second user");

			Assert.assertTrue(users.contains(user3), "Results contained inactive user");

		}
		catch(Exception e)
		{
			actuallyThrewException = true;
            exceptionMsg = "Failed to retrieve active users for " + INTERNAL_USER_CREATOR;
            e.printStackTrace();
		}

//        System.out.println(" exception thrown?  expected false actual " + actuallyThrewException);
		Assert.assertFalse(actuallyThrewException, exceptionMsg);
	}

	@DataProvider(name="internalUserProvider")
	public Object[][] internalUserProvider() throws Exception
	{
		return new Object[][] {
			{ createInternalUser(), INTERNAL_USER_CREATOR, true, "User found for creating user.", false },
			{ createInternalUser(), INTERNAL_USER_STRANGER, false, "User not found for creating user.", false },
			{ createInternalUser(), SYSTEM_PUBLIC_USER, false, "User not found for creating user.", false },
		};
	}

	@Test( dataProvider="internalUserProvider")
	public void getInternalUserByAPIUserAndUsername(InternalUser internalUser, String username, boolean shouldFindUser, String message, boolean shouldThrowException )
	{
		InternalUserDao dao = new InternalUserDao();
		boolean actuallyThrewException = false;
		String exceptionMsg = "";

		try
		{
			dao.persist(internalUser);

			Assert.assertTrue(internalUser.getId() > 0, "InternalUser got an id after persisting.");

			InternalUser savedUser = dao.getInternalUserByAPIUserAndUsername(username, internalUser.getUsername());

			Assert.assertEquals(internalUser.equals(savedUser), shouldFindUser, "Persisted user was found.");

		}
		catch(Exception e)
		{
			actuallyThrewException = true;
            exceptionMsg = "Error persisting user " + internalUser.getUsername() + ": " + message;
            if (actuallyThrewException != shouldThrowException) e.printStackTrace();
		}

//        System.out.println(" exception thrown?  expected " + shouldThrowException + " actual " + actuallyThrewException);
		Assert.assertEquals(actuallyThrewException, shouldThrowException, exceptionMsg);
	}

	@Test
	public void getInternalUserById()
	{
		InternalUserDao dao = new InternalUserDao();
		boolean actuallyThrewException = false;
		String exceptionMsg = "";
		InternalUser user1;

		try
		{
			user1 = createInternalUser();

			dao.persist(user1);

			Assert.assertTrue(user1.getId() > 0, "InternalUser got an id after persisting.");

			InternalUser savedUser = dao.getInternalUserById(user1.getId());

			Assert.assertNotNull(savedUser, "InternalUsers were found matching example.");

			Assert.assertEquals(user1, savedUser, "Persisted user was found.");

		}
		catch(Exception e)
		{
			actuallyThrewException = true;
            exceptionMsg = "Error finding user by username.";
            e.printStackTrace();
		}

//        System.out.println(" exception thrown?  expected false actual " + actuallyThrewException);
		Assert.assertFalse(actuallyThrewException, exceptionMsg);
	}
}
