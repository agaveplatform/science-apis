package org.iplantc.service.tags.dao;

import org.iplantc.service.tags.AbstractTagTest;
import org.iplantc.service.tags.model.Tag;
import org.iplantc.service.tags.model.TagEvent;
import org.iplantc.service.tags.model.enumerations.TagEventType;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

import static org.iplantc.service.tags.TestDataHelper.TEST_SHAREUSER;
import static org.iplantc.service.tags.TestDataHelper.TEST_USER;

@Test(groups={"integration"})
public class TagEventDaoIT extends AbstractTagTest {
	
	private TagEventDao tagEventDao;
	
	@BeforeClass
	public void beforeClass() throws Exception
	{
		super.beforeClass();
		tagEventDao = new TagEventDao();
	}
	
	@AfterMethod
	public void afterMethod() throws Exception
	{
		clearTags();
	}
	
	@Test
	public void persist() throws Exception
	{
		TagEvent entityEvent = new TagEvent(createTag().getUuid(), 
				TagEventType.CREATED, TagEventType.CREATED.getDescription(), TEST_USER );
		tagEventDao.persist(entityEvent);
		Assert.assertNotNull(entityEvent.getId(), "tag event did not persist.");
	}

	@Test//(dependsOnMethods={"persist"})
	public void delete() throws Exception
	{
		TagEvent entityEvent = new TagEvent(createTag().getUuid(), TagEventType.CREATED, TEST_USER);
		tagEventDao.persist(entityEvent);
		Assert.assertNotNull(entityEvent.getId(), "tag event did not persist.");
		
		tagEventDao.delete(entityEvent);
		TagEvent userPem = tagEventDao.getById(entityEvent.getId());
		Assert.assertNull(userPem, "A tag event should be returned after deleting.");
	}

	@Test//(dependsOnMethods={"delete"})
	public void getEntityEventByEntityUuid() throws Exception
	{
		Tag tag = createTag();
		TagEvent entityEvent = new TagEvent(tag.getUuid(), TagEventType.CREATED, TEST_USER);
		tagEventDao.persist(entityEvent);
		Assert.assertNotNull(entityEvent.getId(), "tag event did not persist.");
		
		Tag tag2 = createTag();
		TagEvent entityEvent2 = new TagEvent(tag2.getUuid(), TagEventType.CREATED, TEST_USER);
		tagEventDao.persist(entityEvent2);
		Assert.assertNotNull(entityEvent2.getId(), "tag event did not persist.");
		
		
		List<TagEvent> pems = tagEventDao.getByTagUuid(entityEvent.getEntity());
		Assert.assertNotNull(pems, "getByTagUuid did not return any permissions.");
		Assert.assertEquals(pems.size(), 1, "getByTagUuid did not return the correct number of permissions.");
		Assert.assertFalse(pems.contains(entityEvent2), "getByTagUuid returned a permission from another tag.");
	}

	@Test//(dependsOnMethods={"getEntityEventByEntityUuid"})
	public void getAllEntityEventWithStatusForEntityUuid() throws Exception
	{
		Tag tag = createTag();
		TagEvent entityEvent1 = new TagEvent(tag.getUuid(), TagEventType.CREATED, TEST_USER);
		tagEventDao.persist(entityEvent1);
		Assert.assertNotNull(entityEvent1.getId(), "tag event 1 did not persist.");
		
		TagEvent entityEvent2 = new TagEvent(tag.getUuid(), TagEventType.UPDATED, TEST_SHAREUSER);
		tagEventDao.persist(entityEvent2);
		Assert.assertNotNull(entityEvent2.getId(), "tag event 2 did not persist.");
		
		List<TagEvent> results = tagEventDao.getByTagUuidAndStatus(tag.getUuid(), TagEventType.UPDATED);
		Assert.assertNotNull(results, "getByTagUuidAndStatus did not return the status events for the tag.");
		Assert.assertEquals(results.size(), 1, "getByTagUuidAndStatus did not return the status events for the tag.");
		Assert.assertEquals(results.get(0).getUuid(), entityEvent2.getUuid(), "getByTagUuidAndStatus did not return the correct tag event for the user.");
	}

	@Test
	public void deleteByTagId() throws Exception
	{
		Tag tag = createTag();

		for (TagEventType eventType: List.of(TagEventType.CREATED, TagEventType.UPDATED, TagEventType.UPDATED, TagEventType.UPDATED, TagEventType.DELETED)) {
			TagEvent event = new TagEvent(tag.getUuid(), eventType, TEST_USER);
			tagEventDao.persist(event);
			Assert.assertNotNull(event.getId(), "Tag " + eventType.name() + " event did not persist.");
		}

		// delete all events for the tag
		tagEventDao.deleteByTagId(tag.getUuid());

		// verify no events remain
		List<TagEvent> results = tagEventDao.getByTagUuid(tag.getUuid());
		Assert.assertNotNull(results, "getByTagUuid should never return a null list");
		Assert.assertTrue(results.isEmpty(), "getByTagUuid should not return any events after deleteByTagId");


	}
}
