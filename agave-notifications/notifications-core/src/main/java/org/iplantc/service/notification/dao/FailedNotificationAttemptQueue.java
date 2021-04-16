package org.iplantc.service.notification.dao;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.client.*;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.IndexOptions;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.ClassModel;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;
import org.iplantc.service.common.search.SearchTerm;
import org.iplantc.service.notification.Settings;
import org.iplantc.service.notification.exceptions.NotificationException;
import org.iplantc.service.notification.managers.NotificationManager;
import org.iplantc.service.notification.model.Notification;
import org.iplantc.service.notification.model.NotificationAttempt;
import org.iplantc.service.notification.model.NotificationAttemptCodec;
import org.iplantc.service.notification.model.NotificationAttemptResponse;

import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.mongodb.client.model.Filters.*;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

/**
 * Manages the failed notification stack in MongoDB. The stack is implemented
 * as a capped collection with a fixed of 1000 entries.
 * 
 * @author dooley
 *
 */
public class FailedNotificationAttemptQueue {

	private static final Logger	log	= Logger.getLogger(FailedNotificationAttemptQueue.class);

	private MongoClient mongov4Client = null;
	
	private static FailedNotificationAttemptQueue queue;
	
	private FailedNotificationAttemptQueue() {
		
	}
	
	public static FailedNotificationAttemptQueue getInstance() {
		if (queue == null) {
			queue = new FailedNotificationAttemptQueue();
		}
		
		return queue;
	}
	
	/**
	 * Establishes a connection to the mongo server
	 * 
	 * @return a valid connected client connection
	 * @throws UnknownHostException if unable to locate the mongo server
	 */
	public MongoClient getMongoClient() throws UnknownHostException
    {
		if (mongov4Client == null )
    	{
			ClassModel<JsonNode> valueModel = ClassModel.builder(JsonNode.class).build();
			ClassModel<NotificationAttemptResponse> metadataPermissionModel = ClassModel.builder(NotificationAttemptResponse.class).build();
			PojoCodecProvider pojoCodecProvider = PojoCodecProvider.builder().register(valueModel, metadataPermissionModel).build();
			CodecRegistry registry = CodecRegistries.fromCodecs(new NotificationAttemptCodec());
			CodecRegistry pojoCodecRegistry = fromRegistries(MongoClientSettings.getDefaultCodecRegistry(),
					fromProviders(pojoCodecProvider),
					registry);

			mongov4Client = MongoClients.create(MongoClientSettings.builder()
					.applyToClusterSettings(builder -> builder.hosts(Collections.singletonList(
							new ServerAddress(Settings.FAILED_NOTIFICATION_DB_HOST, Settings.FAILED_NOTIFICATION_DB_PORT))))
					.codecRegistry(pojoCodecRegistry)
					.credential(getMongoCredential())

					.build());
    	}

        return mongov4Client;
    }
    
    /**
	 * Creates a new MongoDB credential for the database collections
	 *
	 * @return valid mongo credential for this instance
	 */
	private MongoCredential getMongoCredential() {
		return MongoCredential.createScramSha1Credential(
                Settings.FAILED_NOTIFICATION_DB_USER, Settings.FAILED_NOTIFICATION_DB_SCHEME, Settings.FAILED_NOTIFICATION_DB_PWD.toCharArray());
    }
    
    /**
     * Fetches or creates a MongoDB capped collection with the given name, 
     * bound in byte size by {@link Settings#FAILED_NOTIFICATION_COLLECTION_SIZE} and
     * bound in length by {@link Settings#FAILED_NOTIFICATION_COLLECTION_SIZE}.
     * 
     * @param collectionName  name of the capped collection to return
     * @return the named collection, converted to capped collection if needed
     * @throws NotificationException if unable to fetch the collection
     */
    private MongoCollection<NotificationAttempt> getOrCreateCappedCollection(String collectionName)
    throws NotificationException 
    {	
    	// Set up MongoDB connection
		MongoDatabase db;
        try
        {
			db = getMongoClient().getDatabase(Settings.FAILED_NOTIFICATION_DB_SCHEME);
        	
            // Gets a collection, if it does not exist creates it
			MongoCollection<NotificationAttempt> cappedCollection;
        	
        	try {
				CreateCollectionOptions options = new CreateCollectionOptions()
						.capped(true)
						.sizeInBytes(Settings.FAILED_NOTIFICATION_COLLECTION_SIZE)
						.maxDocuments(Settings.FAILED_NOTIFICATION_COLLECTION_LIMIT);


                db.createCollection(collectionName, options);

                cappedCollection = db.getCollection(collectionName, NotificationAttempt.class);
                Document createdIndex = new Document(Map.of("created", 1));
                // bound the max attempt lifetime at 30 days
                cappedCollection.createIndex(createdIndex, new IndexOptions().expireAfter(30L, TimeUnit.DAYS));
        	}
        	catch (Throwable e) {
        		Document collStats = db.runCommand(new Document("collStats", collectionName));
				if (!collStats.getBoolean("capped", false)) {
					try {
						Document command = new Document("convertToCapped", collectionName);
						command.append("size", Settings.FAILED_NOTIFICATION_COLLECTION_SIZE);
						command.append("max", Settings.FAILED_NOTIFICATION_COLLECTION_LIMIT);
						db.runCommand(command);
					} catch (Throwable t) {
						log.error("Failed to convert standard collection " +
								collectionName + " into capped collection", t);
					}
				}

				// get the modified collection and check for the TTL index
				cappedCollection = db.getCollection(collectionName, NotificationAttempt.class);

				AtomicBoolean foundMatch = new AtomicBoolean(false);
				cappedCollection.listIndexes().forEach((Consumer<? super Document>) document -> {
					if (document.containsKey("created") && document.get("created").equals(1)) {
						foundMatch.set(true);
					}
				});
				// if the index is missing, add it.
				if (!foundMatch.get()) {
					Document createdIndex = new Document(Map.of("created", 1));
					cappedCollection.createIndex(createdIndex, new IndexOptions().expireAfter(30L, TimeUnit.DAYS));
				}
        	}
        	
        	return cappedCollection;
        }
        catch (Exception e) {
        	throw new NotificationException("Failed to get capped collection " + collectionName, e);
        }
    }
    
	/**
	 * Pushes a {@link NotificationAttempt} into a MongoDB capped collection
	 * named after the associationed {@link Notification} uuid.
	 * 
	 * @param attempt the attempt to add to the collection
	 * @throws NotificationException if unable to persist attempt
	 */
	public void push(NotificationAttempt attempt) 
	throws NotificationException
	{
		String json = null;
		try {
			MongoCollection<NotificationAttempt> cappedCollection = getOrCreateCappedCollection(attempt.getNotificationId());

			cappedCollection.insertOne(attempt);
		}
//		catch (JsonProcessingException e) {
//			log.error("Failed to serialize attempt " + attempt.getUuid() +
//					" prior to sending to the failed queue for "  + attempt.getNotificationId(), e);
//		}
		catch (MongoException e) {
			log.error("Failed to push failed notification attempt for " + attempt.getUuid() + 
					" on to the failed queue for "  + attempt.getNotificationId(), e);
		}
		catch (Exception e) {
			log.error("Unexpected server error while writing attempt " + attempt.getUuid() + 
					" on to the failed queue for "  + attempt.getNotificationId(), e);
		}
		finally {
			try {
				NotificationManager.process(attempt.getNotificationId(), "FAILURE", attempt.getOwner(), json);
			} 
			catch (Exception e) {
				log.error("Failed to raise failed notification event for " + attempt.getNotificationId() + 
						" after attempt " + attempt.getUuid() + " failed.", e);
			}
		}
	}
	
	/**
	 * Deletes a single {@link NotificationAttempt} from the queue.
	 * 
	 * @param notificationId notification uuid
	 * @param attemptId the attempt uuid
	 * @return the {@link NotificationAttempt} at the top of the queue or null if the queue is empty.
	 * @throws NotificationException if unable to remove attempt
	 */
	public void remove(String notificationId, String attemptId)
	throws NotificationException
	{
		try {
			MongoCollection<NotificationAttempt> cappedCollection = getOrCreateCappedCollection(notificationId);

			Document query = new Document(Map.of("id", attemptId));

			cappedCollection.deleteOne(query);
		}
		catch (MongoException e) {
			log.error("Failed to fetch last failed notification attempt for " + notificationId, e);
		}
		catch (Exception e) {
			log.error("Unexpected server error while fetching last failed notification attempt for " + 
					notificationId, e);
		}
	}
	
	/**
	 * Returns the next entry on the queue. This will be the oldest item,
	 * or the "top" of the queue.
	 * 
	 * @param notificationId notification uuid
	 * @return the {@link NotificationAttempt} at the top of the queue or null if the queue is empty.
	 * @throws NotificationException if unable to fetch attempt
	 */
	public NotificationAttempt next(String notificationId) 
	throws NotificationException
	{
		NotificationAttempt attempt = null;
		try {
			MongoCollection<NotificationAttempt> cappedCollection = getOrCreateCappedCollection(notificationId);

			// capped collections guarantee results return in the insertion order
			attempt = cappedCollection.find().first();
		}
		catch (MongoException e) {
			log.error("Failed to fetch last failed notification attempt for " + notificationId, e);
		}
		catch (Exception e) {
			log.error("Unexpected server error while fetching last failed notification attempt for " + 
					notificationId, e);
		}
		
		return attempt;
	}
	
	/**
	 * Removes all {@link NotificationAttempt} from a {@link Notification} attempt queue.
	 * 
	 * @param notificationId notification uuid
	 * @return the {@link NotificationAttempt} at the top of the queue or null if the queue is empty.
	 * @throws NotificationException if unable to clear all attempts for the notification
	 */
	public void removeAll(String notificationId)
	throws NotificationException
	{
		try {
			getMongoClient().getDatabase(Settings.FAILED_NOTIFICATION_DB_SCHEME)
					.getCollection(notificationId)
					.drop();
		}
		catch (MongoException e) {
			log.error("Failed to clear attempt history for notification " + notificationId, e);
		}
		catch (Exception e) {
			log.error("Unexpected server error while clearing attempt history for notification " +
					notificationId, e);
		}
	}
	
	/**
	 * Finds matching 
	 * 
	 * @param notificationId notification uuid
	 * @return the {@link NotificationAttempt} at the top of the queue or null if the queue is empty.
	 * @throws NotificationException if unable to fetch attempts
	 */
	public List<NotificationAttempt> findMatching(String notificationId, Map<SearchTerm, Object> searchCriteria, int limit, int offset)  
	throws NotificationException
	{
		List<NotificationAttempt> attempts = new ArrayList<>();
		try {
			SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.ENGLISH);
			SimpleDateFormat beforeFormatter = new SimpleDateFormat("yyyy-MM-dd'T'00:00:00'Z'", Locale.ENGLISH);
			SimpleDateFormat afterFormatter = new SimpleDateFormat("yyyy-MM-dd'T'23:59:59'Z'", Locale.ENGLISH);
			MongoCollection<NotificationAttempt> cappedCollection = getOrCreateCappedCollection(notificationId);

			List<Bson> filterList = new ArrayList<Bson>();
			filterList.add(eq("notificationId", notificationId));

			for (SearchTerm searchTerm: searchCriteria.keySet()) {
				if (searchTerm.getOperator() == SearchTerm.Operator.EQ) {
					filterList.add(eq(searchTerm.getSearchField(), searchCriteria.get(searchTerm)));
				} 
				else if (searchTerm.getOperator() == SearchTerm.Operator.NEQ) {
					filterList.add(ne(searchTerm.getSearchField(), searchCriteria.get(searchTerm)));
				} 
				else if (searchTerm.getOperator() == SearchTerm.Operator.GT) {
					filterList.add(gt(searchTerm.getSearchField(), searchCriteria.get(searchTerm)));
				} 
				else if (searchTerm.getOperator() == SearchTerm.Operator.GTE) {
					filterList.add(gte(searchTerm.getSearchField(), searchCriteria.get(searchTerm)));
				} 
				else if (searchTerm.getOperator() == SearchTerm.Operator.LT) {
					filterList.add(lt(searchTerm.getSearchField(), searchCriteria.get(searchTerm)));
				} 
				else if (searchTerm.getOperator() == SearchTerm.Operator.LTE) {
					filterList.add(lte(searchTerm.getSearchField(), searchCriteria.get(searchTerm)));
				}
				else if (searchTerm.getOperator() == SearchTerm.Operator.IN) {
					filterList.add(in(searchTerm.getSearchField(), (List<Object>)searchCriteria.get(searchTerm)));
				}
				else if (searchTerm.getOperator() == SearchTerm.Operator.ON) {
					filterList.add(or(
							gte(searchTerm.getSearchField(), afterFormatter.format((Date)searchCriteria.get(searchTerm))),
							lte(searchTerm.getSearchField(), beforeFormatter.format((Date)searchCriteria.get(searchTerm)))));
				}
				else if (searchTerm.getOperator() == SearchTerm.Operator.AFTER) {
					filterList.add(gt(searchTerm.getSearchField(), formatter.format((Date)searchCriteria.get(searchTerm))));
				}
				else if (searchTerm.getOperator() == SearchTerm.Operator.BEFORE) {
					filterList.add(gt(searchTerm.getSearchField(), formatter.format((Date)searchCriteria.get(searchTerm))));
				}
			}
			// now make the query and get the range result set
			MongoCursor<NotificationAttempt> cursor = cappedCollection
					.find(and(filterList))
					.skip(offset)
					.limit(limit).cursor();

			// convert to a list and deserialized objects and return
			List<NotificationAttempt> resultList = new ArrayList<>();
			while (cursor.hasNext()) {
				resultList.add(cursor.next());
			}
			return resultList;
		}
		catch (MongoException e) {
			log.error("Failed to fetch last failed notification attempt for " + notificationId, e);
		}
		catch (Exception e) {
			log.error("Unexpected server error while fetching last failed notification attempt for " + 
					notificationId, e);
		}
		
		return attempts;
	}

}
