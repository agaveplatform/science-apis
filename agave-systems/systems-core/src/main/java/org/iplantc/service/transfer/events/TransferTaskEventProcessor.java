package org.iplantc.service.transfer.events;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.log4j.Logger;
import org.iplantc.service.common.exceptions.EntityEventPersistenceException;
import org.iplantc.service.common.exceptions.EntityEventProcessingException;
import org.iplantc.service.notification.managers.NotificationManager;
import org.iplantc.service.systems.model.RemoteSystem;
import org.iplantc.service.transfer.dao.TransferTaskEventDao;
import org.iplantc.service.transfer.model.TransferTask;
import org.iplantc.service.transfer.model.TransferTaskEvent;
import org.iplantc.service.transfer.model.TransferTaskImpl;

/**
 * Handles sending and propagation of events on {@link TransferTask} objects.
 * 
 * TODO: Make this class the default mechanism for adding events
 * TODO: Refactor this as an async factory using vert.x
 *
 * @author dooley
 *
 */
public class TransferTaskEventProcessor {
	private static final Logger log = Logger.getLogger(TransferTaskEventProcessor.class);
	private final ObjectMapper mapper = new ObjectMapper();
	
	public TransferTaskEventProcessor(){}
	
	/**
	 * Generates notification events for {@link TransferTaskImpl}.
	 * 
	 * @param transferTask the {@link TransferTaskImpl} on which the even is triggered
	 * @param event the transfer event to process
	 * @return the {@link TransferTaskEvent} with the association to the {@link TransferTaskImpl}
	 */
	public void processTransferTaskEvent(TransferTaskImpl transferTask, TransferTaskEvent event) {

		
		try {
			if (transferTask == null) {
				throw new EntityEventProcessingException("Valid transfer task must be provided to process event.");
			}
			
			event.setEntity(transferTask.getUuid());
			
			String sJson = null;
			try {
				ObjectNode json = mapper.createObjectNode();
			    json.set("transferTask", mapper.readTree(transferTask.toJSON()));
//			    ArrayNode systems = mapper.createArrayNode()
//			    		.add(mapper.readTree(sourceSystem.toJSON()))
//			    		.add(mapper.readTree(destSystem.toJSON()));
//			    json.set("system", systems);
			    sJson = json.toString();
			} catch (Throwable e) {
			    log.error(String.format("Failed to serialize transfer "
			            + "%s to json for %s event notification", 
			            transferTask.getUuid(), event.getStatus()), e);
			}
			
			// fire event on transfer
			processNotification(transferTask.getUuid(), event.getStatus(), event.getCreatedBy(), sJson);

			// We fire delegated transfer chec events on the system being transfered.
//			processNotification(sourceSystem.getUuid(), "TRANSFER_" + event.getStatus(), event.getCreatedBy(), sJson);
//			processNotification(destSystem.getUuid(), "TRANSFER_" + event.getStatus(), event.getCreatedBy(), sJson);
//			
			try {
				new TransferTaskEventDao().persist(event);
			}
			catch (EntityEventPersistenceException e) {
				log.error(String.format("Failed to persist %s transfer task event "
			            + "to history for %s", 
			            event.getStatus(), transferTask.getUuid()), e);
			}
		}
		catch (EntityEventProcessingException e) {
			log.error(e.getMessage());
		}
	}
	
	/**
	 * Publishes a {@link TransferEvent} event to the
	 * {@link RemoteSystem} on which the {@link ScheduledTransfer} resides.
	 * 
	 * @param associatedUuid the uuid of the subject of the event
	 * @param event the event to process
	 * @param createdBy the user who created the event
	 * @param sJson the serialized json representation of the event body
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
}
