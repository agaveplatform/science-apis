package org.agaveplatform.service.transfers.enumerations;

import io.vertx.core.json.JsonObject;
import org.agaveplatform.service.transfers.model.TransferTask;

@Deprecated
public enum EventChannelTyp2e {
	TRANSFER_COMPLETED("foo", "Some meaningful templated message that we can use to describe the fact that task %uuid% completed data movement from %source% to %dest% at %lastModified%");

	public String getEventChannel() {
		return eventChannel;
	}
	public String getDescription() {
		return description;
	}

	public String getFormattedDescription(TransferTask transferTask) {
		return getFormattedDescription(transferTask.toJson());
	}

	public String getFormattedDescription(JsonObject json) {
		String formattedDescription = getDescription();
		for (String fieldName : json.fieldNames()) {
			String token = "%" + fieldName + "%";
			if (formattedDescription.contains(token)) {
				formattedDescription = formattedDescription.replaceAll(token, String.valueOf(json.getValue(fieldName)));
			}
		}

		return formattedDescription;
	}


	String description;
	String eventChannel;

	EventChannelTyp2e(String eventChannel, String description) {
		this.eventChannel = eventChannel;
		this.description = description;
	}
}
