package org.agaveplatform.service.transfers.model;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

public class AgaveResourceSerializer extends JsonSerializer<TransferTask> {

    @Override
    public void serialize(
            TransferTask value, JsonGenerator jgen, SerializerProvider provider)
            throws IOException, JsonProcessingException {
        jgen.writeStartObject();
        jgen.writeStringField("id", value.getUuid());
        jgen.writeNumberField("attempts", value.getAttempts());
        jgen.writeNumberField("bytesTransferred", value.getBytesTransferred());
        jgen.writeStringField("created", value.getCreated() == null ? null : value.getCreated().toString());
        jgen.writeStringField("dest", value.getDest());
        jgen.writeStringField("endTime", value.getEndTime() == null ? null : value.getEndTime().toString());
        jgen.writeStringField("lastUpdated", value.getLastUpdated() == null ? null : value.getLastUpdated().toString());
        jgen.writeStringField("owner", value.getOwner());
        jgen.writeStringField("parentTask", value.getParentTaskId());
        jgen.writeStringField("rootTask", value.getRootTaskId());
        jgen.writeStringField("source", value.getSource());
        jgen.writeStringField("startTime", value.getEndTime() == null ? null : value.getEndTime().toString());
        jgen.writeStringField("status", value.getStatus().name());
        jgen.writeNumberField("totalFiles", value.getTotalFiles());
        jgen.writeNumberField("totalSize", value.getTotalSize());
        jgen.writeNumberField("totalSkippedFiles", value.getTotalSkippedFiles());
        jgen.writeEndObject();
    }
}
