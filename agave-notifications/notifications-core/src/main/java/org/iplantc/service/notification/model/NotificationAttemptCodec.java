package org.iplantc.service.notification.model;

import org.apache.commons.lang.StringUtils;
import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.Document;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.EncoderContext;

import java.util.Date;

public class NotificationAttemptCodec implements Codec<NotificationAttempt> {

    private final Codec<Document> documentCodec;

    public NotificationAttemptCodec() {
        this.documentCodec = new DocumentCodec();
    }

    public NotificationAttemptCodec(Codec<Document> codec) {
        this.documentCodec = codec;
    }

    /**
     * Decodes a BSON value from the given reader into an instance of the type parameter {@code T}.
     *
     * @param reader         the BSON reader
     * @param decoderContext the decoder context
     * @return an instance of the type parameter {@code T}.
     */
    @Override
    public NotificationAttempt decode(BsonReader reader, DecoderContext decoderContext) {
        NotificationAttempt notificationAttempt = new NotificationAttempt();
        Document document = documentCodec.decode(reader, decoderContext);

        try {
            Date created = document.getDate("created");
            if (created != null)
                notificationAttempt.setCreated(created.toInstant());
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            Date instant = document.getDate("startedTime");
            if (instant != null)
                notificationAttempt.setStartTime(instant.toInstant());
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            Date instant = document.getDate("endedTime");
            if (instant != null)
                notificationAttempt.setEndTime(instant.toInstant());
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            Date instant = document.getDate("scheduledTime");
            if (instant != null)
                notificationAttempt.setScheduledTime(instant.toInstant());
        } catch (Exception e) {
            e.printStackTrace();
        }

        String associatedUuid = document.getString("associatedUuid");
        notificationAttempt.setAssociatedUuid(associatedUuid);

        int attemptNumber = document.getInteger("attemptNumber");
        notificationAttempt.setAttemptNumber(attemptNumber);


        String content = document.getString("content");
        if (StringUtils.isNotEmpty(content))
            notificationAttempt.setContent(content);

        String event = document.getString("event");
        if (StringUtils.isNotEmpty(event))
            notificationAttempt.setEventName(event);

        String uuid = document.getString("id");
        if (StringUtils.isNotEmpty(uuid))
            notificationAttempt.setUuid(uuid);

        String notificationId = document.getString("notificationId");
        if (StringUtils.isNotEmpty(notificationId))
            notificationAttempt.setNotificationId(notificationId);

        String owner = document.getString("owner");
        if (StringUtils.isNotEmpty(owner)) {
            notificationAttempt.setOwner(owner);
        }

        String tenantId = document.getString("tenantId");
        if (StringUtils.isNotEmpty(tenantId)) {
            notificationAttempt.setTenantId(tenantId);
        }

        String callbackUrl = document.getString("url");
        if (StringUtils.isNotEmpty(callbackUrl)) {
            notificationAttempt.setCallbackUrl(callbackUrl);
        }

        Document documentResponse = document.get("response", Document.class);
        if (documentResponse != null) {
            NotificationAttemptResponse response = new NotificationAttemptResponse();

            String message = documentResponse.getString("message");
            response.setMessage(message);

            int code = documentResponse.getInteger("code");
            response.setCode(code);

            notificationAttempt.setResponse(response);
        }

        return notificationAttempt;
    }

    /**
     * Encode an instance of the type parameter {@code T} into a BSON value.
     *
     * @param writer         the BSON writer to encode into
     * @param value          the value to encode
     * @param encoderContext the encoder context
     */
    @Override
    public void encode(BsonWriter writer, NotificationAttempt value, EncoderContext encoderContext) {
        writer.writeStartDocument();

        if ( value.getAssociatedUuid() == null) {
            writer.writeNull("associatedUuid");
        } else {
            writer.writeString("associatedUuid", value.getAssociatedUuid());
        }

        writer.writeInt32("attemptNumber", value.getAttemptNumber());

        if ( value.getContent() == null) {
            writer.writeNull("content");
        } else {
            writer.writeString("content", value.getContent());
        }

        if ( value.getCreated() == null) {
            writer.writeNull("created");
        } else {
            writer.writeDateTime("created", value.getCreated().toEpochMilli());
        }

        if ( value.getEndTime() == null) {
            writer.writeNull("endTime");
        } else {
            writer.writeDateTime("endTime", value.getEndTime().toEpochMilli());
        }

        if (value.getEventName() == null) {
            writer.writeNull("event");
        } else {
            writer.writeString("event", value.getEventName());
        }

        if (value.getNotificationId() == null) {
            writer.writeNull("notificationId");
        } else {
            writer.writeString("notificationId", value.getNotificationId());
        }

        if (value.getOwner() == null) {
            writer.writeNull("owner");
        } else {
            writer.writeString("owner", value.getOwner());
        }

        if (value.getResponse() == null) {
            writer.writeNull("response");
        } else {
            writer.writeStartDocument("response");
            writer.writeInt32("code", value.getResponse().getCode());

            if ( value.getResponse().getMessage() == null) {
                writer.writeNull("message");
            } else {
                writer.writeString("message", value.getResponse().getMessage());
            }
            writer.writeEndDocument();
        }

        if ( value.getStartTime() == null) {
            writer.writeNull("startTime");
        } else {
            writer.writeDateTime("startTime", value.getStartTime().toEpochMilli());
        }

        if ( value.getTenantId() == null) {
            writer.writeNull("tenantId");
        } else {
            writer.writeString("tenantId", value.getTenantId());
        }

        if ( value.getCallbackUrl() == null) {
            writer.writeNull("url");
        } else {
            writer.writeString("url", value.getCallbackUrl());
        }

        if ( value.getUuid() == null) {
            writer.writeNull("id");
        } else {
            writer.writeString("id", value.getUuid());
        }

        writer.writeEndDocument();
    }

    /**
     * Returns the Class instance that this encodes. This is necessary because Java does not reify generic types.
     *
     * @return the Class instance that this encodes.
     */
    @Override
    public Class<NotificationAttempt> getEncoderClass() {
        return NotificationAttempt.class;
    }
}
