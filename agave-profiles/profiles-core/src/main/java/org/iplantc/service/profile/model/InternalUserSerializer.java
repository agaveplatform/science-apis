package org.iplantc.service.profile.model;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.iplantc.service.profile.exceptions.ProfileException;

import java.io.IOException;

class InternalUserSerializer extends JsonSerializer<InternalUser> {

    @Override
    public void serialize(InternalUser internalUser, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException {
        try {
            jsonGenerator.writeRaw(internalUser.toJSON());
        } catch (ProfileException e) {
            throw new IOException(e);
        }
    }
}
