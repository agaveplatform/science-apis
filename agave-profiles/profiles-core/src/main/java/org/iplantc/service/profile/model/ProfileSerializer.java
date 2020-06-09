package org.iplantc.service.profile.model;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.iplantc.service.profile.exceptions.ProfileException;

import java.io.IOException;

@JsonSerialize(using = ProfileSerializer.class)
public class ProfileSerializer extends JsonSerializer<Profile> {

    @Override
    public void serialize(Profile profile, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException {
        try {
            jsonGenerator.writeRaw(profile.toJSON());
        } catch (ProfileException e) {
            throw new IOException(e);
        }
    }
}