package org.geogit.web.api.repo;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

import org.geogit.api.GeoGIT;
import org.geogit.api.ObjectId;
import org.geogit.remote.BinaryPackedObjects;
import org.geogit.repository.Repository;
import org.restlet.data.MediaType;
import org.restlet.representation.OutputRepresentation;
import org.restlet.representation.Representation;
import org.restlet.resource.ResourceException;
import org.restlet.resource.ServerResource;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class ObjectResource extends ServerResource {
    @Override
    protected Representation post(Representation entity) throws ResourceException {
        try {
            final Reader body = entity.getReader();
            final JsonParser parser = new JsonParser();
            final JsonElement messageJson = parser.parse(body);
            
            final List<ObjectId> want = new ArrayList<ObjectId>();
            final List<ObjectId> have = new ArrayList<ObjectId>();
            
            if (messageJson.isJsonObject()) {
                final JsonObject message = messageJson.getAsJsonObject();
                final JsonArray wantArray;
                final JsonArray haveArray;
                if (message.has("want") && message.get("want").isJsonArray()) {
                    wantArray = message.get("want").getAsJsonArray();
                } else {
                    wantArray = new JsonArray();
                }
                if (message.has("have") && message.get("have").isJsonArray()) {
                    haveArray = message.get("have").getAsJsonArray();
                } else {
                    haveArray = new JsonArray();
                }
                for (final JsonElement e : wantArray) {
                    if (e.isJsonPrimitive()) {
                        want.add(ObjectId.valueOf(e.getAsJsonPrimitive().getAsString()));
                    }
                }
                for (final JsonElement e : haveArray) {
                    if (e.isJsonPrimitive()) {
                        have.add(ObjectId.valueOf(e.getAsJsonPrimitive().getAsString()));
                    }
                }
            }
           
            final GeoGIT ggit = (GeoGIT) getApplication().getContext().getAttributes().get("geogit");
            final Repository repository = ggit.getRepository();

            return new BinaryPackedObjectsRepresentation(
                    new BinaryPackedObjects(repository), want, have);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    private static final MediaType PACKED_OBJECTS = new MediaType("application/x-geogit-packed");
    
    private class BinaryPackedObjectsRepresentation extends OutputRepresentation {
        private final BinaryPackedObjects packer;
        private final List<ObjectId> want;
        private final List<ObjectId> have;
        
        public BinaryPackedObjectsRepresentation(BinaryPackedObjects packer,
                List<ObjectId> want,
                List<ObjectId> have) {
            super(PACKED_OBJECTS);
            this.want = want;
            this.have = have;
            this.packer = packer;
        }
        
        @Override 
        public void write(OutputStream out) throws IOException {
            packer.write(out, want, have);
        }
    }
}
