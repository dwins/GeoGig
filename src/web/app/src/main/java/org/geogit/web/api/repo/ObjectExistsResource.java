package org.geogit.web.api.repo;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.geogit.api.GeoGIT;
import org.geogit.api.ObjectId;
import org.geogit.api.RevCommit;
import org.restlet.data.MediaType;
import org.restlet.representation.Representation;
import org.restlet.representation.WriterRepresentation;
import org.restlet.resource.ResourceException;
import org.restlet.resource.ServerResource;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.google.gson.stream.JsonWriter;

public class ObjectExistsResource extends ServerResource {
    @Override
    protected Representation post(Representation entity) throws ResourceException {
        return new ObjectExistsRepresentation();
    }
    
    private class ObjectExistsRepresentation extends WriterRepresentation {
        public ObjectExistsRepresentation() {
            super(MediaType.APPLICATION_JSON);
        }

        @Override
        public void write(Writer out) throws IOException {
            final Reader body = getRequest().getEntity().getReader();
            final JsonParser parser = new JsonParser();
            final JsonElement messageJson = parser.parse(body);
            
            final List<ObjectId> want = new ArrayList<ObjectId>();
            final List<ObjectId> have = new ArrayList<ObjectId>();
            
            if (messageJson.isJsonObject()) {
                final JsonObject message = (JsonObject) messageJson;
                final JsonArray wantArray;
                final JsonArray haveArray;
                if (message.has("want") && message.get("want").isJsonArray()) {
                    wantArray = (JsonArray) message.get("want");
                } else {
                    wantArray = new JsonArray();
                }
                if (message.has("have") && message.get("have").isJsonArray()) {
                    haveArray = (JsonArray) message.get("have");
                } else {
                    haveArray = new JsonArray();
                }
                for (JsonElement e : wantArray) {
                    if (e.isJsonPrimitive()) {
                        want.add(ObjectId.valueOf(e.getAsJsonPrimitive().getAsString()));
                    }
                }
                for (JsonElement e : haveArray) {
                    if (e.isJsonPrimitive()) {
                        have.add(ObjectId.valueOf(e.getAsJsonPrimitive().getAsString()));
                    }
                }
            }
            
            JsonWriter jsonWriter = new JsonWriter(out);
            Gson gson = new Gson();
            GeoGIT ggit = (GeoGIT) getApplication().getContext().getAttributes().get("geogit");
            List<ObjectId> queue = new ArrayList<ObjectId>(want);
            Set<ObjectId> sent = new HashSet<ObjectId>();
            int sendLimit = 1000;
            
            jsonWriter.beginObject();
            jsonWriter.name("have");
            jsonWriter.beginArray();
            
            while (queue.size() > 0 && sendLimit > 0) {
                ObjectId commitId = queue.remove(0);
                RevCommit commit = ggit.getRepository().getCommit(commitId);
                List<ObjectId> parentIds = commit.getParentIds();
                for (ObjectId p : parentIds) {
                    if (! sent.contains(p)) {
                        queue.add(p);
                    }
                }
                JsonObject entry = new JsonObject();
                entry.addProperty("id", formatId(commitId));
                JsonArray parentList = new JsonArray();
                for (ObjectId p : parentIds) {
                    parentList.add(new JsonPrimitive(formatId(p)));
                }
                entry.add("parents", parentList);
                gson.toJson(entry, jsonWriter);
                
                sent.add(commitId);
                sendLimit--;
            }
            jsonWriter.endArray();
            jsonWriter.endObject();
        }
        
        private String formatId(ObjectId id) {
            StringBuilder builder = new StringBuilder();
            for (int n = 0; n < ObjectId.HASH_FUNCTION.bits() / 8; n++) {
                builder.append(String.format("%02x", id.byteN(n)));
            }
            return builder.toString();
        }
    }
}
