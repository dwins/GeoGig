/* Copyright (c) 2011 TOPP - www.openplans.org. All rights reserved.
 * This code is licensed under the LGPL 2.1 license, available at the root
 * application directory.
 */

package org.geogit.rest.repository;

import static org.geogit.rest.repository.GeogitResourceUtils.getGeogit;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.geogit.api.GeoGIT;
import org.geogit.api.ObjectId;
import org.geogit.api.RevCommit;
import org.geogit.repository.Repository;
import org.geogit.web.api.commands.PushManager;
import org.restlet.Context;
import org.restlet.data.ClientInfo;
import org.restlet.data.Form;
import org.restlet.data.MediaType;
import org.restlet.data.Request;
import org.restlet.data.Response;
import org.restlet.resource.Representation;
import org.restlet.resource.Resource;
import org.restlet.resource.Variant;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.google.gson.stream.JsonWriter;

public class ObjectExistsResource extends Resource {

    @Override
    public boolean allowPost() {
        return true;
    }
    
    @Override
    public void post(Representation entity) {
        getResponse().setEntity(new ObjectExistsRepresentation());
    }

    private class ObjectExistsRepresentation extends WriterRepresentation {
        public ObjectExistsRepresentation() {
            super(MediaType.TEXT_PLAIN);
        }

        @Override
        public void write(Writer w) throws IOException {
            final InputStream inStream = getRequest().getEntity().getStream();
            final Reader body = new InputStreamReader(inStream);
            final JsonParser parser = new JsonParser();
            final JsonElement messageJson = parser.parse(body);
            
            final List<ObjectId> want = new ArrayList<ObjectId>();
            final List<ObjectId> have = new ArrayList<ObjectId>();
            
            if (messageJson.isJsonObject()) {
                final JsonObject message = (JsonObject) messageJson;
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
            
            JsonWriter jsonWriter = new JsonWriter(w);
            Gson gson = new Gson();
            GeoGIT ggit = getGeogit(getRequest()).get();
            List<ObjectId> queue = new ArrayList<ObjectId>(want);
            Set<ObjectId> sent = new HashSet<ObjectId>(have);
            int sendLimit = 1000;
            
            jsonWriter.beginObject();
            jsonWriter.name("history");
            jsonWriter.beginArray();
            
            while (!queue.isEmpty() && sendLimit > 0) {
                System.out.println(queue);
                ObjectId commitId = queue.remove(0);
                RevCommit commit = ggit.getRepository().getCommit(commitId);
                List<ObjectId> parentIds = commit.getParentIds();
                for (ObjectId p : parentIds) {
                    if (!sent.contains(p)) {
                        queue.add(p);
                    }
                }
                JsonObject entry = new JsonObject();
                entry.addProperty("id", commitId.toString());
                JsonArray parentList = new JsonArray();
                for (ObjectId p : parentIds) {
                    parentList.add(new JsonPrimitive(p.toString()));
                }
                entry.add("parents", parentList);
                gson.toJson(entry, jsonWriter);
                
                sent.add(commitId);
                sendLimit--;
            }
            jsonWriter.endArray();
            jsonWriter.name("missing");
            jsonWriter.beginArray();
            for (ObjectId h : have) {
                if (!ggit.getRepository().blobExists(h)) {
                    jsonWriter.value(h.toString());
                }
            }
            jsonWriter.endArray();
            jsonWriter.endObject();
            jsonWriter.flush();
        }
    }
}
