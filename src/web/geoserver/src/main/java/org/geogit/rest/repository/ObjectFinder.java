/* Copyright (c) 2011 TOPP - www.openplans.org. All rights reserved.
 * This code is licensed under the LGPL 2.1 license, available at the root
 * application directory.
 */

package org.geogit.rest.repository;

import static org.geogit.rest.repository.GeogitResourceUtils.getGeogit;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

import org.geogit.api.GeoGIT;
import org.geogit.api.ObjectId;
import org.geogit.remote.BinaryPackedObjects;
import org.geogit.repository.Repository;
import org.restlet.Context;
import org.restlet.Finder;
import org.restlet.data.MediaType;
import org.restlet.data.Request;
import org.restlet.data.Response;
import org.restlet.resource.OutputRepresentation;
import org.restlet.resource.Representation;
import org.restlet.resource.Resource;

import com.google.common.base.Throwables;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * Expects an {@code id} request attribute containing the string representation of an
 * {@link ObjectId} (40 char hex string) to look up for in the repository database and return it as
 * a plain byte stream.
 */
public class ObjectFinder extends Finder {
    
    @Override
    public Resource findTarget(Request request, Response response) {
        return new ObjectResource(getContext(), request, response);
    }

    private static class ObjectResource extends Resource {
        public ObjectResource(//
                Context context, //
                Request request, //
                Response response) //
        {
            super(context, request, response);
        }

        @Override
        public boolean allowPost() {
            return true;
        }
        
        @Override
        public void post(Representation entity) {
            final InputStream inStream;
            try {
                inStream = entity.getStream();
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
            
            final Reader body = new InputStreamReader(inStream);
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
            
            final GeoGIT ggit = getGeogit(getRequest()).get();
            final Repository repository = ggit.getRepository();
            
            BinaryPackedObjects packer = new BinaryPackedObjects(repository.getIndex().getDatabase());
            getResponse().setEntity(new RevObjectBinaryRepresentation(packer, want, have));
        }
    }

    private static class RevObjectBinaryRepresentation extends OutputRepresentation {
        private final BinaryPackedObjects packer;
        private final List<ObjectId> want;
        private final List<ObjectId> have;
        
        public RevObjectBinaryRepresentation( //
                BinaryPackedObjects packer, //
                List<ObjectId> want, //
                List<ObjectId> have) // 
        {
            super(MediaType.APPLICATION_OCTET_STREAM);
            this.packer = packer;
            this.want = want;
            this.have = have;
        }

        @Override
        public void write(OutputStream out) throws IOException {
            packer.write(out, want, have);
        }
    }

}
