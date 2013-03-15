package org.geogit.web.api.repo;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import org.geogit.api.Bucket;
import org.geogit.api.GeoGIT;
import org.geogit.api.Node;
import org.geogit.api.ObjectId;
import org.geogit.api.RevCommit;
import org.geogit.api.RevFeature;
import org.geogit.api.RevFeatureType;
import org.geogit.api.RevObject;
import org.geogit.api.RevTree;
import org.geogit.repository.Repository;
import org.geogit.storage.ObjectSerializingFactory;
import org.geogit.storage.ObjectWriter;
import org.geogit.storage.datastream.DataStreamSerializationFactory;
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
            return new BinaryPackedObjectsRepresentation(want, have);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    private static final MediaType PACKED_OBJECTS = new MediaType("application/x-geogit-packed");
    
    private class BinaryPackedObjectsRepresentation extends OutputRepresentation {
        private final List<ObjectId> want;
        private final List<ObjectId> have;
        private final ObjectWriter<RevCommit> commitWriter;
        private final ObjectWriter<RevTree> treeWriter;
        private final ObjectWriter<RevFeatureType> featureTypeWriter;
        private final ObjectWriter<RevFeature> featureWriter;
        private final int CAP = 100;
        
        public BinaryPackedObjectsRepresentation(List<ObjectId> want, List<ObjectId> have) {
            super(PACKED_OBJECTS);
            this.want = want;
            this.have = have;
            final ObjectSerializingFactory factory = new DataStreamSerializationFactory();
            this.commitWriter = factory.createObjectWriter(RevObject.TYPE.COMMIT);
            this.treeWriter = factory.createObjectWriter(RevObject.TYPE.TREE);
            this.featureTypeWriter = factory.createObjectWriter(RevObject.TYPE.FEATURETYPE);
            this.featureWriter = factory.createObjectWriter(RevObject.TYPE.FEATURE);
        }

        @Override
        public void write(OutputStream out) throws IOException {
            final GeoGIT ggit = (GeoGIT) getApplication().getContext().getAttributes().get("geogit");
            final Repository repository = ggit.getRepository();
            for (ObjectId i : want) {
                if (! repository.blobExists(i)) { 
                    throw new NoSuchElementException("Wanted id: " + i + " is not known");
                }
            }
            for (ObjectId i : have) {
                if (! repository.blobExists(i)) {
                    throw new NoSuchElementException("Stop-list id: " + i + " is not known");
                }
            }
            List<ObjectId> toSend = new ArrayList<ObjectId>(CAP);
            List<ObjectId> front = new ArrayList<ObjectId>(want);
            Set<ObjectId> visited = new HashSet<ObjectId>(have);
            while (! front.isEmpty()) {
                final ObjectId curr = front.remove(0);
                if (!visited.contains(curr)) {
                    visited.add(curr);
                    insertWithCap(toSend, curr, CAP);
                    final RevCommit commit = repository.getCommit(curr);
                    for (ObjectId parent : commit.getParentIds()) {
                        front.add(parent);
                    }
                }
            }
            Collections.reverse(toSend);
            Set<ObjectId> sent = new HashSet<ObjectId>();
            for (ObjectId id : have) {
                previsit(repository, id, sent);
            }
            for (ObjectId id : toSend) {
                send(repository, id, sent, out);
            }
        }
        
        private void insertWithCap(List<ObjectId> accum, ObjectId newId, int cap) {
            accum.add(newId);
            while (accum.size() > cap) {
                accum.remove(0);
            }
        }

        private void send(final Repository repo, ObjectId id, Set<ObjectId> sent, OutputStream out) throws IOException {
            List<ObjectId> toInspect = new ArrayList<ObjectId>();
            List<ObjectId> toVisit = new ArrayList<ObjectId>();
            toInspect.add(id);
            while (true) {
                if (!toVisit.isEmpty()) {
                    ObjectId here = toVisit.remove(0);
                    if (sent.add(here)) { // add returns TRUE if the added element is new
                        out.write(here.getRawValue());
                        RevObject revObj = repo.getObjectDatabase().get(here);
                        if (revObj instanceof RevCommit) {
                            commitWriter.write((RevCommit)revObj, out);
                        } else if (revObj instanceof RevTree) {
                            treeWriter.write((RevTree)revObj, out);
                        } else if (revObj instanceof RevFeature) {
                            featureWriter.write((RevFeature) revObj, out);
                        } else if (revObj instanceof RevFeatureType) {
                            featureTypeWriter.write((RevFeatureType) revObj, out);
                        }
                    }
                } else if (!toInspect.isEmpty()) {
                    ObjectId here = toInspect.remove(0);
                    if (sent.contains(here)) continue;
                    RevObject revObject = repo.getObjectDatabase().get(here);
                    if (revObject instanceof RevCommit) {
                        RevCommit commit = (RevCommit) revObject;
//                        toInspect.addAll(commit.getParentIds()); // skipping this - we don't want the parents in this use case
                        toInspect.add(commit.getTreeId());
                        toVisit.add(here);
                    } else if (revObject instanceof RevTree) {
                        RevTree tree = (RevTree) revObject;
                        if (tree.features().isPresent()) {
                            for (Node n : tree.features().get()) {
                                if (n.getMetadataId().isPresent()) {
                                    toInspect.add(n.getMetadataId().get());
                                }
                                toInspect.add(n.getObjectId());
                            }
                        }
                        if (tree.trees().isPresent()) {
                            for (Node n : tree.trees().get()) {
                                if (n.getMetadataId().isPresent()) {
                                    toInspect.add(n.getMetadataId().get());
                                }
                                toInspect.add(n.getObjectId());
                            }
                        }
                        if (tree.buckets().isPresent()) {
                            for (Bucket b : tree.buckets().get().values()) {
                                toInspect.add(b.id());
                            }
                        }
                        toVisit.add(here);
                    } else if (revObject instanceof RevFeatureType) {
                        toVisit.add(here);
                    } else if (revObject instanceof RevFeature) {
                        toVisit.add(here);
                    }
                } else {
                    break;
                }
            }
        }

        private void previsit(final Repository repo, ObjectId id, Set<ObjectId> sent) throws IOException {
            List<ObjectId> toInspect = new ArrayList<ObjectId>();
            toInspect.add(id);
            while (!toInspect.isEmpty()) {
                ObjectId here = toInspect.remove(0);
                if (sent.contains(here)) continue;
                RevObject revObject = repo.getObjectDatabase().get(here);
                if (revObject instanceof RevCommit) {
                    RevCommit commit = (RevCommit) revObject;
//                        toInspect.addAll(commit.getParentIds()); // skipping this - we don't want the parents in this use case
                    toInspect.add(commit.getTreeId());
                } else if (revObject instanceof RevTree) {
                    RevTree tree = (RevTree) revObject;
                    if (tree.features().isPresent()) {
                        for (Node n : tree.features().get()) {
                            if (n.getMetadataId().isPresent()) {
                                toInspect.add(n.getMetadataId().get());
                            }
                            toInspect.add(n.getObjectId());
                        }
                    }
                    if (tree.trees().isPresent()) {
                        for (Node n : tree.trees().get()) {
                            if (n.getMetadataId().isPresent()) {
                                toInspect.add(n.getMetadataId().get());
                            }
                            toInspect.add(n.getObjectId());
                        }
                    }
                    if (tree.buckets().isPresent()) {
                        for (Bucket b : tree.buckets().get().values()) {
                            toInspect.add(b.id());
                        }
                    }
                }
                sent.add(here);
            }
        }
    }
}
