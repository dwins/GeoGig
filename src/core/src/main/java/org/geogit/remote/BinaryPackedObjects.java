package org.geogit.remote;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import org.geogit.api.Bucket;
import org.geogit.api.Node;
import org.geogit.api.ObjectId;
import org.geogit.api.RevCommit;
import org.geogit.api.RevFeature;
import org.geogit.api.RevFeatureType;
import org.geogit.api.RevObject;
import org.geogit.api.RevTree;
import org.geogit.repository.Repository;
import org.geogit.storage.ObjectReader;
import org.geogit.storage.ObjectSerializingFactory;
import org.geogit.storage.ObjectWriter;
import org.geogit.storage.datastream.DataStreamSerializationFactory;

import com.google.common.base.Throwables;

public final class BinaryPackedObjects {
    private final ObjectWriter<RevCommit> commitWriter;
    private final ObjectWriter<RevTree> treeWriter;
    private final ObjectWriter<RevFeatureType> featureTypeWriter;
    private final ObjectWriter<RevFeature> featureWriter;
    private final ObjectReader<RevObject> objectReader;
    
    private final int CAP = 10000;
    private final Repository repository;
    
    public BinaryPackedObjects(Repository repository) {
        this.repository = repository;
        final ObjectSerializingFactory factory = new DataStreamSerializationFactory();
        this.commitWriter = factory.createObjectWriter(RevObject.TYPE.COMMIT);
        this.treeWriter = factory.createObjectWriter(RevObject.TYPE.TREE);
        this.featureTypeWriter = factory.createObjectWriter(RevObject.TYPE.FEATURETYPE);
        this.featureWriter = factory.createObjectWriter(RevObject.TYPE.FEATURE);
        this.objectReader = factory.createObjectReader();
    }

    public void write(OutputStream out, List<ObjectId> want, List<ObjectId> have) throws IOException {
        write(out, want, have, new HashSet<ObjectId>(), DEFAULT_CALLBACK);
    }

    public <T> T write(OutputStream out, List<ObjectId> want, List<ObjectId> have, Set<ObjectId> sent, Callback<T> callback) throws IOException {
        T state = null;
        for (ObjectId i : want) {
            if (! repository.blobExists(i)) { 
                throw new NoSuchElementException("Wanted id: " + i + " is not known");
            }
        }
        List<ObjectId> toSend = new ArrayList<ObjectId>(CAP);
        List<ObjectId> front = new ArrayList<ObjectId>(want);
        Set<ObjectId> visited = new HashSet<ObjectId>(have);
        while (! front.isEmpty()) {
            final ObjectId curr = front.remove(0);
            if (!visited.contains(curr)) {
                visited.add(curr);
                insertWithCap(toSend, curr);
                final RevCommit commit = repository.getCommit(curr);
                for (ObjectId parent : commit.getParentIds()) {
                    front.add(parent);
                }
            }
        }
        Collections.reverse(toSend);
        for (ObjectId id : have) {
            previsit(id, sent);
        }
        for (ObjectId id : toSend) {
            state = send(id, sent, callback, state, out);
        }
        
        return null;
    }
    
    private void insertWithCap(List<ObjectId> accum, ObjectId newId) {
        accum.add(newId);
        while (accum.size() > CAP) {
            accum.remove(0);
        }
    }

    private <T> T send(ObjectId id, Set<ObjectId> sent, Callback<T> callback, T state, OutputStream out) throws IOException {
        List<ObjectId> toInspect = new ArrayList<ObjectId>();
        List<ObjectId> toVisit = new ArrayList<ObjectId>();
        toInspect.add(id);
        while (true) {
            if (!toVisit.isEmpty()) {
                ObjectId here = toVisit.remove(0);
                if (sent.add(here)) { // add returns TRUE if the added element is new
                    out.write(here.getRawValue());
                    RevObject revObj = repository.getObjectDatabase().get(here);
                    if (revObj instanceof RevCommit) {
                        commitWriter.write((RevCommit)revObj, out);
                    } else if (revObj instanceof RevTree) {
                        treeWriter.write((RevTree)revObj, out);
                    } else if (revObj instanceof RevFeature) {
                        featureWriter.write((RevFeature) revObj, out);
                    } else if (revObj instanceof RevFeatureType) {
                        featureTypeWriter.write((RevFeatureType) revObj, out);
                    }
                    state = callback.callback(revObj, state);
                }
            } else if (!toInspect.isEmpty()) {
                ObjectId here = toInspect.remove(0);
                if (sent.contains(here)) continue;
                RevObject revObject = repository.getObjectDatabase().get(here);
                if (revObject instanceof RevCommit) {
                    RevCommit commit = (RevCommit) revObject;
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
        return state;
    }

    private void previsit(ObjectId id, Set<ObjectId> sent) throws IOException {
        List<ObjectId> toInspect = new ArrayList<ObjectId>();
        toInspect.add(id);
        while (!toInspect.isEmpty()) {
            ObjectId here = toInspect.remove(0);
            if (sent.contains(here)) continue;
            RevObject revObject = repository.getObjectDatabase().getIfPresent(here);
            if (revObject == null) continue;
            if (revObject instanceof RevCommit) {
                RevCommit commit = (RevCommit) revObject;
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
    
    public void ingest(final InputStream in) {
        ingest(in, DEFAULT_CALLBACK);
    }
    
    public <T> T ingest(final InputStream in, Callback<T> callback) {
        T state = null;
        while (true) {
            try {
                state = ingestOne(in, callback, state);
            } catch (EOFException e) {
                break;
            } catch (IOException e) {
                Throwables.propagate(e);
            }
        }
        return state;
    }
    
    private <T> T ingestOne(final InputStream in, Callback<T> callback, T state) throws IOException {
        ObjectId id = readObjectId(in);
        RevObject revObj = objectReader.read(id, in);
        final T result;
        if (!repository.getObjectDatabase().exists(id)) {
            result = callback.callback(revObj, state);
            repository.getObjectDatabase().put(revObj);
        } else {
            result = state;
        }
        return result;
    }
    
    private ObjectId readObjectId(final InputStream in) throws IOException {
        byte[] rawBytes = new byte[20];
        int amount = 0;
        int len = 20;
        int offset = 0;
        while ((amount = in.read(rawBytes, offset, len - offset)) != 0) {
            if (amount < 0) throw new EOFException("Came to end of input");
            offset += amount;
            if (offset == len) break;
        }
        ObjectId id = new ObjectId(rawBytes);
        return id;
    }
    
    public static interface Callback<T> {
        public abstract T callback(RevObject object, T state);
    }
    
    private static final Callback<Void> DEFAULT_CALLBACK = new Callback<Void>() {
        @Override
        public Void callback(RevObject object, Void state) {
            return null;
        }
    };
}