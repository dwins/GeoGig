package org.geogit.remote;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import org.geogit.api.ObjectId;
import org.geogit.api.RevCommit;
import org.geogit.api.RevFeature;
import org.geogit.api.RevFeatureType;
import org.geogit.api.RevObject;
import org.geogit.api.RevTree;
import org.geogit.repository.PostOrderIterator;
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
    
    private final int CAP = 100;
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

        int commitsSent = 0;
        System.out.println("Want: " + want);
        System.out.println("Have: " + have);
        Iterator<RevObject> objects = PostOrderIterator.range(want, have, repository);
        while (objects.hasNext() && commitsSent < CAP) {
            RevObject object = objects.next();

            out.write(object.getId().getRawValue());
            if (object instanceof RevCommit) {
                commitWriter.write((RevCommit)object, out);
                commitsSent ++;
            } else if (object instanceof RevTree) {
                treeWriter.write((RevTree)object, out);
            } else if (object instanceof RevFeature) {
                featureWriter.write((RevFeature) object, out);
            } else if (object instanceof RevFeatureType) {
                featureTypeWriter.write((RevFeatureType) object, out);
            }
            state = callback.callback(object, state);
        }
        
        return state;
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