/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.geogit.storage.datastream.v2;

import static org.geogit.storage.datastream.v2.FormatCommonV2.readCommit;
import static org.geogit.storage.datastream.v2.FormatCommonV2.readFeature;
import static org.geogit.storage.datastream.v2.FormatCommonV2.readFeatureType;
import static org.geogit.storage.datastream.v2.FormatCommonV2.readHeader;
import static org.geogit.storage.datastream.v2.FormatCommonV2.readTag;
import static org.geogit.storage.datastream.v2.FormatCommonV2.readTree;
import static org.geogit.storage.datastream.v2.FormatCommonV2.requireHeader;
import static org.geogit.storage.datastream.v2.FormatCommonV2.writeCommit;
import static org.geogit.storage.datastream.v2.FormatCommonV2.writeFeature;
import static org.geogit.storage.datastream.v2.FormatCommonV2.writeFeatureType;
import static org.geogit.storage.datastream.v2.FormatCommonV2.writeHeader;
import static org.geogit.storage.datastream.v2.FormatCommonV2.writeTag;
import static org.geogit.storage.datastream.v2.FormatCommonV2.writeTree;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Map;

import org.geogit.api.ObjectId;
import org.geogit.api.RevCommit;
import org.geogit.api.RevFeature;
import org.geogit.api.RevFeatureType;
import org.geogit.api.RevObject;
import org.geogit.api.RevObject.TYPE;
import org.geogit.api.RevTag;
import org.geogit.api.RevTree;
import org.geogit.storage.ObjectReader;
import org.geogit.storage.ObjectSerializingFactory;
import org.geogit.storage.ObjectWriter;

import com.google.common.base.Throwables;

/**
 * Serialization factory for serial version 2
 */
public class DataStreamSerializationFactoryV2 implements ObjectSerializingFactory {

    private final static ObjectReader<RevCommit> COMMIT_READER = new CommitReaderV2();

    private final static ObjectWriter<RevCommit> COMMIT_WRITER = new CommitWriterV2();

    private final static ObjectReader<RevTree> TREE_READER = new TreeReaderV2();

    private final static ObjectWriter<RevTree> TREE_WRITER = new TreeWriterV2();

    private final static ObjectReader<RevFeature> FEATURE_READER = new FeatureReaderV2();

    private final static ObjectWriter<RevFeature> FEATURE_WRITER = new FeatureWriterV2();

    private final static ObjectReader<RevFeatureType> FEATURETYPE_READER = new FeatureTypeReaderV2();

    private final static ObjectReader<RevTag> TAG_READER = new TagReaderV2();

    private final static ObjectWriter<RevFeatureType> FEATURETYPE_WRITER = new FeatureTypeWriterV2();

    private final static ObjectWriter<RevTag> TAG_WRITER = new TagWriterV2();

    private final static ObjectReader<RevObject> OBJECT_READER = new ObjectReaderV2();

    public static final DataStreamSerializationFactoryV2 INSTANCE = new DataStreamSerializationFactoryV2();

    @Override
    public ObjectReader<RevCommit> createCommitReader() {
        return COMMIT_READER;
    }

    @Override
    public ObjectReader<RevTree> createRevTreeReader() {
        return TREE_READER;
    }

    @Override
    public ObjectReader<RevFeature> createFeatureReader() {
        return FEATURE_READER;
    }

    @Override
    public ObjectReader<RevFeature> createFeatureReader(Map<String, Serializable> hints) {
        return FEATURE_READER;
    }

    @Override
    public ObjectReader<RevFeatureType> createFeatureTypeReader() {
        return FEATURETYPE_READER;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends RevObject> ObjectWriter<T> createObjectWriter(TYPE type) {
        switch (type) {
        case COMMIT:
            return (ObjectWriter<T>) COMMIT_WRITER;
        case TREE:
            return (ObjectWriter<T>) TREE_WRITER;
        case FEATURE:
            return (ObjectWriter<T>) FEATURE_WRITER;
        case FEATURETYPE:
            return (ObjectWriter<T>) FEATURETYPE_WRITER;
        case TAG:
            return (ObjectWriter<T>) TAG_WRITER;
        default:
            throw new UnsupportedOperationException("No writer for " + type);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> ObjectReader<T> createObjectReader(TYPE type) {
        switch (type) {
        case COMMIT:
            return (ObjectReader<T>) COMMIT_READER;
        case TREE:
            return (ObjectReader<T>) TREE_READER;
        case FEATURE:
            return (ObjectReader<T>) FEATURE_READER;
        case FEATURETYPE:
            return (ObjectReader<T>) FEATURETYPE_READER;
        case TAG:
            return (ObjectReader<T>) TAG_READER;
        default:
            throw new UnsupportedOperationException("No reader for " + type);
        }
    }

    @Override
    public ObjectReader<RevObject> createObjectReader() {
        return OBJECT_READER;
    }

    private static final class CommitReaderV2 implements ObjectReader<RevCommit> {
        @Override
        public RevCommit read(ObjectId id, InputStream rawData) throws IllegalArgumentException {
            DataInput in = new DataInputStream(rawData);
            try {
                requireHeader(in, TYPE.COMMIT);
                return readCommit(id, in);
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
    }

    private static final class CommitWriterV2 extends ObjectWriterV2<RevCommit> {
        @Override
        public void write(RevCommit commit, DataOutput data) throws IOException {
            writeCommit(commit, data);
        }
    }

    private static final class FeatureReaderV2 implements ObjectReader<RevFeature> {
        @Override
        public RevFeature read(ObjectId id, InputStream rawData) throws IllegalArgumentException {
            DataInput in = new DataInputStream(rawData);
            try {
                requireHeader(in, TYPE.FEATURE);
                return readFeature(id, in);
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
    }

    private static final class FeatureWriterV2 extends ObjectWriterV2<RevFeature> {
        @Override
        public void write(RevFeature feature, DataOutput data) throws IOException {
            writeFeature(feature, data);
        }
    }

    private static final class FeatureTypeReaderV2 implements ObjectReader<RevFeatureType> {
        @Override
        public RevFeatureType read(ObjectId id, InputStream rawData)
                throws IllegalArgumentException {
            DataInput in = new DataInputStream(rawData);
            try {
                requireHeader(in, TYPE.FEATURETYPE);
                return readFeatureType(id, in);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static final class FeatureTypeWriterV2 extends ObjectWriterV2<RevFeatureType> {
        @Override
        public void write(RevFeatureType object, DataOutput data) throws IOException {
            writeFeatureType(object, data);
        }
    }

    private static final class TagReaderV2 implements ObjectReader<RevTag> {
        @Override
        public RevTag read(ObjectId id, InputStream in) {
            DataInput data = new DataInputStream(in);
            try {
                requireHeader(data, TYPE.TAG);
                return readTag(id, data);
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }
    }

    private static final class TagWriterV2 extends ObjectWriterV2<RevTag> {
        @Override
        public void write(RevTag tag, DataOutput data) throws IOException {
            writeTag(tag, data);
        }
    }

    private static final class TreeReaderV2 implements ObjectReader<RevTree> {
        @Override
        public RevTree read(ObjectId id, InputStream rawData) throws IllegalArgumentException {
            DataInput in = new DataInputStream(rawData);
            try {
                requireHeader(in, RevObject.TYPE.TREE);
                return readTree(id, in);
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
    }

    private static final class TreeWriterV2 extends ObjectWriterV2<RevTree> {
        @Override
        public void write(RevTree tree, DataOutput data) throws IOException {
            writeTree(tree, data);
        }
    }

    /**
     * Provides an interface for writing objects to a given output stream.
     */
    private static abstract class ObjectWriterV2<T extends RevObject> implements ObjectWriter<T> {

        /**
         * Writers must call
         * {@link FormatCommonV2#writeHeader(java.io.DataOutput, org.geogit.api.RevObject.TYPE)},
         * readers must not, in order for {@link ObjectReaderV2} to be able of parsing the header
         * and call the appropriate read method.
         */
        @Override
        public void write(T object, OutputStream out) throws IOException {
            DataOutput data = new DataOutputStream(out);
            writeHeader(data, object.getType());
            write(object, data);
        }

        public abstract void write(T object, DataOutput data) throws IOException;
    }

    private static final class ObjectReaderV2 implements org.geogit.storage.ObjectReader<RevObject> {
        @Override
        public RevObject read(ObjectId id, InputStream rawData) throws IllegalArgumentException {
            DataInput in = new DataInputStream(rawData);
            try {
                return readData(id, in);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private RevObject readData(ObjectId id, DataInput in) throws IOException {
            final RevObject.TYPE type = readHeader(in);
            switch (type) {
            case COMMIT:
                return readCommit(id, in);
            case TREE:
                return readTree(id, in);
            case FEATURE:
                return readFeature(id, in);
            case TAG:
                return readTag(id, in);
            case FEATURETYPE:
                return readFeatureType(id, in);
            default:
                throw new IllegalArgumentException(String.format("Unrecognized object header: %s)",
                        type));
            }
        }
    }
}
