/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.geogit.storage.datastream.v2;

import java.io.Serializable;
import java.util.Map;

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
}
