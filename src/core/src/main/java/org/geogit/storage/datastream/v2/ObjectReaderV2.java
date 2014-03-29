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

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.geogit.api.ObjectId;
import org.geogit.api.RevObject;

final class ObjectReaderV2 implements org.geogit.storage.ObjectReader<RevObject> {
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
