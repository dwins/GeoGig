/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.geogit.storage.datastream.v2;

import static org.geogit.storage.datastream.v2.FormatCommonV2.readFeature;
import static org.geogit.storage.datastream.v2.FormatCommonV2.requireHeader;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.geogit.api.ObjectId;
import org.geogit.api.RevFeature;
import org.geogit.api.RevObject.TYPE;
import org.geogit.storage.ObjectReader;

import com.google.common.base.Throwables;

public class FeatureReaderV2 implements ObjectReader<RevFeature> {

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
