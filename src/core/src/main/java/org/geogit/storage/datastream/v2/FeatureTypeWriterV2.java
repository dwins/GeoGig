/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.geogit.storage.datastream.v2;

import static org.geogit.storage.datastream.v2.FormatCommonV2.writeFeatureType;

import java.io.DataOutput;
import java.io.IOException;

import org.geogit.api.RevFeatureType;

final class FeatureTypeWriterV2 extends ObjectWriterV2<RevFeatureType> {

    @Override
    public void write(RevFeatureType object, DataOutput data) throws IOException {
        writeFeatureType(object, data);
    }
}
