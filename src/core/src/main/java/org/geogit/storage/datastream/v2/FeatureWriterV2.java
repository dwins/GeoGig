/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.geogit.storage.datastream.v2;

import static org.geogit.storage.datastream.v2.FormatCommonV2.writeFeature;

import java.io.DataOutput;
import java.io.IOException;

import org.geogit.api.RevFeature;

final class FeatureWriterV2 extends ObjectWriterV2<RevFeature> {

    @Override
    public void write(RevFeature feature, DataOutput data) throws IOException {
        writeFeature(feature, data);
    }
}
