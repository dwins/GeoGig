/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.geogit.storage.datastream.v2;

import static org.geogit.storage.datastream.v2.FormatCommonV2.writeTag;

import java.io.DataOutput;
import java.io.IOException;

import org.geogit.api.RevTag;

final class TagWriterV2 extends ObjectWriterV2<RevTag> {

    @Override
    public void write(RevTag tag, DataOutput data) throws IOException {
        writeTag(tag, data);
    }
}
