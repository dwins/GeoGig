/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.geogit.storage.datastream.v2;

import static org.geogit.storage.datastream.v2.FormatCommonV2.writeTree;

import java.io.DataOutput;
import java.io.IOException;

import org.geogit.api.RevTree;

final class TreeWriterV2 extends ObjectWriterV2<RevTree> {

    @Override
    public void write(RevTree tree, DataOutput data) throws IOException {
        writeTree(tree, data);
    }
}
