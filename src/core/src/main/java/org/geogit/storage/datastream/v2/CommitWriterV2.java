/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.geogit.storage.datastream.v2;

import static org.geogit.storage.datastream.v2.FormatCommonV2.writeCommit;

import java.io.DataOutput;
import java.io.IOException;

import org.geogit.api.RevCommit;

final class CommitWriterV2 extends ObjectWriterV2<RevCommit> {

    @Override
    public void write(RevCommit commit, DataOutput data) throws IOException {
        writeCommit(commit, data);
    }
}
