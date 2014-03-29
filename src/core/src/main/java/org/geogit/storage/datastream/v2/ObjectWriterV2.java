/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.geogit.storage.datastream.v2;

import static org.geogit.storage.datastream.v2.FormatCommonV2.*;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.geogit.api.RevObject;
import org.geogit.storage.ObjectWriter;

/**
 * Provides an interface for writing objects to a given output stream.
 */
abstract class ObjectWriterV2<T extends RevObject> implements ObjectWriter<T> {

    /**
     * Writers must call
     * {@link FormatCommonV2#writeHeader(java.io.DataOutput, org.geogit.api.RevObject.TYPE)},
     * readers must not, in order for {@link ObjectReaderV2} to be able of parsing the header and
     * call the appropriate read method.
     */
    @Override
    public void write(T object, OutputStream out) throws IOException {
        DataOutput data = new DataOutputStream(out);
        writeHeader(data, object.getType());
        write(object, data);
    }

    public abstract void write(T object, DataOutput data) throws IOException;
}
