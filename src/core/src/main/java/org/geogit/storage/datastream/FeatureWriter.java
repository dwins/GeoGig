package org.geogit.storage.datastream;

import static org.geogit.storage.datastream.FormatCommon.writeHeader;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.geogit.api.RevFeature;
import org.geogit.storage.ObjectWriter;
import org.geogit.storage.datastream.FormatCommon.FieldType;

import com.google.common.base.Optional;

public class FeatureWriter implements ObjectWriter<RevFeature> {
    @Override
    public void write(RevFeature feature, OutputStream out) throws IOException {
        DataOutput data = new DataOutputStream(out);
        writeHeader(data, "feature");
        data.writeInt(feature.getValues().size());
        for (Optional<Object> field : feature.getValues()) {
            FieldType type = FieldType.forValue(field);
            data.writeByte(type.getTag());
            if (type != FieldType.NULL) {
                type.write(field.get(), data);
            }
        }
    }
}
