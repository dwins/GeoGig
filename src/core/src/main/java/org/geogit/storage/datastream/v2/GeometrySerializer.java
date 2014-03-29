package org.geogit.storage.datastream.v2;

import static org.geogit.storage.datastream.v2.Varint.readUnsignedVarInt;
import static org.geogit.storage.datastream.v2.Varint.readUnsignedVarLong;
import static org.geogit.storage.datastream.v2.Varint.writeUnsignedVarInt;
import static org.geogit.storage.datastream.v2.Varint.writeUnsignedVarLong;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.geogit.storage.datastream.v2.DataStreamValueSerializerV2.ValueSerializer;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.CoordinateSequence;
import com.vividsolutions.jts.geom.CoordinateSequenceFilter;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.MultiLineString;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.geom.impl.PackedCoordinateSequenceFactory;

public class GeometrySerializer implements ValueSerializer {

    private static final int POINT = 0x01;

    private static final int LINESTRING = 0x02;

    private static final int POLYGON = 0x03;

    private static final int MULTIPOINT = 0x04;

    private static final int MULTILINESTRING = 0x05;

    private static final int MULTIPOLYGON = 0x06;

    private static final int GEOMETRYCOLLECTION = 0x07;

    private static final double FIXED_PRECISION_FACTOR = 1e7;

    private static final GeometryFactory GEOMFAC = new GeometryFactory(
            new PackedCoordinateSequenceFactory());

    @Override
    public void write(Object obj, final DataOutput out) throws IOException {

        final Geometry geom = (Geometry) obj;
        final int geometryType = getGeometryType(geom);
        final int typeAndMasks = geometryType;

        writeUnsignedVarInt(typeAndMasks, out);
        geom.apply(new EncodingSequenceFilter(out));
    }

    @Override
    public Object read(DataInput in) throws IOException {

        final int typeAndMasks = readUnsignedVarInt(in);

        if ((typeAndMasks & POINT) == POINT) {
            int xl = readUnsignedVarInt(in);
            int yl = readUnsignedVarInt(in);
            double x = toDoublePrecision(xl);
            double y = toDoublePrecision(yl);
            Point p = GEOMFAC.createPoint(new Coordinate(x, y));
        }
        return null;
    }

    private static final class EncodingSequenceFilter implements CoordinateSequenceFilter {

        private DataOutput out;

        public EncodingSequenceFilter(DataOutput out) {
            this.out = out;
        }

        @Override
        public void filter(CoordinateSequence seq, int index) {
            double ordinate1 = seq.getOrdinate(index, 0);
            double ordinate2 = seq.getOrdinate(index, 1);
            int fixedO1 = toFixedPrecision(ordinate1);
            int fixedO2 = toFixedPrecision(ordinate2);
            try {
                writeUnsignedVarInt(fixedO1, out);
                writeUnsignedVarInt(fixedO2, out);
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }

        @Override
        public boolean isGeometryChanged() {
            return false;
        }

        @Override
        public boolean isDone() {
            return false;
        }
    }

    private int getGeometryType(Geometry geom) {
        Preconditions.checkNotNull(geom, "null geometry");
        if (geom instanceof Point)
            return POINT;
        if (geom instanceof LineString)
            return LINESTRING;
        if (geom instanceof Polygon)
            return POLYGON;
        if (geom instanceof MultiPoint)
            return MULTIPOINT;
        if (geom instanceof MultiLineString)
            return MULTILINESTRING;
        if (geom instanceof MultiPolygon)
            return MULTIPOLYGON;
        if (geom instanceof GeometryCollection)
            return GEOMETRYCOLLECTION;
        throw new IllegalArgumentException("Unknown geometry type: " + geom.getClass());
    }

    /**
     * Converts the requested coordinate from double to fixed precision.
     */
    public static int toFixedPrecision(double ordinate) {
        int fixedPrecisionOrdinate = (int) Math.round(ordinate * FIXED_PRECISION_FACTOR);
        return fixedPrecisionOrdinate;
    }

    /**
     * Converts the requested coordinate from fixed to double precision.
     */
    public static double toDoublePrecision(int fixedPrecisionOrdinate) {
        double ordinate = fixedPrecisionOrdinate / FIXED_PRECISION_FACTOR;
        return ordinate;
    }
}
