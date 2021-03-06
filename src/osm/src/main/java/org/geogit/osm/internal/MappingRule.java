/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.geogit.osm.internal;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.geogit.storage.FieldType;
import org.geogit.storage.text.TextValueSerializer;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.opengis.feature.Feature;
import org.opengis.feature.GeometryAttribute;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.gson.annotations.Expose;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

/**
 * A rule used to convert an OSM entity into a feature with a custom feature type. Attributes values
 * for the attributes in the feature type are taken from the tags and geometry of the feature to
 * convert
 * 
 */
public class MappingRule {

    /**
     * The name of the rule
     */
    @Expose
    private String name;

    /**
     * A map of key, list_of_accepted_values, to be used to filter features. If a feature has any of
     * the keys in this map with any of the accepted values, it will be transformed by this rule
     */
    @Expose
    private Map<String, List<String>> filter;

    /**
     * The fields to use for the custom feature type of the transformed feature
     */
    @Expose
    private Map<String, AttributeDefinition> fields;

    private SimpleFeatureType featureType;

    private SimpleFeatureBuilder featureBuilder;

    private Class<?> geometryType;

    private static GeometryFactory gf = new GeometryFactory();

    public MappingRule(String name, Map<String, List<String>> filter,
            Map<String, AttributeDefinition> fields) {
        Preconditions.checkNotNull(name);
        Preconditions.checkNotNull(filter);
        Preconditions.checkNotNull(fields);
        this.name = name;
        this.filter = filter;
        this.fields = fields;
        ArrayList<String> names = Lists.newArrayList();
        for (AttributeDefinition ad : fields.values()) {
            Preconditions.checkState(!names.contains(ad.getName()),
                    "Duplicated alias in mapping rule: " + ad.getName());
            names.add(ad.getName());
        }
    }

    /**
     * Returns the feature type defined by this rule. This is the feature type that features
     * transformed by this rule will have
     * 
     * @return
     */
    public SimpleFeatureType getFeatureType() {
        if (featureType == null) {
            SimpleFeatureTypeBuilder fb = new SimpleFeatureTypeBuilder();
            fb.setName(name);
            fb.add("id", Long.class);
            Set<String> keys = this.fields.keySet();
            for (String key : keys) {
                AttributeDefinition field = fields.get(key);
                Class<?> clazz = field.getType().getBinding();
                if (Geometry.class.isAssignableFrom(clazz)) {
                    Preconditions.checkArgument(geometryType == null,
                            "The mapping more than one geometry attribute");
                    fb.add(field.getName(), clazz, DefaultGeographicCRS.WGS84);
                    geometryType = clazz;
                } else {
                    fb.add(field.getName(), clazz);
                }
            }
            Preconditions.checkNotNull(geometryType,
                    "The mapping rule does not define a geometry field");
            if (!geometryType.equals(Point.class)) {
                fb.add("nodes", String.class);
            }
            featureType = fb.buildFeatureType();

            featureBuilder = new SimpleFeatureBuilder(featureType);

        }
        return featureType;

    }

    /**
     * Returns the feature resulting from transforming a given feature using this rule
     * 
     * @param feature
     * @param tags
     * @return
     */
    public Optional<Feature> apply(Feature feature, Collection<Tag> tags) {
        if (!canBeApplied(feature, tags)) {
            return Optional.absent();
        }
        for (AttributeDescriptor attribute : getFeatureType().getAttributeDescriptors()) {
            String attrName = attribute.getName().toString();
            Class<?> clazz = attribute.getType().getBinding();
            if (Geometry.class.isAssignableFrom(clazz)) {
                Geometry geom = prepareGeometry((Geometry) feature.getDefaultGeometryProperty()
                        .getValue());
                featureBuilder.set(attrName, geom);
            } else {
                Object value = null;
                for (Tag tag : tags) {
                    if (fields.containsKey(tag.getKey())) {
                        if (fields.get(tag.getKey()).getName().equals(attrName)) {
                            FieldType type = FieldType.forBinding(clazz);
                            value = getAttributeValue(tag.getValue(), type);
                            break;
                        }
                    }
                }
                featureBuilder.set(attribute.getName(), value);
            }
        }

        String id = feature.getIdentifier().getID();
        featureBuilder.set("id", id);
        if (!featureType.getGeometryDescriptor().getType().getBinding().equals(Point.class)) {
            featureBuilder.set("nodes", feature.getProperty("nodes").getValue());
        }
        return Optional.of((Feature) featureBuilder.buildFeature(id));

    }

    private Geometry prepareGeometry(Geometry geom) {
        if (geometryType.equals(Polygon.class)) {
            Coordinate[] coords = geom.getCoordinates();
            if (!coords[0].equals(coords[coords.length - 1])) {
                Coordinate[] newCoords = new Coordinate[coords.length + 1];
                System.arraycopy(coords, 0, newCoords, 0, coords.length);
                newCoords[coords.length] = coords[0];
                coords = newCoords;
            }
            return gf.createPolygon(coords);
        } else {
            return geom;
        }

    }

    private Object getAttributeValue(String value, FieldType type) {
        return TextValueSerializer.fromString(type, value);
    }

    public boolean canBeApplied(Feature feature, Collection<Tag> tags) {
        return hasRequiredTags(feature, tags) && hasCompatibleGeometryType(feature);
    }

    private boolean hasCompatibleGeometryType(Feature feature) {
        getFeatureType();
        GeometryAttribute property = feature.getDefaultGeometryProperty();
        Object geom = property.getValue();
        if (geom.getClass().equals(Point.class)) {
            return geometryType == Point.class;
        } else {
            return !geometryType.equals(Point.class);
        }
    }

    private boolean hasRequiredTags(Feature feature, Collection<Tag> tags) {
        if (filter.isEmpty()) {
            return true;
        }
        for (Tag tag : tags) {
            if (filter.keySet().contains(tag.getKey())) {
                List<String> values = filter.get(tag.getKey());
                if (values.isEmpty()) {
                    return true;
                }
                if (values.contains(tag.getValue())) {
                    return true;
                }
            }
        }
        return false;
    }

    public String getName() {
        return name;
    }

    /**
     * Returns true if this rule generates feature with a line or polygon geometry, or it doesn't
     * have a geometry attribute, so it can take ways as inputs
     * 
     * @return
     */
    public boolean canUseWays() {
        getFeatureType();
        return !geometryType.equals(Point.class);
    }

    /**
     * Returns true if this rule generates feature with a point geometry, or it doesn't have a
     * geometry attribute, so it can take nodes as inputs
     * 
     * @return
     */
    public boolean canUseNodes() {
        getFeatureType();
        return geometryType.equals(Point.class);
    }

    /**
     * Resolves the original tag name based on the name of a field created by this mapping rule (an
     * alias for a tag name) *
     * 
     * @param field the name of the field
     * @return the name of the tag from which the passed field was created in the specified mapped
     *         tree. If the alias cannot be resolved, that passed alias itself is returned
     */
    public String getTagNameFromAlias(String alias) {
        Set<String> keys = this.fields.keySet();
        for (String key : keys) {
            AttributeDefinition field = fields.get(key);
            if (field.getName().equals(alias)) {
                return key;
            }
        }
        return alias;
    }

    public boolean equals(Object o) {
        if (o instanceof MappingRule) {
            MappingRule m = (MappingRule) o;
            return name.equals(m.name) && m.fields.equals(fields) && m.filter.equals(filter);
        } else {
            return false;
        }
    }

}
