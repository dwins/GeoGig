package org.geogit.geotools.plumbing;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;

import org.geogit.api.data.ForwardingFeatureCollection;
import org.geogit.api.data.ForwardingFeatureIterator;
import org.geogit.api.data.ForwardingFeatureSource;
import org.geotools.data.FeatureSource;
import org.geotools.data.Query;
import org.geotools.feature.DecoratingFeature;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.filter.identity.FeatureIdImpl;
import org.opengis.feature.Feature;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.FeatureType;
import org.opengis.filter.identity.FeatureId;

class ForceTypeAndFidFeatureSource<T extends FeatureType, F extends Feature> extends
        ForwardingFeatureSource<T, F> {

    private T forceType;

    private String fidPrefix;

    public ForceTypeAndFidFeatureSource(final FeatureSource<T, F> source, final T forceType,
            final String fidPrefix) {

        super(source);
        this.forceType = forceType;
        this.fidPrefix = fidPrefix;
    }

    @Override
    public T getSchema() {
        return forceType;
    }

    @Override
    public FeatureCollection<T, F> getFeatures(Query query) throws IOException {

        final FeatureCollection<T, F> features = super.getFeatures(query);
        return new ForwardingFeatureCollection<T, F>(features) {

            @Override
            public FeatureIterator<F> features() {

                FeatureIterator<F> iterator = delegate.features();

                return new FidPrefixRemovingIterator<F>(iterator, fidPrefix,
                        (SimpleFeatureType) forceType);
            }

            @Override
            public T getSchema() {
                return forceType;
            }
        };
    }

    private static class FidPrefixRemovingIterator<F extends Feature> extends
            ForwardingFeatureIterator<F> {

        private final String fidPrefix;

        private SimpleFeatureType forcedType;

        public FidPrefixRemovingIterator(final FeatureIterator<F> iterator, final String fidPrefix,
                SimpleFeatureType forcedType) {
            super(iterator);
            checkNotNull(fidPrefix);
            checkNotNull(forcedType);
            this.fidPrefix = fidPrefix;
            this.forcedType = forcedType;
        }

        @Override
        public F next() {
            F next = super.next();
            String fid = ((SimpleFeature) next).getID();
            if (fid.startsWith(fidPrefix)) {
                fid = fid.substring(fidPrefix.length());
            }
            return (F) new FidAndFtOverrideFeature((SimpleFeature) next, fid, forcedType);
        }
    }

    private static final class FidAndFtOverrideFeature extends DecoratingFeature {

        private String fid;

        private SimpleFeatureType featureType;

        public FidAndFtOverrideFeature(SimpleFeature delegate, String fid,
                SimpleFeatureType featureType) {
            super(delegate);
            this.fid = fid;
            this.featureType = featureType;
        }

        @Override
        public SimpleFeatureType getType() {
            return featureType;
        }

        @Override
        public String getID() {
            return fid;
        }

        @Override
        public FeatureId getIdentifier() {
            return new FeatureIdImpl(fid);
        }
    }
}
