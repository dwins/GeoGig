package org.geogit.di;

import java.util.Map;
import org.geogit.storage.ConfigDatabase;
import org.geogit.storage.GraphDatabase;
import org.geogit.storage.ObjectDatabase;
import org.geogit.storage.RefDatabase;
import org.geogit.storage.StagingDatabase;
import org.geogit.storage.memory.HeapGraphDatabase;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.multibindings.MapBinder;

public class PluginsModule extends GeogitModule {
    protected void configure() {
        bind(ObjectDatabase.class).toProvider(PluginObjectDatabaseProvider.class);
        bind(StagingDatabase.class).toProvider(PluginStagingDatabaseProvider.class);
        bind(RefDatabase.class).toProvider(PluginRefDatabaseProvider.class);
        bind(GraphDatabase.class).toProvider(PluginGraphDatabaseProvider.class);
    }

    private static class PluginObjectDatabaseProvider extends FormatSelector<ObjectDatabase> {
        private final PluginDefaults defaults;


        @Override
        protected final VersionedFormat readConfig(ConfigDatabase config) {
            try {
            final String format = config.get("storage.objects").orNull();
            final String version = config.get(format + ".version").orNull();
            if (format == null || version == null) {
                return defaults.getObjects();
            } else {
                return new VersionedFormat(format, version);
            }
            } catch (RuntimeException e) {
                return defaults.getObjects();
            }
        }

        @Inject
        public PluginObjectDatabaseProvider(PluginDefaults defaults, ConfigDatabase config, Map<VersionedFormat, Provider<ObjectDatabase>> plugins) {
            super(config, plugins);
            this.defaults = defaults;
        }
    }

    private static class PluginStagingDatabaseProvider extends FormatSelector<StagingDatabase> {
        private final PluginDefaults defaults;

        @Override
        protected final VersionedFormat readConfig(ConfigDatabase config) {
            try {
                final String format = config.get("storage.staging").orNull();
                final String version = config.get(format + ".version").orNull();
                if (format == null || version == null) {
                    return defaults.getStaging();
                } else {
                    return new VersionedFormat(format, version);
                }
            } catch (RuntimeException e) {
                return defaults.getStaging();
            }
        }

        @Inject
        public PluginStagingDatabaseProvider(PluginDefaults defaults, ConfigDatabase config, Map<VersionedFormat, Provider<StagingDatabase>> plugins) {
            super(config, plugins);
            this.defaults = defaults;
        }
    }

    private static class PluginRefDatabaseProvider extends FormatSelector<RefDatabase> {
        private final PluginDefaults defaults;

        @Override
        protected final VersionedFormat readConfig(ConfigDatabase config) {
            try {
                final String format = config.get("storage.refs").orNull();
                final String version = config.get(format + ".version").orNull();
                if (format == null || version == null) {
                    return defaults.getRefs();
                } else {
                    return new VersionedFormat(format, version);
                }
            } catch (RuntimeException e) {
                return defaults.getRefs();
            }
        }

        @Inject
        public PluginRefDatabaseProvider(PluginDefaults defaults, ConfigDatabase config, Map<VersionedFormat, Provider<RefDatabase>> plugins) {
            super(config, plugins);
            this.defaults = defaults;
        }
    }

    private static class PluginGraphDatabaseProvider extends FormatSelector<GraphDatabase> {
        private final PluginDefaults defaults;

        @Override
        protected final VersionedFormat readConfig(ConfigDatabase config) {
            try {
                final String format = config.get("storage.graph").orNull();
                final String version = config.get(format + ".version").orNull();
                if (format == null || version == null) {
                    return defaults.getGraph();
                } else {
                    return new VersionedFormat(format, version);
                }
            } catch (RuntimeException e) {
                return defaults.getGraph();
            }
        }

        @Inject
        public PluginGraphDatabaseProvider(PluginDefaults defaults, ConfigDatabase config, Map<VersionedFormat, Provider<GraphDatabase>> plugins) {
            super(config, plugins);
            this.defaults = defaults;
        }
    }
}
