/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.geogit.cli;

import org.geogit.api.InjectorBuilder;
import org.geogit.di.GeogitModule;
import org.geogit.di.PluginsModule;
import org.geogit.di.PluginDefaults;
import org.geogit.di.VersionedFormat;
import org.geogit.di.caching.CachingModule;
import org.geogit.metrics.MetricsModule;
import org.geogit.storage.GraphDatabase;
import org.geogit.storage.ObjectDatabase;
import org.geogit.storage.StagingDatabase;
import org.geogit.storage.RefDatabase;
import org.geogit.storage.bdbje.JEObjectDatabase;
import org.geogit.storage.bdbje.JEStagingDatabase;
import org.geogit.storage.bdbje.JEStorageModule;
import org.geogit.storage.blueprints.BlueprintsGraphModule;
import org.geogit.storage.blueprints.TinkerGraphDatabase;
import org.geogit.storage.fs.FileRefDatabase;
import org.geogit.storage.neo4j.Neo4JGraphDatabase;
import org.geogit.storage.mongo.MongoGraphDatabase;
import org.geogit.storage.mongo.MongoObjectDatabase;
import org.geogit.storage.mongo.MongoStagingDatabase;
import org.geogit.storage.mongo.MongoStorageModule;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.util.Modules;

public class CLIInjectorBuilder extends InjectorBuilder {
    private static final PluginDefaults defaults =
        new PluginDefaults(new VersionedFormat("bdbje", "0.1"),
                           new VersionedFormat("bdbje", "0.1"),
                           new VersionedFormat("file", "1.0"),
                           new VersionedFormat("tinkergraph", "0.1"));
    @Override
    public Injector build() {
        return Guice.createInjector(Modules.override(new GeogitModule(), new CachingModule()).with(new MetricsModule(),
                    // new JEStorageModule(), new BlueprintsGraphModule()));
                    new PluginsModule(), new DefaultPlugins()));
    }

    private class DefaultPlugins extends AbstractModule {
        @Override
        protected void configure() {
            bind(PluginDefaults.class).toInstance(defaults);
            MapBinder<VersionedFormat, RefDatabase> refPlugins = MapBinder.newMapBinder(binder(), VersionedFormat.class, RefDatabase.class);
            refPlugins //
                .addBinding(new VersionedFormat("file", "1.0"))
                .to(FileRefDatabase.class)
                .in(Scopes.SINGLETON);
            MapBinder<VersionedFormat, ObjectDatabase> objectPlugins = MapBinder.newMapBinder(binder(), VersionedFormat.class, ObjectDatabase.class);
            objectPlugins //
                .addBinding(new VersionedFormat("bdbje", "0.1"))
                .to(JEObjectDatabase.class)
                .in(Scopes.SINGLETON);
            objectPlugins //
                .addBinding(new VersionedFormat("mongodb", "0.1"))
                .to(MongoObjectDatabase.class)
                .in(Scopes.SINGLETON);
            MapBinder<VersionedFormat, StagingDatabase> stagingPlugins = MapBinder.newMapBinder(binder(), VersionedFormat.class, StagingDatabase.class);
            stagingPlugins //
                .addBinding(new VersionedFormat("mongodb", "0.1"))
                .to(MongoStagingDatabase.class)
                .in(Scopes.SINGLETON);
            stagingPlugins //
                .addBinding(new VersionedFormat("bdbje", "0.1"))
                .to(JEStagingDatabase.class)
                .in(Scopes.SINGLETON);
            MapBinder<VersionedFormat, GraphDatabase> graphPlugins = MapBinder.newMapBinder(binder(), VersionedFormat.class, GraphDatabase.class);
            graphPlugins //
                .addBinding(new VersionedFormat("tinkergraph", "0.1")) //
                .to(TinkerGraphDatabase.class) //
                .in(Scopes.SINGLETON);
            graphPlugins //
                .addBinding(new VersionedFormat("mongodb", "0.1")) //
                .to(MongoGraphDatabase.class) //
                .in(Scopes.SINGLETON);
            graphPlugins //
                .addBinding(new VersionedFormat("neo4j", "0.1")) //
                .to(Neo4JGraphDatabase.class) //
                .in(Scopes.SINGLETON);
        }
    }
}
