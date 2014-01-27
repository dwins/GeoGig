/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */

package org.geogit.storage.mongo;

import org.geogit.storage.GraphDatabase;
import org.geogit.storage.ObjectDatabase;
import org.geogit.storage.StagingDatabase;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;

public class MongoStorageModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(ObjectDatabase.class).to(MongoObjectDatabase.class).in(Scopes.SINGLETON);
        bind(StagingDatabase.class).to(MongoStagingDatabase.class).in(Scopes.SINGLETON);
        bind(GraphDatabase.class).to(MongoGraphDatabase.class).in(Scopes.SINGLETON);
        bind(MongoConnectionManager.class).in(Scopes.NO_SCOPE);
    }
}
