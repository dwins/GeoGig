/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.geogit.storage.integration.mongo;

import org.geogit.di.GeogitModule;
import org.geogit.storage.GraphDatabaseTest;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.util.Modules;
import com.mongodb.DB;
import com.mongodb.MongoClient;

public class MongoGraphDatabaseTest extends GraphDatabaseTest {
    @Override 
    public void setUpInternal() throws Exception {
        super.setUpInternal();
        IniMongoProperties properties = new IniMongoProperties();
        String host = properties.get("mongo.host", String.class).or("localhost"); 
        int port = properties.get("mongodb.port", Integer.class).or(27017);
        MongoClient client = new MongoClient(host, Integer.valueOf(port));
        DB db = client.getDB("geogit");
        db.dropDatabase();
    }

    @Override
    protected Injector createInjector() {
        return Guice.createInjector(Modules.override(new GeogitModule())
                .with(new MongoTestStorageModule()));
    }
}
