/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.geogit.storage.bdbje;

import org.geogit.repository.Hints;
import org.geogit.storage.ConfigDatabase;
import org.geogit.storage.datastream.DataStreamSerializationFactory;
import com.google.inject.Inject;

public final class JEObjectDatabase_v0_1 extends JEObjectDatabase {
    @Inject
    public JEObjectDatabase_v0_1(
            final ConfigDatabase configDB,
            final EnvironmentBuilder envProvider,
            final Hints hints)
    {
        this(
                configDB,
                envProvider,
                hints.getBoolean(Hints.OBJECTS_READ_ONLY),
                JEObjectDatabase.ENVIRONMENT_NAME
        );
    }

    public JEObjectDatabase_v0_1(
            final ConfigDatabase configDB,
            final EnvironmentBuilder envProvider,
            final boolean readOnly,
            final String envName)
    {
        super(
                DataStreamSerializationFactory.INSTANCE,
                configDB,
                envProvider,
                readOnly,
                envName
        );
    }
}
