/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.geogit.storage.datastream.v2;

import org.geogit.storage.ObjectSerializingFactory;
import org.geogit.storage.RevTreeSerializationTest;
import org.geogit.storage.datastream.v2.DataStreamSerializationFactoryV2;

public class DataStreamV2RevTreeSerializationTest extends RevTreeSerializationTest {
    @Override
    protected ObjectSerializingFactory getObjectSerializingFactory() {
        return new DataStreamSerializationFactoryV2();
    }
}
