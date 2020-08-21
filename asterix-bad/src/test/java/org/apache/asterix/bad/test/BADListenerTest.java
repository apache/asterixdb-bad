/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.bad.test;

import org.apache.asterix.active.ActiveEvent;
import org.apache.asterix.active.ActivityState;
import org.apache.asterix.active.EntityId;
import org.apache.asterix.bad.BADConstants;
import org.apache.asterix.bad.metadata.DeployedJobSpecEventListener;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class BADListenerTest {

    private static DeployedJobSpecEventListener djsel;

    private class suspend extends Thread {
        @Override
        public void run() {
            try {
                djsel.suspend();
                Thread.sleep(5000);
                djsel.resume();
            } catch (HyracksDataException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

    private class run extends Thread {
        @Override
        public void run() {
            try {
                djsel.notify(new ActiveEvent(null, ActiveEvent.Kind.JOB_STARTED, null, null));
                djsel.notify(new ActiveEvent(null, ActiveEvent.Kind.JOB_STARTED, null, null));
                djsel.notify(new ActiveEvent(null, ActiveEvent.Kind.JOB_FINISHED, null, null));
                Thread.sleep(5000);
                djsel.notify(new ActiveEvent(null, ActiveEvent.Kind.JOB_FINISHED, null, null));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

    @BeforeClass
    public static void init() {
        djsel = new DeployedJobSpecEventListener(null,
                new EntityId(BADConstants.RUNTIME_ENTITY_CHANNEL, DataverseName.createSinglePartName("test"), "test"),
                DeployedJobSpecEventListener.PrecompiledType.CHANNEL);
    }

    @Test
    public void DistributedTest() throws Exception {
        new suspend().run();
        djsel.waitWhileAtState(ActivityState.SUSPENDED);
        new run().run();
        djsel.suspend();
    }

    @AfterClass
    public static void deinit() throws Exception {

    }
}
