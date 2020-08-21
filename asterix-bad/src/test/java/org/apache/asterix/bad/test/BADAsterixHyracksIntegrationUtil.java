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

import org.apache.asterix.api.common.AsterixHyracksIntegrationUtil;
import org.apache.hyracks.test.support.TestUtils;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class BADAsterixHyracksIntegrationUtil extends AsterixHyracksIntegrationUtil {

    public static void main(String[] args) throws Exception {
        TestUtils.redirectLoggingToConsole();
        BADAsterixHyracksIntegrationUtil integrationUtil = new BADAsterixHyracksIntegrationUtil();
        try {
            integrationUtil.run(true, Boolean.getBoolean("cleanup.shutdown"),
                    System.getProperty("external.lib", "asterixdb/asterix-opt/asterix-bad/src/main/resources/cc.conf"));
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
