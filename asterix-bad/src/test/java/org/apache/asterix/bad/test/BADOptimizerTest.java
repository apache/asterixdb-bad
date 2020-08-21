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

import java.io.File;
import java.util.logging.Logger;

import org.apache.asterix.bad.lang.BADCompilationProvider;
import org.apache.asterix.bad.lang.BADQueryTranslatorFactory;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.IdentitiyResolverFactory;
import org.apache.asterix.test.optimizer.OptimizerTest;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class BADOptimizerTest extends OptimizerTest {

    private static final Logger LOGGER = Logger.getLogger(BADOptimizerTest.class.getName());

    @BeforeClass
    public static void setUp() throws Exception {
        final File outdir = new File(PATH_ACTUAL);
        outdir.mkdirs();
        extensionLangCompilationProvider = new BADCompilationProvider();
        statementExecutorFactory = new BADQueryTranslatorFactory();
        integrationUtil = new BADAsterixHyracksIntegrationUtil();

        integrationUtil.init(true, TEST_CONFIG_FILE_NAME);
        // Set the node resolver to be the identity resolver that expects node names
        // to be node controller ids; a valid assumption in test environment.
        System.setProperty(ExternalDataConstants.NODE_RESOLVER_FACTORY_PROPERTY,
                IdentitiyResolverFactory.class.getName());
    }

    public BADOptimizerTest(File queryFile, File expectedFile, File actualFile) {
        super(queryFile, expectedFile, actualFile);
    }

}
