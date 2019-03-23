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
import java.util.ArrayList;
import java.util.Collection;
import java.util.logging.Logger;

import org.apache.asterix.api.common.AsterixHyracksIntegrationUtil;
import org.apache.asterix.common.config.TransactionProperties;
import org.apache.asterix.test.common.TestExecutor;
import org.apache.asterix.test.runtime.ExecutionTestUtil;
import org.apache.asterix.testframework.context.TestCaseContext;
import org.apache.asterix.testframework.xml.TestGroup;
import org.apache.commons.lang3.StringUtils;
import org.apache.hyracks.storage.am.lsm.btree.impl.TestLsmBtreeLocalResource;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Runs the runtime test cases under 'src/test/resources/runtimets'.
 */
@RunWith(Parameterized.class)
public class BADExecutionTest {

    protected static final Logger LOGGER = Logger.getLogger(BADExecutionTest.class.getName());

    protected static final String PATH_ACTUAL = "target/rttest" + File.separator;
    protected static final String PATH_BASE =
            StringUtils.join(new String[] { "src", "test", "resources", "runtimets" }, File.separator);

    protected static final String TEST_CONFIG_FILE_NAME = "src/main/resources/cc.conf";

    protected static TransactionProperties txnProperties;
    private static final TestExecutor testExecutor = new TestExecutor();
    private static final boolean cleanupOnStart = true;
    private static final boolean cleanupOnStop = true;

    protected static TestGroup FailedGroup;
    protected TestLsmBtreeLocalResource resource = null;

    @BeforeClass
    public static void setUp() throws Exception {
        File outdir = new File(PATH_ACTUAL);
        outdir.mkdirs();
        ExecutionTestUtil.setUp(cleanupOnStart, TEST_CONFIG_FILE_NAME, new AsterixHyracksIntegrationUtil(), false,
                null);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        ExecutionTestUtil.tearDown(cleanupOnStop);
        ExecutionTestUtil.integrationUtil.removeTestStorageFiles();
    }

    @Parameters(name = "BADExecutionTest {index}: {0}")
    public static Collection<Object[]> tests() throws Exception {
        return buildTestsInXml("testsuite.xml");
    }

    protected static Collection<Object[]> buildTestsInXml(String xmlfile) throws Exception {
        Collection<Object[]> testArgs = new ArrayList<Object[]>();
        TestCaseContext.Builder b = new TestCaseContext.Builder();
        for (TestCaseContext ctx : b.build(new File(PATH_BASE), xmlfile)) {
            testArgs.add(new Object[] { ctx });
        }
        return testArgs;

    }

    protected TestCaseContext tcCtx;

    public BADExecutionTest(TestCaseContext tcCtx) {
        this.tcCtx = tcCtx;
    }

    @Test
    public void test() throws Exception {
        testExecutor.executeTest(PATH_ACTUAL, tcCtx, null, false, FailedGroup);
    }
}
