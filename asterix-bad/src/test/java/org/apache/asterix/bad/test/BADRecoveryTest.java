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
import java.util.Map;

import org.apache.asterix.test.common.TestExecutor;
import org.apache.asterix.testframework.context.TestCaseContext;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class BADRecoveryTest {

    private static final java.util.logging.Logger LOGGER =
            java.util.logging.Logger.getLogger(BADRecoveryTest.class.getName());

    private static final String PATH_ACTUAL = "target" + File.separator + "rttest" + File.separator;
    private static final String PATH_BASE = "src/test/resources/recoveryts/";
    private TestCaseContext tcCtx;
    private static ProcessBuilder pb;
    private static Map<String, String> env;
    private final TestExecutor testExecutor = new TestExecutor();
    private static int testNumber;
    private static File asterixInstallerPath;
    private static File installerTargetPath;
    private static String ncServiceHomeDirName;
    private static String ncServiceHomePath;
    private static String ncServiceSubDirName;
    private static String ncServiceSubPath;
    private static String scriptHomePath;
    private static String reportPath;

    @BeforeClass
    public static void setUp() throws Exception {
        File outdir = new File(PATH_ACTUAL);
        outdir.mkdirs();

        asterixInstallerPath = new File(System.getProperty("user.dir"));
        installerTargetPath =
                new File(new File(asterixInstallerPath.getParentFile().getParentFile(), "asterix-server"), "target");
        reportPath = new File(installerTargetPath, "failsafe-reports").getAbsolutePath();
        ncServiceSubDirName =
                installerTargetPath.list((dir, name) -> name.matches("asterix-server.*binary-assembly"))[0];
        ncServiceSubPath = new File(installerTargetPath, ncServiceSubDirName).getAbsolutePath();
        ncServiceHomeDirName = new File(ncServiceSubPath).list(((dir, name) -> name.matches("apache-asterixdb.*")))[0];
        ncServiceHomePath = new File(ncServiceSubPath, ncServiceHomeDirName).getAbsolutePath();
        pb = new ProcessBuilder();
        env = pb.environment();
        env.put("JAVA_HOME", System.getProperty("java.home"));
        //Create the folder to run asterix with extensions
        String asterixInstallerTarget = asterixInstallerPath + File.separator + "target";
        Process p = Runtime.getRuntime().exec("cp -R " + ncServiceHomePath + " " + asterixInstallerTarget);
        p.waitFor();

        ncServiceHomePath = asterixInstallerTarget + File.separator + ncServiceHomeDirName;

        String confDir = File.separator + "opt" + File.separator + "local" + File.separator + "conf" + File.separator;
        p = Runtime.getRuntime().exec("rm " + ncServiceHomePath + confDir + "cc.conf");
        p.waitFor();

        String BADconf = asterixInstallerPath + File.separator + "src" + File.separator + "main" + File.separator
                + "resources" + File.separator + "cc.conf";
        p = Runtime.getRuntime().exec("cp " + BADconf + " " + ncServiceHomePath + confDir);
        p.waitFor();

        LOGGER.info("NCSERVICE_HOME=" + ncServiceHomePath);
        env.put("NCSERVICE_HOME", ncServiceHomePath);
        env.put("JAVA_HOME", System.getProperty("java.home"));
        scriptHomePath = asterixInstallerPath + File.separator + "src" + File.separator + "test" + File.separator
                + "resources" + File.separator + "recoveryts" + File.separator + "scripts";
        env.put("SCRIPT_HOME", scriptHomePath);

        TestExecutor.executeScript(pb,
                scriptHomePath + File.separator + "setup_teardown" + File.separator + "stop_and_delete.sh");

        TestExecutor.executeScript(pb,
                scriptHomePath + File.separator + "setup_teardown" + File.separator + "configure_and_validate.sh");

    }

    @AfterClass
    public static void tearDown() throws Exception {
        TestExecutor.executeScript(pb,
                scriptHomePath + File.separator + "setup_teardown" + File.separator + "stop_and_delete.sh");
        File outdir = new File(PATH_ACTUAL);
        FileUtils.deleteDirectory(outdir);
        File dataCopyDir = new File(ncServiceHomePath);
        FileUtils.deleteDirectory(dataCopyDir);

    }

    @Parameters(name = "RecoveryIT {index}: {0}")
    public static Collection<Object[]> tests() throws Exception {
        Collection<Object[]> testArgs = new ArrayList<Object[]>();
        TestCaseContext.Builder b = new TestCaseContext.Builder();
        for (TestCaseContext ctx : b.build(new File(PATH_BASE))) {
            testArgs.add(new Object[] { ctx });
        }
        return testArgs;
    }

    public BADRecoveryTest(TestCaseContext tcCtx) {
        this.tcCtx = tcCtx;
    }

    @Test
    public void test() throws Exception {
        testExecutor.executeTest(PATH_ACTUAL, tcCtx, pb, false);
    }

}
