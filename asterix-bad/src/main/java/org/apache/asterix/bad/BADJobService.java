/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.bad;

import java.io.StringReader;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.active.ActivityState;
import org.apache.asterix.active.EntityId;
import org.apache.asterix.app.active.ActiveNotificationHandler;
import org.apache.asterix.app.result.ResultReader;
import org.apache.asterix.app.result.fields.ResultsPrinter;
import org.apache.asterix.app.translator.QueryTranslator;
import org.apache.asterix.bad.lang.BADParserFactory;
import org.apache.asterix.bad.lang.BADStatementExecutor;
import org.apache.asterix.bad.metadata.DeployedJobSpecEventListener;
import org.apache.asterix.common.api.IResponsePrinter;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.transactions.ITxnIdFactory;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.statement.Query;
import org.apache.asterix.lang.common.statement.SetStatement;
import org.apache.asterix.lang.sqlpp.visitor.SqlppDeleteRewriteVisitor;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.translator.IRequestParameters;
import org.apache.asterix.translator.IStatementExecutor;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.job.DeployedJobSpecId;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.api.result.IResultSet;
import org.apache.hyracks.api.result.ResultSetId;

/**
 * Provides functionality for channel jobs
 */
public class BADJobService {

    private static final Logger LOGGER = Logger.getLogger(BADJobService.class.getName());

    //pool size one (only running one thread at a time)
    private static final int POOL_SIZE = 1;

    private static final long millisecondTimeout = BADConstants.EXECUTOR_TIMEOUT * 1000;

    public static void setupExecutorJob(EntityId entityId, JobSpecification channeljobSpec,
            IHyracksClientConnection hcc, DeployedJobSpecEventListener listener, ITxnIdFactory txnIdFactory,
            String duration) throws Exception {
        if (channeljobSpec != null) {
            channeljobSpec.setProperty(ActiveNotificationHandler.ACTIVE_ENTITY_PROPERTY_NAME, entityId);
            DeployedJobSpecId deployedId = hcc.deployJobSpec(channeljobSpec);
            ScheduledExecutorService ses = startRepetitiveDeployedJobSpec(deployedId, hcc, findPeriod(duration),
                    new HashMap<>(), entityId, txnIdFactory, listener);
            listener.setDeployedJobSpecId(deployedId);
            listener.setExecutorService(ses);
        }

    }

    //Starts running a deployed job specification periodically with an interval of "period" seconds
    public static ScheduledExecutorService startRepetitiveDeployedJobSpec(DeployedJobSpecId distributedId,
            IHyracksClientConnection hcc, long period, Map<byte[], byte[]> jobParameters, EntityId entityId,
            ITxnIdFactory txnIdFactory, DeployedJobSpecEventListener listener) {
        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(POOL_SIZE);
        scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    if (!runDeployedJobSpecCheckPeriod(distributedId, hcc, jobParameters, period, entityId,
                            txnIdFactory, listener)) {
                        scheduledExecutorService.shutdown();
                    }
                } catch (Exception e) {
                    LOGGER.log(Level.SEVERE, "Job Failed to run for " + entityId.getExtensionName() + " "
                            + entityId.getDataverseName() + "." + entityId.getEntityName() + ".", e);
                }
            }
        }, period, period, TimeUnit.MILLISECONDS);
        return scheduledExecutorService;
    }

    public static boolean runDeployedJobSpecCheckPeriod(DeployedJobSpecId distributedId, IHyracksClientConnection hcc,
            Map<byte[], byte[]> jobParameters, long period, EntityId entityId, ITxnIdFactory txnIdFactory,
            DeployedJobSpecEventListener listener) throws Exception {
        long executionMilliseconds = runDeployedJobSpec(distributedId, hcc, null, jobParameters, entityId, txnIdFactory,
                null, listener, null);
        if (executionMilliseconds > period) {
            LOGGER.log(Level.SEVERE,
                    "Periodic job for " + entityId.getExtensionName() + " " + entityId.getDataverseName() + "."
                            + entityId.getEntityName() + " was unable to meet the required period of " + period
                            + " milliseconds. Actually took " + executionMilliseconds + " execution will shutdown"
                            + new Date());
            return false;
        }
        return true;
    }

    public static long runDeployedJobSpec(DeployedJobSpecId distributedId, IHyracksClientConnection hcc,
            IResultSet resultSet, Map<byte[], byte[]> jobParameters, EntityId entityId, ITxnIdFactory txnIdFactory,
            ICcApplicationContext appCtx, DeployedJobSpecEventListener listener, QueryTranslator statementExecutor)
            throws Exception {
        listener.waitWhileAtState(ActivityState.SUSPENDED);

        //Add the Asterix Transaction Id to the map
        jobParameters.put(BADConstants.TRANSACTION_ID_PARAMETER_NAME,
                String.valueOf(txnIdFactory.create().getId()).getBytes());

        long startTime = Instant.now().toEpochMilli();
        JobId jobId = hcc.startJob(distributedId, jobParameters);

        hcc.waitForCompletion(jobId);
        long executionMilliseconds = Instant.now().toEpochMilli() - startTime;

        if (listener.getType() == DeployedJobSpecEventListener.PrecompiledType.QUERY) {
            ResultReader resultReader = new ResultReader(resultSet, jobId, new ResultSetId(0));
            IResponsePrinter printer = statementExecutor.getResponsePrinter();
            printer.addResultPrinter(new ResultsPrinter(appCtx, resultReader, null, new IStatementExecutor.Stats(),
                    statementExecutor.getSessionOutput()));
            printer.printResults();
        }

        LOGGER.log(Level.SEVERE,
                "Deployed Job execution completed for " + entityId.getExtensionName() + " "
                        + entityId.getDataverseName() + "." + entityId.getEntityName() + ". Took "
                        + executionMilliseconds + " milliseconds ");

        return executionMilliseconds;

    }

    public static long findPeriod(String duration) {
        //TODO: Allow Repetitive Channels to use YMD durations
        String hoursMinutesSeconds = "";
        if (duration.indexOf('T') != -1) {
            hoursMinutesSeconds = duration.substring(duration.indexOf('T') + 1);
        }
        double seconds = 0;
        if (hoursMinutesSeconds != "") {
            int pos = 0;
            if (hoursMinutesSeconds.indexOf('H') != -1) {
                Double hours = Double.parseDouble(hoursMinutesSeconds.substring(pos, hoursMinutesSeconds.indexOf('H')));
                seconds += (hours * 60 * 60);
                pos = hoursMinutesSeconds.indexOf('H') + 1;
            }
            if (hoursMinutesSeconds.indexOf('M') != -1) {
                Double minutes =
                        Double.parseDouble(hoursMinutesSeconds.substring(pos, hoursMinutesSeconds.indexOf('M')));
                seconds += (minutes * 60);
                pos = hoursMinutesSeconds.indexOf('M') + 1;
            }
            if (hoursMinutesSeconds.indexOf('S') != -1) {
                Double s = Double.parseDouble(hoursMinutesSeconds.substring(pos, hoursMinutesSeconds.indexOf('S')));
                seconds += (s);
            }
        }
        return (long) (seconds * 1000);
    }

    public static JobSpecification compilePushChannel(IStatementExecutor statementExecutor,
            MetadataProvider metadataProvider, IHyracksClientConnection hcc, Query q) throws Exception {
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        boolean bActiveTxn = true;
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        JobSpecification jobSpec = null;
        try {
            jobSpec = ((QueryTranslator) statementExecutor).rewriteCompileQuery(hcc, metadataProvider, q, null, null,
                    null);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            bActiveTxn = false;
        } catch (Exception e) {
            LOGGER.log(Level.INFO, e.getMessage(), e);
            if (bActiveTxn) {
                ((QueryTranslator) statementExecutor).abort(e, e, mdTxnCtx);
            }
            throw e;
        } finally {
            metadataProvider.getLocks().unlock();
        }
        return jobSpec;
    }

    public static void redeployJobSpec(EntityId entityId, String queryBodyString, MetadataProvider metadataProvider,
            BADStatementExecutor badStatementExecutor, IHyracksClientConnection hcc,
            IRequestParameters requestParameters, boolean useNewId) throws Exception {

        ICcApplicationContext appCtx = metadataProvider.getApplicationContext();
        ActiveNotificationHandler activeEventHandler =
                (ActiveNotificationHandler) appCtx.getActiveNotificationHandler();
        DeployedJobSpecEventListener listener = (DeployedJobSpecEventListener) activeEventHandler.getListener(entityId);
        if (listener == null) {
            LOGGER.severe("Tried to redeploy the job for " + entityId + " but no listener exists.");
            return;
        }

        BADParserFactory factory = new BADParserFactory();
        List<Statement> fStatements = factory.createParser(new StringReader(queryBodyString)).parse();
        JobSpecification jobSpec = null;
        if (listener.getType().equals(DeployedJobSpecEventListener.PrecompiledType.PUSH_CHANNEL)
                || listener.getType().equals(DeployedJobSpecEventListener.PrecompiledType.CHANNEL)) {
            //Channels
            SetStatement ss = (SetStatement) fStatements.get(0);
            metadataProvider.getConfig().put(ss.getPropName(), ss.getPropValue());
            if (listener.getType().equals(DeployedJobSpecEventListener.PrecompiledType.PUSH_CHANNEL)) {
                jobSpec = compilePushChannel(badStatementExecutor, metadataProvider, hcc, (Query) fStatements.get(1));
            } else {
                jobSpec = badStatementExecutor.handleInsertUpsertStatement(metadataProvider, fStatements.get(1), hcc,
                        null, null, null, null, true, null, null, null);
            }
        } else {
            //Procedures
            metadataProvider.setResultSetId(new ResultSetId(0));
            IStatementExecutor.ResultDelivery resultDelivery = requestParameters.getResultProperties().getDelivery();
            IResultSet hdc = requestParameters.getResultSet();
            IStatementExecutor.Stats stats = requestParameters.getStats();
            boolean resultsAsync = resultDelivery == IStatementExecutor.ResultDelivery.ASYNC
                    || resultDelivery == IStatementExecutor.ResultDelivery.DEFERRED;
            metadataProvider.setResultAsyncMode(resultsAsync);
            metadataProvider.setMaxResultReads(1);

            jobSpec = compileProcedureJob(badStatementExecutor, metadataProvider, hcc, hdc, stats, fStatements.get(1));

        }
        if (useNewId) {
            DeployedJobSpecId id = hcc.deployJobSpec(jobSpec);
            listener.setDeployedJobSpecId(id);
        } else {
            hcc.redeployJobSpec(listener.getDeployedJobSpecId(), jobSpec);
        }

        listener.resume();

    }

    public static JobSpecification compileQueryJob(IStatementExecutor statementExecutor,
            MetadataProvider metadataProvider, IHyracksClientConnection hcc, Query q) throws Exception {
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        JobSpecification jobSpec;
        try {
            jobSpec = statementExecutor.rewriteCompileQuery(hcc, metadataProvider, q, null, null, null);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            ((QueryTranslator) statementExecutor).abort(e, e, mdTxnCtx);
            throw e;
        }
        return jobSpec;
    }

    private static JobSpecification compileProcedureJob(IStatementExecutor statementExecutor,
            MetadataProvider metadataProvider, IHyracksClientConnection hcc, IResultSet resultSet,
            IStatementExecutor.Stats stats, Statement procedureStatement) throws Exception {
        if (procedureStatement.getKind() == Statement.Kind.INSERT) {
            return ((QueryTranslator) statementExecutor).handleInsertUpsertStatement(metadataProvider,
                    procedureStatement, hcc, resultSet, IStatementExecutor.ResultDelivery.ASYNC, null, stats, true,
                    null, null, null);
        } else if (procedureStatement.getKind() == Statement.Kind.QUERY) {
            return compileQueryJob(statementExecutor, metadataProvider, hcc, (Query) procedureStatement);
        } else {
            procedureStatement.accept(SqlppDeleteRewriteVisitor.INSTANCE, metadataProvider);
            return ((QueryTranslator) statementExecutor).handleDeleteStatement(metadataProvider, procedureStatement,
                    hcc, true, null, null);
        }
    }

    @Override
    public String toString() {
        return "BADJobService";
    }

}
