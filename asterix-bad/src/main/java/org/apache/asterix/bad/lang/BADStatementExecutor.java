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
package org.apache.asterix.bad.lang;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.asterix.app.active.ActiveNotificationHandler;
import org.apache.asterix.app.translator.QueryTranslator;
import org.apache.asterix.app.translator.RequestParameters;
import org.apache.asterix.bad.BADJobService;
import org.apache.asterix.bad.lang.statement.BrokerDropStatement;
import org.apache.asterix.bad.lang.statement.ChannelDropStatement;
import org.apache.asterix.bad.lang.statement.ProcedureDropStatement;
import org.apache.asterix.bad.metadata.Broker;
import org.apache.asterix.bad.metadata.Channel;
import org.apache.asterix.bad.metadata.DeployedJobSpecEventListener;
import org.apache.asterix.bad.metadata.Procedure;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.compiler.provider.ILangCompilationProvider;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.statement.CreateIndexStatement;
import org.apache.asterix.lang.common.statement.DataverseDropStatement;
import org.apache.asterix.lang.common.statement.DropDatasetStatement;
import org.apache.asterix.lang.common.statement.FunctionDropStatement;
import org.apache.asterix.lang.common.statement.IndexDropStatement;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataverse;
import org.apache.asterix.metadata.entities.Function;
import org.apache.asterix.translator.IRequestParameters;
import org.apache.asterix.translator.SessionOutput;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.client.IHyracksClientConnection;

public class BADStatementExecutor extends QueryTranslator {

    public BADStatementExecutor(ICcApplicationContext appCtx, List<Statement> statements, SessionOutput output,
            ILangCompilationProvider compliationProvider, ExecutorService executorService) {
        super(appCtx, statements, output, compliationProvider, executorService);
    }

    //TODO: Most of this file could go away if we had metadata dependencies

    private Pair<List<Channel>, List<Procedure>> checkIfDatasetIsInUse(MetadataTransactionContext mdTxnCtx,
            String dataverse, String dataset, boolean checkAll) throws AlgebricksException {
        List<Channel> channelsUsingDataset = new ArrayList<>();
        List<Procedure> proceduresUsingDataset = new ArrayList<>();
        List<Channel> channels = BADLangExtension.getAllChannels(mdTxnCtx);
        for (Channel channel : channels) {
            List<List<List<String>>> dependencies = channel.getDependencies();
            List<List<String>> datasetDependencies = dependencies.get(0);
            for (List<String> dependency : datasetDependencies) {
                if (dependency.get(0).equals(dataverse) && dependency.get(1).equals(dataset)) {
                    channelsUsingDataset.add(channel);
                    if (!checkAll) {
                        return new Pair<>(channelsUsingDataset, proceduresUsingDataset);
                    }

                }
            }

        }
        List<Procedure> procedures = BADLangExtension.getAllProcedures(mdTxnCtx);
        for (Procedure procedure : procedures) {
            List<List<List<String>>> dependencies = procedure.getDependencies();
            List<List<String>> datasetDependencies = dependencies.get(0);
            for (List<String> dependency : datasetDependencies) {
                if (dependency.get(0).equals(dataverse) && dependency.get(1).equals(dataset)) {
                    proceduresUsingDataset.add(procedure);
                    if (!checkAll) {
                        return new Pair<>(channelsUsingDataset, proceduresUsingDataset);
                    }
                }
            }

        }
        return new Pair<>(channelsUsingDataset, proceduresUsingDataset);
    }

    private Pair<List<Channel>, List<Procedure>> checkIfFunctionIsInUse(MetadataTransactionContext mdTxnCtx,
            String dvId, String function, String arity, boolean checkAll)
            throws CompilationException, AlgebricksException {
        List<Channel> channelsUsingFunction = new ArrayList<>();
        List<Procedure> proceduresUsingFunction = new ArrayList<>();

        List<Channel> channels = BADLangExtension.getAllChannels(mdTxnCtx);
        for (Channel channel : channels) {
            List<List<List<String>>> dependencies = channel.getDependencies();
            List<List<String>> datasetDependencies = dependencies.get(1);
            for (List<String> dependency : datasetDependencies) {
                if (dependency.get(0).equals(dvId) && dependency.get(1).equals(function)
                        && dependency.get(2).equals(arity)) {
                    channelsUsingFunction.add(channel);
                    if (!checkAll) {
                        return new Pair<>(channelsUsingFunction, proceduresUsingFunction);
                    }
                }
            }

        }
        List<Procedure> procedures = BADLangExtension.getAllProcedures(mdTxnCtx);
        for (Procedure procedure : procedures) {
            List<List<List<String>>> dependencies = procedure.getDependencies();
            List<List<String>> datasetDependencies = dependencies.get(1);
            for (List<String> dependency : datasetDependencies) {
                if (dependency.get(0).equals(dvId) && dependency.get(1).equals(function)
                        && dependency.get(2).equals(arity)) {
                    proceduresUsingFunction.add(procedure);
                    if (!checkAll) {
                        return new Pair<>(channelsUsingFunction, proceduresUsingFunction);
                    }
                }
            }

        }
        return new Pair<>(channelsUsingFunction, proceduresUsingFunction);
    }

    private void throwErrorIfDatasetUsed(MetadataTransactionContext mdTxnCtx, String dataverse, String dataset)
            throws CompilationException, AlgebricksException {
        Pair<List<Channel>, List<Procedure>> dependents = checkIfDatasetIsInUse(mdTxnCtx, dataverse, dataset, false);
        if (dependents.first.size() > 0) {
            throw new CompilationException("Cannot alter dataset " + dataverse + "." + dataset + ". "
                    + dependents.first.get(0).getChannelId() + " depends on it!");
        }
        if (dependents.second.size() > 0) {
            throw new CompilationException("Cannot alter dataset " + dataverse + "." + dataset + ". "
                    + dependents.second.get(0).getEntityId() + " depends on it!");
        }
    }

    private void throwErrorIfFunctionUsed(MetadataTransactionContext mdTxnCtx, String dataverse, String function,
            String arity, FunctionSignature sig) throws CompilationException, AlgebricksException {
        Pair<List<Channel>, List<Procedure>> dependents =
                checkIfFunctionIsInUse(mdTxnCtx, dataverse, function, arity, false);
        String errorStart = sig != null ? "Cannot drop function " + sig + "." : "Cannot drop index.";
        if (dependents.first.size() > 0) {
            throw new CompilationException(
                    errorStart + " " + dependents.first.get(0).getChannelId() + " depends on it!");
        }
        if (dependents.second.size() > 0) {
            throw new CompilationException(
                    errorStart + " " + dependents.second.get(0).getEntityId() + " depends on it!");
        }
    }

    @Override
    public void handleDatasetDropStatement(MetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc, IRequestParameters requestParameters) throws Exception {
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        String dvId = getActiveDataverse(((DropDatasetStatement) stmt).getDataverseName());
        Identifier dsId = ((DropDatasetStatement) stmt).getDatasetName();

        throwErrorIfDatasetUsed(mdTxnCtx, dvId, dsId.getValue());

        MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        super.handleDatasetDropStatement(metadataProvider, stmt, hcc, requestParameters);
    }

    @Override
    public void handleCreateIndexStatement(MetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc, IRequestParameters requestParameters) throws Exception {

        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        //Allow channels to use the new index
        String dvId = getActiveDataverse(((CreateIndexStatement) stmt).getDataverseName());
        String dsId = ((CreateIndexStatement) stmt).getDatasetName().getValue();

        Pair<List<Channel>, List<Procedure>> usages = checkIfDatasetIsInUse(mdTxnCtx, dvId, dsId, true);

        List<Dataverse> dataverseList = MetadataManager.INSTANCE.getDataverses(mdTxnCtx);
        for (Dataverse dv : dataverseList) {
            List<Function> functions = MetadataManager.INSTANCE.getFunctions(mdTxnCtx, dv.getDataverseName());
            for (Function function : functions) {
                for (List<String> datasetDependency : function.getDependencies().get(0)) {
                    if (datasetDependency.get(0).equals(dvId) && datasetDependency.get(1).equals(dsId)) {
                        Pair<List<Channel>, List<Procedure>> functionUsages =
                                checkIfFunctionIsInUse(mdTxnCtx, function.getDataverseName(), function.getName(),
                                        Integer.toString(function.getArity()), true);
                        for (Channel channel : functionUsages.first) {
                            if (!usages.first.contains(channel)) {
                                usages.first.add(channel);
                            }
                        }
                        for (Procedure procedure : functionUsages.second) {
                            if (!usages.second.contains(procedure)) {
                                usages.second.add(procedure);
                            }
                        }
                    }
                }
            }
        }

        ActiveNotificationHandler activeEventHandler =
                (ActiveNotificationHandler) appCtx.getActiveNotificationHandler();

        for (Channel channel : usages.first) {
            DeployedJobSpecEventListener listener =
                    (DeployedJobSpecEventListener) activeEventHandler.getListener(channel.getChannelId());
            listener.suspend();
        }
        for (Procedure procedure : usages.second) {
            DeployedJobSpecEventListener listener =
                    (DeployedJobSpecEventListener) activeEventHandler.getListener(procedure.getEntityId());
            listener.suspend();
        }

        MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        metadataProvider.getLocks().unlock();

        metadataProvider = new MetadataProvider(appCtx, activeDataverse);
        super.handleCreateIndexStatement(metadataProvider, stmt, hcc, requestParameters);

        for (Channel channel : usages.first) {
            metadataProvider = new MetadataProvider(appCtx, activeDataverse);
            BADJobService.redeployJobSpec(channel.getChannelId(), channel.getChannelBody(), metadataProvider, this, hcc,
                    requestParameters, false);
            metadataProvider.getLocks().unlock();
        }
        for (Procedure procedure : usages.second) {
            metadataProvider = new MetadataProvider(appCtx, activeDataverse);
            BADJobService.redeployJobSpec(procedure.getEntityId(), procedure.getBody(), metadataProvider, this, hcc,
                    requestParameters, false);
            metadataProvider.getLocks().unlock();
        }

    }

    @Override
    protected void handleIndexDropStatement(MetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc, IRequestParameters requestParameters) throws Exception {
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        String dvId = getActiveDataverse(((IndexDropStatement) stmt).getDataverseName());
        Identifier dsId = ((IndexDropStatement) stmt).getDatasetName();

        throwErrorIfDatasetUsed(mdTxnCtx, dvId, dsId.getValue());

        List<Dataverse> dataverseList = MetadataManager.INSTANCE.getDataverses(mdTxnCtx);
        for (Dataverse dv : dataverseList) {
            List<Function> functions = MetadataManager.INSTANCE.getFunctions(mdTxnCtx, dv.getDataverseName());
            for (Function function : functions) {
                for (List<String> datasetDependency : function.getDependencies().get(0)) {
                    if (datasetDependency.get(0).equals(dvId) && datasetDependency.get(1).equals(dsId.getValue())) {
                        throwErrorIfFunctionUsed(mdTxnCtx, function.getDataverseName(), function.getName(),
                                Integer.toString(function.getArity()), null);
                    }
                }
            }
        }

        MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        super.handleIndexDropStatement(metadataProvider, stmt, hcc, requestParameters);
    }

    @Override
    protected void handleFunctionDropStatement(MetadataProvider metadataProvider, Statement stmt) throws Exception {
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        FunctionSignature sig = ((FunctionDropStatement) stmt).getFunctionSignature();

        String dvId = getActiveDataverseName(sig.getNamespace());
        String function = sig.getName();
        String arity = Integer.toString(sig.getArity());

        throwErrorIfFunctionUsed(mdTxnCtx, dvId, function, arity, sig);

        MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        super.handleFunctionDropStatement(metadataProvider, stmt);
    }

    @Override
    protected void handleDataverseDropStatement(MetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc, IRequestParameters requestParameters) throws Exception {
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        Identifier dvId = ((DataverseDropStatement) stmt).getDataverseName();
        MetadataProvider tempMdProvider = new MetadataProvider(appCtx, metadataProvider.getDefaultDataverse());
        tempMdProvider.getConfig().putAll(metadataProvider.getConfig());
        List<Channel> channels = BADLangExtension.getAllChannels(mdTxnCtx);
        for (Channel channel : channels) {
            if (channel.getChannelId().getDataverse().equals(dvId.getValue())) {
                continue;
            }
            List<List<List<String>>> dependencies = channel.getDependencies();
            for (List<List<String>> dependencyList : dependencies) {
                for (List<String> dependency : dependencyList) {
                    if (dependency.get(0).equals(dvId.getValue())) {
                        throw new CompilationException("Cannot drop dataverse " + dvId.getValue() + ". "
                                + channel.getChannelId() + " depends on it!");
                    }
                }
            }
        }
        List<Procedure> procedures = BADLangExtension.getAllProcedures(mdTxnCtx);
        for (Procedure procedure : procedures) {
            if (procedure.getEntityId().getDataverse().equals(dvId.getValue())) {
                continue;
            }
            List<List<List<String>>> dependencies = procedure.getDependencies();
            for (List<List<String>> dependencyList : dependencies) {
                for (List<String> dependency : dependencyList) {
                    if (dependency.get(0).equals(dvId.getValue())) {
                        throw new CompilationException("Cannot drop dataverse " + dvId.getValue() + ". "
                                + procedure.getEntityId() + " depends on it!");
                    }
                }
            }
        }
        for (Channel channel : channels) {
            if (!channel.getChannelId().getDataverse().equals(dvId.getValue())) {
                continue;
            }
            tempMdProvider.getLocks().reset();
            ChannelDropStatement drop =
                    new ChannelDropStatement(dvId, new Identifier(channel.getChannelId().getEntityName()), false);
            drop.handle(hcc, this, requestParameters, tempMdProvider, 0, null);
        }
        for (Procedure procedure : procedures) {
            if (!procedure.getEntityId().getDataverse().equals(dvId.getValue())) {
                continue;
            }
            tempMdProvider.getLocks().reset();
            ProcedureDropStatement drop = new ProcedureDropStatement(new FunctionSignature(dvId.getValue(),
                    procedure.getEntityId().getEntityName(), procedure.getArity()), false);
            drop.handle(hcc, this, requestParameters, tempMdProvider, 0, null);
        }
        List<Broker> brokers = BADLangExtension.getBrokers(mdTxnCtx, dvId.getValue());
        for (Broker broker : brokers) {
            tempMdProvider.getLocks().reset();
            BrokerDropStatement drop = new BrokerDropStatement(dvId, new Identifier(broker.getBrokerName()), false);
            drop.handle(hcc, this, requestParameters, tempMdProvider, 0, null);
        }
        MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        super.handleDataverseDropStatement(metadataProvider, stmt, hcc, requestParameters);
    }

}
