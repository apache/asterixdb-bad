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
import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.apache.asterix.app.active.ActiveNotificationHandler;
import org.apache.asterix.app.translator.QueryTranslator;
import org.apache.asterix.bad.BADConstants;
import org.apache.asterix.bad.BADJobService;
import org.apache.asterix.bad.extension.BADLangExtension;
import org.apache.asterix.bad.lang.statement.BrokerDropStatement;
import org.apache.asterix.bad.lang.statement.ChannelDropStatement;
import org.apache.asterix.bad.lang.statement.ProcedureDropStatement;
import org.apache.asterix.bad.metadata.BADMetadataProvider;
import org.apache.asterix.bad.metadata.Broker;
import org.apache.asterix.bad.metadata.Channel;
import org.apache.asterix.bad.metadata.DeployedJobSpecEventListener;
import org.apache.asterix.bad.metadata.Procedure;
import org.apache.asterix.common.api.IResponsePrinter;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.compiler.provider.ILangCompilationProvider;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.statement.CreateFeedStatement;
import org.apache.asterix.lang.common.statement.CreateIndexStatement;
import org.apache.asterix.lang.common.statement.DataverseDropStatement;
import org.apache.asterix.lang.common.statement.DropDatasetStatement;
import org.apache.asterix.lang.common.statement.FunctionDropStatement;
import org.apache.asterix.lang.common.statement.IndexDropStatement;
import org.apache.asterix.lang.common.statement.StartFeedStatement;
import org.apache.asterix.lang.common.statement.StopFeedStatement;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataverse;
import org.apache.asterix.metadata.entities.Feed;
import org.apache.asterix.metadata.entities.Function;
import org.apache.asterix.metadata.feeds.FeedMetadataUtil;
import org.apache.asterix.translator.IRequestParameters;
import org.apache.asterix.translator.SessionOutput;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.api.client.IHyracksClientConnection;

public class BADQueryTranslator extends QueryTranslator {

    public BADQueryTranslator(ICcApplicationContext appCtx, List<Statement> statements, SessionOutput output,
            ILangCompilationProvider compliationProvider, ExecutorService executorService, IResponsePrinter printer) {
        super(appCtx, statements, output, compliationProvider, executorService, printer);
    }

    //TODO: Most of this file could go away if we had metadata dependencies

    private Pair<List<Channel>, List<Procedure>> checkIfDatasetIsInUse(MetadataTransactionContext mdTxnCtx,
            DataverseName dataverse, String dataset, boolean checkAll) throws AlgebricksException {
        List<Channel> channelsUsingDataset = new ArrayList<>();
        List<Procedure> proceduresUsingDataset = new ArrayList<>();
        List<Channel> channels = BADLangExtension.getAllChannels(mdTxnCtx);
        for (Channel channel : channels) {
            List<List<Triple<DataverseName, String, String>>> dependencies = channel.getDependencies();
            List<Triple<DataverseName, String, String>> datasetDependencies = dependencies.get(0);
            for (Triple<DataverseName, String, String> dependency : datasetDependencies) {
                if (dependency.first.equals(dataverse) && dependency.second.equals(dataset)) {
                    channelsUsingDataset.add(channel);
                    if (!checkAll) {
                        return new Pair<>(channelsUsingDataset, proceduresUsingDataset);
                    }
                }
            }

        }
        List<Procedure> procedures = BADLangExtension.getAllProcedures(mdTxnCtx);
        for (Procedure procedure : procedures) {
            List<List<Triple<DataverseName, String, String>>> dependencies = procedure.getDependencies();
            List<Triple<DataverseName, String, String>> datasetDependencies = dependencies.get(0);
            for (Triple<DataverseName, String, String> dependency : datasetDependencies) {
                if (dependency.first.equals(dataverse) && dependency.second.equals(dataset)) {
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
            DataverseName dvId, String function, String arity, boolean checkAll) throws AlgebricksException {
        List<Channel> channelsUsingFunction = new ArrayList<>();
        List<Procedure> proceduresUsingFunction = new ArrayList<>();

        List<Channel> channels = BADLangExtension.getAllChannels(mdTxnCtx);
        for (Channel channel : channels) {
            List<List<Triple<DataverseName, String, String>>> dependencies = channel.getDependencies();
            List<Triple<DataverseName, String, String>> datasetDependencies = dependencies.get(1);
            for (Triple<DataverseName, String, String> dependency : datasetDependencies) {
                if (dependency.first.equals(dvId) && dependency.second.equals(function)
                        && dependency.third.equals(arity)) {
                    channelsUsingFunction.add(channel);
                    if (!checkAll) {
                        return new Pair<>(channelsUsingFunction, proceduresUsingFunction);
                    }
                }
            }

        }
        List<Procedure> procedures = BADLangExtension.getAllProcedures(mdTxnCtx);
        for (Procedure procedure : procedures) {
            List<List<Triple<DataverseName, String, String>>> dependencies = procedure.getDependencies();
            List<Triple<DataverseName, String, String>> datasetDependencies = dependencies.get(1);
            for (Triple<DataverseName, String, String> dependency : datasetDependencies) {
                if (dependency.first.equals(dvId) && dependency.second.equals(function)
                        && dependency.third.equals(arity)) {
                    proceduresUsingFunction.add(procedure);
                    if (!checkAll) {
                        return new Pair<>(channelsUsingFunction, proceduresUsingFunction);
                    }
                }
            }

        }
        return new Pair<>(channelsUsingFunction, proceduresUsingFunction);
    }

    private void throwErrorIfDatasetUsed(MetadataTransactionContext mdTxnCtx, DataverseName dataverse, String dataset)
            throws AlgebricksException {
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

    private void throwErrorIfFunctionUsed(MetadataTransactionContext mdTxnCtx, DataverseName dataverse, String function,
            String arity, FunctionSignature sig) throws AlgebricksException {
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
    protected void handleCreateFeedStatement(MetadataProvider metadataProvider, Statement stmt) throws Exception {
        CreateFeedStatement cfs = (CreateFeedStatement) stmt;
        Map<String, String> feedConfig = cfs.getConfiguration();
        if (feedConfig.containsKey(BADConstants.BAD_FEED_FIELD_NAME_HOST)) {

            // check parameters
            if (!feedConfig.containsKey(BADConstants.BAD_FEED_FIELD_NAME_HOST)
                    || !feedConfig.containsKey(BADConstants.BAD_FEED_FIELD_NAME_CHANNEL)
                    || !feedConfig.containsKey(BADConstants.BAD_FEED_FIELD_NAME_PARAMETERS)
                    || !feedConfig.containsKey(BADConstants.BAD_FEED_FIELD_NAME_CHANNEL_DV)) {
                throw new AlgebricksException(
                        "A BAD feed requires the host, dataverse name, channel name, and channel parameters of the other BAD system.");
            }

            // check format and http feed
            if (!feedConfig.containsKey(ExternalDataConstants.KEY_ADAPTER_NAME)
                    || !feedConfig.get(ExternalDataConstants.KEY_ADAPTER_NAME).toLowerCase().equals("http_adapter")) {
                throw new AlgebricksException("A BAD feed needs a http adapter.");
            }
            if (!feedConfig.containsKey(ExternalDataConstants.KEY_FORMAT)
                    || !feedConfig.get(ExternalDataConstants.KEY_FORMAT).toLowerCase().equals("adm")) {
                throw new AlgebricksException("A BAD feed requires incoming data to be in ADM format.");
            }
            if (!feedConfig.containsKey(ExternalDataConstants.KEY_MODE)
                    || !feedConfig.get(ExternalDataConstants.KEY_MODE).toLowerCase().equals("ip")) {
                throw new AlgebricksException("A BAD feed requires an IP address.");
            }
        }
        super.handleCreateFeedStatement(metadataProvider, stmt);
    }

    @Override
    protected void handleStartFeedStatement(MetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc) throws Exception {
        StartFeedStatement sfs = (StartFeedStatement) stmt;
        DataverseName dataverseName = getActiveDataverseName(sfs.getDataverseName());
        String feedName = sfs.getFeedName().getValue();

        // Retrieve Feed entity from Metadata
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        Feed feed = FeedMetadataUtil.validateIfFeedExists(dataverseName, feedName,
                metadataProvider.getMetadataTxnContext());
        MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);

        // If it's a BAD feed
        Map<String, String> feedConfig = feed.getConfiguration();
        if (feedConfig.containsKey(BADConstants.BAD_FEED_FIELD_NAME_HOST)) {
            String badHost = feedConfig.getOrDefault(BADConstants.BAD_FEED_FIELD_NAME_HOST, null);
            String badChannelName = feedConfig.getOrDefault(BADConstants.BAD_FEED_FIELD_NAME_CHANNEL, null);
            String badParameters = feedConfig.getOrDefault(BADConstants.BAD_FEED_FIELD_NAME_PARAMETERS, null);
            String badDataverseName = feedConfig.getOrDefault(BADConstants.BAD_FEED_FIELD_NAME_CHANNEL_DV, null);

            // construct statements
            // add Broker to feed name as the broker name
            try {
                // create broker
                String connStmtStr = String.format(
                        "USE %s;\n"
                                + "DROP BROKER %BROKER IF EXISTS; CREATE BROKER %sBroker AT \"http://%s\" with {\"broker-type\" : \"BAD\"};\n",
                        badDataverseName, feed.getFeedName(), feed.getFeedName(),
                        feed.getConfiguration().get("addresses"));
                BADLangUtils.executeStatement(badHost, connStmtStr);

                // create subs
                String[] params = badParameters.split(";");
                StringBuilder subStmtStr = new StringBuilder(String.format("USE %s;\n ", badDataverseName));
                for (String param : params) {
                    subStmtStr.append(String.format("SUBSCRIBE TO %s(%s) on %sBroker;", badChannelName, param,
                            feed.getFeedName()));
                }
                BADLangUtils.executeStatement(badHost, subStmtStr.toString());
            } catch (Exception e) {
                // drop broker and all subs if anything goes wrong
                String dropStmtStr = String.format(
                        "USE %s;\n"
                                + "DELETE FROM %sSubscriptions s WHERE s.BrokerName = \"%sBroker\"; DROP BROKER %sBroker",
                        badDataverseName, badChannelName, feed.getFeedName(), feed.getFeedName());
                BADLangUtils.executeStatement(badHost, dropStmtStr);
                throw e;
            }
        }
        MetadataProvider badMetadataProvider = BADMetadataProvider.create(metadataProvider.getApplicationContext(),
                metadataProvider.getDefaultDataverse());
        super.handleStartFeedStatement(badMetadataProvider, stmt, hcc);
    }

    @Override
    protected void handleStopFeedStatement(MetadataProvider metadataProvider, Statement stmt) throws Exception {
        StopFeedStatement sfst = (StopFeedStatement) stmt;
        DataverseName dataverseName = getActiveDataverseName(sfst.getDataverseName());
        String feedName = sfst.getFeedName().getValue();

        // Retrieve Feed entity from Metadata
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        Feed feed = FeedMetadataUtil.validateIfFeedExists(dataverseName, feedName,
                metadataProvider.getMetadataTxnContext());
        MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);

        Map<String, String> feedConfig = feed.getConfiguration();
        if (feedConfig.containsKey(BADConstants.BAD_FEED_FIELD_NAME_HOST)) {
            String badHost = feedConfig.getOrDefault(BADConstants.BAD_FEED_FIELD_NAME_HOST, null);
            String badChannelName = feedConfig.getOrDefault(BADConstants.BAD_FEED_FIELD_NAME_CHANNEL, null);
            String badDataverseName = feedConfig.getOrDefault(BADConstants.BAD_FEED_FIELD_NAME_CHANNEL_DV, null);

            // construct statements
            String dropStmtStr = String.format(
                    "USE %s;\n"
                            + "DELETE FROM %sSubscriptions s WHERE s.BrokerName = \"%sBroker\"; DROP BROKER %sBroker",
                    badDataverseName, badChannelName, feed.getFeedName(), feed.getFeedName());

            // make request
            int responseCode = BADLangUtils.executeStatement(badHost, dropStmtStr);
            if (responseCode != 200) {
                throw new AlgebricksException("Connecting to " + badHost + " failed");
            }
        }
    }

    @Override
    public void handleDatasetDropStatement(MetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc, IRequestParameters requestParameters) throws Exception {
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        DataverseName dvId = getActiveDataverseName(((DropDatasetStatement) stmt).getDataverseName());
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
        DataverseName dvId = getActiveDataverseName(((CreateIndexStatement) stmt).getDataverseName());
        String dsId = ((CreateIndexStatement) stmt).getDatasetName().getValue();

        Pair<List<Channel>, List<Procedure>> usages = checkIfDatasetIsInUse(mdTxnCtx, dvId, dsId, true);

        List<Dataverse> dataverseList = MetadataManager.INSTANCE.getDataverses(mdTxnCtx);
        for (Dataverse dv : dataverseList) {
            List<Function> functions = MetadataManager.INSTANCE.getDataverseFunctions(mdTxnCtx, dv.getDataverseName());
            for (Function function : functions) {
                for (Triple<DataverseName, String, String> datasetDependency : function.getDependencies().get(0)) {
                    if (datasetDependency.first.equals(dvId) && datasetDependency.second.equals(dsId)) {
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

        metadataProvider = MetadataProvider.create(appCtx, activeDataverse);
        super.handleCreateIndexStatement(metadataProvider, stmt, hcc, requestParameters);

        for (Channel channel : usages.first) {
            metadataProvider = MetadataProvider.create(appCtx, activeDataverse);
            BADJobService.redeployJobSpec(channel.getChannelId(), channel.getChannelBody(), metadataProvider, this, hcc,
                    requestParameters, false);
            metadataProvider.getLocks().unlock();
        }
        for (Procedure procedure : usages.second) {
            metadataProvider = MetadataProvider.create(appCtx, activeDataverse);
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
        DataverseName dvId = getActiveDataverseName(((IndexDropStatement) stmt).getDataverseName());
        Identifier dsId = ((IndexDropStatement) stmt).getDatasetName();

        throwErrorIfDatasetUsed(mdTxnCtx, dvId, dsId.getValue());

        List<Dataverse> dataverseList = MetadataManager.INSTANCE.getDataverses(mdTxnCtx);
        for (Dataverse dv : dataverseList) {
            List<Function> functions = MetadataManager.INSTANCE.getDataverseFunctions(mdTxnCtx, dv.getDataverseName());
            for (Function function : functions) {
                for (Triple<DataverseName, String, String> datasetDependency : function.getDependencies().get(0)) {
                    if (datasetDependency.first.equals(dvId) && datasetDependency.second.equals(dsId.getValue())) {
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

        DataverseName dvId = getActiveDataverseName(sig.getDataverseName());
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
        DataverseName dvId = ((DataverseDropStatement) stmt).getDataverseName();
        MetadataProvider tempMdProvider = MetadataProvider.create(appCtx, metadataProvider.getDefaultDataverse());
        tempMdProvider.getConfig().putAll(metadataProvider.getConfig());
        List<Channel> channels = BADLangExtension.getAllChannels(mdTxnCtx);
        for (Channel channel : channels) {
            if (channel.getChannelId().getDataverseName().equals(dvId)) {
                continue;
            }
            List<List<Triple<DataverseName, String, String>>> dependencies = channel.getDependencies();
            for (List<Triple<DataverseName, String, String>> dependencyList : dependencies) {
                for (Triple<DataverseName, String, String> dependency : dependencyList) {
                    if (dependency.first.equals(dvId)) {
                        throw new CompilationException(
                                "Cannot drop dataverse " + dvId + ". " + channel.getChannelId() + " depends on it!");
                    }
                }
            }
        }
        List<Procedure> procedures = BADLangExtension.getAllProcedures(mdTxnCtx);
        for (Procedure procedure : procedures) {
            if (procedure.getEntityId().getDataverseName().equals(dvId)) {
                continue;
            }
            List<List<Triple<DataverseName, String, String>>> dependencies = procedure.getDependencies();
            for (List<Triple<DataverseName, String, String>> dependencyList : dependencies) {
                for (Triple<DataverseName, String, String> dependency : dependencyList) {
                    if (dependency.first.equals(dvId)) {
                        throw new CompilationException(
                                "Cannot drop dataverse " + dvId + ". " + procedure.getEntityId() + " depends on it!");
                    }
                }
            }
        }
        for (Channel channel : channels) {
            if (!channel.getChannelId().getDataverseName().equals(dvId)) {
                continue;
            }
            tempMdProvider.getLocks().reset();
            ChannelDropStatement drop =
                    new ChannelDropStatement(dvId, new Identifier(channel.getChannelId().getEntityName()), false);
            drop.handle(hcc, this, requestParameters, tempMdProvider, 0);
        }
        for (Procedure procedure : procedures) {
            if (!procedure.getEntityId().getDataverseName().equals(dvId)) {
                continue;
            }
            tempMdProvider.getLocks().reset();
            ProcedureDropStatement drop = new ProcedureDropStatement(
                    new FunctionSignature(dvId, procedure.getEntityId().getEntityName(), procedure.getArity()), false);
            drop.handle(hcc, this, requestParameters, tempMdProvider, 0);
        }
        List<Broker> brokers = BADLangExtension.getBrokers(mdTxnCtx, dvId);
        for (Broker broker : brokers) {
            tempMdProvider.getLocks().reset();
            BrokerDropStatement drop = new BrokerDropStatement(dvId, new Identifier(broker.getBrokerName()), false);
            drop.handle(hcc, this, requestParameters, tempMdProvider, 0);
        }
        MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        super.handleDataverseDropStatement(metadataProvider, stmt, hcc, requestParameters);
    }

}
