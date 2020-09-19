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
package org.apache.asterix.bad.lang.statement;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.active.EntityId;
import org.apache.asterix.algebra.extension.ExtensionStatement;
import org.apache.asterix.app.active.ActiveNotificationHandler;
import org.apache.asterix.app.translator.QueryTranslator;
import org.apache.asterix.bad.BADConstants;
import org.apache.asterix.bad.BADJobService;
import org.apache.asterix.bad.BADUtils;
import org.apache.asterix.bad.extension.BADLangExtension;
import org.apache.asterix.bad.lang.BADParserFactory;
import org.apache.asterix.bad.metadata.Channel;
import org.apache.asterix.bad.metadata.DeployedJobSpecEventListener;
import org.apache.asterix.bad.metadata.DeployedJobSpecEventListener.PrecompiledType;
import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.config.DatasetConfig.IndexType;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.IndexedTypeExpression;
import org.apache.asterix.lang.common.expression.TypeExpression;
import org.apache.asterix.lang.common.expression.TypeReferenceExpression;
import org.apache.asterix.lang.common.statement.CreateIndexStatement;
import org.apache.asterix.lang.common.statement.DatasetDecl;
import org.apache.asterix.lang.common.statement.IDatasetDetailsDecl;
import org.apache.asterix.lang.common.statement.InternalDetailsDecl;
import org.apache.asterix.lang.common.statement.Query;
import org.apache.asterix.lang.common.statement.SetStatement;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.lang.sqlpp.util.SqlppStatementUtil;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.utils.MetadataConstants;
import org.apache.asterix.translator.IRequestParameters;
import org.apache.asterix.translator.IStatementExecutor;
import org.apache.asterix.translator.IStatementExecutor.ResultDelivery;
import org.apache.asterix.translator.IStatementExecutor.Stats;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.api.result.IResultSet;

public abstract class AbstractCreateChannelStatement extends ExtensionStatement {

    private static final Logger LOGGER = Logger.getLogger(AbstractCreateChannelStatement.class.getName());
    protected String duration;
    protected FunctionSignature function;
    protected final CallExpr period;
    private DataverseName dataverseName;
    private String body;
    private String subscriptionsTableName;
    private String resultsTableName;
    private final boolean push;
    private final Identifier channelName;

    public AbstractCreateChannelStatement(DataverseName dataverseName, Identifier channelName, Expression period,
            boolean push) {
        this.channelName = channelName;
        this.dataverseName = dataverseName;
        this.period = (CallExpr) period;
        this.duration = "";
        this.push = push;
    }

    public DataverseName getDataverseName() {
        return dataverseName;
    }

    public String getDuration() {
        return duration;
    }

    public FunctionSignature getFunction() {
        return function;
    }

    public Expression getPeriod() {
        return period;
    }

    @Override
    public byte getCategory() {
        return Category.DDL;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return null;
    }

    protected abstract void initialize(IStatementExecutor statementExecutor, MetadataProvider metadataProvider,
            MetadataTransactionContext mdTxnCtx) throws Exception;

    private void createDatasets(IStatementExecutor statementExecutor, MetadataProvider metadataProvider,
            IHyracksClientConnection hcc) throws Exception {

        Identifier subscriptionsTypeName = new Identifier(BADConstants.METADATA_TYPENAME_SUBSCRIPTIONS);
        Identifier resultsTypeName = new Identifier(BADConstants.ChannelResultsType);
        //Setup the subscriptions dataset
        List<List<String>> partitionFields = new ArrayList<>();
        List<Integer> keyIndicators = new ArrayList<>();
        keyIndicators.add(0);
        List<String> fieldNames = new ArrayList<>();
        fieldNames.add(BADConstants.SubscriptionId);
        partitionFields.add(fieldNames);
        IDatasetDetailsDecl idd = new InternalDetailsDecl(partitionFields, keyIndicators, true, null, null);
        TypeExpression subItemType = new TypeReferenceExpression(
                new Pair<>(MetadataConstants.METADATA_DATAVERSE_NAME, subscriptionsTypeName));
        DatasetDecl createSubscriptionsDataset = new DatasetDecl(dataverseName, new Identifier(subscriptionsTableName),
                subItemType, null, new HashMap<>(), DatasetType.INTERNAL, idd, null, true);

        ((QueryTranslator) statementExecutor).handleCreateDatasetStatement(metadataProvider, createSubscriptionsDataset,
                hcc, null);

        if (!push) {
            //Setup the results dataset
            partitionFields = new ArrayList<>();
            fieldNames = new ArrayList<>();
            fieldNames.add(BADConstants.ResultId);
            partitionFields.add(fieldNames);
            idd = new InternalDetailsDecl(partitionFields, keyIndicators, true, null, null);
            TypeExpression resultItemType =
                    new TypeReferenceExpression(new Pair<>(MetadataConstants.METADATA_DATAVERSE_NAME, resultsTypeName));
            DatasetDecl createResultsDataset = new DatasetDecl(dataverseName, new Identifier(resultsTableName),
                    resultItemType, null, new HashMap<>(), DatasetType.INTERNAL, idd, null, true);

            //Create an index on timestamp for results
            CreateIndexStatement createTimeIndex = new CreateIndexStatement();
            createTimeIndex.setDatasetName(new Identifier(resultsTableName));
            createTimeIndex.setDataverseName(dataverseName);
            createTimeIndex.setIndexName(new Identifier(resultsTableName + "TimeIndex"));
            createTimeIndex.setIfNotExists(false);
            createTimeIndex.setIndexType(IndexType.BTREE);
            createTimeIndex.setEnforced(false);
            createTimeIndex.setGramLength(0);
            List<String> fNames = new ArrayList<>();
            fNames.add(BADConstants.ChannelExecutionTime);
            Pair<List<String>, IndexedTypeExpression> fields = new Pair<>(fNames, null);
            createTimeIndex.addFieldExprPair(fields);
            createTimeIndex.addFieldIndexIndicator(0);
            metadataProvider.getLocks().reset();
            ((QueryTranslator) statementExecutor).handleCreateDatasetStatement(metadataProvider, createResultsDataset,
                    hcc, null);
            metadataProvider.getLocks().reset();

            //Create a time index for the results
            ((QueryTranslator) statementExecutor).handleCreateIndexStatement(metadataProvider, createTimeIndex, hcc,
                    null);

        }

    }

    private JobSpecification createChannelJob(IStatementExecutor statementExecutor, MetadataProvider metadataProvider,
            IHyracksClientConnection hcc, IResultSet resultSet, Stats stats) throws Exception {
        StringBuilder builder = new StringBuilder();
        CharSequence dataverse = SqlppStatementUtil.encloseDataverseName(new StringBuilder(), dataverseName);
        builder.append("SET inline_with \"false\";\n");
        if (!push) {
            builder.append("insert into " + dataverse + "." + resultsTableName);
            builder.append(" as a (\n");
        }
        builder.append("with " + BADConstants.ChannelExecutionTime + " as current_datetime() \n");
        builder.append("select result, ");
        builder.append(BADConstants.ChannelExecutionTime + ", ");
        builder.append("sub." + BADConstants.SubscriptionId + " as " + BADConstants.SubscriptionId + ",");
        // builder.append("b." + BADConstants.METADATA_TYPE_FIELD_NAME_BROKER_TYPE + " as "
        //         + BADConstants.METADATA_TYPE_FIELD_NAME_BROKER_TYPE + ",");
        // builder.append("b." + BADConstants.METADATA_TYPE_FIELD_NAME_BROKER_END_POINT + " as " + BADConstants.METADATA_TYPE_FIELD_NAME_BROKER_END_POINT + ",");
        builder.append("current_datetime() as " + BADConstants.DeliveryTime + "\n");
        builder.append("from " + dataverse + "." + subscriptionsTableName + " sub,\n");
        builder.append(
                MetadataConstants.METADATA_DATAVERSE_NAME + ".`" + BADConstants.METADATA_DATASET_BROKER + "` b, \n");
        builder.append(function.getDataverseName() + "." + function.getName() + "(");
        for (int iter1 = 0; iter1 < function.getArity(); iter1++) {
            if (iter1 > 0) {
                builder.append(", ");
            }
            builder.append("sub.param" + iter1);
        }
        builder.append(") result \n");
        builder.append("where sub." + BADConstants.METADATA_TYPE_FIELD_NAME_BROKERNAME + " /*+ bcast */ = b."
                + BADConstants.METADATA_TYPE_FIELD_NAME_BROKERNAME + "\n");
        builder.append("and sub." + BADConstants.METADATA_TYPE_NAME_DATAVERSENAME + " /*+ bcast */ = b."
                + BADConstants.METADATA_TYPE_NAME_DATAVERSENAME + "\n");
        if (!push) {
            builder.append(")");
            builder.append(" returning a");
        }
        builder.append(";");
        body = builder.toString();
        BADParserFactory factory = new BADParserFactory();
        List<Statement> fStatements = factory.createParser(builder.toString()).parse();

        SetStatement ss = (SetStatement) fStatements.get(0);
        metadataProvider.getConfig().put(ss.getPropName(), ss.getPropValue());
        if (push) {
            return BADJobService.compilePushChannel(statementExecutor, metadataProvider, hcc,
                    (Query) fStatements.get(1));
        }
        return ((QueryTranslator) statementExecutor).handleInsertUpsertStatement(metadataProvider, fStatements.get(1),
                hcc, resultSet, ResultDelivery.ASYNC, null, stats, true, null, null, null);
    }

    @Override
    public String getName() {
        return AbstractCreateChannelStatement.class.getName();
    }

    @Override
    public void handle(IHyracksClientConnection hcc, IStatementExecutor statementExecutor,
            IRequestParameters requestContext, MetadataProvider metadataProvider, int resultSetId)
            throws HyracksDataException, AlgebricksException {
        //This function performs three tasks:
        //1. Create datasets for the Channel
        //2. Create and run the Channel Job
        //3. Create the metadata entry for the channel

        //TODO: Figure out how to handle when a subset of the 3 tasks fails

        dataverseName = statementExecutor.getActiveDataverseName(dataverseName);
        subscriptionsTableName = channelName + BADConstants.subscriptionEnding;
        resultsTableName = push ? "" : channelName + BADConstants.resultsEnding;

        EntityId entityId = new EntityId(BADConstants.RUNTIME_ENTITY_CHANNEL, dataverseName, channelName.getValue());
        ICcApplicationContext appCtx = metadataProvider.getApplicationContext();
        ActiveNotificationHandler activeEventHandler =
                (ActiveNotificationHandler) appCtx.getActiveNotificationHandler();
        DeployedJobSpecEventListener listener = (DeployedJobSpecEventListener) activeEventHandler.getListener(entityId);
        boolean alreadyActive = false;
        Channel channel;

        MetadataTransactionContext mdTxnCtx = null;
        try {
            mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
            metadataProvider.setMetadataTxnContext(mdTxnCtx);
            channel = BADLangExtension.getChannel(mdTxnCtx, dataverseName, channelName.getValue());
            if (channel != null) {
                throw new AlgebricksException("A channel with this name " + channelName + " already exists.");
            }
            if (listener != null) {
                alreadyActive = listener.isActive();
            }
            if (alreadyActive) {
                throw new AsterixException("Channel " + channelName + " is already running");
            }
            initialize(statementExecutor, metadataProvider, mdTxnCtx);

            //check if names are available before creating anything
            if (MetadataManager.INSTANCE.getDataset(mdTxnCtx, dataverseName, subscriptionsTableName) != null) {
                throw new AsterixException("The channel name:" + channelName + " is not available.");
            }
            if (!push && MetadataManager.INSTANCE.getDataset(mdTxnCtx, dataverseName, resultsTableName) != null) {
                throw new AsterixException("The channel name:" + channelName + " is not available.");
            }
            MetadataProvider tempMdProvider = BADUtils.replicateMetadataProvider(metadataProvider);
            tempMdProvider.setMaxResultReads(requestContext.getResultProperties().getMaxReads());
            final IResultSet resultSet = requestContext.getResultSet();
            final Stats stats = requestContext.getStats();
            tempMdProvider.getConfig().put(BADConstants.CONFIG_CHANNEL_NAME, channelName.getValue());
            //Create Channel Datasets
            createDatasets(statementExecutor, tempMdProvider, hcc);
            tempMdProvider.getLocks().reset();
            //Create Channel Internal Job
            JobSpecification channeljobSpec =
                    createChannelJob(statementExecutor, tempMdProvider, hcc, resultSet, stats);

            // Now we subscribe
            if (listener == null) {
                listener = new DeployedJobSpecEventListener(appCtx, entityId,
                        push ? PrecompiledType.PUSH_CHANNEL : PrecompiledType.CHANNEL);
                activeEventHandler.registerListener(listener);
            }

            BADJobService.setupExecutorJob(entityId, channeljobSpec, hcc, listener, metadataProvider.getTxnIdFactory(),
                    duration);
            channel = new Channel(dataverseName, channelName.getValue(), subscriptionsTableName, resultsTableName,
                    function, duration, null, body);

            MetadataManager.INSTANCE.addEntity(mdTxnCtx, channel);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            if (mdTxnCtx != null) {
                QueryTranslator.abort(e, e, mdTxnCtx);
            }
            LOGGER.log(Level.WARNING, "Failed creating a channel", e);
            throw HyracksDataException.create(e);
        } finally {
            metadataProvider.getLocks().unlock();
        }

    }

}