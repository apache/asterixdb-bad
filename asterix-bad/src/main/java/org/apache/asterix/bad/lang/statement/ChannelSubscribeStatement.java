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
import java.util.List;

import org.apache.asterix.algebra.extension.ExtensionStatement;
import org.apache.asterix.app.translator.QueryTranslator;
import org.apache.asterix.bad.BADConstants;
import org.apache.asterix.bad.lang.BADLangExtension;
import org.apache.asterix.bad.metadata.Broker;
import org.apache.asterix.bad.metadata.Channel;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.FieldAccessor;
import org.apache.asterix.lang.common.expression.FieldBinding;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.literal.StringLiteral;
import org.apache.asterix.lang.common.statement.InsertStatement;
import org.apache.asterix.lang.common.statement.Query;
import org.apache.asterix.lang.common.statement.UpsertStatement;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.translator.IRequestParameters;
import org.apache.asterix.translator.IStatementExecutor;
import org.apache.asterix.translator.IStatementExecutor.ResultDelivery;
import org.apache.asterix.translator.IStatementExecutor.Stats;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.result.IResultSet;
import org.apache.hyracks.api.result.ResultSetId;

public class ChannelSubscribeStatement extends ExtensionStatement {

    private final DataverseName dataverseName;
    private final Identifier channelName;
    private final DataverseName brokerDataverseName;
    private final Identifier brokerName;
    private final List<Expression> argList;
    private final String subscriptionId;
    private final int varCounter;

    public ChannelSubscribeStatement(DataverseName dataverseName, Identifier channelName, List<Expression> argList,
            int varCounter, DataverseName brokerDataverseName, Identifier brokerName, String subscriptionId) {
        this.channelName = channelName;
        this.dataverseName = dataverseName;
        this.brokerDataverseName = brokerDataverseName;
        this.brokerName = brokerName;
        this.argList = argList;
        this.subscriptionId = subscriptionId;
        this.varCounter = varCounter;
    }

    public DataverseName getDataverseName() {
        return dataverseName;
    }

    public DataverseName getBrokerDataverseName() {
        return brokerDataverseName;
    }

    public Identifier getChannelName() {
        return channelName;
    }

    public Identifier getBrokerName() {
        return brokerName;
    }

    public List<Expression> getArgList() {
        return argList;
    }

    public int getVarCounter() {
        return varCounter;
    }

    public String getSubscriptionId() {
        return subscriptionId;
    }

    @Override
    public byte getCategory() {
        return Category.QUERY;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return null;
    }

    @Override
    public void handle(IHyracksClientConnection hcc, IStatementExecutor statementExecutor,
            IRequestParameters requestParameters, MetadataProvider metadataProvider, int resultSetId)
            throws HyracksDataException, AlgebricksException {
        DataverseName dataverse = statementExecutor.getActiveDataverseName(dataverseName);
        DataverseName brokerDataverse = statementExecutor.getActiveDataverseName(brokerDataverseName);

        MetadataTransactionContext mdTxnCtx = null;
        try {
            mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();

            Channel channel = BADLangExtension.getChannel(mdTxnCtx, dataverse, channelName.getValue());
            if (channel == null) {
                throw new AsterixException("There is no channel with this name " + channelName + ".");
            }
            Broker broker = BADLangExtension.getBroker(mdTxnCtx, brokerDataverse, brokerName.getValue());
            if (broker == null) {
                throw new AsterixException("There is no broker with this name " + brokerName + ".");
            }

            String subscriptionsDatasetName = channel.getSubscriptionsDataset();

            if (argList.size() != channel.getFunction().getArity()) {
                throw new AsterixException("Channel expected " + channel.getFunction().getArity()
                        + " parameters but got " + argList.size());
            }

            Query subscriptionTuple = new Query(false);

            List<FieldBinding> fb = new ArrayList<>();
            LiteralExpr leftExpr = new LiteralExpr(new StringLiteral(BADConstants.DataverseName));
            Expression rightExpr = new LiteralExpr(new StringLiteral(brokerDataverse.getCanonicalForm()));
            fb.add(new FieldBinding(leftExpr, rightExpr));

            leftExpr = new LiteralExpr(new StringLiteral(BADConstants.BrokerName));
            rightExpr = new LiteralExpr(new StringLiteral(broker.getBrokerName()));
            fb.add(new FieldBinding(leftExpr, rightExpr));

            if (subscriptionId != null) {
                leftExpr = new LiteralExpr(new StringLiteral(BADConstants.SubscriptionId));

                List<Expression> UUIDList = new ArrayList<>();
                UUIDList.add(new LiteralExpr(new StringLiteral(subscriptionId)));
                FunctionIdentifier function = BuiltinFunctions.UUID_CONSTRUCTOR;
                FunctionSignature UUIDfunc = new FunctionSignature(function);
                CallExpr UUIDCall = new CallExpr(UUIDfunc, UUIDList);

                rightExpr = UUIDCall;
                fb.add(new FieldBinding(leftExpr, rightExpr));
            }

            for (int i = 0; i < argList.size(); i++) {
                leftExpr = new LiteralExpr(new StringLiteral("param" + i));
                rightExpr = argList.get(i);
                fb.add(new FieldBinding(leftExpr, rightExpr));
            }
            RecordConstructor recordCon = new RecordConstructor(fb);
            subscriptionTuple.setBody(recordCon);
            subscriptionTuple.setVarCounter(varCounter);
            MetadataProvider tempMdProvider = MetadataProvider.create(metadataProvider.getApplicationContext(),
                    metadataProvider.getDefaultDataverse());
            tempMdProvider.getConfig().putAll(metadataProvider.getConfig());

            final ResultDelivery resultDelivery = requestParameters.getResultProperties().getDelivery();
            final IResultSet resultSet = requestParameters.getResultSet();
            final Stats stats = requestParameters.getStats();
            if (subscriptionId == null) {
                //To create a new subscription
                VariableExpr resultVar = new VariableExpr(new VarIdentifier("$result", 0));
                VariableExpr useResultVar = new VariableExpr(new VarIdentifier("$result", 0));
                useResultVar.setIsNewVar(false);
                FieldAccessor accessor = new FieldAccessor(useResultVar, new Identifier(BADConstants.SubscriptionId));

                metadataProvider.setResultSetId(new ResultSetId(resultSetId));
                boolean resultsAsync =
                        resultDelivery == ResultDelivery.ASYNC || resultDelivery == ResultDelivery.DEFERRED;
                metadataProvider.setResultAsyncMode(resultsAsync);
                tempMdProvider.setResultSetId(metadataProvider.getResultSetId());
                tempMdProvider.setResultAsyncMode(resultsAsync);
                tempMdProvider.setWriterFactory(metadataProvider.getWriterFactory());
                tempMdProvider
                        .setResultSerializerFactoryProvider(metadataProvider.getResultSerializerFactoryProvider());
                tempMdProvider.setOutputFile(metadataProvider.getOutputFile());
                tempMdProvider.setMaxResultReads(requestParameters.getResultProperties().getMaxReads());

                InsertStatement insert = new InsertStatement(dataverse, new Identifier(subscriptionsDatasetName),
                        subscriptionTuple, varCounter, resultVar, accessor);
                ((QueryTranslator) statementExecutor).handleInsertUpsertStatement(tempMdProvider, insert, hcc,
                        resultSet, resultDelivery, null, stats, false, requestParameters, null, null);
            } else {
                //To update an existing subscription
                UpsertStatement upsert = new UpsertStatement(dataverse, new Identifier(subscriptionsDatasetName),
                        subscriptionTuple, varCounter, null, null);
                ((QueryTranslator) statementExecutor).handleInsertUpsertStatement(tempMdProvider, upsert, hcc,
                        resultSet, resultDelivery, null, stats, false, requestParameters, null, null);
            }

            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            QueryTranslator.abort(e, e, mdTxnCtx);
            throw HyracksDataException.create(e);
        } finally {
            metadataProvider.getLocks().unlock();
        }

    }

}