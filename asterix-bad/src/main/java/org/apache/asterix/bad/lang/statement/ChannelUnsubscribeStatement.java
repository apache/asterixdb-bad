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
import org.apache.asterix.bad.metadata.Channel;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.FieldAccessor;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.OperatorExpr;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.literal.StringLiteral;
import org.apache.asterix.lang.common.statement.DeleteStatement;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.lang.sqlpp.visitor.SqlppDeleteRewriteVisitor;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.translator.IRequestParameters;
import org.apache.asterix.translator.IStatementExecutor;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class ChannelUnsubscribeStatement extends ExtensionStatement {

    private final Identifier dataverseName;
    private final Identifier channelName;
    private final String subscriptionId;
    private final int varCounter;
    private VariableExpr vars;

    public ChannelUnsubscribeStatement(VariableExpr vars, Identifier dataverseName, Identifier channelName,
            String subscriptionId, int varCounter) {
        this.vars = vars;
        this.channelName = channelName;
        this.dataverseName = dataverseName;
        this.subscriptionId = subscriptionId;
        this.varCounter = varCounter;
    }

    public Identifier getDataverseName() {
        return dataverseName;
    }

    public VariableExpr getVariableExpr() {
        return vars;
    }

    public Identifier getChannelName() {
        return channelName;
    }

    public String getsubScriptionId() {
        return subscriptionId;
    }

    public int getVarCounter() {
        return varCounter;
    }

    @Override
    public byte getCategory() {
        return Category.UPDATE;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return null;
    }

    @Override
    public void handle(IHyracksClientConnection hcc, IStatementExecutor statementExecutor,
            IRequestParameters requestParameters, MetadataProvider metadataProvider, int resultSetId)
            throws HyracksDataException, AlgebricksException {
        String dataverse = ((QueryTranslator) statementExecutor).getActiveDataverse(dataverseName);

        MetadataTransactionContext mdTxnCtx = null;
        try {
            mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();

            Channel channel = BADLangExtension.getChannel(mdTxnCtx, dataverse, channelName.getValue());
            if (channel == null) {
                throw new AsterixException("There is no channel with this name " + channelName + ".");
            }

            String subscriptionsDatasetName = channel.getSubscriptionsDataset();

            //Need a condition to say subscription-id = sid
            OperatorExpr condition = new OperatorExpr();
            FieldAccessor fa = new FieldAccessor(vars, new Identifier(BADConstants.SubscriptionId));
            condition.addOperand(fa);
            condition.setCurrentop(true);
            condition.addOperator("=");

            List<Expression> UUIDList = new ArrayList<>();
            UUIDList.add(new LiteralExpr(new StringLiteral(subscriptionId)));

            FunctionIdentifier function = BuiltinFunctions.UUID_CONSTRUCTOR;
            FunctionSignature UUIDfunc =
                    new FunctionSignature(function.getNamespace(), function.getName(), function.getArity());
            CallExpr UUIDCall = new CallExpr(UUIDfunc, UUIDList);

            condition.addOperand(UUIDCall);

            DeleteStatement delete = new DeleteStatement(vars, new Identifier(dataverse),
                    new Identifier(subscriptionsDatasetName), condition, varCounter);
            SqlppDeleteRewriteVisitor visitor = new SqlppDeleteRewriteVisitor();
            delete.accept(visitor, null);
            MetadataProvider tempMdProvider = new MetadataProvider(metadataProvider.getApplicationContext(),
                    metadataProvider.getDefaultDataverse());
            tempMdProvider.getConfig().putAll(metadataProvider.getConfig());
            ((QueryTranslator) statementExecutor).handleDeleteStatement(tempMdProvider, delete, hcc, false, null, null);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            QueryTranslator.abort(e, e, mdTxnCtx);
            throw HyracksDataException.create(e);
        } finally {
            metadataProvider.getLocks().unlock();
        }
    }
}