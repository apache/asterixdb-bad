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

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.algebra.extension.ExtensionStatement;
import org.apache.asterix.app.translator.QueryTranslator;
import org.apache.asterix.bad.BADConstants;
import org.apache.asterix.bad.extension.BADLangExtension;
import org.apache.asterix.bad.metadata.Broker;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.util.ExpressionUtils;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.object.base.AdmObjectNode;
import org.apache.asterix.translator.IRequestParameters;
import org.apache.asterix.translator.IStatementExecutor;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class CreateBrokerStatement extends ExtensionStatement {

    private static final Logger LOGGER = Logger.getLogger(CreateBrokerStatement.class.getName());
    private final DataverseName dataverseName;
    private final Identifier brokerName;
    private String endPointName;
    private String brokerType;
    private AdmObjectNode withObjectNode;

    public CreateBrokerStatement(DataverseName dataverseName, Identifier brokerName, String endPointName,
            RecordConstructor withRecord) throws CompilationException {
        this.brokerName = brokerName;
        this.dataverseName = dataverseName;
        this.endPointName = endPointName;
        if (withRecord != null) {
            this.withObjectNode = ExpressionUtils.toNode(withRecord);
            this.brokerType = withObjectNode.getOptionalString(BADConstants.BAD_BROKER_FIELD_NAME_TYPE);
        }
        this.brokerType = brokerType == null ? BADConstants.GENERAL_BROKER_TYPE_NAME : brokerType.toLowerCase();
    }

    public String getEndPointName() {
        return endPointName;
    }

    public DataverseName getDataverseName() {
        return dataverseName;
    }

    public Identifier getBrokerName() {
        return brokerName;
    }

    @Override
    public byte getCategory() {
        return Category.DDL;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return null;
    }

    @Override
    public String getName() {
        return CreateBrokerStatement.class.getName();
    }

    @Override
    public void handle(IHyracksClientConnection hcc, IStatementExecutor statementExecutor,
            IRequestParameters requestParameters, MetadataProvider metadataProvider, int resultSetId)
            throws HyracksDataException, AlgebricksException {
        DataverseName dataverse = statementExecutor.getActiveDataverseName(dataverseName);
        MetadataTransactionContext mdTxnCtx = null;
        try {
            mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
            metadataProvider.setMetadataTxnContext(mdTxnCtx);
            Broker broker = BADLangExtension.getBroker(mdTxnCtx, dataverse, brokerName.getValue());
            if (broker != null) {
                throw new AlgebricksException("A broker with this name " + brokerName + " already exists.");
            }
            broker = new Broker(dataverse, brokerName.getValue(), endPointName, brokerType);
            MetadataManager.INSTANCE.addEntity(mdTxnCtx, broker);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            if (mdTxnCtx != null) {
                QueryTranslator.abort(e, e, mdTxnCtx);
            }
            LOGGER.log(Level.WARNING, "Failed creating a broker", e);
            throw HyracksDataException.create(e);
        } finally {
            metadataProvider.getLocks().unlock();
        }
    }
}