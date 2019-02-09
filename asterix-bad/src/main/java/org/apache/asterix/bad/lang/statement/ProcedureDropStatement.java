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

import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.active.EntityId;
import org.apache.asterix.algebra.extension.ExtensionStatement;
import org.apache.asterix.app.active.ActiveNotificationHandler;
import org.apache.asterix.app.translator.QueryTranslator;
import org.apache.asterix.bad.BADConstants;
import org.apache.asterix.bad.lang.BADLangExtension;
import org.apache.asterix.bad.metadata.DeployedJobSpecEventListener;
import org.apache.asterix.bad.metadata.Procedure;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.translator.IRequestParameters;
import org.apache.asterix.translator.IStatementExecutor;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.DeployedJobSpecId;

public class ProcedureDropStatement extends ExtensionStatement {
    private static final Logger LOGGER = Logger.getLogger(ProcedureDropStatement.class.getName());

    private final FunctionSignature signature;
    private boolean ifExists;

    public ProcedureDropStatement(FunctionSignature signature, boolean ifExists) {
        this.signature = signature;
        this.ifExists = ifExists;
    }

    public FunctionSignature getFunctionSignature() {
        return signature;
    }

    public boolean getIfExists() {
        return ifExists;
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
    public void handle(IHyracksClientConnection hcc, IStatementExecutor statementExecutor,
            IRequestParameters requestParameters, MetadataProvider metadataProvider, int resultSetId)
            throws HyracksDataException, AlgebricksException {
        ICcApplicationContext appCtx = metadataProvider.getApplicationContext();
        ActiveNotificationHandler activeEventHandler =
                (ActiveNotificationHandler) appCtx.getActiveNotificationHandler();
        FunctionSignature signature = getFunctionSignature();
        String dataverse =
                ((QueryTranslator) statementExecutor).getActiveDataverse(new Identifier(signature.getNamespace()));
        signature.setNamespace(dataverse);
        boolean txnActive = false;
        EntityId entityId = new EntityId(BADConstants.PROCEDURE_KEYWORD, dataverse, signature.getName());
        DeployedJobSpecEventListener listener = (DeployedJobSpecEventListener) activeEventHandler.getListener(entityId);

        if (listener.isActive()) {
            throw new AlgebricksException("Cannot drop running procedure. There are " + listener.getRunningInstance()
                    + " running instances.");
        }

        Procedure procedure;

        MetadataTransactionContext mdTxnCtx = null;
        try {
            mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
            txnActive = true;
            procedure = BADLangExtension.getProcedure(mdTxnCtx, dataverse, signature.getName(),
                    Integer.toString(signature.getArity()));
            txnActive = false;
            if (procedure == null) {
                if (ifExists) {
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    return;
                } else {
                    throw new AlgebricksException("There is no procedure with this name " + signature.getName() + ".");
                }
            }

            if (listener == null) {
                //TODO: Channels need to better handle cluster failures
                LOGGER.log(Level.SEVERE,
                        "Tried to drop a Deployed Job  whose listener no longer exists:  " + entityId.getExtensionName()
                                + " " + entityId.getDataverse() + "." + entityId.getEntityName() + ".");
            } else {
                if (listener.getExecutorService() != null) {
                    listener.getExecutorService().shutdown();
                    if (!listener.getExecutorService().awaitTermination(BADConstants.EXECUTOR_TIMEOUT,
                            TimeUnit.SECONDS)) {
                        LOGGER.log(Level.SEVERE,
                                "Executor Service is terminating non-gracefully for: " + entityId.getExtensionName()
                                        + " " + entityId.getDataverse() + "." + entityId.getEntityName());
                    }
                }
                DeployedJobSpecId deployedJobSpecId = listener.getDeployedJobSpecId();
                listener.deActivate();
                activeEventHandler.unregisterListener(listener);
                if (deployedJobSpecId != null) {
                    hcc.undeployJobSpec(deployedJobSpecId);
                }

            }

            //Remove the Channel Metadata
            MetadataManager.INSTANCE.deleteEntity(mdTxnCtx, procedure);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            e.printStackTrace();
            if (txnActive) {
                QueryTranslator.abort(e, e, mdTxnCtx);
            }
            throw HyracksDataException.create(e);
        } finally {
            metadataProvider.getLocks().unlock();
        }
    }

}