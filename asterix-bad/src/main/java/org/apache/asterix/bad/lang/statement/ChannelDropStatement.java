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
import org.apache.asterix.bad.metadata.Channel;
import org.apache.asterix.bad.metadata.DeployedJobSpecEventListener;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.lang.common.statement.DropDatasetStatement;
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

public class ChannelDropStatement extends ExtensionStatement {
    private static final Logger LOGGER = Logger.getLogger(ChannelDropStatement.class.getName());

    private final DataverseName dataverseName;
    private final Identifier channelName;
    private boolean ifExists;

    public ChannelDropStatement(DataverseName dataverseName, Identifier channelName, boolean ifExists) {
        this.dataverseName = dataverseName;
        this.channelName = channelName;
        this.ifExists = ifExists;
    }

    public DataverseName getDataverseName() {
        return dataverseName;
    }

    public Identifier getChannelName() {
        return channelName;
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
    public String getName() {
        return ChannelDropStatement.class.getName();
    }

    @Override
    public void handle(IHyracksClientConnection hcc, IStatementExecutor statementExecutor,
            IRequestParameters requestParameters, MetadataProvider metadataProvider, int resultSetId)
            throws HyracksDataException, AlgebricksException {
        DataverseName dataverse = statementExecutor.getActiveDataverseName(dataverseName);
        boolean txnActive = false;
        EntityId entityId = new EntityId(BADConstants.CHANNEL_EXTENSION_NAME, dataverse, channelName.getValue());
        ICcApplicationContext appCtx = metadataProvider.getApplicationContext();
        ActiveNotificationHandler activeEventHandler =
                (ActiveNotificationHandler) appCtx.getActiveNotificationHandler();
        DeployedJobSpecEventListener listener = (DeployedJobSpecEventListener) activeEventHandler.getListener(entityId);
        Channel channel = null;

        MetadataTransactionContext mdTxnCtx = null;
        try {
            mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
            txnActive = true;
            channel = BADLangExtension.getChannel(mdTxnCtx, dataverse, channelName.getValue());
            txnActive = false;
            if (channel == null) {
                if (ifExists) {
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    return;
                } else {
                    throw new AlgebricksException("There is no channel with this name " + channelName + ".");
                }
            }

            if (listener == null) {
                //TODO: Channels need to better handle cluster failures
                LOGGER.log(Level.SEVERE,
                        "Tried to drop a Deployed Job  whose listener no longer exists:  " + entityId.getExtensionName()
                                + " " + entityId.getDataverseName() + "." + entityId.getEntityName() + ".");

            } else {
                listener.getExecutorService().shutdown();
                if (!listener.getExecutorService().awaitTermination(BADConstants.EXECUTOR_TIMEOUT, TimeUnit.SECONDS)) {
                    LOGGER.log(Level.SEVERE,
                            "Executor Service is terminating non-gracefully for: " + entityId.getExtensionName() + " "
                                    + entityId.getDataverseName() + "." + entityId.getEntityName());
                }
                DeployedJobSpecId deployedJobSpecId = listener.getDeployedJobSpecId();
                listener.deActivate();
                activeEventHandler.unregisterListener(listener);
                if (deployedJobSpecId != null) {
                    hcc.undeployJobSpec(deployedJobSpecId);
                }
            }
            //Create a metadata provider to use in nested jobs.
            MetadataProvider tempMdProvider = MetadataProvider.create(appCtx, metadataProvider.getDefaultDataverse());
            tempMdProvider.getConfig().putAll(metadataProvider.getConfig());
            //Drop the Channel Datasets
            //TODO: Need to find some way to handle if this fails.

            //Remove the Channel Metadata
            MetadataManager.INSTANCE.deleteEntity(mdTxnCtx, channel);
            DropDatasetStatement dropStmt =
                    new DropDatasetStatement(dataverse, new Identifier(channel.getResultsDatasetName()), true);
            ((QueryTranslator) statementExecutor).handleDatasetDropStatement(tempMdProvider, dropStmt, hcc, null);
            tempMdProvider.getLocks().reset();
            dropStmt = new DropDatasetStatement(dataverse, new Identifier(channel.getSubscriptionsDataset()), true);
            ((QueryTranslator) statementExecutor).handleDatasetDropStatement(tempMdProvider, dropStmt, hcc, null);
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