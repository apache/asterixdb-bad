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
package org.apache.asterix.bad.extension;

import java.rmi.RemoteException;
import java.util.Arrays;
import java.util.List;

import org.apache.asterix.bad.BADConstants;
import org.apache.asterix.bad.function.BADFunctions;
import org.apache.asterix.bad.function.rewriter.BADFeedRewriter;
import org.apache.asterix.bad.metadata.BADMetadataIndexes;
import org.apache.asterix.bad.metadata.BADMetadataRecordTypes;
import org.apache.asterix.common.api.ExtensionId;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.api.ExtensionMetadataDataset;
import org.apache.asterix.metadata.api.IMetadataExtension;
import org.apache.asterix.metadata.api.IMetadataIndex;
import org.apache.asterix.metadata.bootstrap.MetadataBootstrap;
import org.apache.asterix.metadata.entities.Datatype;
import org.apache.asterix.metadata.entities.Dataverse;
import org.apache.asterix.metadata.entitytupletranslators.MetadataTupleTranslatorProvider;
import org.apache.asterix.metadata.utils.MetadataConstants;
import org.apache.asterix.metadata.utils.MetadataUtil;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.typecomputer.impl.ABooleanTypeComputer;
import org.apache.asterix.om.typecomputer.impl.ADateTimeTypeComputer;
import org.apache.asterix.runtime.formats.NonTaggedDataFormat;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class BADMetadataExtension implements IMetadataExtension {

    public static final ExtensionId BAD_METADATA_EXTENSION_ID =
            new ExtensionId(BADConstants.BAD_METADATA_EXTENSION_NAME, 0);
    public static final Dataverse BAD_DATAVERSE = new Dataverse(MetadataConstants.METADATA_DATAVERSE_NAME,
            NonTaggedDataFormat.class.getName(), MetadataUtil.PENDING_NO_OP);

    public static final Datatype BAD_SUBSCRIPTION_DATATYPE = new Datatype(MetadataConstants.METADATA_DATAVERSE_NAME,
            BADConstants.METADATA_TYPENAME_SUBSCRIPTIONS, BADMetadataRecordTypes.channelSubscriptionsType, false);
    public static final Datatype BAD_RESULT_DATATYPE = new Datatype(MetadataConstants.METADATA_DATAVERSE_NAME,
            BADConstants.ChannelResultsType, BADMetadataRecordTypes.channelResultsType, false);

    public static final Datatype BAD_BROKER_DATATYPE = new Datatype(MetadataConstants.METADATA_DATAVERSE_NAME,
            BADConstants.METADATA_TYPENAME_BROKER, BADMetadataRecordTypes.BROKER_RECORDTYPE, false);

    public static final Datatype BAD_CHANNEL_DATATYPE = new Datatype(MetadataConstants.METADATA_DATAVERSE_NAME,
            BADConstants.METADATA_TYPENAME_CHANNEL, BADMetadataRecordTypes.CHANNEL_RECORDTYPE, false);

    public static final Datatype BAD_PROCEDURE_DATATYPE = new Datatype(MetadataConstants.METADATA_DATAVERSE_NAME,
            BADConstants.METADATA_TYPENAME_PROCEDURE, BADMetadataRecordTypes.PROCEDURE_RECORDTYPE, false);

    public static final Datatype BAD_ACTIVE_RECORD_TYPE = new Datatype(MetadataConstants.METADATA_DATAVERSE_NAME,
            BADConstants.RECORD_TYPENAME_ACTIVE_RECORD, BADMetadataRecordTypes.ACTIVE_RECORD_RECORD_TYPE, false);

    @Override
    public ExtensionId getId() {
        return BAD_METADATA_EXTENSION_ID;
    }

    @Override
    public void configure(List<Pair<String, String>> args) {
        // do nothing??
    }

    @Override
    public MetadataTupleTranslatorProvider getMetadataTupleTranslatorProvider() {
        return new MetadataTupleTranslatorProvider();
    }

    @SuppressWarnings("rawtypes")
    @Override
    public List<ExtensionMetadataDataset> getExtensionIndexes() {
        try {
            return Arrays.asList(BADMetadataIndexes.CHANNEL_DATASET, BADMetadataIndexes.BROKER_DATASET,
                    BADMetadataIndexes.PROCEDURE_DATASET);
        } catch (Throwable th) {
            th.printStackTrace();
            throw th;
        }
    }

    @Override
    public void initializeMetadata(INCServiceContext appCtx)
            throws HyracksDataException, RemoteException, ACIDException {
        // enlist datasets
        MetadataBootstrap.enlistMetadataDataset(appCtx, BADMetadataIndexes.CHANNEL_DATASET);
        MetadataBootstrap.enlistMetadataDataset(appCtx, BADMetadataIndexes.BROKER_DATASET);
        MetadataBootstrap.enlistMetadataDataset(appCtx, BADMetadataIndexes.PROCEDURE_DATASET);
        if (MetadataBootstrap.isNewUniverse()) {
            MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
            try {
                // add metadata datasets
                MetadataBootstrap.insertMetadataDatasets(mdTxnCtx,
                        new IMetadataIndex[] { BADMetadataIndexes.CHANNEL_DATASET, BADMetadataIndexes.BROKER_DATASET,
                                BADMetadataIndexes.PROCEDURE_DATASET });
                // insert default dataverse
                // TODO prevent user from dropping this dataverse
                // MetadataManager.INSTANCE.addDataverse(mdTxnCtx, BAD_DATAVERSE);
                // insert default data type
                MetadataManager.INSTANCE.addDatatype(mdTxnCtx, BAD_RESULT_DATATYPE);
                MetadataManager.INSTANCE.addDatatype(mdTxnCtx, BAD_SUBSCRIPTION_DATATYPE);
                MetadataManager.INSTANCE.addDatatype(mdTxnCtx, BAD_BROKER_DATATYPE);
                MetadataManager.INSTANCE.addDatatype(mdTxnCtx, BAD_CHANNEL_DATATYPE);
                MetadataManager.INSTANCE.addDatatype(mdTxnCtx, BAD_PROCEDURE_DATATYPE);
                MetadataManager.INSTANCE.addDatatype(mdTxnCtx, BAD_ACTIVE_RECORD_TYPE);
                // TODO prevent user from dropping these types
                MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            } catch (Exception e) {
                e.printStackTrace();
                MetadataManager.INSTANCE.abortTransaction(mdTxnCtx);
            }
        }
        // local recovery?
        // nothing for now
        BuiltinFunctions.addFunction(BADFunctions.CURRENT_CHANNEL_TIME, ADateTimeTypeComputer.INSTANCE, false);
        BuiltinFunctions.addFunction(BADFunctions.PREVIOUS_CHANNEL_TIME, ADateTimeTypeComputer.INSTANCE, false);
        BuiltinFunctions.addFunction(BADFunctions.IS_NEW, ABooleanTypeComputer.INSTANCE, true);
        BuiltinFunctions.addFunction(BADFunctions.ACTIVE_TIMESTAMP, ADateTimeTypeComputer.INSTANCE, true);

        // to shadow the master feed rewriter
        BuiltinFunctions.addPrivateFunction(BuiltinFunctions.FEED_COLLECT, BADFeedRewriter.INSTANCE, true);
        BuiltinFunctions.addUnnestFun(BuiltinFunctions.FEED_COLLECT, false);
        BuiltinFunctions.addDatasourceFunction(BuiltinFunctions.FEED_COLLECT, BADFeedRewriter.INSTANCE);
    }
}
