/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.asterix.bad.metadata;

import org.apache.asterix.bad.feed.operators.BADLSMPrimaryInsertOperatorDescriptor;
import org.apache.asterix.bad.feed.operators.BADLSMPrimaryUpsertOperatorDescriptor;
import org.apache.asterix.common.context.IStorageComponentProvider;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.formats.base.IDataFormat;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Dataverse;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.utils.DatasetUtil;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.runtime.operators.LSMPrimaryInsertOperatorDescriptor;
import org.apache.asterix.runtime.operators.LSMPrimaryUpsertOperatorDescriptor;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.value.IMissingWriterFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.storage.am.common.api.IModificationOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.dataflow.IndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;

public class BADMetadataProvider extends MetadataProvider {

    public static MetadataProvider create(ICcApplicationContext appCtx, Dataverse defaultDataverse) {
        MetadataProvider mp = new BADMetadataProvider(appCtx);
        mp.setDefaultDataverse(defaultDataverse);
        return mp;
    }

    protected BADMetadataProvider(ICcApplicationContext appCtx) {
        super(appCtx);
    }

    @Override
    protected LSMPrimaryInsertOperatorDescriptor createLSMPrimaryInsertOperatorDescriptor(JobSpecification spec,
            RecordDescriptor inputRecordDesc, int[] fieldPermutation, IIndexDataflowHelperFactory idfh,
            IIndexDataflowHelperFactory pkidfh, IModificationOperationCallbackFactory modificationCallbackFactory,
            ISearchOperationCallbackFactory searchCallbackFactory, int numKeys, int[] filterFields) {
        return new BADLSMPrimaryInsertOperatorDescriptor(spec, inputRecordDesc, fieldPermutation, idfh, pkidfh,
                modificationCallbackFactory, searchCallbackFactory, numKeys, filterFields);
    }

    // This method uses a static method from DatasetUtil, so it cannot be further simplified.
    @Override
    protected Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> createPrimaryIndexUpsertOp(JobSpecification spec,
            MetadataProvider metadataProvider, Dataset dataset, RecordDescriptor inputRecordDesc,
            int[] fieldPermutation, IMissingWriterFactory missingWriterFactory) throws AlgebricksException {
        int numKeys = dataset.getPrimaryKeys().size();
        int numFilterFields = DatasetUtil.getFilterField(dataset) == null ? 0 : 1;
        ARecordType itemType = (ARecordType) metadataProvider.findType(dataset);
        ARecordType metaItemType = (ARecordType) metadataProvider.findMetaType(dataset);
        Index primaryIndex = metadataProvider.getIndex(dataset.getDataverseName(), dataset.getDatasetName(),
                dataset.getDatasetName());
        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint =
                metadataProvider.getSplitProviderAndConstraints(dataset);

        // prepare callback
        int[] primaryKeyFields = new int[numKeys];
        for (int i = 0; i < numKeys; i++) {
            primaryKeyFields[i] = i;
        }
        boolean hasSecondaries =
                metadataProvider.getDatasetIndexes(dataset.getDataverseName(), dataset.getDatasetName()).size() > 1;
        IStorageComponentProvider storageComponentProvider = metadataProvider.getStorageComponentProvider();
        IModificationOperationCallbackFactory modificationCallbackFactory = dataset.getModificationCallbackFactory(
                storageComponentProvider, primaryIndex, IndexOperation.UPSERT, primaryKeyFields);
        ISearchOperationCallbackFactory searchCallbackFactory = dataset.getSearchCallbackFactory(
                storageComponentProvider, primaryIndex, IndexOperation.UPSERT, primaryKeyFields);
        IIndexDataflowHelperFactory idfh =
                new IndexDataflowHelperFactory(storageComponentProvider.getStorageManager(), splitsAndConstraint.first);
        LSMPrimaryUpsertOperatorDescriptor op;
        ITypeTraits[] outputTypeTraits = new ITypeTraits[inputRecordDesc.getFieldCount() + 1
                + (dataset.hasMetaPart() ? 2 : 1) + numFilterFields];
        ISerializerDeserializer<?>[] outputSerDes = new ISerializerDeserializer[inputRecordDesc.getFieldCount() + 1
                + (dataset.hasMetaPart() ? 2 : 1) + numFilterFields];
        IDataFormat dataFormat = metadataProvider.getDataFormat();

        int f = 0;
        // add the upsert indicator var
        outputSerDes[f] = dataFormat.getSerdeProvider().getSerializerDeserializer(BuiltinType.ABOOLEAN);
        outputTypeTraits[f] = dataFormat.getTypeTraitProvider().getTypeTrait(BuiltinType.ABOOLEAN);
        f++;
        // add the previous record
        outputSerDes[f] = dataFormat.getSerdeProvider().getSerializerDeserializer(itemType);
        outputTypeTraits[f] = dataFormat.getTypeTraitProvider().getTypeTrait(itemType);
        f++;
        // add the previous meta second
        if (dataset.hasMetaPart()) {
            outputSerDes[f] = dataFormat.getSerdeProvider().getSerializerDeserializer(metaItemType);
            outputTypeTraits[f] = dataFormat.getTypeTraitProvider().getTypeTrait(metaItemType);
            f++;
        }
        // add the previous filter third
        int fieldIdx = -1;
        if (numFilterFields > 0) {
            String filterField = DatasetUtil.getFilterField(dataset).get(0);
            String[] fieldNames = itemType.getFieldNames();
            int i = 0;
            for (; i < fieldNames.length; i++) {
                if (fieldNames[i].equals(filterField)) {
                    break;
                }
            }
            fieldIdx = i;
            outputTypeTraits[f] = dataFormat.getTypeTraitProvider().getTypeTrait(itemType.getFieldTypes()[fieldIdx]);
            outputSerDes[f] =
                    dataFormat.getSerdeProvider().getSerializerDeserializer(itemType.getFieldTypes()[fieldIdx]);
            f++;
        }
        for (int j = 0; j < inputRecordDesc.getFieldCount(); j++) {
            outputTypeTraits[j + f] = inputRecordDesc.getTypeTraits()[j];
            outputSerDes[j + f] = inputRecordDesc.getFields()[j];
        }
        RecordDescriptor outputRecordDesc = new RecordDescriptor(outputSerDes, outputTypeTraits);
        op = new BADLSMPrimaryUpsertOperatorDescriptor(spec, outputRecordDesc, fieldPermutation, idfh,
                missingWriterFactory, modificationCallbackFactory, searchCallbackFactory,
                dataset.getFrameOpCallbackFactory(metadataProvider), numKeys, itemType, fieldIdx, hasSecondaries);
        return new Pair<>(op, splitsAndConstraint.second);
    }
}
