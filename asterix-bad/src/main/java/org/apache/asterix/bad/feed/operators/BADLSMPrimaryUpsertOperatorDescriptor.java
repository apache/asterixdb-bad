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

package org.apache.asterix.bad.feed.operators;

import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.runtime.operators.LSMPrimaryUpsertOperatorDescriptor;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IMissingWriterFactory;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.storage.am.common.api.IModificationOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.lsm.common.api.IFrameOperationCallbackFactory;

public class BADLSMPrimaryUpsertOperatorDescriptor extends LSMPrimaryUpsertOperatorDescriptor {
    public BADLSMPrimaryUpsertOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor outRecDesc,
            int[] fieldPermutation, IIndexDataflowHelperFactory indexHelperFactory,
            IMissingWriterFactory missingWriterFactory,
            IModificationOperationCallbackFactory modificationOpCallbackFactory,
            ISearchOperationCallbackFactory searchOpCallbackFactory,
            IFrameOperationCallbackFactory frameOpCallbackFactory, int numPrimaryKeys, ARecordType recordType,
            int filterIndex, boolean hasSecondaries) {
        super(spec, outRecDesc, fieldPermutation, indexHelperFactory, missingWriterFactory,
                modificationOpCallbackFactory, searchOpCallbackFactory, frameOpCallbackFactory, numPrimaryKeys,
                recordType, filterIndex, hasSecondaries);
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        RecordDescriptor intputRecDesc = recordDescProvider.getInputRecordDescriptor(getActivityId(), 0);
        return new BADLSMPrimaryUpsertOperatorNodePushable(ctx, partition, indexHelperFactory, fieldPermutation,
                intputRecDesc, modCallbackFactory, searchOpCallbackFactory, numPrimaryKeys, recordType, filterIndex,
                frameOpCallbackFactory, missingWriterFactory, hasSecondaries);
    }
}
