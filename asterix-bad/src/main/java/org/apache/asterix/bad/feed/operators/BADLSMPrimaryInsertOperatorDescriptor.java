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

import org.apache.asterix.runtime.operators.LSMPrimaryInsertOperatorDescriptor;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.storage.am.common.api.IModificationOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;

public class BADLSMPrimaryInsertOperatorDescriptor extends LSMPrimaryInsertOperatorDescriptor {
    public BADLSMPrimaryInsertOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor outRecDesc,
            int[] fieldPermutation, IIndexDataflowHelperFactory indexHelperFactory,
            IIndexDataflowHelperFactory keyIndexHelperFactory,
            IModificationOperationCallbackFactory modificationOpCallbackFactory,
            ISearchOperationCallbackFactory searchOpCallbackFactory, int numOfPrimaryKeys, int[] filterFields) {
        super(spec, outRecDesc, fieldPermutation, indexHelperFactory, keyIndexHelperFactory,
                modificationOpCallbackFactory, searchOpCallbackFactory, numOfPrimaryKeys, filterFields);
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        RecordDescriptor intputRecDesc = recordDescProvider.getInputRecordDescriptor(getActivityId(), 0);
        return new BADLSMPrimaryInsertOperatorNodePushable(ctx, partition, indexHelperFactory, keyIndexHelperFactory,
                fieldPermutation, intputRecDesc, modCallbackFactory, searchOpCallbackFactory, numOfPrimaryKeys,
                filterFields, sourceLoc);
    }
}
