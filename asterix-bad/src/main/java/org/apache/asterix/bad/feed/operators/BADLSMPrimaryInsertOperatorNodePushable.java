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

import java.nio.ByteBuffer;

import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.runtime.operators.LSMPrimaryInsertOperatorNodePushable;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.IModificationOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;

public class BADLSMPrimaryInsertOperatorNodePushable extends LSMPrimaryInsertOperatorNodePushable {

    public BADLSMPrimaryInsertOperatorNodePushable(IHyracksTaskContext ctx, int partition,
            IIndexDataflowHelperFactory indexHelperFactory, IIndexDataflowHelperFactory keyIndexHelperFactory,
            int[] fieldPermutation, RecordDescriptor inputRecDesc,
            IModificationOperationCallbackFactory modCallbackFactory,
            ISearchOperationCallbackFactory searchCallbackFactory, int numOfPrimaryKeys, int[] filterFields,
            SourceLocation sourceLoc) throws HyracksDataException {
        super(ctx, partition, indexHelperFactory, keyIndexHelperFactory, fieldPermutation, inputRecDesc,
                modCallbackFactory, searchCallbackFactory, numOfPrimaryKeys, filterFields, sourceLoc);
    }

    @Override
    protected void beforeModification(ITupleReference tuple) {
        if (tuple.getFieldCount() >= 3
                && tuple.getFieldData(0)[tuple.getFieldStart(2)] == ATypeTag.SERIALIZED_RECORD_TYPE_TAG
                && tuple.getFieldLength(2) == 22) {
            long currMilli = System.currentTimeMillis();
            ByteBuffer tupleBuff = ByteBuffer.wrap(tuple.getFieldData(0));
            tupleBuff.putLong(tuple.getFieldStart(2) + 14, currMilli);
            if (tuple.getFieldCount() == 4) {
                tupleBuff.putLong(tuple.getFieldStart(3) + 1, currMilli);
            }
        }
    }
}
