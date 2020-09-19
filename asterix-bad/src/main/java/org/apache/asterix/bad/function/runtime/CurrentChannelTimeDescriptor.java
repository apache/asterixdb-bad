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
package org.apache.asterix.bad.function.runtime;

import java.io.DataOutput;

import org.apache.asterix.bad.function.BADFunctions;
import org.apache.asterix.bad.runtime.ActiveTimestampManager;
import org.apache.asterix.bad.runtime.ActiveTimestampState;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.ADateTime;
import org.apache.asterix.om.base.AMutableDateTime;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksJobletContext;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class CurrentChannelTimeDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    private final static FunctionIdentifier FID = BADFunctions.CURRENT_CHANNEL_TIME;
    private final static long ActiveStateKey = 1024;

    public final static IFunctionDescriptorFactory FACTORY = CurrentChannelTimeDescriptor::new;

    private CurrentChannelTimeDescriptor() {
    }

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IEvaluatorContext ctx) throws HyracksDataException {
                return new IScalarEvaluator() {

                    private ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
                    private DataOutput out = resultStorage.getDataOutput();

                    private final IPointable argPtr0 = new VoidPointable();
                    private final IScalarEvaluator eval0 = args[0].createScalarEvaluator(ctx);

                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<ADateTime> datetimeSerde =
                            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ADATETIME);
                    private AMutableDateTime aDateTime = new AMutableDateTime(0);
                    private IHyracksJobletContext jobletCtx = ctx.getTaskContext().getJobletContext();

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                        ActiveTimestampState existingState =
                                (ActiveTimestampState) ctx.getTaskContext().getStateObject(ActiveStateKey);
                        resultStorage.reset();

                        if (existingState == null) {
                            eval0.evaluate(tuple, argPtr0);
                            String channelName = new String(argPtr0.getByteArray(), argPtr0.getStartOffset() + 2,
                                    argPtr0.getLength() - 2);
                            ActiveTimestampManager.progressChannelExecutionTimestamps(jobletCtx.getJobId(), channelName,
                                    jobletCtx.getServiceContext().getNodeId());
                            String nodeId = jobletCtx.getServiceContext().getNodeId();
                            long previousChannelTime =
                                    ActiveTimestampManager.getPreviousChannelExecutionTimestamp(channelName, nodeId);
                            long currentChannelTime =
                                    ActiveTimestampManager.getCurrentChannelExecutionTimestamp(channelName, nodeId);
                            existingState = new ActiveTimestampState(jobletCtx.getJobId(), ActiveStateKey);
                            existingState.setExecutionTime(previousChannelTime, currentChannelTime);
                            ctx.getTaskContext().setStateObject(existingState);
                        }
                        aDateTime.setValue(existingState.getCurrentChannelExecutionTime());
                        datetimeSerde.serialize(aDateTime, out);
                        result.set(resultStorage);
                    }
                };
            }
        };
    }

    /* (non-Javadoc)
     * @see org.apache.asterix.om.functions.IFunctionDescriptor#getIdentifier()
     */
    @Override
    public FunctionIdentifier getIdentifier() {
        return FID;
    }

}
