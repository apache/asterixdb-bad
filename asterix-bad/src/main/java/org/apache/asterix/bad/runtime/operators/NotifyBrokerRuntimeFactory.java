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

package org.apache.asterix.bad.runtime.operators;

import org.apache.asterix.active.EntityId;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.runtime.base.IPushRuntime;
import org.apache.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class NotifyBrokerRuntimeFactory implements IPushRuntimeFactory {

    private static final long serialVersionUID = 1L;

    private final IScalarEvaluatorFactory brokerEvalFactory;
    private final IScalarEvaluatorFactory pushListEvalFactory;
    private final IScalarEvaluatorFactory channelExecutionEvalFactory;
    private final IScalarEvaluatorFactory brokerTypeEvalFactory;
    private final EntityId entityId;
    private final boolean push;
    private final IAType recordType;

    public NotifyBrokerRuntimeFactory(IScalarEvaluatorFactory brokerEvalFactory,
            IScalarEvaluatorFactory pushListEvalFactory, IScalarEvaluatorFactory channelExecutionEvalFactory,
            IScalarEvaluatorFactory brokerTypeEvalFactory, EntityId entityId, boolean push, IAType recordType) {
        this.brokerEvalFactory = brokerEvalFactory;
        this.pushListEvalFactory = pushListEvalFactory;
        this.channelExecutionEvalFactory = channelExecutionEvalFactory;
        this.brokerTypeEvalFactory = brokerTypeEvalFactory;
        this.entityId = entityId;
        this.push = push;
        this.recordType = recordType;
    }

    @Override
    public String toString() {
        return "notify-broker";
    }

    @Override
    public IPushRuntime[] createPushRuntime(IHyracksTaskContext ctx) throws HyracksDataException {
        return new IPushRuntime[] { new NotifyBrokerRuntime(ctx, brokerEvalFactory, pushListEvalFactory,
                channelExecutionEvalFactory, brokerTypeEvalFactory, entityId, push, recordType) };
    }
}
