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
package org.apache.asterix.bad.runtime;

import java.util.Collection;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractDelegatedLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorDelegate;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionReferenceTransform;

/**
 * An operator for sending broker notifications
 */
public class NotifyBrokerOperator extends AbstractDelegatedLogicalOperator {
    private final LogicalVariable brokerEndpointVar;
    private final LogicalVariable channelExecutionVar;
    private final LogicalVariable pushListVar;
    private final boolean push;

    public NotifyBrokerOperator(LogicalVariable brokerEndpointVar, LogicalVariable pushListVar,
            LogicalVariable resultSetVar, boolean push) {
        this.brokerEndpointVar = brokerEndpointVar;
        this.channelExecutionVar = resultSetVar;
        this.pushListVar = pushListVar;
        this.push = push;
    }

    public LogicalVariable getPushListVar() {
        return pushListVar;
    }

    public LogicalVariable getBrokerEndpointVariable() {
        return brokerEndpointVar;
    }

    public LogicalVariable getChannelExecutionVariable() {
        return channelExecutionVar;
    }

    public boolean getPush() {
        return push;
    }

    @Override
    public String toString() {
        return "notify-brokers (" + brokerEndpointVar.toString() + "," + channelExecutionVar.toString() + ","
                + pushListVar.toString() + ")";
    }

    @Override
    public boolean isMap() {
        return false;
    }

    @Override
    public IOperatorDelegate newInstance() {
        return new NotifyBrokerOperator(brokerEndpointVar, pushListVar, channelExecutionVar, push);
    }

    @Override
    public boolean acceptExpressionTransform(ILogicalExpressionReferenceTransform transform)
            throws AlgebricksException {
        return false;
    }

    @Override
    public void getUsedVariables(Collection<LogicalVariable> usedVars) {
        usedVars.add(pushListVar);
        usedVars.add(brokerEndpointVar);
        usedVars.add(channelExecutionVar);
    }

    @Override
    public void getProducedVariables(Collection<LogicalVariable> producedVars) {
        // none produced

    }

}
