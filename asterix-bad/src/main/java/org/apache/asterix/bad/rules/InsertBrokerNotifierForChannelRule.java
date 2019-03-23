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
package org.apache.asterix.bad.rules;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.active.EntityId;
import org.apache.asterix.algebra.operators.CommitOperator;
import org.apache.asterix.bad.BADConstants;
import org.apache.asterix.bad.runtime.NotifyBrokerOperator;
import org.apache.asterix.bad.runtime.NotifyBrokerPOperator;
import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.metadata.declared.DatasetDataSource;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DelegateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DistinctOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DistributeResultOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InsertDeleteUpsertOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.NestedTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ProjectOperator;
import org.apache.hyracks.algebricks.core.algebra.plan.ALogicalPlanImpl;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class InsertBrokerNotifierForChannelRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op1 = (AbstractLogicalOperator) opRef.getValue();
        if (op1.getOperatorTag() != LogicalOperatorTag.DISTRIBUTE_RESULT) {
            return false;
        }
        boolean push = false;

        AbstractLogicalOperator op = (AbstractLogicalOperator) op1.getInputs().get(0).getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.DELEGATE_OPERATOR) {
            if (op.getOperatorTag() != LogicalOperatorTag.PROJECT) {
                return false;
            }
            push = true;
        }
        DataSourceScanOperator subscriptionsScan;
        String channelDataverse;
        String channelName;

        if (!push) {
            DelegateOperator eOp = (DelegateOperator) op;
            if (!(eOp.getDelegate() instanceof CommitOperator)) {
                return false;
            }
            AbstractLogicalOperator descendantOp = (AbstractLogicalOperator) eOp.getInputs().get(0).getValue();
            if (descendantOp.getOperatorTag() != LogicalOperatorTag.INSERT_DELETE_UPSERT) {
                return false;
            }
            InsertDeleteUpsertOperator insertOp = (InsertDeleteUpsertOperator) descendantOp;
            if (insertOp.getOperation() != InsertDeleteUpsertOperator.Kind.INSERT) {
                return false;
            }
            DatasetDataSource dds = (DatasetDataSource) insertOp.getDataSource();
            String datasetName = dds.getDataset().getDatasetName();
            if (!dds.getDataset().getItemTypeDataverseName().equals("Metadata")
                    || !dds.getDataset().getItemTypeName().equals("ChannelResultsType")
                    || !datasetName.endsWith("Results")) {
                return false;
            }
            channelDataverse = dds.getDataset().getDataverseName();
            //Now we know that we are inserting into results

            channelName = datasetName.substring(0, datasetName.length() - 7);
            String subscriptionsName = channelName + "Subscriptions";
            subscriptionsScan = (DataSourceScanOperator) findOp(op, subscriptionsName);
            if (subscriptionsScan == null) {
                return false;
            }

        } else {
            //if push, get the channel name here instead
            subscriptionsScan = (DataSourceScanOperator) findOp(op, "");
            if (subscriptionsScan == null) {
                return false;
            }
            DatasetDataSource dds = (DatasetDataSource) subscriptionsScan.getDataSource();
            String datasetName = dds.getDataset().getDatasetName();
            channelDataverse = dds.getDataset().getDataverseName();
            channelName = datasetName.substring(0, datasetName.length() - 13);
        }

        //Now we need to get the broker EndPoint
        LogicalVariable brokerEndpointVar = context.newVar();
        AbstractLogicalOperator opAboveBrokersScan = findOp(op, "brokers");
        if (opAboveBrokersScan == null) {
            return false;
        }

        //get subscriptionIdVar
        LogicalVariable subscriptionIdVar = subscriptionsScan.getVariables().get(0);

        //The channelExecutionTime is created just before the scan
        ILogicalOperator channelExecutionAssign = subscriptionsScan.getInputs().get(0).getValue();
        if (channelExecutionAssign.getOperatorTag() != LogicalOperatorTag.ASSIGN) {
            return false;
        }
        LogicalVariable channelExecutionVar = ((AssignOperator) channelExecutionAssign).getVariables().get(0);
        if (!channelExecutionVar.toString().equals("$$" + BADConstants.ChannelExecutionTime)) {
            return false;
        }

        if (!push) {
            ((CommitOperator) ((DelegateOperator) op).getDelegate()).setSink(false);
        }

        AssignOperator assignOp = createbrokerEndPointAssignOperator(brokerEndpointVar, opAboveBrokersScan);
        //now brokerNameVar holds the brokerName for use farther up in the plan

        context.computeAndSetTypeEnvironmentForOperator(assignOp);
        context.computeAndSetTypeEnvironmentForOperator(opAboveBrokersScan);
        context.computeAndSetTypeEnvironmentForOperator(op);

        ProjectOperator badProject = (ProjectOperator) findOp(op1, "project");
        badProject.getVariables().add(subscriptionIdVar);
        badProject.getVariables().add(brokerEndpointVar);
        badProject.getVariables().add(channelExecutionVar);
        context.computeAndSetTypeEnvironmentForOperator(badProject);

        //Create my brokerNotify plan above the extension Operator
        DelegateOperator dOp = push
                ? createNotifyBrokerPushPlan(brokerEndpointVar, badProject.getVariables().get(0), channelExecutionVar,
                        context, op, (DistributeResultOperator) op1, channelDataverse, channelName)
                : createNotifyBrokerPullPlan(brokerEndpointVar, subscriptionIdVar, channelExecutionVar, context, op,
                        (DistributeResultOperator) op1, channelDataverse, channelName);

        opRef.setValue(dOp);

        return true;
    }

    private DelegateOperator createBrokerOp(LogicalVariable brokerEndpointVar, LogicalVariable sendVar,
            LogicalVariable channelExecutionVar, String channelDataverse, String channelName, boolean push) {
        NotifyBrokerOperator notifyBrokerOp =
                new NotifyBrokerOperator(brokerEndpointVar, sendVar, channelExecutionVar, push);
        EntityId activeId = new EntityId(BADConstants.CHANNEL_EXTENSION_NAME, channelDataverse, channelName);
        NotifyBrokerPOperator notifyBrokerPOp = new NotifyBrokerPOperator(activeId);
        notifyBrokerOp.setPhysicalOperator(notifyBrokerPOp);
        DelegateOperator extensionOp = new DelegateOperator(notifyBrokerOp);
        extensionOp.setPhysicalOperator(notifyBrokerPOp);
        return extensionOp;
    }

    private DelegateOperator createNotifyBrokerPushPlan(LogicalVariable brokerEndpointVar, LogicalVariable sendVar,
            LogicalVariable channelExecutionVar, IOptimizationContext context, ILogicalOperator eOp,
            DistributeResultOperator distributeOp, String channelDataverse, String channelName)
            throws AlgebricksException {
        //Find the assign operator to get the result type that we need
        AbstractLogicalOperator assign = (AbstractLogicalOperator) eOp.getInputs().get(0).getValue();
        while (assign.getOperatorTag() != LogicalOperatorTag.ASSIGN) {
            assign = (AbstractLogicalOperator) assign.getInputs().get(0).getValue();
        }

        //Create the NotifyBrokerOperator
        DelegateOperator extensionOp =
                createBrokerOp(brokerEndpointVar, sendVar, channelExecutionVar, channelDataverse, channelName, true);

        extensionOp.getInputs().add(new MutableObject<>(eOp));
        context.computeAndSetTypeEnvironmentForOperator(extensionOp);

        return extensionOp;

    }

    private DelegateOperator createNotifyBrokerPullPlan(LogicalVariable brokerEndpointVar, LogicalVariable sendVar,
            LogicalVariable channelExecutionVar, IOptimizationContext context, ILogicalOperator eOp,
            DistributeResultOperator distributeOp, String channelDataverse, String channelName)
            throws AlgebricksException {

        //Create the Distinct Op
        ArrayList<Mutable<ILogicalExpression>> expressions = new ArrayList<>();
        VariableReferenceExpression vExpr = new VariableReferenceExpression(sendVar);
        expressions.add(new MutableObject<>(vExpr));
        DistinctOperator distinctOp = new DistinctOperator(expressions);

        List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> groupByList = new ArrayList<>();
        List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> groupByDecorList = new ArrayList<>();
        List<ILogicalPlan> nestedPlans = new ArrayList<>();

        //Create GroupBy operator
        GroupByOperator groupbyOp = new GroupByOperator(groupByList, groupByDecorList, nestedPlans);
        groupbyOp.addGbyExpression(null, new VariableReferenceExpression(brokerEndpointVar));
        groupbyOp.addGbyExpression(null, new VariableReferenceExpression(channelExecutionVar));

        //Set the distinct as input
        groupbyOp.getInputs().add(new MutableObject<>(distinctOp));

        //create nested plan for subscription ids in group by
        NestedTupleSourceOperator nestedTupleSourceOp = new NestedTupleSourceOperator(new MutableObject<>(groupbyOp));
        LogicalVariable sendListVar = context.newVar();
        List<LogicalVariable> aggVars = new ArrayList<>();
        aggVars.add(sendListVar);
        AggregateFunctionCallExpression funAgg =
                BuiltinFunctions.makeAggregateFunctionExpression(BuiltinFunctions.LISTIFY, new ArrayList<>());
        funAgg.getArguments().add(new MutableObject<>(new VariableReferenceExpression(sendVar)));
        List<Mutable<ILogicalExpression>> aggExpressions = new ArrayList<>();
        aggExpressions.add(new MutableObject<>(funAgg));
        AggregateOperator listifyOp = new AggregateOperator(aggVars, aggExpressions);
        listifyOp.getInputs().add(new MutableObject<>(nestedTupleSourceOp));

        //add nested plans
        nestedPlans.add(new ALogicalPlanImpl(new MutableObject<>(listifyOp)));

        //Create the NotifyBrokerOperator
        DelegateOperator extensionOp = createBrokerOp(brokerEndpointVar, sendListVar, channelExecutionVar,
                channelDataverse, channelName, false);

        //Set the input for the distinct as the old top
        extensionOp.getInputs().add(new MutableObject<>(groupbyOp));
        distinctOp.getInputs().add(new MutableObject<>(eOp));

        //compute environment bottom up
        context.computeAndSetTypeEnvironmentForOperator(distinctOp);
        context.computeAndSetTypeEnvironmentForOperator(groupbyOp);
        context.computeAndSetTypeEnvironmentForOperator(nestedTupleSourceOp);
        context.computeAndSetTypeEnvironmentForOperator(listifyOp);
        context.computeAndSetTypeEnvironmentForOperator(extensionOp);

        return extensionOp;

    }

    private AssignOperator createbrokerEndPointAssignOperator(LogicalVariable brokerEndpointVar,
            AbstractLogicalOperator opAboveBrokersScan) {
        Mutable<ILogicalExpression> fieldRef = new MutableObject<ILogicalExpression>(
                new ConstantExpression(new AsterixConstantValue(new AString(BADConstants.BrokerEndPoint))));
        DataSourceScanOperator brokerScan = null;
        int index = 0;
        for (Mutable<ILogicalOperator> subOp : opAboveBrokersScan.getInputs()) {
            if (isBrokerScan((AbstractLogicalOperator) subOp.getValue())) {
                brokerScan = (DataSourceScanOperator) subOp.getValue();
                break;
            }
            index++;
        }
        Mutable<ILogicalExpression> varRef = new MutableObject<ILogicalExpression>(
                new VariableReferenceExpression(brokerScan.getVariables().get(2)));

        ScalarFunctionCallExpression fieldAccessByName = new ScalarFunctionCallExpression(
                FunctionUtil.getFunctionInfo(BuiltinFunctions.FIELD_ACCESS_BY_NAME), varRef, fieldRef);
        ArrayList<LogicalVariable> varArray = new ArrayList<LogicalVariable>(1);
        varArray.add(brokerEndpointVar);
        ArrayList<Mutable<ILogicalExpression>> exprArray = new ArrayList<Mutable<ILogicalExpression>>(1);
        exprArray.add(new MutableObject<ILogicalExpression>(fieldAccessByName));

        AssignOperator assignOp = new AssignOperator(varArray, exprArray);

        //Place assignOp between the scan and the op above it
        assignOp.getInputs().add(new MutableObject<ILogicalOperator>(brokerScan));
        opAboveBrokersScan.getInputs().set(index, new MutableObject<ILogicalOperator>(assignOp));

        return assignOp;
    }

    /*This function is used to find specific operators within the plan, either
     * A. The brokers dataset scan
     * B. The subscriptions scan
     * C. The highest project of the plan
     */
    private AbstractLogicalOperator findOp(AbstractLogicalOperator op, String lookingForString) {
        if (!op.hasInputs()) {
            return null;
        }
        for (Mutable<ILogicalOperator> subOp : op.getInputs()) {
            if (lookingForString.equals("brokers")) {
                if (isBrokerScan((AbstractLogicalOperator) subOp.getValue())) {
                    return op;
                } else {
                    AbstractLogicalOperator nestedOp =
                            findOp((AbstractLogicalOperator) subOp.getValue(), lookingForString);
                    if (nestedOp != null) {
                        return nestedOp;
                    }
                }

            } else if (lookingForString.equals("project")) {
                if (subOp.getValue().getOperatorTag() == LogicalOperatorTag.PROJECT) {
                    return (AbstractLogicalOperator) subOp.getValue();
                } else {
                    AbstractLogicalOperator nestedOp =
                            findOp((AbstractLogicalOperator) subOp.getValue(), lookingForString);
                    if (nestedOp != null) {
                        return nestedOp;
                    }
                }
            }

            else {
                if (isSubscriptionsScan((AbstractLogicalOperator) subOp.getValue(), lookingForString)) {
                    return (AbstractLogicalOperator) subOp.getValue();
                } else {
                    AbstractLogicalOperator nestedOp =
                            findOp((AbstractLogicalOperator) subOp.getValue(), lookingForString);
                    if (nestedOp != null) {
                        return nestedOp;
                    }
                }

            }
        }
        return null;
    }

    private boolean isBrokerScan(AbstractLogicalOperator op) {
        if (op instanceof DataSourceScanOperator) {
            if (((DataSourceScanOperator) op).getDataSource() instanceof DatasetDataSource) {
                DatasetDataSource dds = (DatasetDataSource) ((DataSourceScanOperator) op).getDataSource();
                if (dds.getDataset().getDataverseName().equals("Metadata")
                        && dds.getDataset().getDatasetName().equals("Broker")) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean isSubscriptionsScan(AbstractLogicalOperator op, String subscriptionsName) {
        if (op instanceof DataSourceScanOperator) {
            if (((DataSourceScanOperator) op).getDataSource() instanceof DatasetDataSource) {
                DatasetDataSource dds = (DatasetDataSource) ((DataSourceScanOperator) op).getDataSource();
                if (dds.getDataset().getItemTypeDataverseName().equals("Metadata")
                        && dds.getDataset().getItemTypeName().equals("ChannelSubscriptionsType")) {
                    if (subscriptionsName.equals("") || dds.getDataset().getDatasetName().equals(subscriptionsName)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

}
