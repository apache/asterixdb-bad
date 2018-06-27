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
import org.apache.asterix.om.functions.FunctionInfo;
import org.apache.asterix.om.types.IAType;
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
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.UnnestingFunctionCallExpression;
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
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
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
        AbstractLogicalOperator op = findOp(op1, 4, "", "");
        if (op == null) {
            push = true;
        }

        DataSourceScanOperator subscriptionsScan;
        String channelDataverse;
        String channelName;

        if (!push) {
            AbstractLogicalOperator descendantOp = (AbstractLogicalOperator) op.getInputs().get(0).getValue();
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
            String subscriptionsName = channelName + "ChannelSubscriptions";
            subscriptionsScan =
                    (DataSourceScanOperator) findOp(op, 3, subscriptionsName, BADConstants.ChannelSubscriptionsType);
            if (subscriptionsScan == null) {
                return false;
            }

        } else {
            op = (AbstractLogicalOperator) op1.getInputs().get(0).getValue();
            if (op.getOperatorTag() != LogicalOperatorTag.PROJECT) {
                return false;
            }
            //if push, get the channel name here instead
            subscriptionsScan = (DataSourceScanOperator) findOp(op, 3, "", BADConstants.ChannelSubscriptionsType);
            if (subscriptionsScan == null) {
                return false;
            }
            DatasetDataSource dds = (DatasetDataSource) subscriptionsScan.getDataSource();
            String datasetName = dds.getDataset().getDatasetName();
            channelDataverse = dds.getDataset().getDataverseName();
            channelName = datasetName.substring(0, datasetName.length() - 13);
        }

        //The channelExecutionTime is created just before the scan
        ILogicalOperator channelExecutionAssign = subscriptionsScan.getInputs().get(0).getValue();
        if (channelExecutionAssign.getOperatorTag() != LogicalOperatorTag.ASSIGN) {
            return false;
        }
        LogicalVariable channelExecutionVar = ((AssignOperator) channelExecutionAssign).getVariables().get(0);
        if (!channelExecutionVar.toString().equals("$$" + BADConstants.ChannelExecutionTime)) {
            return false;
        }

        //move broker scan
        SubplanOperator subplanOperator = (SubplanOperator) findOp(op, 1, "", "");
        if (subplanOperator == null) {
            return false;
        }

        LogicalVariable brokerEndpoint = context.newVar();
        LogicalVariable brokerSubId = context.newVar();
        LogicalVariable brokerSubsVar =
                ((AggregateOperator) subplanOperator.getNestedPlans().get(0).getRoots().get(0).getValue())
                        .getVariables().get(0);

        AssignOperator newAssign = createAssignsAndUnnest(brokerSubsVar, brokerEndpoint, brokerSubId, op, context);

        context.computeAndSetTypeEnvironmentForOperator(op1);

        //Maybe we need to add a project???
        ProjectOperator badProject = (ProjectOperator) findOp(op1, 2, "", "");
        badProject.getVariables().add(channelExecutionVar);
        badProject.getVariables().add(brokerSubsVar);
        context.computeAndSetTypeEnvironmentForOperator(badProject);

        //Create my brokerNotify plan above the extension Operator
        DelegateOperator dOp = push
                ? createNotifyBrokerPushPlan(brokerEndpoint, badProject.getVariables().get(0), channelExecutionVar,
                        context, newAssign, (DistributeResultOperator) op1, channelDataverse, channelName)
                : createNotifyBrokerPullPlan(brokerEndpoint, brokerSubId, channelExecutionVar, context, newAssign,
                        (DistributeResultOperator) op1, channelDataverse, channelName);

        opRef.setValue(dOp);

        return true;
    }

    private AssignOperator createAssignsAndUnnest(LogicalVariable brokerSubsVar, LogicalVariable brokerEndpoint,
            LogicalVariable brokerSubId, AbstractLogicalOperator op, IOptimizationContext context)
            throws AlgebricksException {

        FunctionInfo finfoGetField =
                (FunctionInfo) FunctionUtil.getFunctionInfo(BuiltinFunctions.FIELD_ACCESS_BY_NAME);
        FunctionInfo finfoScanCollection =
                (FunctionInfo) FunctionUtil.getFunctionInfo(BuiltinFunctions.SCAN_COLLECTION);

        UnnestingFunctionCallExpression scanBrokerSubsVar = new UnnestingFunctionCallExpression(finfoScanCollection,
                new MutableObject<>(new VariableReferenceExpression(brokerSubsVar)));
        LogicalVariable unnestedBrokerSubsVar = context.newVar();
        UnnestOperator unnestBrokerSubsVar =
                new UnnestOperator(unnestedBrokerSubsVar, new MutableObject<>(scanBrokerSubsVar));
        unnestBrokerSubsVar.getInputs().add(new MutableObject<>(op));


        ScalarFunctionCallExpression getBrokerEndPoint = new ScalarFunctionCallExpression(finfoGetField,
                new MutableObject<>(new VariableReferenceExpression(unnestedBrokerSubsVar)), new MutableObject<>(
                        new ConstantExpression(new AsterixConstantValue(new AString(BADConstants.BrokerEndPoint)))));

        AssignOperator assignBrokerEndPoint =
                new AssignOperator(brokerEndpoint, new MutableObject<>(getBrokerEndPoint));
        assignBrokerEndPoint.getInputs().add(new MutableObject<>(unnestBrokerSubsVar));


        ScalarFunctionCallExpression getBrokerSubId = new ScalarFunctionCallExpression(finfoGetField,
                new MutableObject<>(new VariableReferenceExpression(unnestedBrokerSubsVar)),
                new MutableObject<>(new ConstantExpression(
                        new AsterixConstantValue(new AString(BADConstants.BrokerSubscriptionId)))));


        AssignOperator assignBrokerSubId = new AssignOperator(brokerSubId, new MutableObject<>(getBrokerSubId));
        assignBrokerSubId.getInputs().add(new MutableObject<>(assignBrokerEndPoint));


        context.computeAndSetTypeEnvironmentForOperator(unnestBrokerSubsVar);
        context.computeAndSetTypeEnvironmentForOperator(assignBrokerEndPoint);
        context.computeAndSetTypeEnvironmentForOperator(assignBrokerSubId);

        return assignBrokerSubId;

    }



    private DelegateOperator createBrokerOp(LogicalVariable brokerEndpointVar, LogicalVariable sendVar,
            LogicalVariable channelExecutionVar, String channelDataverse, String channelName, boolean push,
            IAType resultType) {
        NotifyBrokerOperator notifyBrokerOp =
                new NotifyBrokerOperator(brokerEndpointVar, sendVar, channelExecutionVar, push, resultType);
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
        IVariableTypeEnvironment env = assign.computeOutputTypeEnvironment(context);
        IAType resultType = (IAType) env.getVarType(sendVar);

        //Create the NotifyBrokerOperator
        DelegateOperator extensionOp = createBrokerOp(brokerEndpointVar, sendVar, channelExecutionVar, channelDataverse,
                channelName, true, resultType);

        extensionOp.getInputs().add(new MutableObject<>(eOp));
        context.computeAndSetTypeEnvironmentForOperator(extensionOp);

        return extensionOp;

    }

    private DelegateOperator createNotifyBrokerPullPlan(LogicalVariable brokerEndpointVar,
            LogicalVariable sendVar, LogicalVariable channelExecutionVar, IOptimizationContext context,
            ILogicalOperator eOp, DistributeResultOperator distributeOp, String channelDataverse, String channelName)
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
                channelDataverse, channelName, false, null);

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


    /*This function is used to find specific operators within the plan, either
     * 1. The assign for the record being inserted
     * 2. The highest project of the plan
     * 3. The subscriptions scan
     * 4. Commit operator
     *
     * param1 is the name of the expected subscriptions dataset when searching for a subscriptions scan
     * and param2 is the type of the subscriptions dataset (channel or broker)
     */
    private AbstractLogicalOperator findOp(AbstractLogicalOperator op, int searchId, String param1, String param2) {
        if (!op.hasInputs()) {
            return null;
        }
        for (Mutable<ILogicalOperator> subOp : op.getInputs()) {
            if (searchId == 1) {
                if (subOp.getValue().getOperatorTag() == LogicalOperatorTag.SUBPLAN) {
                    if (op.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
                        ScalarFunctionCallExpression resultCreation =
                                (ScalarFunctionCallExpression) ((AssignOperator) op).getExpressions().get(0).getValue();

                        resultCreation.getArguments().remove(resultCreation.getArguments().size() - 1);
                        resultCreation.getArguments().remove(resultCreation.getArguments().size() - 1);
                        return (AbstractLogicalOperator) subOp.getValue();
                    }
                } else {
                    AbstractLogicalOperator nestedOp = findOp((AbstractLogicalOperator) subOp.getValue(),
                            searchId, param1, param2);
                    if (nestedOp != null) {
                        return nestedOp;
                    }
                }

            } else if (searchId == 2) {
                if (subOp.getValue().getOperatorTag() == LogicalOperatorTag.PROJECT) {
                    return (AbstractLogicalOperator) subOp.getValue();
                } else {
                    AbstractLogicalOperator nestedOp = findOp((AbstractLogicalOperator) subOp.getValue(),
                            searchId, param1, param2);
                    if (nestedOp != null) {
                        return nestedOp;
                    }
                }
            }

            else if (searchId == 3) {
                if (isSubscriptionsScan((AbstractLogicalOperator) subOp.getValue(), param1, param2)) {
                    return (AbstractLogicalOperator) subOp.getValue();
                } else {
                    AbstractLogicalOperator nestedOp = findOp((AbstractLogicalOperator) subOp.getValue(),
                            searchId, param1, param2);
                    if (nestedOp != null) {
                        return nestedOp;
                    }
                }

            } else if (searchId == 4) {
                if (subOp.getValue().getOperatorTag() == LogicalOperatorTag.DELEGATE_OPERATOR) {
                    DelegateOperator dOp = (DelegateOperator) subOp.getValue();
                    if (dOp.getDelegate() instanceof CommitOperator) {
                        return (AbstractLogicalOperator) subOp.getValue();
                    }
                } else {
                    AbstractLogicalOperator nestedOp =
                            findOp((AbstractLogicalOperator) subOp.getValue(), searchId, param1, param2);
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

    private boolean isSubscriptionsScan(AbstractLogicalOperator op, String subscriptionsName, String subscriptionType) {
        if (op instanceof DataSourceScanOperator) {
            if (((DataSourceScanOperator) op).getDataSource() instanceof DatasetDataSource) {
                DatasetDataSource dds = (DatasetDataSource) ((DataSourceScanOperator) op).getDataSource();
                if (dds.getDataset().getItemTypeDataverseName().equals("Metadata")
                        && dds.getDataset().getItemTypeName().equals(subscriptionType)) {
                    if (subscriptionsName.equals("") || dds.getDataset().getDatasetName().equals(subscriptionsName)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

}
