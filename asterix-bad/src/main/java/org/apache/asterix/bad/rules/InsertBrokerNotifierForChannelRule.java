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
import org.apache.asterix.bad.runtime.operators.NotifyBrokerOperator;
import org.apache.asterix.bad.runtime.operators.NotifyBrokerPOperator;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.metadata.declared.DatasetDataSource;
import org.apache.asterix.metadata.utils.MetadataConstants;
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
        DataverseName channelDataverse;
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
            if (!dds.getDataset().getItemTypeDataverseName().equals(MetadataConstants.METADATA_DATAVERSE_NAME)
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
        LogicalVariable brokerTypeVar = context.newVar();

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

        AssignOperator assignOp =
                createbrokerEndPointAssignOperator(brokerEndpointVar, brokerTypeVar, opAboveBrokersScan);

        visitGroupBy(op, assignOp, brokerEndpointVar, brokerTypeVar);

        context.computeAndSetTypeEnvironmentForOperator(assignOp);
        context.computeAndSetTypeEnvironmentForOperator(opAboveBrokersScan);
        context.computeAndSetTypeEnvironmentForOperator(op);

        ProjectOperator badProject = (ProjectOperator) findOp(op1, "project");
        badProject.getVariables().add(subscriptionIdVar);
        badProject.getVariables().add(brokerEndpointVar);
        badProject.getVariables().add(channelExecutionVar);
        badProject.getVariables().add(brokerTypeVar);
        context.computeAndSetTypeEnvironmentForOperator(badProject);

        //Create my brokerNotify plan above the extension Operator
        DelegateOperator dOp = push
                ? createNotifyBrokerPushPlan(brokerEndpointVar, badProject.getVariables().get(0), channelExecutionVar,
                        brokerTypeVar, context, op, (DistributeResultOperator) op1, channelDataverse, channelName)
                : createNotifyBrokerPullPlan(brokerEndpointVar, subscriptionIdVar, channelExecutionVar, brokerTypeVar,
                        context, op, (DistributeResultOperator) op1, channelDataverse, channelName);

        opRef.setValue(dOp);

        return true;
    }

    private boolean visitGroupBy(ILogicalOperator currOp, ILogicalOperator brokerAssignOp, LogicalVariable endpointVar,
            LogicalVariable typeVar) {
        // this method makes sure even the broker information is not projected out
        boolean containsBroker = false;
        if (currOp == brokerAssignOp) {
            return true;
        } else {
            for (Mutable<ILogicalOperator> input : currOp.getInputs()) {
                containsBroker = containsBroker || visitGroupBy(input.getValue(), brokerAssignOp, endpointVar, typeVar);
            }
        }
        if (currOp.getOperatorTag() == LogicalOperatorTag.GROUP && containsBroker) {
            GroupByOperator groupByOperator = (GroupByOperator) currOp;
            groupByOperator.addDecorExpression(null, new VariableReferenceExpression(endpointVar));
            groupByOperator.addDecorExpression(null, new VariableReferenceExpression(typeVar));
        }
        return containsBroker;
    }

    private DelegateOperator createBrokerOp(LogicalVariable brokerEndpointVar, LogicalVariable sendVar,
            LogicalVariable channelExecutionVar, LogicalVariable brokerTypeVar, DataverseName channelDataverse,
            String channelName, boolean push) {
        NotifyBrokerOperator notifyBrokerOp =
                new NotifyBrokerOperator(brokerEndpointVar, sendVar, channelExecutionVar, brokerTypeVar, push);
        EntityId activeId = new EntityId(BADConstants.RUNTIME_ENTITY_CHANNEL, channelDataverse, channelName);
        NotifyBrokerPOperator notifyBrokerPOp = new NotifyBrokerPOperator(activeId);
        notifyBrokerOp.setPhysicalOperator(notifyBrokerPOp);
        DelegateOperator extensionOp = new DelegateOperator(notifyBrokerOp);
        extensionOp.setPhysicalOperator(notifyBrokerPOp);
        return extensionOp;
    }

    private DelegateOperator createNotifyBrokerPushPlan(LogicalVariable brokerEndpointVar, LogicalVariable sendVar,
            LogicalVariable channelExecutionVar, LogicalVariable brokerTypeVar, IOptimizationContext context,
            ILogicalOperator eOp, DistributeResultOperator distributeOp, DataverseName channelDataverse,
            String channelName) throws AlgebricksException {
        //Find the assign operator to get the result type that we need
        AbstractLogicalOperator assign = (AbstractLogicalOperator) eOp.getInputs().get(0).getValue();
        while (assign.getOperatorTag() != LogicalOperatorTag.ASSIGN) {
            assign = (AbstractLogicalOperator) assign.getInputs().get(0).getValue();
        }

        //Create the NotifyBrokerOperator
        DelegateOperator extensionOp = createBrokerOp(brokerEndpointVar, sendVar, channelExecutionVar, brokerTypeVar,
                channelDataverse, channelName, true);

        extensionOp.getInputs().add(new MutableObject<>(eOp));
        context.computeAndSetTypeEnvironmentForOperator(extensionOp);

        return extensionOp;

    }

    private DelegateOperator createNotifyBrokerPullPlan(LogicalVariable brokerEndpointVar, LogicalVariable sendVar,
            LogicalVariable channelExecutionVar, LogicalVariable brokerTypeVar, IOptimizationContext context,
            ILogicalOperator eOp, DistributeResultOperator distributeOp, DataverseName channelDataverse,
            String channelName) throws AlgebricksException {

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
        groupbyOp.addGbyExpression(null, new VariableReferenceExpression(brokerTypeVar));
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
                brokerTypeVar, channelDataverse, channelName, false);

        //Set the input for the distinct as the old top
        extensionOp.getInputs().add(new MutableObject<>(groupbyOp));
        distinctOp.getInputs().add(new MutableObject<>(eOp));

        //compute environment bottom up
        context.computeAndSetTypeEnvironmentForOperator(distinctOp);
        context.computeAndSetTypeEnvironmentForOperator(nestedTupleSourceOp);
        context.computeAndSetTypeEnvironmentForOperator(listifyOp);
        context.computeAndSetTypeEnvironmentForOperator(groupbyOp);
        context.computeAndSetTypeEnvironmentForOperator(extensionOp);

        return extensionOp;

    }

    private AssignOperator createbrokerEndPointAssignOperator(LogicalVariable brokerEndpointVar,
            LogicalVariable brokerTypeVar, AbstractLogicalOperator opAboveBrokersScan) {
        Mutable<ILogicalExpression> endpointFieldName = new MutableObject<ILogicalExpression>(new ConstantExpression(
                new AsterixConstantValue(new AString(BADConstants.METADATA_TYPE_FIELD_NAME_BROKER_END_POINT))));
        Mutable<ILogicalExpression> brokerTypeFieldName = new MutableObject<ILogicalExpression>(new ConstantExpression(
                new AsterixConstantValue(new AString(BADConstants.METADATA_TYPE_FIELD_NAME_BROKER_TYPE))));
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

        ScalarFunctionCallExpression brokerEndpointFieldAccessor = new ScalarFunctionCallExpression(
                FunctionUtil.getFunctionInfo(BuiltinFunctions.FIELD_ACCESS_BY_NAME), varRef, endpointFieldName);
        ScalarFunctionCallExpression brokerTYpeFieldAccessor = new ScalarFunctionCallExpression(
                FunctionUtil.getFunctionInfo(BuiltinFunctions.FIELD_ACCESS_BY_NAME), varRef, brokerTypeFieldName);

        ArrayList<LogicalVariable> varArray = new ArrayList<LogicalVariable>(2);
        varArray.add(brokerEndpointVar);
        varArray.add(brokerTypeVar);

        ArrayList<Mutable<ILogicalExpression>> exprArray = new ArrayList<Mutable<ILogicalExpression>>(2);
        exprArray.add(new MutableObject<>(brokerEndpointFieldAccessor));
        exprArray.add(new MutableObject<>(brokerTYpeFieldAccessor));

        AssignOperator assignOp = new AssignOperator(varArray, exprArray);

        //Place assignOp between the scan and the op above it
        assignOp.getInputs().add(new MutableObject<>(brokerScan));
        opAboveBrokersScan.getInputs().set(index, new MutableObject<>(assignOp));

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
            if (lookingForString.equals("brokers") && isBrokerScan((AbstractLogicalOperator) subOp.getValue())) {
                return op;
            } else if (lookingForString.equals("project")
                    && subOp.getValue().getOperatorTag() == LogicalOperatorTag.PROJECT) {
                return (AbstractLogicalOperator) subOp.getValue();
            } else if (isSubscriptionsScan((AbstractLogicalOperator) subOp.getValue(), lookingForString)) {
                return (AbstractLogicalOperator) subOp.getValue();
            } else {
                AbstractLogicalOperator nestedOp = findOp((AbstractLogicalOperator) subOp.getValue(), lookingForString);
                if (nestedOp != null) {
                    return nestedOp;
                }
            }
        }
        return null;
    }

    private boolean isBrokerScan(AbstractLogicalOperator op) {
        if (op instanceof DataSourceScanOperator) {
            if (((DataSourceScanOperator) op).getDataSource() instanceof DatasetDataSource) {
                DatasetDataSource dds = (DatasetDataSource) ((DataSourceScanOperator) op).getDataSource();
                return dds.getDataset().getDataverseName().equals(MetadataConstants.METADATA_DATAVERSE_NAME)
                        && dds.getDataset().getDatasetName().equals(BADConstants.METADATA_DATASET_BROKER);
            }
        }
        return false;
    }

    private boolean isSubscriptionsScan(AbstractLogicalOperator op, String subscriptionsName) {
        if (op instanceof DataSourceScanOperator) {
            if (((DataSourceScanOperator) op).getDataSource() instanceof DatasetDataSource) {
                DatasetDataSource dds = (DatasetDataSource) ((DataSourceScanOperator) op).getDataSource();
                if (dds.getDataset().getItemTypeDataverseName().equals(MetadataConstants.METADATA_DATAVERSE_NAME)
                        && dds.getDataset().getItemTypeName().equals(BADConstants.METADATA_TYPENAME_SUBSCRIPTIONS)) {
                    return subscriptionsName.equals("") || dds.getDataset().getDatasetName().equals(subscriptionsName);
                }
            }
        }
        return false;
    }

}
