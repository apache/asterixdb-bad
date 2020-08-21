/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements. See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership. The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License. You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied. See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */
package org.apache.asterix.bad.rules;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.bad.BADConstants;
import org.apache.asterix.bad.function.BADFunctions;
import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.metadata.declared.DataSource;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class RewriteChannelTimeFunctionToLocalVarRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {

        Mutable<ILogicalExpression> exprRef;
        if (opRef.getValue().getOperatorTag() == LogicalOperatorTag.INNERJOIN) {
            InnerJoinOperator selectOp = (InnerJoinOperator) opRef.getValue();
            exprRef = selectOp.getCondition();
        } else if (opRef.getValue().getOperatorTag() == LogicalOperatorTag.SELECT) {
            SelectOperator selectOp = (SelectOperator) opRef.getValue();
            exprRef = selectOp.getCondition();
        } else {
            return false;
        }

        Set<Mutable<ILogicalExpression>> activeFunctionSet = new HashSet<>();
        Set<LogicalVariable> needPrevDsSet = new HashSet<>();
        Set<LogicalVariable> needCurrDsSet = new HashSet<>();
        Set<LogicalVariable> needActiveDsSet = new HashSet<>();

        // collect active functions
        collectChannelTimeFunctions(exprRef, activeFunctionSet, needPrevDsSet, needCurrDsSet, needActiveDsSet);
        if (activeFunctionSet.size() == 0) {
            return false;
        }

        // add assigns for active functions
        Map<LogicalVariable, LogicalVariable> prevMap = new HashMap<>();
        Map<LogicalVariable, LogicalVariable> currMap = new HashMap<>();
        Map<LogicalVariable, LogicalVariable> activeMap = new HashMap<>();
        createChannelTimeAssignOps(opRef, needPrevDsSet, needCurrDsSet, needActiveDsSet, prevMap, currMap, activeMap,
                context);

        // update expressions with new vars
        updateActiveFuncExprsWithVars(prevMap, currMap, activeMap, activeFunctionSet);

        context.computeAndSetTypeEnvironmentForOperator(opRef.getValue());
        return true;
    }

    private void updateActiveFuncExprsWithVars(Map<LogicalVariable, LogicalVariable> prevMap,
            Map<LogicalVariable, LogicalVariable> currMap, Map<LogicalVariable, LogicalVariable> activeMap,
            Set<Mutable<ILogicalExpression>> activeFuncExprs) {

        for (Mutable<ILogicalExpression> expr : activeFuncExprs) {
            AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr.getValue();
            LogicalVariable dsVar = ((VariableReferenceExpression) ((AbstractFunctionCallExpression) expr.getValue())
                    .getArguments().get(0).getValue()).getVariableReference();
            if (funcExpr.getFunctionIdentifier() == BADFunctions.CURRENT_CHANNEL_TIME) {
                expr.setValue(new VariableReferenceExpression(currMap.get(dsVar)));
            } else if (funcExpr.getFunctionIdentifier() == BADFunctions.PREVIOUS_CHANNEL_TIME) {
                expr.setValue(new VariableReferenceExpression(prevMap.get(dsVar)));
            } else {
                ILogicalExpression lessThanExpr =
                        new ScalarFunctionCallExpression(FunctionUtil.getFunctionInfo(AlgebricksBuiltinFunctions.LT),
                                new MutableObject<>(new VariableReferenceExpression(activeMap.get(dsVar))),
                                new MutableObject<>(new VariableReferenceExpression(currMap.get(dsVar))));
                ILogicalExpression greaterThanExpr =
                        new ScalarFunctionCallExpression(FunctionUtil.getFunctionInfo(AlgebricksBuiltinFunctions.GT),
                                new MutableObject<>(new VariableReferenceExpression(activeMap.get(dsVar))),
                                new MutableObject<>(new VariableReferenceExpression(prevMap.get(dsVar))));
                ScalarFunctionCallExpression andExpr =
                        new ScalarFunctionCallExpression(FunctionUtil.getFunctionInfo(BuiltinFunctions.AND),
                                new MutableObject<>(lessThanExpr), new MutableObject<>(greaterThanExpr));
                expr.setValue(andExpr);
            }
        }
    }

    private void createChannelTimeAssignOps(Mutable<ILogicalOperator> opRef, Set<LogicalVariable> needPrevDsSet,
            Set<LogicalVariable> needCurrDsSet, Set<LogicalVariable> needActiveDsSet,
            Map<LogicalVariable, LogicalVariable> prevMap, Map<LogicalVariable, LogicalVariable> currMap,
            Map<LogicalVariable, LogicalVariable> activeMap, IOptimizationContext context) {
        ILogicalOperator currOp = opRef.getValue();
        String channelName =
                (String) context.getMetadataProvider().getConfig().getOrDefault(BADConstants.CONFIG_CHANNEL_NAME, "");
        if (opRef.getValue().getOperatorTag() == LogicalOperatorTag.DATASOURCESCAN) {
            DataSourceScanOperator dataScanOp = (DataSourceScanOperator) opRef.getValue();
            DataSource ds = (DataSource) dataScanOp.getDataSource();
            LogicalVariable dsVar = ds.getDataRecordVariable(dataScanOp.getScanVariables());

            if (needPrevDsSet.contains(dsVar) || needActiveDsSet.contains(dsVar)) {
                LogicalVariable channelTimeVar = context.newVar();
                ILogicalExpression previousChannelTimeExpr = new ScalarFunctionCallExpression(
                        BuiltinFunctions.getAsterixFunctionInfo(BADFunctions.PREVIOUS_CHANNEL_TIME),
                        new MutableObject<>(
                                new ConstantExpression(new AsterixConstantValue(new AString(channelName)))));
                AssignOperator assignOp =
                        new AssignOperator(channelTimeVar, new MutableObject<>(previousChannelTimeExpr));
                assignOp.getInputs().add(new MutableObject<>(opRef.getValue()));
                opRef.setValue(assignOp);
                prevMap.put(dsVar, channelTimeVar);
            }

            if (needCurrDsSet.contains(dsVar) || needActiveDsSet.contains(dsVar)) {
                LogicalVariable channelTimeVar = context.newVar();
                ILogicalExpression previousChannelTimeExpr = new ScalarFunctionCallExpression(
                        BuiltinFunctions.getAsterixFunctionInfo(BADFunctions.CURRENT_CHANNEL_TIME), new MutableObject<>(
                                new ConstantExpression(new AsterixConstantValue(new AString(channelName)))));
                AssignOperator assignOp =
                        new AssignOperator(channelTimeVar, new MutableObject<>(previousChannelTimeExpr));
                assignOp.getInputs().add(new MutableObject<>(opRef.getValue()));
                opRef.setValue(assignOp);
                currMap.put(dsVar, channelTimeVar);
            }

            if (needActiveDsSet.contains(dsVar)) {
                LogicalVariable channelTimeVar = context.newVar();

                ILogicalExpression activeTsFunc =
                        new ScalarFunctionCallExpression(FunctionUtil.getFunctionInfo(BuiltinFunctions.META),
                                new MutableObject<>(new VariableReferenceExpression(dsVar)));
                ScalarFunctionCallExpression faExpr = new ScalarFunctionCallExpression(
                        FunctionUtil.getFunctionInfo(BuiltinFunctions.FIELD_ACCESS_BY_NAME),
                        new MutableObject<>(activeTsFunc), new MutableObject<>(new ConstantExpression(
                                new AsterixConstantValue(new AString(BADConstants.FIELD_NAME_ACTIVE_TS)))));
                AssignOperator assignOp = new AssignOperator(channelTimeVar, new MutableObject<>(faExpr));
                assignOp.getInputs().add(new MutableObject<>(opRef.getValue()));
                opRef.setValue(assignOp);
                activeMap.put(dsVar, channelTimeVar);
            }
        }
        for (Mutable<ILogicalOperator> input : currOp.getInputs()) {
            createChannelTimeAssignOps(input, needPrevDsSet, needCurrDsSet, needActiveDsSet, prevMap, currMap,
                    activeMap, context);
        }
    }

    private void collectChannelTimeFunctions(Mutable<ILogicalExpression> exprRef,
            Set<Mutable<ILogicalExpression>> activeFunctionSet, Set<LogicalVariable> needPrevDsSet,
            Set<LogicalVariable> needCurrDsSet, Set<LogicalVariable> needActive) {
        if (exprRef.getValue().getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) exprRef.getValue();
            if (funcExpr.getFunctionIdentifier() == BADFunctions.IS_NEW
                    || funcExpr.getFunctionIdentifier() == BADFunctions.PREVIOUS_CHANNEL_TIME
                    || funcExpr.getFunctionIdentifier() == BADFunctions.CURRENT_CHANNEL_TIME) {
                // add to active func set for later replacement
                activeFunctionSet.add(exprRef);
                // collect ds var to see what assign op needs to be added
                LogicalVariable dsVar = ((VariableReferenceExpression) funcExpr.getArguments().get(0).getValue())
                        .getVariableReference();
                if (funcExpr.getFunctionIdentifier() == BADFunctions.PREVIOUS_CHANNEL_TIME) {
                    needPrevDsSet.add(dsVar);
                } else if (funcExpr.getFunctionIdentifier() == BADFunctions.CURRENT_CHANNEL_TIME) {
                    needCurrDsSet.add(dsVar);
                } else {
                    needActive.add(dsVar);
                }
            } else {
                for (Mutable<ILogicalExpression> argExpr : funcExpr.getArguments()) {
                    collectChannelTimeFunctions(argExpr, activeFunctionSet, needPrevDsSet, needCurrDsSet, needActive);
                }
            }
        }
    }

}
