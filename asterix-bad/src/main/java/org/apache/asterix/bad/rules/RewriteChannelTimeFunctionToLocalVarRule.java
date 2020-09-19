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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.bad.BADConstants;
import org.apache.asterix.bad.function.BADFunctions;
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
        String channelName =
                (String) context.getMetadataProvider().getConfig().getOrDefault(BADConstants.CONFIG_CHANNEL_NAME, "");

        // collect active functions
        boolean rewriteFunc = collectChannelTimeFunctions(exprRef, activeFunctionSet, needPrevDsSet, needCurrDsSet,
                true, channelName);
        if (activeFunctionSet.size() == 0) {
            return rewriteFunc;
        }

        // add assigns for active functions
        Map<LogicalVariable, LogicalVariable> prevMap = new HashMap<>();
        Map<LogicalVariable, LogicalVariable> currMap = new HashMap<>();
        createChannelTimeAssignOps(opRef, needPrevDsSet, needCurrDsSet, prevMap, currMap, context, channelName);

        // update expressions with new vars
        updateActiveFuncExprsWithVars(prevMap, currMap, activeFunctionSet);

        context.computeAndSetTypeEnvironmentForOperator(opRef.getValue());
        return true;
    }

    private void updateActiveFuncExprsWithVars(Map<LogicalVariable, LogicalVariable> prevMap,
            Map<LogicalVariable, LogicalVariable> currMap, Set<Mutable<ILogicalExpression>> activeFuncExprs) {
        for (Mutable<ILogicalExpression> expr : activeFuncExprs) {
            AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr.getValue();
            LogicalVariable dsVar = ((VariableReferenceExpression) ((AbstractFunctionCallExpression) expr.getValue())
                    .getArguments().get(0).getValue()).getVariableReference();
            if (funcExpr.getFunctionIdentifier() == BADFunctions.CURRENT_CHANNEL_TIME) {
                expr.setValue(new VariableReferenceExpression(currMap.get(dsVar)));
            } else if (funcExpr.getFunctionIdentifier() == BADFunctions.PREVIOUS_CHANNEL_TIME) {
                expr.setValue(new VariableReferenceExpression(prevMap.get(dsVar)));
            }
        }
    }

    private void createChannelTimeAssignOps(Mutable<ILogicalOperator> opRef, Set<LogicalVariable> needPrevDsSet,
            Set<LogicalVariable> needCurrDsSet, Map<LogicalVariable, LogicalVariable> prevMap,
            Map<LogicalVariable, LogicalVariable> currMap, IOptimizationContext context, String channelName) {
        ILogicalOperator currOp = opRef.getValue();
        if (opRef.getValue().getOperatorTag() == LogicalOperatorTag.DATASOURCESCAN) {
            DataSourceScanOperator dataScanOp = (DataSourceScanOperator) opRef.getValue();
            DataSource ds = (DataSource) dataScanOp.getDataSource();
            LogicalVariable dsVar = ds.getDataRecordVariable(dataScanOp.getScanVariables());

            if (needPrevDsSet.contains(dsVar)) {
                LogicalVariable channelTimeVar = context.newVar();
                ILogicalExpression previousChannelTimeExpr = new ScalarFunctionCallExpression(
                        BuiltinFunctions.getBuiltinFunctionInfo(BADFunctions.PREVIOUS_CHANNEL_TIME),
                        new MutableObject<>(
                                new ConstantExpression(new AsterixConstantValue(new AString(channelName)))));
                AssignOperator assignOp =
                        new AssignOperator(channelTimeVar, new MutableObject<>(previousChannelTimeExpr));
                assignOp.getInputs().add(new MutableObject<>(opRef.getValue()));
                opRef.setValue(assignOp);
                prevMap.put(dsVar, channelTimeVar);
            }

            if (needCurrDsSet.contains(dsVar)) {
                LogicalVariable channelTimeVar = context.newVar();
                ILogicalExpression previousChannelTimeExpr = new ScalarFunctionCallExpression(
                        BuiltinFunctions.getBuiltinFunctionInfo(BADFunctions.CURRENT_CHANNEL_TIME), new MutableObject<>(
                                new ConstantExpression(new AsterixConstantValue(new AString(channelName)))));
                AssignOperator assignOp =
                        new AssignOperator(channelTimeVar, new MutableObject<>(previousChannelTimeExpr));
                assignOp.getInputs().add(new MutableObject<>(opRef.getValue()));
                opRef.setValue(assignOp);
                currMap.put(dsVar, channelTimeVar);
            }
        }
        for (Mutable<ILogicalOperator> input : currOp.getInputs()) {
            createChannelTimeAssignOps(input, needPrevDsSet, needCurrDsSet, prevMap, currMap, context, channelName);
        }
    }

    private boolean collectChannelTimeFunctions(Mutable<ILogicalExpression> exprRef,
            Set<Mutable<ILogicalExpression>> activeFunctionSet, Set<LogicalVariable> needPrevDsSet,
            Set<LogicalVariable> needCurrDsSet, boolean disjunctiveFlag, String channelName) {
        boolean rewriteFunc = false;
        if (exprRef.getValue().getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) exprRef.getValue();
            if (funcExpr.getFunctionIdentifier().equals(BuiltinFunctions.OR)) {
                disjunctiveFlag = false;
            }
            if (funcExpr.getFunctionIdentifier() == BADFunctions.PREVIOUS_CHANNEL_TIME
                    || funcExpr.getFunctionIdentifier() == BADFunctions.CURRENT_CHANNEL_TIME) {
                // collect ds var to see what assign op needs to be added
                LogicalExpressionTag arg0ExprTag = funcExpr.getArguments().get(0).getValue().getExpressionTag();
                if (arg0ExprTag == LogicalExpressionTag.CONSTANT) {
                    return false;
                }
                if (!disjunctiveFlag) {
                    LogicalVariable dsVar = ((VariableReferenceExpression) funcExpr.getArguments().get(0).getValue())
                            .getVariableReference();
                    activeFunctionSet.add(exprRef);
                    if (funcExpr.getFunctionIdentifier() == BADFunctions.PREVIOUS_CHANNEL_TIME) {
                        needPrevDsSet.add(dsVar);
                    } else if (funcExpr.getFunctionIdentifier() == BADFunctions.CURRENT_CHANNEL_TIME) {
                        needCurrDsSet.add(dsVar);
                    }
                } else {
                    // if disjunctive, modify to get ts and wait for introduce filter rule to work
                    funcExpr.getArguments().set(0, new MutableObject<>(
                            new ConstantExpression(new AsterixConstantValue(new AString(channelName)))));
                    rewriteFunc = true;
                }
            } else {
                for (Mutable<ILogicalExpression> argExpr : funcExpr.getArguments()) {
                    rewriteFunc = rewriteFunc || collectChannelTimeFunctions(argExpr, activeFunctionSet, needPrevDsSet,
                            needCurrDsSet, disjunctiveFlag, channelName);
                }
            }
        }
        return rewriteFunc;
    }

}
