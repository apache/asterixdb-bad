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
package org.apache.asterix.bad.rules;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.bad.BADConstants;
import org.apache.asterix.bad.function.BADFunctions;
import org.apache.asterix.lang.common.util.FunctionUtil;
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
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class RewriteActiveFunctionsRule implements IAlgebraicRewriteRule {

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
        return visit(exprRef, opRef, context);
    }

    private ScalarFunctionCallExpression makeActiveTsAccessExpr(LogicalVariable dsVar) {
        ILogicalExpression activeTsFunc =
                new ScalarFunctionCallExpression(FunctionUtil.getFunctionInfo(BuiltinFunctions.META),
                        new MutableObject<>(new VariableReferenceExpression(dsVar)));
        return new ScalarFunctionCallExpression(FunctionUtil.getFunctionInfo(BuiltinFunctions.FIELD_ACCESS_BY_NAME),
                new MutableObject<>(activeTsFunc), new MutableObject<>(new ConstantExpression(
                        new AsterixConstantValue(new AString(BADConstants.FIELD_NAME_ACTIVE_TS)))));
    }

    private AbstractFunctionCallExpression makeAndForIsNew(AbstractFunctionCallExpression funcExpr) {
        LogicalVariable dsVar = getDsVar(funcExpr);
        ILogicalExpression previousChannelTimeExpr = new ScalarFunctionCallExpression(
                BuiltinFunctions.getBuiltinFunctionInfo(BADFunctions.PREVIOUS_CHANNEL_TIME),
                new MutableObject<>(new VariableReferenceExpression(dsVar)));
        ILogicalExpression currentChannelTimeExpr = new ScalarFunctionCallExpression(
                BuiltinFunctions.getBuiltinFunctionInfo(BADFunctions.CURRENT_CHANNEL_TIME),
                new MutableObject<>(new VariableReferenceExpression(dsVar)));
        ILogicalExpression lessThanExpr = new ScalarFunctionCallExpression(
                FunctionUtil.getFunctionInfo(AlgebricksBuiltinFunctions.LT),
                new MutableObject<>(makeActiveTsAccessExpr(dsVar)), new MutableObject<>(currentChannelTimeExpr));
        ILogicalExpression greaterThanExpr = new ScalarFunctionCallExpression(
                FunctionUtil.getFunctionInfo(AlgebricksBuiltinFunctions.GT),
                new MutableObject<>(makeActiveTsAccessExpr(dsVar)), new MutableObject<>(previousChannelTimeExpr));
        return new ScalarFunctionCallExpression(FunctionUtil.getFunctionInfo(BuiltinFunctions.AND),
                new MutableObject<>(lessThanExpr), new MutableObject<>(greaterThanExpr));
    }

    private LogicalVariable getDsVar(AbstractFunctionCallExpression funcExpr) {
        return ((VariableReferenceExpression) funcExpr.getArguments().get(0).getValue()).getVariableReference();
    }

    private boolean visit(Mutable<ILogicalExpression> exprRef, Mutable<ILogicalOperator> opRef,
            IOptimizationContext context) throws AlgebricksException {
        boolean changed = false;
        if (exprRef.getValue().getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) exprRef.getValue();

            if (funcExpr.getFunctionIdentifier() == BADFunctions.IS_NEW) {
                exprRef.setValue(makeAndForIsNew(funcExpr));
                changed = true;
            } else if (funcExpr.getFunctionIdentifier() == BADFunctions.ACTIVE_TIMESTAMP) {
                LogicalVariable channelTimeVar = context.newVar();
                LogicalVariable dsVar = getDsVar(funcExpr);
                ILogicalExpression activeTsFunc =
                        new ScalarFunctionCallExpression(FunctionUtil.getFunctionInfo(BuiltinFunctions.META),
                                new MutableObject<>(new VariableReferenceExpression(dsVar)));
                ScalarFunctionCallExpression faExpr = new ScalarFunctionCallExpression(
                        FunctionUtil.getFunctionInfo(BuiltinFunctions.FIELD_ACCESS_BY_NAME),
                        new MutableObject<>(activeTsFunc), new MutableObject<>(new ConstantExpression(
                                new AsterixConstantValue(new AString(BADConstants.FIELD_NAME_ACTIVE_TS)))));

                AssignOperator assignOp = new AssignOperator(channelTimeVar, new MutableObject<>(faExpr));

                List<LogicalVariable> liveVars = new ArrayList<>();
                for (int i = 0; i < opRef.getValue().getInputs().size(); i++) {
                    Mutable<ILogicalOperator> inputOpRef = opRef.getValue().getInputs().get(i);
                    VariableUtilities.getLiveVariables(inputOpRef.getValue(), liveVars);
                    if (liveVars.contains(dsVar)) {
                        assignOp.getInputs().add(new MutableObject<>(inputOpRef.getValue()));
                        inputOpRef.setValue(assignOp);
                        exprRef.setValue(new VariableReferenceExpression(channelTimeVar));
                        context.computeAndSetTypeEnvironmentForOperator(assignOp);
                    }
                }

                changed = true;
            } else {
                for (Mutable<ILogicalExpression> argExpr : funcExpr.getArguments()) {
                    changed = changed || visit(argExpr, opRef, context);
                }
            }
        }
        return changed;
    }

}
