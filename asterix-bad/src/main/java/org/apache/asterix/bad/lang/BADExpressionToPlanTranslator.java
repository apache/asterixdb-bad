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

package org.apache.asterix.bad.lang;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.asterix.algebra.operators.CommitOperator;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.metadata.declared.DatasetDataSource;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.utils.DatasetUtil;
import org.apache.asterix.om.base.ADateTime;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.translator.CompiledStatements;
import org.apache.asterix.translator.SqlppExpressionToPlanTranslator;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IAlgebricksConstantValue;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DelegateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InsertDeleteUpsertOperator;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.api.result.IResultMetadata;

/**
 * This class overrides the SqlppExpressionToPlanTranslator in AsterixDB to allow insert/upsert to
 * datasets with meta records (active datasets). If inserting/upserting into an active dataset, the plan
 * translator would attach a dummy active records containing the current timestamp as the active timestamp.
 * In the case of data feeds, this active timestamp would then be updated by
 * BADLSMPrimaryInsertOperatorNodePushable/BADLSMPrimaryUpsertOperatorNodePushable.
 * Updates to SqlppExpressionToPlanTranslator in the AsterixDB master needs to be propagated into this class when
 * bringing the BAD codebase to latest master.
 *
 * IMPORTANT NOTE: Currently, we assume active datasets are the only user of the datasets with meta records.
 * If one want to use meta datasets in the BAD branch in the future, this needs to be refactored.
 */
public class BADExpressionToPlanTranslator extends SqlppExpressionToPlanTranslator {

    public BADExpressionToPlanTranslator(MetadataProvider metadataProvider, int currentVarCounter,
            Map<VarIdentifier, IAObject> externalVars) throws AlgebricksException {
        super(metadataProvider, currentVarCounter, externalVars);
    }

    @Override
    protected ILogicalOperator translateDelete(DatasetDataSource targetDatasource, Mutable<ILogicalExpression> varRef,
            List<Mutable<ILogicalExpression>> varRefsForLoading, LogicalVariable seqVar, ILogicalOperator pkeyAssignOp,
            CompiledStatements.ICompiledDmlStatement stmt) throws AlgebricksException {
        SourceLocation sourceLoc = stmt.getSourceLocation();
        InsertDeleteUpsertOperator deleteOp;
        LogicalVariable metaVar = null;

        if (!targetDatasource.getDataset().hasMetaPart()) {
            deleteOp = new InsertDeleteUpsertOperator(targetDatasource, varRef, varRefsForLoading,
                    InsertDeleteUpsertOperator.Kind.DELETE, false);
        } else {
            // prepare meta record
            IAType metaType = metadataProvider.findMetaType(targetDatasource.getDataset());
            metaVar = context.newVar();
            AssignOperator metaVariableAssignOp =
                    new AssignOperator(metaVar, new MutableObject<>(makeMetaRecordExpr(metaType)));
            metaVariableAssignOp.getInputs().add(new MutableObject<>(pkeyAssignOp));
            metaVariableAssignOp.setSourceLocation(sourceLoc);
            // create insert op uses meta record
            deleteOp = new InsertDeleteUpsertOperator(targetDatasource, varRef, varRefsForLoading,
                    Collections.singletonList(new MutableObject<>(new VariableReferenceExpression(metaVar))),
                    InsertDeleteUpsertOperator.Kind.DELETE, false);
            // change current inputOp to be meta op
            pkeyAssignOp = metaVariableAssignOp;
        }

        deleteOp.getInputs().add(new MutableObject<>(pkeyAssignOp));
        deleteOp.setSourceLocation(sourceLoc);

        List<String> filterField = DatasetUtil.getFilterField(targetDatasource.getDataset());
        List<Mutable<ILogicalExpression>> filterExprs = null;
        Integer filterSourceIndicator = DatasetUtil.getFilterSourceIndicator(targetDatasource.getDataset());
        if (filterField != null) {
            filterExprs = generatedFilterExprs(deleteOp, filterField, filterSourceIndicator == 0 ? seqVar : metaVar,
                    sourceLoc);
        }
        deleteOp.setAdditionalFilteringExpressions(filterExprs);

        DelegateOperator leafOperator = new DelegateOperator(new CommitOperator(true));
        leafOperator.getInputs().add(new MutableObject<>(deleteOp));
        leafOperator.setSourceLocation(sourceLoc);
        return leafOperator;
    }

    @Override
    protected ILogicalOperator translateUpsert(DatasetDataSource targetDatasource,
            Mutable<ILogicalExpression> payloadVarRef, List<Mutable<ILogicalExpression>> varRefsForLoading,
            ILogicalOperator pkeyAssignOp, LogicalVariable unnestVar, ILogicalOperator topOp,
            List<Mutable<ILogicalExpression>> pkeyExprs, LogicalVariable seqVar,
            CompiledStatements.ICompiledDmlStatement stmt, IResultMetadata resultMetadata) throws AlgebricksException {
        SourceLocation sourceLoc = stmt.getSourceLocation();
        CompiledStatements.CompiledUpsertStatement compiledUpsert = (CompiledStatements.CompiledUpsertStatement) stmt;
        Expression returnExpression = compiledUpsert.getReturnExpression();
        InsertDeleteUpsertOperator upsertOp;
        ILogicalOperator rootOperator;
        LogicalVariable metaVar = null;

        ARecordType recordType = (ARecordType) targetDatasource.getItemType();

        if (targetDatasource.getDataset().hasMetaPart()) {
            IAType metaType = metadataProvider.findMetaType(targetDatasource.getDataset());
            metaVar = context.newVar();
            AssignOperator metaVariableAssignOp =
                    new AssignOperator(metaVar, new MutableObject<>(makeMetaRecordExpr(metaType)));
            metaVariableAssignOp.getInputs().add(new MutableObject<>(pkeyAssignOp));
            pkeyAssignOp = metaVariableAssignOp;

            metaVariableAssignOp.setSourceLocation(sourceLoc);
            List<Mutable<ILogicalExpression>> metaExprs = new ArrayList<>(1);
            VariableReferenceExpression metaVarRef = new VariableReferenceExpression(metaVar);
            metaExprs.add(new MutableObject<>(metaVarRef));
            upsertOp = new InsertDeleteUpsertOperator(targetDatasource, payloadVarRef, varRefsForLoading, metaExprs,
                    InsertDeleteUpsertOperator.Kind.UPSERT, false);

            // set previous meta vars
            List<LogicalVariable> metaVars = new ArrayList<>();
            metaVars.add(context.newVar());
            upsertOp.setPrevAdditionalNonFilteringVars(metaVars);
            List<Object> metaTypes = new ArrayList<>();
            metaTypes.add(targetDatasource.getMetaItemType());
            upsertOp.setPrevAdditionalNonFilteringTypes(metaTypes);
        } else {
            upsertOp = new InsertDeleteUpsertOperator(targetDatasource, payloadVarRef, varRefsForLoading,
                    InsertDeleteUpsertOperator.Kind.UPSERT, false);
        }

        // Create and add a new variable used for representing the original record
        upsertOp.setUpsertIndicatorVar(context.newVar());
        upsertOp.setUpsertIndicatorVarType(BuiltinType.ABOOLEAN);
        upsertOp.setPrevRecordVar(context.newVar());
        upsertOp.setPrevRecordType(recordType);
        upsertOp.setSourceLocation(sourceLoc);
        upsertOp.getInputs().add(new MutableObject<>(pkeyAssignOp));

        List<String> filterField = DatasetUtil.getFilterField(targetDatasource.getDataset());
        List<Mutable<ILogicalExpression>> filterExprs = null;
        Integer filterSourceIndicator = DatasetUtil.getFilterSourceIndicator(targetDatasource.getDataset());
        if (filterField != null) {
            filterExprs = generatedFilterExprs(upsertOp, filterField, filterSourceIndicator == 0 ? seqVar : metaVar,
                    sourceLoc);
            ARecordType filterSourceType = filterSourceIndicator == 0 ? (ARecordType) targetDatasource.getItemType()
                    : (ARecordType) targetDatasource.getMetaItemType();
            upsertOp.setAdditionalFilteringExpressions(filterExprs);
            upsertOp.setPrevFilterVar(context.newVar());
            upsertOp.setPrevFilterType(filterSourceType.getFieldType(filterField.get(0)));
        } else {
            upsertOp.setAdditionalFilteringExpressions(null);
        }

        // Set up delegate operator
        DelegateOperator delegateOperator = new DelegateOperator(new CommitOperator(returnExpression == null));
        delegateOperator.getInputs().add(new MutableObject<>(upsertOp));
        delegateOperator.setSourceLocation(sourceLoc);
        rootOperator = delegateOperator;

        // Compiles the return expression.
        return processReturningExpression(rootOperator, upsertOp, compiledUpsert, resultMetadata);
    }

    @Override
    protected ILogicalOperator translateInsert(DatasetDataSource targetDatasource, Mutable<ILogicalExpression> varRef,
            List<Mutable<ILogicalExpression>> varRefsForLoading, LogicalVariable seqVar, ILogicalOperator pkeyAssignOp,
            CompiledStatements.ICompiledDmlStatement stmt, IResultMetadata resultMetadata) throws AlgebricksException {
        SourceLocation sourceLoc = stmt.getSourceLocation();
        LogicalVariable metaVar = null;

        InsertDeleteUpsertOperator insertOp;
        if (!targetDatasource.getDataset().hasMetaPart()) {
            insertOp = new InsertDeleteUpsertOperator(targetDatasource, varRef, varRefsForLoading,
                    InsertDeleteUpsertOperator.Kind.INSERT, false);
        } else {
            // prepare meta record
            IAType metaType = metadataProvider.findMetaType(targetDatasource.getDataset());
            metaVar = context.newVar();
            AssignOperator metaVariableAssignOp =
                    new AssignOperator(metaVar, new MutableObject<>(makeMetaRecordExpr(metaType)));
            metaVariableAssignOp.getInputs().add(new MutableObject<>(pkeyAssignOp));
            metaVariableAssignOp.setSourceLocation(sourceLoc);
            // create insert op uses meta record
            insertOp = new InsertDeleteUpsertOperator(targetDatasource, varRef, varRefsForLoading,
                    Collections.singletonList(new MutableObject<>(new VariableReferenceExpression(metaVar))),
                    InsertDeleteUpsertOperator.Kind.INSERT, false);
            // change current inputOp to be meta op
            pkeyAssignOp = metaVariableAssignOp;
        }

        insertOp.getInputs().add(new MutableObject<>(pkeyAssignOp));
        insertOp.setSourceLocation(sourceLoc);

        List<String> filterField = DatasetUtil.getFilterField(targetDatasource.getDataset());
        List<Mutable<ILogicalExpression>> filterExprs = null;
        Integer filterSourceIndicator = DatasetUtil.getFilterSourceIndicator(targetDatasource.getDataset());
        if (filterField != null) {
            filterExprs = generatedFilterExprs(insertOp, filterField, filterSourceIndicator == 0 ? seqVar : metaVar,
                    sourceLoc);
        }
        insertOp.setAdditionalFilteringExpressions(filterExprs);

        // Adds the commit operator.
        CompiledStatements.CompiledInsertStatement compiledInsert = (CompiledStatements.CompiledInsertStatement) stmt;
        Expression returnExpression = compiledInsert.getReturnExpression();
        DelegateOperator rootOperator = new DelegateOperator(new CommitOperator(returnExpression == null));
        rootOperator.getInputs().add(new MutableObject<>(insertOp));
        rootOperator.setSourceLocation(sourceLoc);

        // Compiles the return expression.
        return processReturningExpression(rootOperator, insertOp, compiledInsert, resultMetadata);
    }

    private ILogicalExpression makeMetaRecordExpr(IAType metaRecordType) {
        ARecord metaRecord =
                new ARecord((ARecordType) metaRecordType, new IAObject[] { new ADateTime(System.currentTimeMillis()) });
        IAlgebricksConstantValue metaConstantVal = new AsterixConstantValue(metaRecord);
        ILogicalExpression expr = new ConstantExpression(metaConstantVal);
        return expr;
    }
}
