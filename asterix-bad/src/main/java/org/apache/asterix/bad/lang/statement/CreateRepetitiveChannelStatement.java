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
package org.apache.asterix.bad.lang.statement;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;

import org.apache.asterix.common.exceptions.MetadataException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.literal.StringLiteral;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Function;
import org.apache.asterix.om.base.temporal.ADurationParserFactory;
import org.apache.asterix.translator.IStatementExecutor;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.parsers.IValueParser;

public class CreateRepetitiveChannelStatement extends AbstractCreateChannelStatement {
    public CreateRepetitiveChannelStatement(DataverseName dataverseName, Identifier channelName,
            FunctionSignature function, Expression period, boolean push) {
        super(dataverseName, channelName, period, push);
        this.function = function;
    }

    @Override
    protected void initialize(IStatementExecutor statementExecutor, MetadataProvider metadataProvider,
            MetadataTransactionContext mdTxnCtx) throws AlgebricksException, HyracksDataException {
        Function lookup = MetadataManager.INSTANCE.getFunction(mdTxnCtx, function);
        if (lookup == null) {
            throw new MetadataException(" Unknown function " + function.getName());
        }

        if (!period.getFunctionSignature().getName().equals("duration")) {
            throw new MetadataException(
                    "Expected argument period as a duration, but got " + period.getFunctionSignature().getName() + ".");
        }
        duration = ((StringLiteral) ((LiteralExpr) period.getExprList().get(0)).getValue()).getValue();
        IValueParser durationParser = ADurationParserFactory.INSTANCE.createValueParser();
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream outputStream = new DataOutputStream(bos);
        durationParser.parse(duration.toCharArray(), 0, duration.toCharArray().length, outputStream);
    }
}
