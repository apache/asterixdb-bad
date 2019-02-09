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
package org.apache.asterix.bad.lang.statement;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.asterix.active.EntityId;
import org.apache.asterix.algebra.extension.ExtensionStatement;
import org.apache.asterix.app.active.ActiveNotificationHandler;
import org.apache.asterix.app.translator.QueryTranslator;
import org.apache.asterix.bad.BADConstants;
import org.apache.asterix.bad.BADJobService;
import org.apache.asterix.bad.lang.BADLangExtension;
import org.apache.asterix.bad.lang.BADParserFactory;
import org.apache.asterix.bad.metadata.DeployedJobSpecEventListener;
import org.apache.asterix.bad.metadata.DeployedJobSpecEventListener.PrecompiledType;
import org.apache.asterix.bad.metadata.Procedure;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.MetadataException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.literal.StringLiteral;
import org.apache.asterix.lang.common.statement.DeleteStatement;
import org.apache.asterix.lang.common.statement.InsertStatement;
import org.apache.asterix.lang.common.statement.Query;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.lang.sqlpp.rewrites.SqlppRewriterFactory;
import org.apache.asterix.lang.sqlpp.visitor.SqlppDeleteRewriteVisitor;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Function;
import org.apache.asterix.om.base.temporal.ADurationParserFactory;
import org.apache.asterix.translator.IRequestParameters;
import org.apache.asterix.translator.IStatementExecutor;
import org.apache.asterix.translator.IStatementExecutor.ResultDelivery;
import org.apache.asterix.translator.IStatementExecutor.Stats;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.DeployedJobSpecId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.api.result.ResultSetId;
import org.apache.hyracks.dataflow.common.data.parsers.IValueParser;

public class CreateProcedureStatement extends ExtensionStatement {

    private static final Logger LOGGER = Logger.getLogger(CreateProcedureStatement.class.getName());

    private final FunctionSignature signature;
    private final String procedureBody;
    private Statement procedureBodyStatement;
    private final List<String> paramList;
    private final List<VariableExpr> varList;
    private final CallExpr period;
    private String duration = "";
    private List<List<List<String>>> dependencies;

    public CreateProcedureStatement(FunctionSignature signature, List<VarIdentifier> parameterList,
            List<Integer> paramIds, String functionBody, Expression period) {
        this.signature = signature;
        this.paramList = new ArrayList<>();
        this.varList = new ArrayList<>();
        for (int i = 0; i < parameterList.size(); i++) {
            this.paramList.add(parameterList.get(i).getValue().substring(1));
            this.varList.add(
                    new VariableExpr(new VarIdentifier(parameterList.get(i).getValue().substring(1), paramIds.get(i))));
        }
        procedureBody = rewriteJobParams(functionBody);
        this.period = (CallExpr) period;
        this.dependencies = new ArrayList<>();
        this.dependencies.add(new ArrayList<>());
        this.dependencies.add(new ArrayList<>());
    }

    private String rewriteJobParams(String body) {
        String newBody = body;
        for (VariableExpr var : varList) {
            Pattern variableReference = Pattern.compile("([^\\w\\d])" + var.getVar() + "([^\\w\\d]|$)");
            Matcher matcher = variableReference.matcher(newBody);
            newBody = matcher.replaceAll("$1get_job_param(\"" + var.getVar() + "\")$2");
        }
        return "use " + signature.getNamespace() + ";\n" + newBody + ";";
    }

    public String getProcedureBody() {
        return procedureBody;
    }

    public Statement getProcedureBodyStatement() {
        return procedureBodyStatement;
    }

    public List<String> getParamList() {
        return paramList;
    }

    public List<VariableExpr> getVarList() {
        return varList;
    }

    public FunctionSignature getSignature() {
        return signature;
    }

    @Override
    public byte getCategory() {
        return Category.DDL;
    }

    public Expression getPeriod() {
        return period;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return null;
    }

    private void initialize() throws CompilationException, HyracksDataException {
        BADParserFactory factory = new BADParserFactory();
        List<Statement> fStatements = factory.createParser(new StringReader(procedureBody)).parse();
        if (fStatements.size() != 2) {
            //TODO: Add a test for this error
            throw new CompilationException("Procedure can only execute a single statement");
        }
        procedureBodyStatement = fStatements.get(1);
        if (period == null) {
            return;
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

    private Pair<JobSpecification, PrecompiledType> createProcedureJob(IStatementExecutor statementExecutor,
            MetadataProvider metadataProvider, IHyracksClientConnection hcc, Stats stats) throws Exception {
        if (getProcedureBodyStatement().getKind() == Statement.Kind.INSERT) {
            if (!varList.isEmpty()) {
                throw new CompilationException("Insert procedures cannot have parameters");
            }
            InsertStatement insertStatement = (InsertStatement) getProcedureBodyStatement();
            dependencies.get(0).add(Arrays.asList(
                    ((QueryTranslator) statementExecutor).getActiveDataverse(insertStatement.getDataverseName()),
                    insertStatement.getDatasetName().getValue()));
            return new Pair<>(((QueryTranslator) statementExecutor).handleInsertUpsertStatement(metadataProvider,
                    getProcedureBodyStatement(), hcc, null, ResultDelivery.ASYNC, null, stats, true, null, null, null),
                    PrecompiledType.INSERT);
        } else if (getProcedureBodyStatement().getKind() == Statement.Kind.QUERY) {
            SqlppRewriterFactory fact = new SqlppRewriterFactory();
            dependencies.get(1).addAll(FunctionUtil.getFunctionDependencies(fact.createQueryRewriter(),
                    ((Query) getProcedureBodyStatement()).getBody(), metadataProvider).get(1));
            Pair<JobSpecification, PrecompiledType> pair = new Pair<>(BADJobService.compileQueryJob(statementExecutor,
                    metadataProvider, hcc, (Query) getProcedureBodyStatement()), PrecompiledType.QUERY);
            dependencies.get(0).addAll(FunctionUtil.getFunctionDependencies(fact.createQueryRewriter(),
                    ((Query) getProcedureBodyStatement()).getBody(), metadataProvider).get(0));
            return pair;
        } else if (getProcedureBodyStatement().getKind() == Statement.Kind.DELETE) {
            SqlppDeleteRewriteVisitor visitor = new SqlppDeleteRewriteVisitor();
            getProcedureBodyStatement().accept(visitor, null);
            DeleteStatement delete = (DeleteStatement) getProcedureBodyStatement();

            SqlppRewriterFactory fact = new SqlppRewriterFactory();
            dependencies = FunctionUtil.getFunctionDependencies(fact.createQueryRewriter(), delete.getQuery().getBody(),
                    metadataProvider);
            Pair<JobSpecification, PrecompiledType> pair =
                    new Pair<>(((QueryTranslator) statementExecutor).handleDeleteStatement(metadataProvider,
                            getProcedureBodyStatement(), hcc, true, null, null), PrecompiledType.DELETE);
            return pair;
        } else {
            throw new CompilationException("Procedure can only execute a single delete, insert, or query");
        }
    }

    private void setupDeployedJobSpec(EntityId entityId, JobSpecification jobSpec, IHyracksClientConnection hcc,
            DeployedJobSpecEventListener listener, ResultSetId resultSetId, Stats stats) throws Exception {
        jobSpec.setProperty(ActiveNotificationHandler.ACTIVE_ENTITY_PROPERTY_NAME, entityId);
        DeployedJobSpecId deployedJobSpecId = hcc.deployJobSpec(jobSpec);
        listener.setDeployedJobSpecId(deployedJobSpecId);
    }

    @Override
    public void handle(IHyracksClientConnection hcc, IStatementExecutor statementExecutor,
            IRequestParameters requestParameters, MetadataProvider metadataProvider, int resultSetId)
            throws HyracksDataException, AlgebricksException {
        ICcApplicationContext appCtx = metadataProvider.getApplicationContext();
        ActiveNotificationHandler activeEventHandler =
                (ActiveNotificationHandler) appCtx.getActiveNotificationHandler();
        initialize();
        String dataverse =
                ((QueryTranslator) statementExecutor).getActiveDataverse(new Identifier(signature.getNamespace()));
        EntityId entityId = new EntityId(BADConstants.PROCEDURE_KEYWORD, dataverse, signature.getName());
        DeployedJobSpecEventListener listener = (DeployedJobSpecEventListener) activeEventHandler.getListener(entityId);
        boolean alreadyActive = false;
        Procedure procedure = null;

        MetadataTransactionContext mdTxnCtx = null;
        try {
            mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
            metadataProvider.setMetadataTxnContext(mdTxnCtx);
            procedure = BADLangExtension.getProcedure(mdTxnCtx, dataverse, signature.getName(),
                    Integer.toString(signature.getArity()));
            if (procedure != null) {
                throw new AlgebricksException("A procedure with this name " + signature.getName() + " already exists.");
            }
            if (listener != null) {
                alreadyActive = listener.isActive();
            }
            if (alreadyActive) {
                throw new AsterixException("Procedure " + signature.getName() + " is already running");
            }
            metadataProvider.setResultSetId(new ResultSetId(0));
            final Stats stats = requestParameters.getStats();
            metadataProvider.setResultAsyncMode(false);
            metadataProvider.setMaxResultReads(1);
            //Create Procedure Internal Job
            Pair<JobSpecification, PrecompiledType> procedureJobSpec =
                    createProcedureJob(statementExecutor, metadataProvider, hcc, stats);

            // Now we subscribe
            if (listener == null) {
                listener = new DeployedJobSpecEventListener(appCtx, entityId, procedureJobSpec.second);
                activeEventHandler.registerListener(listener);
            }
            setupDeployedJobSpec(entityId, procedureJobSpec.first, hcc, listener, metadataProvider.getResultSetId(),
                    stats);

            procedure = new Procedure(dataverse, signature.getName(), signature.getArity(), getParamList(),
                    procedureJobSpec.second.toString(), getProcedureBody(), Function.LANGUAGE_SQLPP, duration,
                    dependencies);

            MetadataManager.INSTANCE.addEntity(mdTxnCtx, procedure);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            if (mdTxnCtx != null) {
                QueryTranslator.abort(e, e, mdTxnCtx);
            }
            LOGGER.log(Level.WARNING, "Failed creating a procedure", e);
            throw HyracksDataException.create(e);
        } finally {
            metadataProvider.getLocks().unlock();
        }

    }

}
