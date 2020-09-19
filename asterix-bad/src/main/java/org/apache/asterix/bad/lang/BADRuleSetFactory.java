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
package org.apache.asterix.bad.lang;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.bad.rules.InsertBrokerNotifierForChannelRule;
import org.apache.asterix.bad.rules.RewriteActiveFunctionsRule;
import org.apache.asterix.bad.rules.RewriteChannelTimeFunctionToLocalVarRule;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.compiler.provider.DefaultRuleSetFactory;
import org.apache.asterix.compiler.provider.IRuleSetFactory;
import org.apache.asterix.optimizer.base.RuleCollections;
import org.apache.asterix.optimizer.rules.FeedScanCollectionToUnnest;
import org.apache.asterix.optimizer.rules.MetaFunctionToMetaVariableRule;
import org.apache.asterix.optimizer.rules.UnnestToDataScanRule;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.compiler.rewriter.rulecontrollers.SequentialFixpointRuleController;
import org.apache.hyracks.algebricks.core.rewriter.base.AbstractRuleController;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class BADRuleSetFactory implements IRuleSetFactory {

    private boolean isSameRuleCollection(List<IAlgebraicRewriteRule> listA, List<IAlgebraicRewriteRule> listB) {
        if (listA.size() != listB.size()) {
            return false;
        }
        for (int i = 0; i < listA.size(); i++) {
            if (!listA.get(i).getClass().equals(listB.get(i).getClass())) {
                return false;
            }
        }
        return true;
    }

    private void updateNormalizationRules(
            List<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>> logicalRuleSet,
            ICcApplicationContext appCtx) {
        // gen original normalization rules
        List<IAlgebraicRewriteRule> originalNormalizationRules =
                RuleCollections.buildNormalizationRuleCollection(appCtx);
        // make a copy
        List<IAlgebraicRewriteRule> alteredNormalizationRules = new ArrayList<>();
        alteredNormalizationRules.addAll(originalNormalizationRules);

        // insert the broker rule
        for (int i = 0; i < alteredNormalizationRules.size(); i++) {
            IAlgebraicRewriteRule rule = alteredNormalizationRules.get(i);
            if (rule instanceof UnnestToDataScanRule) {
                alteredNormalizationRules.add(i + 1, new InsertBrokerNotifierForChannelRule());
            }
        }

        // replace all normalization rule collections with the new one
        SequentialFixpointRuleController seqOnceCtrl = new SequentialFixpointRuleController(true);
        for (int i = 0; i < logicalRuleSet.size(); i++) {
            List<IAlgebraicRewriteRule> existingRuleCollection = logicalRuleSet.get(i).second;
            if (isSameRuleCollection(existingRuleCollection, originalNormalizationRules)) {
                logicalRuleSet.set(i, new Pair<>(seqOnceCtrl, alteredNormalizationRules));
            }
        }
    }

    private void addRewriteChannelTimeFunctionRule(
            List<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>> logicalRuleSet,
            ICcApplicationContext appCtx) {
        // gen original normalization rules
        List<IAlgebraicRewriteRule> originalRuleCollection = RuleCollections.buildLoadFieldsRuleCollection(appCtx);
        // make a copy
        List<IAlgebraicRewriteRule> alteredRuleCollection = new ArrayList<>();
        alteredRuleCollection.addAll(originalRuleCollection);
        // insert the broker rule
        for (int i = 0; i < alteredRuleCollection.size(); i++) {
            IAlgebraicRewriteRule rule = alteredRuleCollection.get(i);
            if (rule instanceof FeedScanCollectionToUnnest) {
                alteredRuleCollection.add(i + 1, new MetaFunctionToMetaVariableRule());
                alteredRuleCollection.add(i + 1, new RewriteChannelTimeFunctionToLocalVarRule());
                alteredRuleCollection.add(i + 1, new RewriteActiveFunctionsRule());
            }
        }

        // replace all normalization rule collections with the new one
        SequentialFixpointRuleController seqCtrlNoDfs = new SequentialFixpointRuleController(true);
        for (int i = 0; i < logicalRuleSet.size(); i++) {
            List<IAlgebraicRewriteRule> existingRuleCollection = logicalRuleSet.get(i).second;
            if (isSameRuleCollection(existingRuleCollection, originalRuleCollection)) {
                logicalRuleSet.set(i, new Pair<>(seqCtrlNoDfs, alteredRuleCollection));
            }
        }
    }

    @Override
    public List<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>> getLogicalRewrites(
            ICcApplicationContext appCtx) throws AlgebricksException {
        List<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>> logicalRuleSet =
                DefaultRuleSetFactory.buildLogical(appCtx);

        updateNormalizationRules(logicalRuleSet, appCtx);
        addRewriteChannelTimeFunctionRule(logicalRuleSet, appCtx);

        return logicalRuleSet;
    }

    @Override
    public List<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>> getPhysicalRewrites(
            ICcApplicationContext appCtx) {
        return DefaultRuleSetFactory.buildPhysical(appCtx);
    }
}
