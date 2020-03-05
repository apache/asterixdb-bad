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
package org.apache.asterix.bad.metadata;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.active.EntityId;
import org.apache.asterix.bad.BADConstants;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.metadata.api.ExtensionMetadataDatasetId;
import org.apache.asterix.metadata.api.IExtensionMetadataEntity;
import org.apache.hyracks.algebricks.common.utils.Triple;

public class Procedure implements IExtensionMetadataEntity {
    private static final long serialVersionUID = 1L;
    public static final String RETURNTYPE_VOID = "VOID";

    private final EntityId procedureId;
    private final int arity;
    private final List<String> params;
    private final String body;
    private final String type;
    private final String language;
    private final String duration;
    /*
    Dependencies are stored as an array of size two:
    element 0 is a list of dataset dependencies
    -stored as triples of [DataverseName, Dataset, null] for the datasets
    element 1 is a list of function dependencies
    -stored as triples of [DataverseName, FunctionName, Arity] for the functions
     */
    private final List<List<Triple<DataverseName, String, String>>> dependencies;

    public Procedure(DataverseName dataverseName, String functionName, int arity, List<String> params, String type,
            String functionBody, String language, String duration,
            List<List<Triple<DataverseName, String, String>>> dependencies) {
        this.procedureId = new EntityId(BADConstants.PROCEDURE_KEYWORD, dataverseName, functionName);
        this.params = params;
        this.body = functionBody;
        this.type = type;
        this.language = language;
        this.arity = arity;
        this.duration = duration;
        if (dependencies == null) {
            this.dependencies = new ArrayList<>();
            this.dependencies.add(new ArrayList<>());
            this.dependencies.add(new ArrayList<>());
        } else {
            this.dependencies = dependencies;
        }
    }

    public EntityId getEntityId() {
        return procedureId;
    }

    public List<String> getParams() {
        return params;
    }

    public String getBody() {
        return body;
    }

    public String getType() {
        return type;
    }

    public String getLanguage() {
        return language;
    }

    public int getArity() {
        return arity;
    }

    public String getDuration() {
        return duration;
    }

    public List<List<Triple<DataverseName, String, String>>> getDependencies() {
        return dependencies;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof Procedure)) {
            return false;
        }
        Procedure otherDataset = (Procedure) other;
        if (!otherDataset.procedureId.equals(procedureId)) {
            return false;
        }
        return true;
    }

    @Override
    public ExtensionMetadataDatasetId getDatasetId() {
        return BADMetadataIndexes.BAD_PROCEDURE_INDEX_ID;
    }
}
