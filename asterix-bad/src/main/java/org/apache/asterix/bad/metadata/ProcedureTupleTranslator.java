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

import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.MetadataException;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.metadata.entities.Function;
import org.apache.asterix.metadata.entitytupletranslators.AbstractTupleTranslator;
import org.apache.asterix.om.base.AOrderedList;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.IACursor;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

/**
 * Translates a Procedure metadata entity to an ITupleReference and vice versa.
 */
public class ProcedureTupleTranslator extends AbstractTupleTranslator<Procedure> {
    // Field indexes of serialized Procedure in a tuple.
    // First key field.
    public static final int PROCEDURE_DATAVERSENAME_TUPLE_FIELD_INDEX = 0;
    // Second key field.
    public static final int PROCEDURE_PROCEDURE_NAME_TUPLE_FIELD_INDEX = 1;
    // Third key field.
    public static final int PROCEDURE_ARITY_TUPLE_FIELD_INDEX = 2;

    // Payload field containing serialized Procedure.
    public static final int PROCEDURE_PAYLOAD_TUPLE_FIELD_INDEX = 3;

    private transient OrderedListBuilder dependenciesListBuilder = new OrderedListBuilder();
    private transient OrderedListBuilder dependencyListBuilder = new OrderedListBuilder();
    private transient OrderedListBuilder dependencyNameListBuilder = new OrderedListBuilder();
    private transient List<String> dependencySubnames = new ArrayList<>();
    private transient AOrderedListType stringList = new AOrderedListType(BuiltinType.ASTRING, null);
    private transient AOrderedListType listOfLists =
            new AOrderedListType(new AOrderedListType(BuiltinType.ASTRING, null), null);

    protected ProcedureTupleTranslator(boolean getTuple) {
        super(getTuple, BADMetadataIndexes.PROCEDURE_DATASET, PROCEDURE_PAYLOAD_TUPLE_FIELD_INDEX);
    }

    @Override
    public Procedure createMetadataEntityFromARecord(ARecord procedureRecord) throws AlgebricksException {
        String dataverseCanonicalName = ((AString) procedureRecord
                .getValueByPos(BADMetadataRecordTypes.PROCEDURE_ARECORD_DATAVERSENAME_FIELD_INDEX)).getStringValue();
        DataverseName dataverseName = DataverseName.createFromCanonicalForm(dataverseCanonicalName);
        String procedureName = ((AString) procedureRecord
                .getValueByPos(BADMetadataRecordTypes.PROCEDURE_ARECORD_PROCEDURE_NAME_FIELD_INDEX)).getStringValue();
        String arity = ((AString) procedureRecord
                .getValueByPos(BADMetadataRecordTypes.PROCEDURE_ARECORD_PROCEDURE_ARITY_FIELD_INDEX)).getStringValue();

        IACursor cursor = ((AOrderedList) procedureRecord
                .getValueByPos(BADMetadataRecordTypes.PROCEDURE_ARECORD_PROCEDURE_PARAM_LIST_FIELD_INDEX)).getCursor();
        List<String> params = new ArrayList<>();
        while (cursor.next()) {
            params.add(((AString) cursor.get()).getStringValue());
        }

        String returnType = ((AString) procedureRecord
                .getValueByPos(BADMetadataRecordTypes.PROCEDURE_ARECORD_PROCEDURE_TYPE_FIELD_INDEX)).getStringValue();

        String definition = ((AString) procedureRecord
                .getValueByPos(BADMetadataRecordTypes.PROCEDURE_ARECORD_PROCEDURE_DEFINITION_FIELD_INDEX))
                        .getStringValue();

        String languageValue = ((AString) procedureRecord
                .getValueByPos(BADMetadataRecordTypes.PROCEDURE_ARECORD_PROCEDURE_LANGUAGE_FIELD_INDEX))
                        .getStringValue();
        Function.FunctionLanguage language = Function.FunctionLanguage.findByName(languageValue);
        if (language == null) {
            throw new AsterixException(ErrorCode.METADATA_ERROR, languageValue);
        }

        String duration = ((AString) procedureRecord
                .getValueByPos(BADMetadataRecordTypes.PROCEDURE_ARECORD_PROCEDURE_DURATION_FIELD_INDEX))
                        .getStringValue();

        IACursor dependenciesCursor = ((AOrderedList) procedureRecord
                .getValueByPos(BADMetadataRecordTypes.PROCEDURE_ARECORD_DEPENDENCIES_FIELD_INDEX)).getCursor();
        List<List<Triple<DataverseName, String, String>>> dependencies = new ArrayList<>();
        while (dependenciesCursor.next()) {
            List<Triple<DataverseName, String, String>> dependencyList = new ArrayList<>();
            IACursor qualifiedDependencyCursor = ((AOrderedList) dependenciesCursor.get()).getCursor();
            while (qualifiedDependencyCursor.next()) {
                Triple<DataverseName, String, String> dependency =
                        getDependency((AOrderedList) qualifiedDependencyCursor.get());
                dependencyList.add(dependency);
            }
            dependencies.add(dependencyList);
        }

        return new Procedure(dataverseName, procedureName, Integer.parseInt(arity), params, returnType, definition,
                language, duration, dependencies);
    }

    static Triple<DataverseName, String, String> getDependency(AOrderedList dependencySubnames) {
        String dataverseCanonicalName = ((AString) dependencySubnames.getItem(0)).getStringValue();
        DataverseName dataverseName = DataverseName.createFromCanonicalForm(dataverseCanonicalName);
        String second = null, third = null;
        int ln = dependencySubnames.size();
        if (ln > 1) {
            second = ((AString) dependencySubnames.getItem(1)).getStringValue();
            if (ln > 2) {
                third = ((AString) dependencySubnames.getItem(2)).getStringValue();
            }
        }
        return new Triple<>(dataverseName, second, third);
    }

    @Override
    public ITupleReference getTupleFromMetadataEntity(Procedure procedure)
            throws HyracksDataException, MetadataException {
        String dataverseCanonicalName = procedure.getEntityId().getDataverseName().getCanonicalForm();

        // write the key in the first 2 fields of the tuple
        tupleBuilder.reset();
        aString.setValue(dataverseCanonicalName);
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();
        aString.setValue(procedure.getEntityId().getEntityName());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();
        aString.setValue(procedure.getArity() + "");
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        // write the pay-load in the fourth field of the tuple

        recordBuilder.reset(BADMetadataRecordTypes.PROCEDURE_RECORDTYPE);

        // write field 0
        fieldValue.reset();
        aString.setValue(dataverseCanonicalName);
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(BADMetadataRecordTypes.PROCEDURE_ARECORD_DATAVERSENAME_FIELD_INDEX, fieldValue);

        // write field 1
        fieldValue.reset();
        aString.setValue(procedure.getEntityId().getEntityName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(BADMetadataRecordTypes.PROCEDURE_ARECORD_PROCEDURE_NAME_FIELD_INDEX, fieldValue);

        // write field 2
        fieldValue.reset();
        aString.setValue(procedure.getArity() + "");
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(BADMetadataRecordTypes.PROCEDURE_ARECORD_PROCEDURE_ARITY_FIELD_INDEX, fieldValue);

        // write field 3
        OrderedListBuilder listBuilder = new OrderedListBuilder();
        ArrayBackedValueStorage itemValue = new ArrayBackedValueStorage();
        listBuilder.reset((AOrderedListType) BADMetadataRecordTypes.PROCEDURE_RECORDTYPE
                .getFieldTypes()[BADMetadataRecordTypes.PROCEDURE_ARECORD_PROCEDURE_PARAM_LIST_FIELD_INDEX]);
        for (String param : procedure.getParams()) {
            itemValue.reset();
            aString.setValue(param);
            stringSerde.serialize(aString, itemValue.getDataOutput());
            listBuilder.addItem(itemValue);
        }
        fieldValue.reset();
        listBuilder.write(fieldValue.getDataOutput(), true);
        recordBuilder.addField(BADMetadataRecordTypes.PROCEDURE_ARECORD_PROCEDURE_PARAM_LIST_FIELD_INDEX, fieldValue);

        // write field 4
        fieldValue.reset();
        aString.setValue(procedure.getType());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(BADMetadataRecordTypes.PROCEDURE_ARECORD_PROCEDURE_TYPE_FIELD_INDEX, fieldValue);

        // write field 5
        fieldValue.reset();
        aString.setValue(procedure.getBody());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(BADMetadataRecordTypes.PROCEDURE_ARECORD_PROCEDURE_DEFINITION_FIELD_INDEX, fieldValue);

        // write field 6
        fieldValue.reset();
        aString.setValue(procedure.getLanguage().getName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(BADMetadataRecordTypes.PROCEDURE_ARECORD_PROCEDURE_LANGUAGE_FIELD_INDEX, fieldValue);

        // write field 7
        fieldValue.reset();
        aString.setValue(procedure.getDuration());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(BADMetadataRecordTypes.PROCEDURE_ARECORD_PROCEDURE_DURATION_FIELD_INDEX, fieldValue);

        // write field 8
        dependenciesListBuilder.reset((AOrderedListType) BADMetadataRecordTypes.PROCEDURE_RECORDTYPE
                .getFieldTypes()[BADMetadataRecordTypes.PROCEDURE_ARECORD_DEPENDENCIES_FIELD_INDEX]);
        List<List<Triple<DataverseName, String, String>>> dependenciesList = procedure.getDependencies();
        for (List<Triple<DataverseName, String, String>> dependencies : dependenciesList) {
            dependencyListBuilder.reset(listOfLists);
            for (Triple<DataverseName, String, String> dependency : dependencies) {
                dependencyNameListBuilder.reset(stringList);
                getDependencySubNames(dependency, dependencySubnames);
                for (String subName : dependencySubnames) {
                    itemValue.reset();
                    aString.setValue(subName);
                    stringSerde.serialize(aString, itemValue.getDataOutput());
                    dependencyNameListBuilder.addItem(itemValue);
                }
                itemValue.reset();
                dependencyNameListBuilder.write(itemValue.getDataOutput(), true);
                dependencyListBuilder.addItem(itemValue);

            }
            itemValue.reset();
            dependencyListBuilder.write(itemValue.getDataOutput(), true);
            dependenciesListBuilder.addItem(itemValue);
        }
        fieldValue.reset();
        dependenciesListBuilder.write(fieldValue.getDataOutput(), true);
        recordBuilder.addField(BADMetadataRecordTypes.PROCEDURE_ARECORD_DEPENDENCIES_FIELD_INDEX, fieldValue);

        // write record
        recordBuilder.write(tupleBuilder.getDataOutput(), true);
        tupleBuilder.addFieldEndOffset();

        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tuple;
    }

    static void getDependencySubNames(Triple<DataverseName, String, String> dependency, List<String> outList) {
        outList.clear();
        outList.add(dependency.first.getCanonicalForm());
        if (dependency.second != null) {
            outList.add(dependency.second);
        }
        if (dependency.third != null) {
            outList.add(dependency.third);
        }
    }
}
