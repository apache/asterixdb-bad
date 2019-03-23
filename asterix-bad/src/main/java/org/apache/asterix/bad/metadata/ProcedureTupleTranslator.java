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

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.common.exceptions.MetadataException;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.metadata.entitytupletranslators.AbstractTupleTranslator;
import org.apache.asterix.om.base.AOrderedList;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.IACursor;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
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

    private ISerializerDeserializer<ARecord> recordSerDes = SerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BADMetadataRecordTypes.PROCEDURE_RECORDTYPE);

    private transient OrderedListBuilder dependenciesListBuilder = new OrderedListBuilder();
    private transient OrderedListBuilder dependencyListBuilder = new OrderedListBuilder();
    private transient OrderedListBuilder dependencyNameListBuilder = new OrderedListBuilder();
    private transient AOrderedListType stringList = new AOrderedListType(BuiltinType.ASTRING, null);
    private transient AOrderedListType ListofLists =
            new AOrderedListType(new AOrderedListType(BuiltinType.ASTRING, null), null);

    protected ProcedureTupleTranslator(boolean getTuple) {
        super(getTuple, BADMetadataIndexes.NUM_FIELDS_PROCEDURE_IDX);
    }

    @Override
    public Procedure getMetadataEntityFromTuple(ITupleReference frameTuple) throws HyracksDataException {
        byte[] serRecord = frameTuple.getFieldData(PROCEDURE_PAYLOAD_TUPLE_FIELD_INDEX);
        int recordStartOffset = frameTuple.getFieldStart(PROCEDURE_PAYLOAD_TUPLE_FIELD_INDEX);
        int recordLength = frameTuple.getFieldLength(PROCEDURE_PAYLOAD_TUPLE_FIELD_INDEX);
        ByteArrayInputStream stream = new ByteArrayInputStream(serRecord, recordStartOffset, recordLength);
        DataInput in = new DataInputStream(stream);
        ARecord procedureRecord = recordSerDes.deserialize(in);
        return createProcedureFromARecord(procedureRecord);
    }

    private Procedure createProcedureFromARecord(ARecord procedureRecord) {
        String dataverseName = ((AString) procedureRecord
                .getValueByPos(BADMetadataRecordTypes.PROCEDURE_ARECORD_DATAVERSENAME_FIELD_INDEX)).getStringValue();
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

        String language = ((AString) procedureRecord
                .getValueByPos(BADMetadataRecordTypes.PROCEDURE_ARECORD_PROCEDURE_LANGUAGE_FIELD_INDEX))
                        .getStringValue();

        String duration = ((AString) procedureRecord
                .getValueByPos(BADMetadataRecordTypes.PROCEDURE_ARECORD_PROCEDURE_DURATION_FIELD_INDEX))
                        .getStringValue();

        IACursor dependenciesCursor = ((AOrderedList) procedureRecord
                .getValueByPos(BADMetadataRecordTypes.PROCEDURE_ARECORD_DEPENDENCIES_FIELD_INDEX)).getCursor();
        List<List<List<String>>> dependencies = new ArrayList<>();
        AOrderedList dependencyList;
        AOrderedList qualifiedList;
        int i = 0;
        while (dependenciesCursor.next()) {
            dependencies.add(new ArrayList<>());
            dependencyList = (AOrderedList) dependenciesCursor.get();
            IACursor qualifiedDependencyCursor = dependencyList.getCursor();
            int j = 0;
            while (qualifiedDependencyCursor.next()) {
                qualifiedList = (AOrderedList) qualifiedDependencyCursor.get();
                IACursor qualifiedNameCursor = qualifiedList.getCursor();
                dependencies.get(i).add(new ArrayList<>());
                while (qualifiedNameCursor.next()) {
                    dependencies.get(i).get(j).add(((AString) qualifiedNameCursor.get()).getStringValue());
                }
                j++;
            }
            i++;

        }

        return new Procedure(dataverseName, procedureName, Integer.parseInt(arity), params, returnType, definition,
                language, duration, dependencies);

    }

    @Override
    public ITupleReference getTupleFromMetadataEntity(Procedure procedure)
            throws HyracksDataException, MetadataException {
        // write the key in the first 2 fields of the tuple
        tupleBuilder.reset();
        aString.setValue(procedure.getEntityId().getDataverse());
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
        aString.setValue(procedure.getEntityId().getDataverse());
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
        aString.setValue(procedure.getLanguage());
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
        List<List<List<String>>> dependenciesList = procedure.getDependencies();
        for (List<List<String>> dependencies : dependenciesList) {
            dependencyListBuilder.reset(ListofLists);
            for (List<String> dependency : dependencies) {
                dependencyNameListBuilder.reset(stringList);
                for (String subName : dependency) {
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

}
