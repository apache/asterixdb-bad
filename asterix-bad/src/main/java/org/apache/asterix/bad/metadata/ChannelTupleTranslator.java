/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.asterix.bad.metadata;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.common.exceptions.MetadataException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.metadata.entitytupletranslators.AbstractTupleTranslator;
import org.apache.asterix.om.base.AOrderedList;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.IACursor;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

/**
 * Translates a Channel metadata entity to an ITupleReference and vice versa.
 */
public class ChannelTupleTranslator extends AbstractTupleTranslator<Channel> {
    // Field indexes of serialized Feed in a tuple.
    // Key field.
    public static final int CHANNEL_DATAVERSE_NAME_FIELD_INDEX = 0;

    public static final int CHANNEL_NAME_FIELD_INDEX = 1;

    // Payload field containing serialized feed.
    public static final int CHANNEL_PAYLOAD_TUPLE_FIELD_INDEX = 2;

    private transient OrderedListBuilder dependenciesListBuilder = new OrderedListBuilder();
    private transient OrderedListBuilder dependencyListBuilder = new OrderedListBuilder();
    private transient OrderedListBuilder dependencyNameListBuilder = new OrderedListBuilder();
    private transient List<String> dependencySubnames = new ArrayList<>();
    private transient AOrderedListType stringList = new AOrderedListType(BuiltinType.ASTRING, null);
    private transient AOrderedListType ListofLists =
            new AOrderedListType(new AOrderedListType(BuiltinType.ASTRING, null), null);

    public ChannelTupleTranslator(boolean getTuple) {
        super(getTuple, BADMetadataIndexes.CHANNEL_DATASET, CHANNEL_PAYLOAD_TUPLE_FIELD_INDEX);
    }

    @Override
    protected Channel createMetadataEntityFromARecord(ARecord channelRecord) {
        String dataverseCanonicalName = ((AString) channelRecord
                .getValueByPos(BADMetadataRecordTypes.CHANNEL_ARECORD_DATAVERSE_NAME_FIELD_INDEX)).getStringValue();
        DataverseName dataverseName = DataverseName.createFromCanonicalForm(dataverseCanonicalName);
        String channelName =
                ((AString) channelRecord.getValueByPos(BADMetadataRecordTypes.CHANNEL_ARECORD_CHANNEL_NAME_FIELD_INDEX))
                        .getStringValue();
        String subscriptionsName = ((AString) channelRecord
                .getValueByPos(BADMetadataRecordTypes.CHANNEL_ARECORD_SUBSCRIPTIONS_NAME_FIELD_INDEX)).getStringValue();
        String resultsName =
                ((AString) channelRecord.getValueByPos(BADMetadataRecordTypes.CHANNEL_ARECORD_RESULTS_NAME_FIELD_INDEX))
                        .getStringValue();

        AOrderedList function = ((AOrderedList) channelRecord
                .getValueByPos(BADMetadataRecordTypes.CHANNEL_ARECORD_FUNCTION_FIELD_INDEX));
        Triple<DataverseName, String, String> functionSignature = ProcedureTupleTranslator.getDependency(function);

        String duration =
                ((AString) channelRecord.getValueByPos(BADMetadataRecordTypes.CHANNEL_ARECORD_DURATION_FIELD_INDEX))
                        .getStringValue();

        IACursor dependenciesCursor = ((AOrderedList) channelRecord
                .getValueByPos(BADMetadataRecordTypes.CHANNEL_ARECORD_DEPENDENCIES_FIELD_INDEX)).getCursor();
        List<List<Triple<DataverseName, String, String>>> dependencies = new ArrayList<>();
        while (dependenciesCursor.next()) {
            List<Triple<DataverseName, String, String>> dependencyList = new ArrayList<>();
            IACursor qualifiedDependencyCursor = ((AOrderedList) dependenciesCursor.get()).getCursor();
            while (qualifiedDependencyCursor.next()) {
                Triple<DataverseName, String, String> dependency =
                        ProcedureTupleTranslator.getDependency((AOrderedList) qualifiedDependencyCursor.get());
                dependencyList.add(dependency);
            }
            dependencies.add(dependencyList);
        }

        String channelBody =
                ((AString) channelRecord.getValueByPos(BADMetadataRecordTypes.CHANNEL_ARECORD_BODY_FIELD_INDEX))
                        .getStringValue();

        FunctionSignature signature = new FunctionSignature(functionSignature.first, functionSignature.second,
                Integer.parseInt(functionSignature.third));

        return new Channel(dataverseName, channelName, subscriptionsName, resultsName, signature, duration,
                dependencies, channelBody);
    }

    @Override
    public ITupleReference getTupleFromMetadataEntity(Channel channel) throws HyracksDataException, MetadataException {
        String dataverseCanonicalName = channel.getChannelId().getDataverseName().getCanonicalForm();

        // write the key in the first fields of the tuple
        tupleBuilder.reset();

        aString.setValue(dataverseCanonicalName);
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        aString.setValue(channel.getChannelId().getEntityName());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        recordBuilder.reset(BADMetadataRecordTypes.CHANNEL_RECORDTYPE);

        // write field 0
        fieldValue.reset();
        aString.setValue(dataverseCanonicalName);
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(BADMetadataRecordTypes.CHANNEL_ARECORD_DATAVERSE_NAME_FIELD_INDEX, fieldValue);

        // write field 1
        fieldValue.reset();
        aString.setValue(channel.getChannelId().getEntityName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(BADMetadataRecordTypes.CHANNEL_ARECORD_CHANNEL_NAME_FIELD_INDEX, fieldValue);

        // write field 2
        fieldValue.reset();
        aString.setValue(channel.getSubscriptionsDataset());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(BADMetadataRecordTypes.CHANNEL_ARECORD_SUBSCRIPTIONS_NAME_FIELD_INDEX, fieldValue);

        // write field 3
        fieldValue.reset();
        aString.setValue(channel.getResultsDatasetName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(BADMetadataRecordTypes.CHANNEL_ARECORD_RESULTS_NAME_FIELD_INDEX, fieldValue);

        // write field 4
        OrderedListBuilder listBuilder = new OrderedListBuilder();
        ArrayBackedValueStorage itemValue = new ArrayBackedValueStorage();
        listBuilder.reset(stringList);
        ProcedureTupleTranslator.getDependencySubNames(channel.getFunctionAsPath(), dependencySubnames);
        for (String pathPart : dependencySubnames) {
            itemValue.reset();
            aString.setValue(pathPart);
            stringSerde.serialize(aString, itemValue.getDataOutput());
            listBuilder.addItem(itemValue);
        }
        fieldValue.reset();
        listBuilder.write(fieldValue.getDataOutput(), true);
        recordBuilder.addField(BADMetadataRecordTypes.CHANNEL_ARECORD_FUNCTION_FIELD_INDEX, fieldValue);

        // write field 5
        fieldValue.reset();
        aString.setValue(channel.getDuration());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(BADMetadataRecordTypes.CHANNEL_ARECORD_DURATION_FIELD_INDEX, fieldValue);

        // write field 6
        dependenciesListBuilder.reset((AOrderedListType) BADMetadataRecordTypes.CHANNEL_RECORDTYPE
                .getFieldTypes()[BADMetadataRecordTypes.CHANNEL_ARECORD_DEPENDENCIES_FIELD_INDEX]);
        List<List<Triple<DataverseName, String, String>>> dependenciesList = channel.getDependencies();
        for (List<Triple<DataverseName, String, String>> dependencies : dependenciesList) {
            dependencyListBuilder.reset(ListofLists);
            for (Triple<DataverseName, String, String> dependency : dependencies) {
                dependencyNameListBuilder.reset(stringList);
                ProcedureTupleTranslator.getDependencySubNames(dependency, dependencySubnames);
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
        recordBuilder.addField(BADMetadataRecordTypes.CHANNEL_ARECORD_DEPENDENCIES_FIELD_INDEX, fieldValue);

        // write field 7
        fieldValue.reset();
        aString.setValue(channel.getChannelBody());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(BADMetadataRecordTypes.CHANNEL_ARECORD_BODY_FIELD_INDEX, fieldValue);

        // write record
        recordBuilder.write(tupleBuilder.getDataOutput(), true);

        tupleBuilder.addFieldEndOffset();

        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tuple;
    }
}