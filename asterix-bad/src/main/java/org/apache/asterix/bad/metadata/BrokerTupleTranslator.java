/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.asterix.bad.metadata;

import org.apache.asterix.common.exceptions.MetadataException;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.metadata.entitytupletranslators.AbstractTupleTranslator;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.AString;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

/**
 * Translates a Channel metadata entity to an ITupleReference and vice versa.
 */
public class BrokerTupleTranslator extends AbstractTupleTranslator<Broker> {
    // Field indexes of serialized Broker in a tuple.
    // Key field.
    public static final int BROKER_DATAVERSE_NAME_FIELD_INDEX = 0;

    public static final int BROKER_NAME_FIELD_INDEX = 1;

    // Payload field containing serialized broker.
    public static final int BROKER_PAYLOAD_TUPLE_FIELD_INDEX = 2;

    public BrokerTupleTranslator(boolean getTuple) {
        super(getTuple, BADMetadataIndexes.BROKER_DATASET, BROKER_PAYLOAD_TUPLE_FIELD_INDEX);
    }

    @Override
    public Broker createMetadataEntityFromARecord(ARecord brokerRecord) throws HyracksDataException {
        String dataverseCanonicalName =
                ((AString) brokerRecord.getValueByPos(BADMetadataRecordTypes.BROKER_DATAVERSE_NAME_FIELD_INDEX))
                        .getStringValue();
        DataverseName dataverseName = DataverseName.createFromCanonicalForm(dataverseCanonicalName);
        String brokerName =
                ((AString) brokerRecord.getValueByPos(BADMetadataRecordTypes.BROKER_NAME_FIELD_INDEX)).getStringValue();
        String endPointName = ((AString) brokerRecord.getValueByPos(BADMetadataRecordTypes.BROKER_ENDPOINT_FIELD_INDEX))
                .getStringValue();
        String brokerType =
                ((AString) brokerRecord.getValueByPos(BADMetadataRecordTypes.BROKER_TYPE_FIELD_INDEX)).getStringValue();

        return new Broker(dataverseName, brokerName, endPointName, brokerType);
    }

    @Override
    public ITupleReference getTupleFromMetadataEntity(Broker broker) throws HyracksDataException, MetadataException {
        String dataverseCanonicalName = broker.getDataverseName().getCanonicalForm();

        // write the key in the first fields of the tuple

        tupleBuilder.reset();
        aString.setValue(dataverseCanonicalName);
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        aString.setValue(broker.getBrokerName());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        recordBuilder.reset(BADMetadataRecordTypes.BROKER_RECORDTYPE);

        // write field 0
        fieldValue.reset();
        aString.setValue(dataverseCanonicalName);
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(BADMetadataRecordTypes.BROKER_DATAVERSE_NAME_FIELD_INDEX, fieldValue);

        // write field 1
        fieldValue.reset();
        aString.setValue(broker.getBrokerName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(BADMetadataRecordTypes.BROKER_NAME_FIELD_INDEX, fieldValue);

        // write field 2
        fieldValue.reset();
        aString.setValue(broker.getEndPointName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(BADMetadataRecordTypes.BROKER_ENDPOINT_FIELD_INDEX, fieldValue);

        // write field 3
        fieldValue.reset();
        aString.setValue(broker.getBrokerType());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(BADMetadataRecordTypes.BROKER_TYPE_FIELD_INDEX, fieldValue);

        // write record
        recordBuilder.write(tupleBuilder.getDataOutput(), true);

        tupleBuilder.addFieldEndOffset();

        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tuple;
    }
}