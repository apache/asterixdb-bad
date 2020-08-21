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

import java.util.Arrays;

import org.apache.asterix.bad.BADConstants;
import org.apache.asterix.bad.extension.BADMetadataExtension;
import org.apache.asterix.common.metadata.MetadataIndexImmutableProperties;
import org.apache.asterix.metadata.api.ExtensionMetadataDataset;
import org.apache.asterix.metadata.api.ExtensionMetadataDatasetId;
import org.apache.asterix.metadata.api.IMetadataEntityTupleTranslatorFactory;
import org.apache.asterix.metadata.bootstrap.MetadataRecordTypes;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;

public class BADMetadataIndexes {

    public static final ExtensionMetadataDatasetId BAD_CHANNEL_INDEX_ID = new ExtensionMetadataDatasetId(
            BADMetadataExtension.BAD_METADATA_EXTENSION_ID, BADConstants.METADATA_DATASET_CHANNEL);
    public static final MetadataIndexImmutableProperties PROPERTIES_CHANNEL =
            new MetadataIndexImmutableProperties(BADConstants.METADATA_DATASET_CHANNEL,
                    MetadataIndexImmutableProperties.FIRST_AVAILABLE_EXTENSION_METADATA_DATASET_ID,
                    MetadataIndexImmutableProperties.FIRST_AVAILABLE_EXTENSION_METADATA_DATASET_ID);

    public static final ExtensionMetadataDatasetId BAD_BROKER_INDEX_ID = new ExtensionMetadataDatasetId(
            BADMetadataExtension.BAD_METADATA_EXTENSION_ID, BADConstants.METADATA_DATASET_BROKER);
    public static final MetadataIndexImmutableProperties PROPERTIES_BROKER =
            new MetadataIndexImmutableProperties(BADConstants.METADATA_DATASET_BROKER,
                    MetadataIndexImmutableProperties.FIRST_AVAILABLE_EXTENSION_METADATA_DATASET_ID + 1,
                    MetadataIndexImmutableProperties.FIRST_AVAILABLE_EXTENSION_METADATA_DATASET_ID + 1);

    public static final ExtensionMetadataDatasetId BAD_PROCEDURE_INDEX_ID = new ExtensionMetadataDatasetId(
            BADMetadataExtension.BAD_METADATA_EXTENSION_ID, BADConstants.METADATA_DATASET_PROCEDURE);
    public static final MetadataIndexImmutableProperties PROPERTIES_PROCEDURE =
            new MetadataIndexImmutableProperties(BADConstants.METADATA_DATASET_PROCEDURE,
                    MetadataIndexImmutableProperties.FIRST_AVAILABLE_EXTENSION_METADATA_DATASET_ID + 2,
                    MetadataIndexImmutableProperties.FIRST_AVAILABLE_EXTENSION_METADATA_DATASET_ID + 2);

    public static final int NUM_FIELDS_CHANNEL_IDX = 3;
    public static final int NUM_FIELDS_BROKER_IDX = 3;
    public static final int NUM_FIELDS_PROCEDURE_IDX = 4;

    public static final ExtensionMetadataDataset CHANNEL_DATASET = new ExtensionMetadataDataset<>(PROPERTIES_CHANNEL,
            NUM_FIELDS_CHANNEL_IDX, new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING },
            Arrays.asList(Arrays.asList(MetadataRecordTypes.FIELD_NAME_DATAVERSE_NAME),
                    Arrays.asList(BADConstants.ChannelName)),
            0, BADMetadataRecordTypes.CHANNEL_RECORDTYPE, true, new int[] { 0, 1 }, BAD_CHANNEL_INDEX_ID,
            (IMetadataEntityTupleTranslatorFactory<Channel>) ChannelTupleTranslator::new);

    public static final ExtensionMetadataDataset BROKER_DATASET = new ExtensionMetadataDataset<>(PROPERTIES_BROKER,
            NUM_FIELDS_BROKER_IDX, new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING },
            Arrays.asList(Arrays.asList(MetadataRecordTypes.FIELD_NAME_DATAVERSE_NAME),
                    Arrays.asList(BADConstants.METADATA_TYPE_FIELD_NAME_BROKERNAME)),
            0, BADMetadataRecordTypes.BROKER_RECORDTYPE, true, new int[] { 0, 1 }, BAD_BROKER_INDEX_ID,
            (IMetadataEntityTupleTranslatorFactory<Broker>) BrokerTupleTranslator::new);

    public static final ExtensionMetadataDataset PROCEDURE_DATASET =
            new ExtensionMetadataDataset<>(PROPERTIES_PROCEDURE, NUM_FIELDS_PROCEDURE_IDX,
                    new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING },
                    Arrays.asList(Arrays.asList(MetadataRecordTypes.FIELD_NAME_DATAVERSE_NAME),
                            Arrays.asList(BADConstants.ProcedureName), Arrays.asList(BADConstants.FIELD_NAME_ARITY)),
                    0, BADMetadataRecordTypes.PROCEDURE_RECORDTYPE, true, new int[] { 0, 1, 2 }, BAD_PROCEDURE_INDEX_ID,
                    (IMetadataEntityTupleTranslatorFactory<Procedure>) ProcedureTupleTranslator::new);

}
