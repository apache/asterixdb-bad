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
package org.apache.asterix.bad;

public interface BADConstants {
    String SubscriptionId = "subscriptionId";
    String ChannelName = "ChannelName";
    String ProcedureName = "ProcedureName";
    String DeliveryTime = "deliveryTime";
    String ResultId = "resultId";
    String ChannelResultsType = "ChannelResultsType";
    String ResultsDatasetName = "ResultsDatasetName";
    String SubscriptionsDatasetName = "SubscriptionsDatasetName";
    String subscriptionEnding = "Subscriptions";
    String resultsEnding = "Results";
    String BAD_METADATA_EXTENSION_NAME = "BADMetadataExtension";
    String Duration = "Duration";
    String Function = "Function";
    String FIELD_NAME_ARITY = "Arity";
    String FIELD_NAME_DEPENDENCIES = "Dependencies";
    String FIELD_NAME_PARAMS = "Params";
    String FIELD_NAME_TYPE = "Type";
    String FIELD_NAME_DEFINITION = "Definition";
    String FIELD_NAME_LANGUAGE = "Language";
    String FIELD_NAME_BODY = "Body";

    /* --- Notification Fields --- */
    String ChannelExecutionTime = "channelExecutionTime";
    String CHANNEL_EXECUTION_EPOCH_TIME = "channelExecutionEpochTime";

    // --- Active Dataset
    String RECORD_TYPENAME_ACTIVE_RECORD = "ActiveRecordType";
    String FIELD_NAME_ACTIVE_TS = "_active_timestamp";

    //To enable new Asterix TxnId for separate deployed job spec invocations
    byte[] TRANSACTION_ID_PARAMETER_NAME = "TxnIdParameter".getBytes();
    int EXECUTOR_TIMEOUT = 20;

    /* --- Metadata Common --- */
    String METADATA_TYPE_NAME_DATAVERSENAME = "DataverseName";

    String METADATA_DATASET_CHANNEL = "Channel";
    String METADATA_DATASET_PROCEDURE = "Procedure";
    String METADATA_DATASET_BROKER = "Broker";

    /* --- Metadata Datatypes --- */
    String METADATA_TYPENAME_SUBSCRIPTIONS = "ChannelSubscriptionsType";
    String METADATA_TYPENAME_BROKER = "BrokerRecordType";
    String METADATA_TYPENAME_CHANNEL = "ChannelRecordType";
    String METADATA_TYPENAME_PROCEDURE = "ProcedureRecordType";

    /* --- Broker Field Names --- */
    String METADATA_TYPE_FIELD_NAME_BROKERNAME = "BrokerName";
    String METADATA_TYPE_FIELD_NAME_BROKER_END_POINT = "BrokerEndPoint";
    String METADATA_TYPE_FIELD_NAME_BROKER_TYPE = "BrokerType";

    /*  ---  Runtime Entities ---  */
    String RUNTIME_ENTITY_PROCEDURE = "Procedure";
    String RUNTIME_ENTITY_CHANNEL = "Channel";

    /* --- Query Compilation --- */
    String CONFIG_CHANNEL_NAME = "_internal_channelName";

    /* --- BAD ISLANDS --- */
    String GENERAL_BROKER_TYPE_NAME = "general";
    String BAD_BROKER_TYPE_NAME = "bad";
    String BAD_BROKER_FIELD_NAME_TYPE = "broker-type";
    String BAD_FEED_FIELD_NAME_HOST = "bad-host";
    String BAD_FEED_FIELD_NAME_CHANNEL = "bad-channel";
    String BAD_FEED_FIELD_NAME_PARAMETERS = "bad-channel-parameters";
    String BAD_FEED_FIELD_NAME_CHANNEL_DV = "bad-dataverse";
    // String BAD_FEED_TYPE = "type"

    public enum ChannelJobType {
        REPETITIVE
    }
}
