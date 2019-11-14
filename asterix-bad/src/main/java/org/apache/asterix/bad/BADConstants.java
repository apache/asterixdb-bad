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

import org.apache.asterix.common.metadata.DataverseName;

public interface BADConstants {
    String SubscriptionId = "subscriptionId";
    String BrokerName = "BrokerName";
    String ChannelName = "ChannelName";
    String ProcedureName = "ProcedureName";
    String DataverseName = "DataverseName";
    String BrokerEndPoint = "BrokerEndPoint";
    String DeliveryTime = "deliveryTime";
    String ResultId = "resultId";
    String ChannelExecutionTime = "channelExecutionTime";
    String ChannelSubscriptionsType = "ChannelSubscriptionsType";
    String ChannelResultsType = "ChannelResultsType";
    String ResultsDatasetName = "ResultsDatasetName";
    String SubscriptionsDatasetName = "SubscriptionsDatasetName";
    String CHANNEL_EXTENSION_NAME = "Channel";
    String PROCEDURE_KEYWORD = "Procedure";
    String BROKER_KEYWORD = "Broker";
    String RECORD_TYPENAME_BROKER = "BrokerRecordType";
    String RECORD_TYPENAME_CHANNEL = "ChannelRecordType";
    String RECORD_TYPENAME_PROCEDURE = "ProcedureRecordType";
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
    //To enable new Asterix TxnId for separate deployed job spec invocations
    byte[] TRANSACTION_ID_PARAMETER_NAME = "TxnIdParameter".getBytes();
    int EXECUTOR_TIMEOUT = 20;

    public enum ChannelJobType {
        REPETITIVE
    }
}
