/*
 * Copyright 2009-2015 by The Regents of the University of California
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

import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.metadata.api.ExtensionMetadataDatasetId;
import org.apache.asterix.metadata.api.IExtensionMetadataEntity;

/**
 * Metadata describing a broker.
 */
public class Broker implements IExtensionMetadataEntity {

    private static final long serialVersionUID = 2L;

    private final DataverseName dataverseName;
    private final String brokerName;
    private final String endPointName;
    private final String brokerType;

    public Broker(DataverseName dataverseName, String brokerName, String endPointName, String brokerType) {
        this.endPointName = endPointName;
        this.dataverseName = dataverseName;
        this.brokerName = brokerName;
        this.brokerType = brokerType;
    }

    public DataverseName getDataverseName() {
        return dataverseName;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public String getEndPointName() {
        return endPointName;
    }

    public String getBrokerType() {
        return brokerType;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof Broker)) {
            return false;
        }
        Broker otherDataset = (Broker) other;
        if (!otherDataset.brokerName.equals(brokerName)) {
            return false;
        }
        return true;
    }

    @Override
    public ExtensionMetadataDatasetId getDatasetId() {
        return BADMetadataIndexes.BAD_BROKER_INDEX_ID;
    }

}