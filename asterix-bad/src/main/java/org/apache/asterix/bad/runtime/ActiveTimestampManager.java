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
package org.apache.asterix.bad.runtime;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hyracks.api.job.JobId;

/*
 * This only supports one channel currently. Can be extended to multiple channels.
 * */
public class ActiveTimestampManager {

    static class ChannelTimeState {
        long previousChannelExecutionTimestamp;
        long currentChannelExecutionTimestamp;
        JobId maxJobId;

        public ChannelTimeState(long prev, long curr, JobId jobId) {
            this.previousChannelExecutionTimestamp = prev;
            this.currentChannelExecutionTimestamp = curr;
            this.maxJobId = jobId;
        }
    }

    private static Map<String, ChannelTimeState> channelTimeStateMap = new ConcurrentHashMap<>();
    private static Logger LOGGER = Logger.getLogger(ActiveTimestampManager.class.getName());

    public static synchronized boolean progressChannelExecutionTimestamps(JobId jobId, String channelName,
            String nodeId) {
        if (channelName.equals("")) {
            return false;
        }
        // In distributed cases, channel name would be sufficient. Since in BADExecutionTest, all nodes share the same
        // JVM, we would need to add the node id as part of the key
        String channelTimeKey = nodeId + channelName;
        if (channelTimeStateMap.containsKey(channelTimeKey)) {
            ChannelTimeState state = channelTimeStateMap.get(channelTimeKey);
            if (state.maxJobId.compareTo(jobId) < 0) {
                state.previousChannelExecutionTimestamp = state.currentChannelExecutionTimestamp;
                state.currentChannelExecutionTimestamp = System.currentTimeMillis();
                state.maxJobId = jobId;
                LOGGER.log(Level.FINE, "CHN TS UPD " + channelName + " at " + jobId + " on " + nodeId + " "
                        + state.previousChannelExecutionTimestamp + "  -  " + state.currentChannelExecutionTimestamp);
                // System.err.println("CHN TS UPD " + channelName + " at " + jobId + " on " + nodeId + " "
                //         + state.previousChannelExecutionTimestamp + "  -  " + state.currentChannelExecutionTimestamp);
                return true;
            }
        } else {
            LOGGER.log(Level.FINE,
                    "CHN TS INIT " + channelName + " at " + jobId + " 0 - " + System.currentTimeMillis());
            // System.err.println("CHN TS INIT " + channelName + " at " + jobId + " on node " + nodeId + " 0 - "
            //         + System.currentTimeMillis());
            channelTimeStateMap.put(channelTimeKey, new ChannelTimeState(0, System.currentTimeMillis(), jobId));
        }
        return false;
    }

    public static long getPreviousChannelExecutionTimestamp(String channelName, String nodeId) {
        String channelTimeKey = nodeId + channelName;
        if (channelTimeStateMap.containsKey(channelTimeKey)) {
            return channelTimeStateMap.get(channelTimeKey).previousChannelExecutionTimestamp;
        } else {
            return 0;
        }
    }

    public static long getCurrentChannelExecutionTimestamp(String channelName, String nodeId) {
        String channelTimeKey = nodeId + channelName;
        if (channelTimeStateMap.containsKey(channelTimeKey)) {
            return channelTimeStateMap.get(channelTimeKey).currentChannelExecutionTimestamp;
        } else {
            return System.currentTimeMillis();
        }
    }
}
