/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.asterix.bad.runtime.operators;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.active.EntityId;
import org.apache.asterix.bad.BADConstants;
import org.apache.asterix.dataflow.data.nontagged.printers.json.clean.AOrderedlistPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.serde.ADateTimeSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AStringSerializerDeserializer;
import org.apache.asterix.om.base.ADateTime;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.data.IPrinter;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.evaluators.EvaluatorContext;
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputOneFramePushRuntime;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.util.ByteBufferInputStream;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.util.string.UTF8StringReader;
import org.apache.hyracks.util.string.UTF8StringWriter;

public class NotifyBrokerRuntime extends AbstractOneInputOneOutputOneFramePushRuntime {
    private static final Logger LOGGER = Logger.getLogger(NotifyBrokerRuntime.class.getName());

    private final ByteBufferInputStream bbis = new ByteBufferInputStream();
    private final DataInputStream di = new DataInputStream(bbis);
    private static final AStringSerializerDeserializer stringSerDes =
            new AStringSerializerDeserializer(new UTF8StringWriter(), new UTF8StringReader());

    private final IPrinter jsonRecordPrinter;
    private final IPrinter admRecordPrinter;
    private final IPrinter subscriptionIdListPrinterFactory;

    private IPointable inputArg0 = new VoidPointable();
    private IPointable inputArg1 = new VoidPointable();
    private IPointable inputArg2 = new VoidPointable();
    private IPointable inputArg3 = new VoidPointable();
    private IScalarEvaluator eval0;
    private IScalarEvaluator eval1;
    private IScalarEvaluator eval2;
    private IScalarEvaluator eval3;

    private final EntityId entityId;
    private final boolean push;
    private final Map<String, String> sendData = new HashMap<>();
    private final Map<String, ByteArrayOutputStream> sendbaos = new HashMap<>();
    private final Map<String, PrintStream> sendStreams = new HashMap<>();
    private final Map<String, String> brokerTypes = new HashMap<>();
    private Long executionTimeMili = (long) -1;

    public NotifyBrokerRuntime(IHyracksTaskContext ctx, IScalarEvaluatorFactory brokerEvalFactory,
            IScalarEvaluatorFactory pushListEvalFactory, IScalarEvaluatorFactory channelExecutionEvalFactory,
            IScalarEvaluatorFactory brokerTypeEvalFactory, EntityId activeJobId, boolean push, IAType recordType)
            throws HyracksDataException {
        this.tRef = new FrameTupleReference();
        IEvaluatorContext evalCtx = new EvaluatorContext(ctx);
        eval0 = brokerEvalFactory.createScalarEvaluator(evalCtx);
        eval1 = pushListEvalFactory.createScalarEvaluator(evalCtx);
        eval2 = channelExecutionEvalFactory.createScalarEvaluator(evalCtx);
        eval3 = brokerTypeEvalFactory.createScalarEvaluator(evalCtx);

        this.entityId = activeJobId;
        this.push = push;
        if (push) {
            //for push-based channel, the recordType is the result record type (records are sent directly)
            jsonRecordPrinter =
                    new org.apache.asterix.dataflow.data.nontagged.printers.json.clean.ARecordPrinterFactory(
                            (ARecordType) recordType).createPrinter();
            admRecordPrinter = new org.apache.asterix.dataflow.data.nontagged.printers.adm.ARecordPrinterFactory(
                    (ARecordType) recordType).createPrinter();
        } else {
            //for pull-based channels, the recordType is a list of subscription ids
            //the subscriptionIdListPrinterFactory is used instead
            jsonRecordPrinter = null;
            admRecordPrinter = null;
        }
        subscriptionIdListPrinterFactory =
                new AOrderedlistPrinterFactory(new AOrderedListType(BuiltinType.AUUID, null)).createPrinter();
    }

    @Override
    public void open() throws HyracksDataException {
        return;
    }

    public String createData(String endpoint) {
        String resultTitle = push ? "\"results\"" : "\"subscriptionIds\"";
        String jsonStr = "{ \"dataverseName\":\"" + entityId.getDataverseName().getCanonicalForm()
                + "\", \"channelName\":\"" + entityId.getEntityName() + "\", \""
                + BADConstants.CHANNEL_EXECUTION_EPOCH_TIME + "\":" + executionTimeMili + ", " + resultTitle + ":[";
        jsonStr += sendData.get(endpoint);
        jsonStr += "]}";
        return jsonStr;
    }

    private void sendGroupOfResults(String endpoint) {
        String urlParameters = createData(endpoint);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(() -> {
            try {
                //Create connection
                URL url = new URL(endpoint);
                HttpURLConnection connection = (HttpURLConnection) url.openConnection();
                connection.setRequestMethod("POST");
                connection.setRequestProperty("Content-Type", "application/json");

                connection.setRequestProperty("Content-Length", Integer.toString(urlParameters.getBytes().length));
                connection.setRequestProperty("Content-Language", "en-US");

                connection.setUseCaches(false);
                connection.setDoOutput(true);
                connection.setConnectTimeout(500);
                DataOutputStream wr = new DataOutputStream(connection.getOutputStream());
                wr.writeBytes(urlParameters);
                if (LOGGER.isLoggable(Level.INFO)) {
                    int responseCode = connection.getResponseCode();
                    LOGGER.info("\nSending 'POST' request to URL : " + url);
                    LOGGER.info("Post parameters : " + urlParameters);
                    LOGGER.info("Response Code : " + responseCode);
                }
                wr.close();
                connection.disconnect();
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Channel Failed to connect to Broker.");
            }
        });
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        tAccess.reset(buffer);
        int nTuple = tAccess.getTupleCount();
        for (int t = 0; t < nTuple; t++) {
            tRef.reset(tAccess, t);

            eval0.evaluate(tRef, inputArg0);
            eval1.evaluate(tRef, inputArg1);
            eval2.evaluate(tRef, inputArg2);
            eval3.evaluate(tRef, inputArg3);

            /*The incoming tuples have three fields:
             1. eval0 will get the serialized broker endpoint string
             2. eval1 will get the payload (either the subscriptionIds or entire results)
             3. eval2 will get the channel execution time stamp (the same for all tuples)
            */
            if (executionTimeMili == -1) {
                int resultSetOffset = inputArg2.getStartOffset();
                bbis.setByteBuffer(tRef.getFrameTupleAccessor().getBuffer(), resultSetOffset + 1);
                ADateTime executionTime = ADateTimeSerializerDeserializer.INSTANCE.deserialize(di);
                executionTimeMili = executionTime.getChrononTime();
            }

            // Get HTTP endpoint
            int serBrokerOffset = inputArg0.getStartOffset();
            bbis.setByteBuffer(tRef.getFrameTupleAccessor().getBuffer(), serBrokerOffset + 1);
            String endpoint = stringSerDes.deserialize(di).getStringValue();

            // Get broker type
            int serTypeOffset = inputArg3.getStartOffset();
            bbis.setByteBuffer(tRef.getFrameTupleAccessor().getBuffer(), serTypeOffset + 1);
            String brokerType = stringSerDes.deserialize(di).getStringValue();
            IPrinter currPrinter =
                    brokerType.equals(BADConstants.BAD_BROKER_TYPE_NAME) ? admRecordPrinter : jsonRecordPrinter;

            PrintStream currPrintStream = sendStreams.getOrDefault(endpoint, null);
            if (currPrintStream == null) {
                try {
                    ByteArrayOutputStream newOutput = new ByteArrayOutputStream();
                    sendbaos.putIfAbsent(endpoint, newOutput);
                    sendStreams.put(endpoint, new PrintStream(newOutput, true, StandardCharsets.UTF_8.name()));
                } catch (UnsupportedEncodingException e) {
                    throw new HyracksDataException(e.getMessage());
                }
                currPrintStream = sendStreams.get(endpoint);
            } else {
                currPrintStream.append(',');
            }

            if (push) {
                int pushOffset = inputArg1.getStartOffset();
                bbis.setByteBuffer(tRef.getFrameTupleAccessor().getBuffer(), pushOffset + 1);
                currPrinter.print(inputArg1.getByteArray(), inputArg1.getStartOffset(), inputArg1.getLength(),
                        currPrintStream);
            } else {
                subscriptionIdListPrinterFactory.print(inputArg1.getByteArray(), inputArg1.getStartOffset(),
                        inputArg1.getLength(), currPrintStream);
            }
        }

    }

    @Override
    public void close() throws HyracksDataException {
        for (String endpoint : sendStreams.keySet()) {
            sendData.put(endpoint, new String(sendbaos.get(endpoint).toByteArray(), StandardCharsets.UTF_8));
            sendGroupOfResults(endpoint);
            sendStreams.get(endpoint).close();
            try {
                sendbaos.get(endpoint).close();
            } catch (IOException e) {
                throw new HyracksDataException(e.getMessage());
            }

        }

        return;
    }

    @Override
    public void setInputRecordDescriptor(int index, RecordDescriptor recordDescriptor) {
        this.inputRecordDesc = recordDescriptor;
        this.tAccess = new FrameTupleAccessor(inputRecordDesc);
    }

    @Override
    public void flush() throws HyracksDataException {
        return;
    }

    @Override
    public void fail() throws HyracksDataException {
        failed = true;
    }
}
