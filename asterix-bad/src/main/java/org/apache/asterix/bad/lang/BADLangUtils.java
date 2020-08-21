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

package org.apache.asterix.bad.lang;

import java.net.URI;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;

public class BADLangUtils {

    private static final org.apache.log4j.Logger LOGGER = org.apache.log4j.Logger.getLogger(BADLangUtils.class);

    public static int executeStatement(String host, String stmt) throws Exception {
        URI badURI = URI.create("http://" + host + ":19002/query/service");
        RequestBuilder requestBuilder = RequestBuilder.post(badURI);
        requestBuilder.addParameter("statement", stmt);
        try {
            HttpResponse response = submitRequest(requestBuilder.build());
            if (response.getStatusLine().getStatusCode() != 200) {
                throw new AlgebricksException("Connecting to " + host + " failed");
            }
            return response.getStatusLine().getStatusCode();
        } catch (Exception e) {
            LOGGER.error("Statement Failed at " + host);
            throw e;
        }
    }

    private static HttpResponse submitRequest(HttpUriRequest request) throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CloseableHttpClient client = HttpClients.createDefault();
        Future<HttpResponse> response = executor.submit(() -> {
            try {
                return client.execute(request);
            } catch (Exception e) {
                throw e;
            }
        });
        try {
            return response.get();
        } catch (Exception e) {
            client.close();
            throw e;
        } finally {
            executor.shutdownNow();
        }
    }
}
