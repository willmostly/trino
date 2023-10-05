/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.io.CountingInputStream;
import jakarta.annotation.Nullable;
import okhttp3.Call;
import okhttp3.ConnectionPool;
import okhttp3.Headers;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import okhttp3.internal.connection.RealConnection;
import okhttp3.internal.connection.RealConnectionPool;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class JsonResponse<T>
{
    private final int statusCode;
    private final Headers headers;
    @Nullable
    private final String responseBody;
    private final boolean hasValue;
    private final T value;
    private final IllegalArgumentException exception;

    private JsonResponse(int statusCode, Headers headers, String responseBody)
    {
        this.statusCode = statusCode;
        this.headers = requireNonNull(headers, "headers is null");
        this.responseBody = requireNonNull(responseBody, "responseBody is null");

        this.hasValue = false;
        this.value = null;
        this.exception = null;
    }

    private JsonResponse(int statusCode, Headers headers, @Nullable String responseBody, @Nullable T value, @Nullable IllegalArgumentException exception)
    {
        this.statusCode = statusCode;
        this.headers = requireNonNull(headers, "headers is null");
        this.responseBody = responseBody;
        this.value = value;
        this.exception = exception;
        this.hasValue = (exception == null);
    }

    public int getStatusCode()
    {
        return statusCode;
    }

    public Headers getHeaders()
    {
        return headers;
    }

    public boolean hasValue()
    {
        return hasValue;
    }

    public T getValue()
    {
        if (!hasValue) {
            throw new IllegalStateException("Response does not contain a JSON value", exception);
        }
        return value;
    }

    public Optional<String> getResponseBody()
    {
        return Optional.ofNullable(responseBody);
    }

    @Nullable
    public IllegalArgumentException getException()
    {
        return exception;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("statusCode", statusCode)
                .add("headers", headers.toMultimap())
                .add("hasValue", hasValue)
                .add("value", value)
                .omitNullValues()
                .toString();
    }

    public static <T> JsonResponse<T> execute(JsonCodec<T> codec, Call.Factory client, Request request, OptionalLong materializedJsonSizeLimit)
    {
        try (Response response = client.newCall(request).execute()) {
            ResponseBody responseBody = requireNonNull(response.body());
            if (isJson(responseBody.contentType())) {
                String body = null;
                T value = null;
                IllegalArgumentException exception = null;
                CountingInputStream countingInputStream = new CountingInputStream(responseBody.byteStream());
                try {
                    if (materializedJsonSizeLimit.isPresent() && (responseBody.contentLength() < 0 || responseBody.contentLength() > materializedJsonSizeLimit.getAsLong())) {
                        // Parse from input stream, response is either of unknown size or too large to materialize. Raw response body
                        // will not be available if parsing fails
                        value = codec.fromJson(countingInputStream);
                    }
                    else {
                        // parse from materialized response body string
                        body = responseBody.string();
                        value = codec.fromJson(body);
                    }
                }
                catch (JsonProcessingException z) {
                    try {
                        System.err.println(countingInputStream.getCount() + " bytes read from response body stream for request to " + request.url());
                        System.err.println("catching: " + z.getMessage());
                        return executeWithRetryLimit(codec, client, request, materializedJsonSizeLimit, 0);
                    }
                    catch (Exception e) {
                        String message;
                        if (body != null) {
                            message = format("Unable to create %s from JSON response:\n[%s]", codec.getType(), body);
                        }
                        else {
                            message = format("Unable to create %s from JSON response", codec.getType());
                        }
                        exception = new IllegalArgumentException(message, e);
                    }
                }
                return new JsonResponse<>(response.code(), response.headers(), body, value, exception);
            }
            return new JsonResponse<>(response.code(), response.headers(), responseBody.string());
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static <T> JsonResponse<T> executeWithRetryLimit(JsonCodec<T> codec, Call.Factory client, Request request, OptionalLong materializedJsonSizeLimit, int retries)
    {
        retries++;
        System.err.println("Clearing OkHttp connection pool and cache, retry: " + Integer.toString(retries));
        try {
            ConnectionPool pool = ((OkHttpClient) client).connectionPool();
            Field delegateField = pool.getClass().getDeclaredField("delegate");
            delegateField.setAccessible(true);
            RealConnectionPool realConnectionPool = (RealConnectionPool) delegateField.get(pool);
            Field connectionsField = realConnectionPool.getClass().getDeclaredField("connections");
            connectionsField.setAccessible(true);
            ConcurrentLinkedQueue<RealConnection> connections = (ConcurrentLinkedQueue<RealConnection>) connectionsField.get(realConnectionPool);
            for (RealConnection connection : connections) {
                System.err.println(connection.toString());
                System.err.println("Healthy: " + connection.isHealthy(false) + ", extensive: " + connection.isHealthy(true));
                System.err.println(connection.getCalls().size() + " calls associated with connection");
            }

            ((OkHttpClient) client).connectionPool().evictAll();
            return execute(codec, client, request, materializedJsonSizeLimit);
        }
        catch (UncheckedIOException e) {
            System.err.println("retry hit UncheckIOException: " + e.getMessage());
            if (retries <= 5) {
                return executeWithRetryLimit(codec, client, request, materializedJsonSizeLimit, retries);
            }
            else {
                throw e;
            }
        }
        catch (NoSuchFieldException ex) {
            throw new RuntimeException(ex);
        }
        catch (IllegalAccessException ex) {
            throw new RuntimeException(ex);
        }
        catch (Exception e) {
            System.err.println("retry hit exception: " + e.getMessage());
            throw e;
        }
    }

    private static boolean isJson(MediaType type)
    {
        return (type != null) && "application".equals(type.type()) && "json".equals(type.subtype());
    }
}
