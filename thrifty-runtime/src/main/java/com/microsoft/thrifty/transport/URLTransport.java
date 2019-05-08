package com.microsoft.thrifty.transport;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;

public class URLTransport extends Transport {
    private URL url;
    private int connectTimeout;
    private int readTimeout;
    private Map<String, String> customHeaders;

    private InputStream inputStream = null;
    private final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

    public static class Builder {
        private URL url;
        private int connectTimeout = 0;
        private int readTimeout = 0;
        private Map<String, String> customHeaders;

        public Builder(URL url) {
            this.url = url;
        }

        public Builder setConnectionTimeout(int timeout) {
            if (timeout < 0) {
                throw new IllegalArgumentException("timeout can not be negative");
            }
            this.connectTimeout = timeout;
            return this;
        }

        public Builder setReadTimeout(int timeout) {
            if (timeout < 0) {
                throw new IllegalArgumentException("timeout can not be negative");
            }
            this.readTimeout = timeout;
            return this;
        }

        public Builder setCustomHeaders(Map<String, String> headers) {
            this.customHeaders = headers;
            return this;
        }

        public URLTransport build() {
            return new URLTransport(this);
        }
    }

    URLTransport(Builder builder) {
        this.url = builder.url;
        this.connectTimeout = builder.connectTimeout;
        this.readTimeout = builder.readTimeout;
        this.customHeaders = builder.customHeaders;
    }

    @Override
    public int read(byte[] buffer, int offset, int count) throws IOException {
        if (inputStream == null) {
            throw new IOException("Response buffer is empty, no request.");
        }
        try {
            int bytes = inputStream.read(buffer, offset, count);
            if (bytes == -1) {
                throw new IOException("No more data available.");
            }
            return bytes;
        } catch (IOException iox) {
            throw new IOException(iox);
        }
    }

    @Override
    public void write(byte[] buffer, int offset, int count) throws IOException {
        outputStream.write(buffer, offset, count);
    }

    @Override
    public void flush() throws IOException {
        // Extract request and reset buffer
        byte[] data = outputStream.toByteArray();
        outputStream.reset();

        // Create connection object
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();

        // Timeouts, only if explicitly set
        if (connectTimeout > 0) {
            connection.setConnectTimeout(connectTimeout);
        }
        if (readTimeout > 0) {
            connection.setReadTimeout(readTimeout);
        }

        // Make the request
        connection.setRequestMethod("POST");
        connection.setRequestProperty("Content-Type", "application/x-thrift");
        connection.setRequestProperty("Accept", "application/x-thrift");
        if (customHeaders != null) {
            for (Map.Entry<String, String> header : customHeaders.entrySet()) {
                connection.setRequestProperty(header.getKey(), header.getValue());
            }
        }
        connection.setDoOutput(true);
        connection.connect();
        connection.getOutputStream().write(data);

        int responseCode = connection.getResponseCode();
        if (responseCode != HttpURLConnection.HTTP_OK) {
            throw new IOException("HTTP Response code: " + responseCode);
        }

        // Read the responses
        inputStream = connection.getInputStream();
    }

    @Override
    public void close() throws IOException {
        if (inputStream != null) {
            try {
                inputStream.close();
            } catch (IOException ignored) {
            } finally {
                inputStream = null;
            }
        }
        try {
            outputStream.close();
        } catch (IOException ignored) {
        }
    }
}
