package org.elasticlib.node.handlers;

import com.google.common.collect.Lists;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import static java.lang.System.lineSeparator;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.elasticlib.common.util.HttpLogBuilder;
import org.elasticlib.common.util.IdProvider;
import org.glassfish.grizzly.Buffer;
import org.glassfish.grizzly.CompletionHandler;
import org.glassfish.grizzly.WriteHandler;
import org.glassfish.grizzly.http.Method;
import org.glassfish.grizzly.http.io.NIOOutputStream;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.grizzly.http.server.Response;
import org.glassfish.grizzly.http.server.StaticHttpHandlerBase;
import org.glassfish.grizzly.http.util.Header;
import org.glassfish.grizzly.http.util.HttpStatus;
import org.glassfish.grizzly.memory.Buffers;
import org.glassfish.grizzly.memory.MemoryManager;
import org.glassfish.grizzly.utils.DelayedExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Alternate implementation of {@link org.glassfish.grizzly.http.server.StaticHttpHandler} class that also provides
 * logging. Simply wrapping the original implementation has been proved unsuccessful as request may be processed
 * asynchronously and response may be recycled by the time it has to be logged.
 */
public class LoggingStaticHttpHandler extends StaticHttpHandlerBase {

    private static final Logger LOG = LoggerFactory.getLogger(LoggingStaticHttpHandler.class);

    private final Path root;
    private final IdProvider idProvider;

    /**
     * Constructor.
     *
     * @param root The folder in which resources are located.
     * @param idProvider Provides identifiers for logged requests and responses.
     */
    public LoggingStaticHttpHandler(Path root, IdProvider idProvider) {
        this.root = root;
        this.idProvider = idProvider;
    }

    @Override
    public void service(Request request, Response response) throws Exception {
        long id = idProvider.get();
        log(request, id);

        File resource = findResource(request);
        if (resource == null) {
            response.sendError(HttpStatus.NOT_FOUND_404.getStatusCode());
            log(response, id);
            return;
        }

        if (!Method.GET.equals(request.getMethod())) {
            response.setHeader(Header.Allow, "GET");
            response.sendError(HttpStatus.METHOD_NOT_ALLOWED_405.getStatusCode());
            log(response, id);
            return;
        }

        pickupContentType(response, resource.getPath());
        addToFileCache(request, response, resource);

        response.setStatus(HttpStatus.OK_200);
        response.setContentLengthLong(resource.length());
        response.addDateHeader(Header.Date, System.currentTimeMillis());

        if (!response.isSendFileEnabled() || response.getRequest().isSecure()) {
            sendUsingBuffers(response, resource, id);
        } else {
            sendZeroCopy(response, resource, id);
        }
    }

    private File findResource(Request request) {
        String uri = getRelativeURI(request);
        if (uri == null) {
            return null;
        }

        File file = new File(root.toFile(), uri);
        boolean exists = file.exists();
        boolean isDirectory = file.isDirectory();

        if (exists && isDirectory) {
            file = new File(file, "/index.html");
            exists = file.exists();
            isDirectory = false;
        }

        if (isDirectory || !exists) {
            return null;
        }
        return file;
    }

    private static void sendUsingBuffers(Response response, File file, long id) throws IOException {
        NIOOutputStream outputStream = response.getNIOOutputStream();
        int chunkSize = 8192;

        response.suspend(DelayedExecutor.UNSET_TIMEOUT, TimeUnit.MILLISECONDS, new LoggingCompletionHandler(id));
        outputStream.notifyCanWrite(new NonBlockingDownloadHandler(response, outputStream, file, chunkSize));
    }

    private static void sendZeroCopy(Response response, File file, long id) throws IOException {
        log(response, id);
        response.getOutputBuffer().sendfile(file, null);
    }

    private static void log(Request request, long id) {
        String method = request.getMethod().toString();
        String uri = request.getRequestURL()
                .append(request.getQueryString() == null ? "" : ("?" + request.getQueryString()))
                .toString();

        Map<String, List<String>> headers = new LinkedHashMap<>();
        request.getHeaderNames().forEach(x -> headers.put(x, Lists.newArrayList(request.getHeaders(x))));

        String message = new HttpLogBuilder(id).request(method, uri, headers);
        LOG.info("Received request{}{}", lineSeparator(), message);
    }

    private static void log(Response response, long id) {
        Map<String, List<String>> headers = new LinkedHashMap<>();
        for (String key : response.getHeaderNames()) {
            headers.put(key, Arrays.asList(response.getHeaderValues(key)));
        }

        String message = new HttpLogBuilder(id).response(response.getStatus(), response.getMessage(), headers);
        LOG.info("Responded with response{}{}", lineSeparator(), message);
    }

    @Override
    protected boolean handle(String uri, Request request, Response response) throws Exception {
        // Not actually used.
        throw new UnsupportedOperationException();
    }

    private static class LoggingCompletionHandler implements CompletionHandler<Response> {

        private final long id;

        public LoggingCompletionHandler(long id) {
            this.id = id;
        }

        @Override
        public void completed(Response response) {
            log(response, id);
        }

        @Override
        public void cancelled() {
        }

        @Override
        public void failed(Throwable throwable) {
        }

        @Override
        public void updated(Response result) {
        }
    }

    private static class NonBlockingDownloadHandler implements WriteHandler {

        private final Response response;
        private final NIOOutputStream outputStream;
        private final FileChannel fileChannel;
        private final MemoryManager mm;
        private final int chunkSize;

        // Keep the remaining size.
        private volatile long size;

        NonBlockingDownloadHandler(Response response, NIOOutputStream outputStream, File file, int chunkSize) {
            try {
                fileChannel = new FileInputStream(file).getChannel();
            } catch (FileNotFoundException e) {
                throw new IllegalStateException("File should have existed", e);
            }

            size = file.length();

            this.response = response;
            this.outputStream = outputStream;
            mm = response.getRequest().getContext().getMemoryManager();
            this.chunkSize = chunkSize;
        }

        @Override
        public void onWritePossible() throws Exception {
            LOG.debug("[onWritePossible]");
            if (sendChunk()) {
                // if there are more bytes to be sent, reregister this WriteHandler.
                outputStream.notifyCanWrite(this);
            }
        }

        @Override
        public void onError(Throwable t) {
            LOG.debug("[onError] ", t);
            response.setStatus(500, t.getMessage());
            complete(true);
        }

        /**
         * Send next CHUNK_SIZE of file.
         */
        private boolean sendChunk() throws IOException {
            // Allocate buffer and mark it available for disposal after content is written.
            final Buffer buffer = mm.allocate(chunkSize);
            buffer.allowBufferDispose(true);

            // Read file to the Buffer
            final int justReadBytes = (int) Buffers.readFromFileChannel(fileChannel, buffer);
            if (justReadBytes <= 0) {
                complete(false);
                return false;
            }

            // Write the Buffer
            buffer.trim();
            outputStream.write(buffer);
            size -= justReadBytes;

            // Check the remaining size here to avoid extra onWritePossible() invocation
            if (size <= 0) {
                complete(false);
                return false;
            }
            return true;
        }

        /**
         * Complete the download
         */
        private void complete(boolean isError) {
            try {
                fileChannel.close();
            } catch (IOException e) {
                if (!isError) {
                    response.setStatus(500, e.getMessage());
                }
            }

            try {
                outputStream.close();
            } catch (IOException e) {
                if (!isError) {
                    response.setStatus(500, e.getMessage());
                }
            }

            if (response.isSuspended()) {
                response.resume();
            } else {
                response.finish();
            }
        }
    }
}
