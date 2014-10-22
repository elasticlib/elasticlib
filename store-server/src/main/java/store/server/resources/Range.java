package store.server.resources;

import com.google.common.base.Splitter;
import com.google.common.net.HttpHeaders;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.core.Response.Status;
import store.common.exception.BadRequestException;
import store.common.exception.RangeNotSatisfiableException;

/**
 * Support class intended to parse Range HTTP request header parameter.
 */
class Range {

    private static final String BYTES = "bytes";
    private static final String INVALID_RANGE = "Invalid range: ";
    private final long offset;
    private final long length;
    private final long totalLength;

    /**
     * Constructor.
     *
     * @param requestRange Range HTTP request header parameter.
     * @param totalLength Requested content total length.
     */
    public Range(String requestRange, long totalLength) {
        List<Long> bounds = parseRange(requestRange);
        offset = extractOffset(bounds, totalLength);
        length = extractLength(bounds, totalLength);
        this.totalLength = totalLength;

        checkRange();
    }

    private static List<Long> parseRange(String requestRange) {
        if (requestRange == null || requestRange.trim().isEmpty()) {
            return Arrays.asList(null, null);
        }
        List<String> parts = Splitter.on('=').trimResults().splitToList(requestRange);
        if (parts.size() != 2) {
            throw new BadRequestException(INVALID_RANGE + requestRange);
        }
        if (!parts.get(0).equalsIgnoreCase(BYTES)) {
            return Arrays.asList(null, null);
        }
        List<String> byteRangeSet = Splitter.on(',').omitEmptyStrings().trimResults().splitToList(parts.get(1));
        if (byteRangeSet.size() > 1) {
            throw new RangeNotSatisfiableException();
        }
        return extractBounds(requestRange, parts.get(1));
    }

    private static List<Long> extractBounds(String requestRange, String byteRange) {
        List<String> bounds = Splitter.on('-').trimResults().splitToList(byteRange);
        if (bounds.size() != 2) {
            throw new BadRequestException(INVALID_RANGE + requestRange);
        }
        try {
            Long start = parseLongIfAny(bounds.get(0));
            Long end = parseLongIfAny(bounds.get(1));
            if (areBothNull(start, end) || areInReverseOrder(start, end)) {
                throw new BadRequestException(INVALID_RANGE + requestRange);
            }
            return Arrays.asList(start, end);

        } catch (NumberFormatException e) {
            throw new BadRequestException(INVALID_RANGE + requestRange, e);
        }
    }

    private static Long parseLongIfAny(String arg) {
        if (arg.length() == 0) {
            return null;
        }
        return Long.parseLong(arg);
    }

    private static boolean areBothNull(Long start, Long end) {
        return start == null && end == null;
    }

    private static boolean areInReverseOrder(Long start, Long end) {
        return start != null && end != null && start > end;
    }

    private static long extractOffset(List<Long> bounds, long totalLength) {
        Long start = bounds.get(0);
        Long end = bounds.get(1);
        if (start != null) {
            return start;
        }
        if (end != null) {
            return totalLength - end;
        }
        return 0;
    }

    private static long extractLength(List<Long> bounds, long totalLength) {
        Long start = bounds.get(0);
        Long end = bounds.get(1);
        if (start != null && end != null) {
            return end - start + 1;
        }
        if (start != null) {
            return totalLength - start;
        }
        if (end != null) {
            return end;
        }
        return totalLength;
    }

    private void checkRange() {
        if (offset < 0 ||
                offset > totalLength ||
                length < 0 ||
                length > totalLength ||
                offset + length > totalLength) {
            throw new RangeNotSatisfiableException();
        }
    }

    /**
     * @return The position of the first byte to request.
     */
    public long getOffset() {
        return offset;
    }

    /**
     * @return The amount of bytes to request.
     */
    public long getLength() {
        return length;
    }

    /**
     * @return The HTTP status to respond.
     */
    public Status getStatus() {
        if (length < totalLength) {
            return Status.PARTIAL_CONTENT;
        }
        return Status.OK;
    }

    /**
     * @return The HTTP headers to respond.
     */
    public Map<String, String> getHttpResponseHeaders() {
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put(HttpHeaders.ACCEPT_RANGES, BYTES);
        headers.put(HttpHeaders.CONTENT_LENGTH, String.valueOf(length));
        if (length < totalLength) {
            headers.put(HttpHeaders.CONTENT_RANGE, String.format("%d-%d/%d", offset, offset + length - 1, totalLength));
        }
        return headers;
    }
}
