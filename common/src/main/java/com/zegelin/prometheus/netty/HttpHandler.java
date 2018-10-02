package com.zegelin.prometheus.netty;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.net.MediaType;
import com.zegelin.netty.Resources;
import com.zegelin.prometheus.cassandra.Harvester;
import com.zegelin.prometheus.domain.Labels;
import com.zegelin.prometheus.domain.MetricFamily;
import com.zegelin.prometheus.exposition.JsonFormatChunkedInput;
import com.zegelin.prometheus.exposition.TextFormatChunkedInput;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

// look ma, it's a mini HTTP server...
// missing a bunch of things, but it works with Prometheus, browsers and curl.. good enough
// (and better than pulling in all of Jersey, and far more performant too -- we can write to byte buffers instead of an OutputStream)
public class HttpHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
    private static final Logger logger = LoggerFactory.getLogger(HttpHandler.class);

    // MediaType defines these MIME types with charsets, which isn't applicable for Accept headers
    private static final MediaType TEXT_PLAIN = MediaType.create("text", "plain");
    private static final MediaType TEXT_HTML = MediaType.create("text", "html");
    private static final MediaType APPLICATION_JSON = MediaType.create("application", "json");

    private static final String TEXT_FORMAT_VERSION_004 = "0.0.4";

    private static final MediaType TEXT_FORMAT_004_TYPE = MediaType.create("text", "plain")
            .withParameter("version", TEXT_FORMAT_VERSION_004);

    private static final ByteBuf ROOT_DOCUMENT = Resources.asByteBuf(HttpHandler.class, "root.html");

    private static class HttpException extends RuntimeException {
        final HttpResponseStatus responseStatus;

        HttpException(final HttpResponseStatus responseStatus, final String message) {
            super(message);
            this.responseStatus = responseStatus;
        }

        HttpException(final HttpResponseStatus responseStatus) {
            this.responseStatus = responseStatus;
        }
    }

    public enum HelpExposition {
        INCLUDE,
        EXCLUDE,
        AUTOMATIC
    }

    private final Harvester harvester;
    private final HelpExposition helpExposition;

    public HttpHandler(final Harvester harvester, final HelpExposition helpExposition) {
        this.harvester = harvester;
        this.helpExposition = helpExposition;
    }

    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, final FullHttpRequest request) throws Exception {
        final QueryStringDecoder queryString = new QueryStringDecoder(request.getUri());

        try {
            switch (queryString.path()) {
                case "/":
                    sendRoot(ctx, request);
                    return;

                case "/metrics":
                    sendMetrics(ctx, request, queryString);
                    return;

                default:
                    throw new HttpException(HttpResponseStatus.NOT_FOUND, "The requested URI could not be found.");
            }

        } catch (final HttpException e) {
            sendError(ctx, e.responseStatus, e.getMessage());

        } catch (final Exception e) {
            logger.error("Exception while processing HTTP request {}.", request, e);

            sendError(ctx, HttpResponseStatus.INTERNAL_SERVER_ERROR, "An internal server error occurred while processing the request for this URI.");
        }
    }

    private void checkRequestMethod(final HttpRequest request, final HttpMethod... methods) {
        for (HttpMethod method : methods) {
            if (method == request.getMethod()) {
                return;
            }
        }

        throw new HttpException(HttpResponseStatus.METHOD_NOT_ALLOWED, "The request method is not allowed for this URI.");
    }

    private static List<MediaType> parseAcceptHeader(final HttpRequest request) {
        final String headerValue = request.headers().get(HttpHeaders.Names.ACCEPT);

        if (headerValue == null) {
            return ImmutableList.of();
        }

        try {
            return parseAcceptHeader(headerValue);

        } catch (final IllegalArgumentException e) {
            throw new HttpException(HttpResponseStatus.BAD_REQUEST, "The Accept header media type is invalid.");
        }
    }

    private static List<MediaType> parseAcceptHeader(final String headerValue) {
        class WeightedMediaType implements Comparable<WeightedMediaType> {
            private final MediaType mediaType;
            private final float weight;

            private WeightedMediaType(final MediaType mediaType) {
                this.mediaType = mediaType;

                final String rawQualityValue = Iterables.getLast(mediaType.parameters().get("q"), "1");

                this.weight = Float.parseFloat(rawQualityValue);
            }

            private WeightedMediaType(final String mediaType) {
                this(MediaType.parse(mediaType));
            }

            @Override
            public int compareTo(final WeightedMediaType o) {
                return Float.compare(o.weight, this.weight);
            }
        }

        final Iterable<String> parts = Splitter.on(',').trimResults().split(headerValue);

        return StreamSupport.stream(parts.spliterator(), false)
                .map(WeightedMediaType::new)
                .sorted()
                .map(w -> w.mediaType)
                .collect(Collectors.toList());
    }

    private Multimap<MediaType, MediaType> checkAndGetPreferredMediaTypes(final HttpRequest request, final MediaType... supportedMediaTypes) {
        return checkAndGetPreferredMediaTypes(parseAcceptHeader(request), supportedMediaTypes);
    }

    /***
     * @return map of matched SupportedMediaTypes -> AcceptedMediaTypes
     */
    private Multimap<MediaType, MediaType> checkAndGetPreferredMediaTypes(final List<MediaType> acceptedMediaTypes, final MediaType... supportedMediaTypes) {
        final ImmutableMultimap.Builder<MediaType, MediaType> preferredMediaTypes = ImmutableMultimap.builder();

        outer:
        for (final MediaType acceptedMediaType : acceptedMediaTypes) {
            for (MediaType supportedMediaType : supportedMediaTypes) {
                if (supportedMediaType.is(acceptedMediaType.withoutParameters())) {
                    preferredMediaTypes.put(supportedMediaType, acceptedMediaType);
                    continue outer;
                }
            }
        }

        final ImmutableMultimap<MediaType, MediaType> map = preferredMediaTypes.build();

        if (map.isEmpty()) {
            throw new HttpException(HttpResponseStatus.UNSUPPORTED_MEDIA_TYPE, "None of the specified acceptable media types are supported for this URI.");
        }

        return map;
    }

    private void sendError(final ChannelHandlerContext ctx, final HttpResponseStatus status, final String message) {
        final FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status, ByteBufUtil.writeUtf8(ctx.alloc(), message));

        response.headers().add(HttpHeaders.Names.CONTENT_TYPE, "text/html");

        ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
    }

    private void sendRoot(final ChannelHandlerContext ctx, final HttpRequest request) {
        checkRequestMethod(request, HttpMethod.GET, HttpMethod.HEAD);
        checkAndGetPreferredMediaTypes(request, TEXT_HTML);

        final DefaultFullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, ROOT_DOCUMENT.slice());
        response.headers().add(HttpHeaders.Names.CONTENT_TYPE, MediaType.HTML_UTF_8);

        ctx.write(response);
        ctx.writeAndFlush(new DefaultLastHttpContent()).addListener(ChannelFutureListener.CLOSE);
    }

    private void sendMetrics(final ChannelHandlerContext ctx, final FullHttpRequest request, final QueryStringDecoder queryString) {
        checkRequestMethod(request, HttpMethod.GET, HttpMethod.HEAD);

        final List<MediaType> acceptedMediaTypes = Optional.ofNullable(queryString.parameters().get("x-accept"))
                .map(v -> Iterables.getLast(v, null))
                .map(v -> {
                    try {
                        return parseAcceptHeader(v);

                    } catch (final IllegalArgumentException e) {
                        throw new HttpException(HttpResponseStatus.BAD_REQUEST, "The media type specified for 'x-accept' is invalid.");
                    }
                })
                .orElseGet(() -> parseAcceptHeader(request));

        final boolean includeHelp = Optional.ofNullable(queryString.parameters().get("help"))
                .map(v -> Iterables.getLast(v, null))
                .map(v -> {
                    if ("true".equalsIgnoreCase(v)) {
                        return true;
                    } else if ("false".equalsIgnoreCase(v)) {
                        return false;
                    } else {
                        throw new HttpException(HttpResponseStatus.BAD_REQUEST, "The value specified for 'help' is invalid.");
                    }
                })
                .orElseGet(() -> {
                    final String userAgent = request.headers().get(HttpHeaders.Names.USER_AGENT);

                    switch (helpExposition) {
                        case AUTOMATIC:
                            // if the requester is Prometheus, exclude the help strings -- they are currently thrown away and just waste bandwidth
                            if (userAgent.startsWith("Prometheus")) {
                                return false;
                            }

                            // fall-through
                        case INCLUDE:
                            return true;

                        case EXCLUDE:
                            return false;

                        default:
                            throw new IllegalStateException();
                    }
                });

        final Multimap<MediaType, MediaType> preferredMediaTypes = checkAndGetPreferredMediaTypes(acceptedMediaTypes, TEXT_FORMAT_004_TYPE, TEXT_PLAIN, APPLICATION_JSON);

        for (final Map.Entry<MediaType, ?> preferredMediaType : preferredMediaTypes.asMap().entrySet()) {
            final MediaType supportedType = preferredMediaType.getKey();

            final DefaultHttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);

            final Stream<MetricFamily> metricFamilyStream = harvester.collect();
            final Instant timestamp = Instant.now();
            final Labels globalLabels = harvester.globalLabels();

            if (supportedType.equals(TEXT_FORMAT_004_TYPE) || supportedType.equals(TEXT_PLAIN)) {
                response.headers().set(HttpHeaders.Names.CONTENT_TYPE, TEXT_FORMAT_004_TYPE);

                ctx.write(response);
                ctx.write(new TextFormatChunkedInput(metricFamilyStream, timestamp, globalLabels, includeHelp));

                ctx.writeAndFlush(new DefaultLastHttpContent()).addListener(ChannelFutureListener.CLOSE);

                return;
            }

            if (supportedType.equals(APPLICATION_JSON)) {
                response.headers().set(HttpHeaders.Names.CONTENT_TYPE, MediaType.JSON_UTF_8);

                ctx.write(response);
                ctx.write(new JsonFormatChunkedInput(metricFamilyStream, timestamp, globalLabels, includeHelp));

                ctx.writeAndFlush(new DefaultLastHttpContent()).addListener(ChannelFutureListener.CLOSE);

                return;
            }
        }

        throw new IllegalStateException();
    }
}
