package se.cgbystrom.netty.thrift.websocket;

import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.*;
import org.apache.thrift.transport.TTransport;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.http.*;
import org.jboss.netty.handler.codec.http.websocketx.*;
import org.jboss.netty.util.CharsetUtil;
import se.cgbystrom.netty.thrift.TNettyChannelBuffer;
import se.cgbystrom.netty.thrift.ThriftHandler;

import static org.jboss.netty.handler.codec.http.HttpHeaders.isKeepAlive;
import static org.jboss.netty.handler.codec.http.HttpHeaders.setContentLength;
import static org.jboss.netty.handler.codec.http.HttpMethod.GET;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;

/**
 * @author Takahiro Hashimoto
 */
public class ThriftWebsocketServerHandler extends SimpleChannelUpstreamHandler implements ThriftHandler {
    private static final String WEBSOCKET_PATH = "/";

    private WebSocketServerHandshaker handshaker;

    private ThriftProcessorRepository processorRepository;

    private TProtocolFactory protocolFactory;

    private int responseSize = 4096;


    /**
     * Creates a Thrift processor handler with the default binary protocol
     */
    public ThriftWebsocketServerHandler(ThriftProcessorRepository processorRepository) {
        this.processorRepository = processorRepository;
        this.protocolFactory = new TCompactProtocol.Factory();
    }

    /**
     * Creates a Thrift processor handler
     * @param protocolFactory Protocol factory to use when encoding/decoding incoming calls.
     */
    public ThriftWebsocketServerHandler(TProtocolFactory protocolFactory) {
        this.processorRepository = new ThriftProcessorRepository();
        this.protocolFactory = protocolFactory;
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        Object msg = e.getMessage();
        if (msg instanceof HttpRequest) {
            handleHttpRequest(ctx, (HttpRequest) msg);
        } else if (msg instanceof WebSocketFrame) {
            handleWebSocketFrame(ctx, (WebSocketFrame) msg);
        }
    }

    private void handleHttpRequest(ChannelHandlerContext ctx, HttpRequest req) throws Exception {
        // Allow only GET methods.
        if (req.getMethod() != GET) {
            sendHttpResponse(ctx, req, new DefaultHttpResponse(HTTP_1_1, FORBIDDEN));
            return;
        }

        // Handshake
        WebSocketServerHandshakerFactory wsFactory = new WebSocketServerHandshakerFactory(
                getWebSocketLocation(req), "thrift-rpc", false);
        handshaker = wsFactory.newHandshaker(req);
        if (handshaker == null) {
            wsFactory.sendUnsupportedWebSocketVersionResponse(ctx.getChannel());
        } else {
            handshaker.handshake(ctx.getChannel(), req).addListener(WebSocketServerHandshaker.HANDSHAKE_LISTENER);
        }
    }

    private void handleWebSocketFrame(ChannelHandlerContext ctx, WebSocketFrame frame) {
        // Check for closing frame
        if (frame instanceof CloseWebSocketFrame) {
            handshaker.close(ctx.getChannel(), (CloseWebSocketFrame) frame);
            return;
        } else if (frame instanceof PingWebSocketFrame) {
            ctx.getChannel().write(new PongWebSocketFrame(frame.getBinaryData()));
            return;
        } else if (!(frame instanceof BinaryWebSocketFrame)) {
            throw new UnsupportedOperationException(String.format("%s frame types not supported", frame.getClass()
                    .getName()));
        }

        ChannelBuffer input = frame.getBinaryData();

        ChannelBuffer output = ChannelBuffers.dynamicBuffer(responseSize);
        TProcessor processor = processorRepository.getProcessor(input);
        TTransport transport = new TNettyChannelBuffer(input, output);
        TProtocol protocol = protocolFactory.getProtocol(transport);

        try {
            processor.process(protocol, protocol);
        }catch(TException e){
            e.printStackTrace();
        }

        ctx.getChannel().write(new BinaryWebSocketFrame(output));
    }

    /**
     * @return Default size for response buffer
     */
    public int getResponseSize() {
        return responseSize;
    }

    /**
     * Sets the default size for response buffer
     * @param responseSize New default size
     */
    public void setResponseSize(int responseSize) {
        this.responseSize = responseSize;
    }

    private static void sendHttpResponse(ChannelHandlerContext ctx, HttpRequest req, HttpResponse res) {
        // Generate an error page if response status code is not OK (200).
        if (res.getStatus().getCode() != 200) {
            res.setContent(ChannelBuffers.copiedBuffer(res.getStatus().toString(), CharsetUtil.UTF_8));
            setContentLength(res, res.getContent().readableBytes());
        }

        // Send the response and close the connection if necessary.
        ChannelFuture f = ctx.getChannel().write(res);
        if (!isKeepAlive(req) || res.getStatus().getCode() != 200) {
            f.addListener(ChannelFutureListener.CLOSE);
        }
    }

    private static String getWebSocketLocation(HttpRequest req) {
        return "ws://" + req.getHeader(HttpHeaders.Names.HOST) + WEBSOCKET_PATH;
    }

}
