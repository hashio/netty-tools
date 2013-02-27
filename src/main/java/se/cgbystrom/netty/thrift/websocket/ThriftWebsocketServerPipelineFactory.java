package se.cgbystrom.netty.thrift.websocket;

import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.handler.codec.http.HttpChunkAggregator;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;
import se.cgbystrom.netty.thrift.ThriftHandler;

import static org.jboss.netty.channel.Channels.pipeline;

/**
 * @author Takahiro Hashimoto
 */
public class ThriftWebsocketServerPipelineFactory implements ChannelPipelineFactory {

    private final ThriftHandler handler;

    public ThriftWebsocketServerPipelineFactory(ThriftHandler handler) {
        this.handler = handler;
    }

    @Override
    public ChannelPipeline getPipeline() throws Exception {
        ChannelPipeline pipeline = pipeline();
        pipeline.addLast("requestDecoder", new HttpRequestDecoder());
        pipeline.addLast("responseEncoder", new HttpResponseEncoder());
        pipeline.addLast("aggregator", new HttpChunkAggregator(1048576));
        pipeline.addLast("thriftHandler", (ChannelHandler) handler);
        return pipeline;
    }
}
