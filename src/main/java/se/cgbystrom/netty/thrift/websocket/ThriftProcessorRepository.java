package se.cgbystrom.netty.thrift.websocket;

import org.apache.thrift.TProcessor;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.jboss.netty.buffer.ChannelBuffer;

import java.nio.charset.Charset;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author Takahiro Hashimoto
 */
public class ThriftProcessorRepository {
    private ConcurrentMap<String, TProcessor> processorMap = new ConcurrentHashMap<String, TProcessor>();

    public ThriftProcessorRepository() {
    }

    /**
     * get processer from websocket frame with thrift-rpc sub protocol
     *
     * short: service name length
     * byte[]: service name
     *
     * @param buffer
     * @return
     */
    public TProcessor getProcessor(ChannelBuffer buffer) {

        short processorNameLength = buffer.readShort();
        String processorName = buffer.readBytes(processorNameLength).toString(Charset.forName("ASCII"));

        TProcessor processor = processorMap.get(processorName);

        return processor;
    }

    public void register(String processorName, TProcessor processor) {
        processorMap.putIfAbsent(processorName, processor);
    }

    public void unregister(String processorName){
        processorMap.remove(processorName);
    }
}
