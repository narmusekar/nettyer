package http.server.netty;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import static io.netty.handler.codec.http.HttpHeaders.Names.*;
import static io.netty.handler.codec.http.HttpHeaders.Values;
import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static io.netty.handler.codec.http.HttpVersion.*;

public class ServerHandler extends ChannelInboundHandlerAdapter
{
	 private static final byte[] data = { 'A', ' ', 'T', ' ','C',' ','k','a','f','k','a',' ','s','t','a','r','t','e','d'};
	 String brokers ="localhost:9772";
     String groupId = "001";
     String topic = "data";
	    @Override
	    public void channelReadComplete(ChannelHandlerContext ctx) 
	    {
	        ctx.flush();
	    }
	    @Override
	    public void channelRead(ChannelHandlerContext ctx, Object msg) 
	    {
	        if (msg instanceof HttpRequest) 
	        {
	            HttpRequest req = (HttpRequest) msg;

	            if (HttpHeaders.is100ContinueExpected(req)) 
	            {
	                ctx.write(new DefaultFullHttpResponse(HTTP_1_1, CONTINUE));
	            }
	            boolean keepAlive = HttpHeaders.isKeepAlive(req);
	            FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, OK, Unpooled.wrappedBuffer(data));
	            for(int i=0;i<5;i++)
	            {
	            	System.out.println("Sent:time:"+System.nanoTime()+" data:"+data[i]);
	            if (data != null && data.length == 3) 
	            {
	            	KafkaProducer producerThread = new KafkaProducer(brokers, topic);
	            	Thread t1 = new Thread(producerThread);
		            t1.start();
	            	KafkaConsumer consumerThread = new KafkaConsumer(brokers, groupId, topic);
	            	Thread t2 = new Thread(consumerThread);
	            	t2.start();
	            }
	            System.out.println("Received:time:"+System.nanoTime()+" data:"+data[i]);
	            }
	            response.headers().set(CONTENT_TYPE, "text/plain");
	            response.headers().set(CONTENT_LENGTH, response.content().readableBytes());
	            if (!keepAlive) 
	            {
	                ctx.write(response).addListener(ChannelFutureListener.CLOSE);
	            } 
	            else 
	            {
	                response.headers().set(CONNECTION, Values.KEEP_ALIVE);
	                ctx.write(response);
	            }
	        }
	    }
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
	{
		cause.printStackTrace();
		ctx.close();
	}
}
