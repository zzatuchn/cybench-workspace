
package org.activemq.app;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import com.gocypher.cybench.core.annotation.BenchmarkMetaData;
import com.gocypher.cybench.core.annotation.BenchmarkTag;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

class MockConsumerListener implements MessageListener {
	
	@Override
	public void onMessage(Message message) {
		
	}
	
}

class MockConsumer implements Runnable, ExceptionListener {
	public final static String URL = "tcp://LAPTOP-BIKFQLST:61616";
	public final static String QUEUE_NAME = "QUEUE_NAME_12345678";
	
    public void run() {
    	try {
    		// Create a ConnectionFactory
            ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(URL);

            // Create a Connection
            Connection connection = connectionFactory.createConnection();
            
            connection.setExceptionListener(this);

            // Create a Session
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // Create the destination (Topic or Queue)
            Destination destination = session.createQueue(QUEUE_NAME);

            // Create a MessageConsumer from the Session to the Topic or Queue
            MessageConsumer consumer = session.createConsumer(destination);
         

            MockConsumerListener listener = new MockConsumerListener();
            
            consumer.setMessageListener(listener);
        
            connection.start();
            
            while (!Thread.currentThread().isInterrupted()) {}
                        
            consumer.close();
            session.close();
            connection.close();
            
	 
    	}
		catch (Exception e) {
			System.out.println("Caught: " + e);
            e.printStackTrace();
		}
    }
    
    public synchronized void onException(JMSException ex) {
        System.out.println("JMS Exception occured.  Shutting down client.");
    }
}

@State(Scope.Benchmark)
@BenchmarkMetaData(key = "domain", value = "java")
@BenchmarkMetaData(key = "context", value = "Message brokers")
@BenchmarkMetaData(key = "version", value = "1.0.0")
@BenchmarkMetaData(key = "api", value = "ActiveMQ")
@BenchmarkMetaData(key = "isLibraryBenchmark", value = "true")
@BenchmarkMetaData(key = "libVersion", value = "5.16.2")
@BenchmarkMetaData(key = "libSymbolicName", value = "org.apache.activemq")
@BenchmarkMetaData(key = "libDescription", value = "Java-based message broker developed by Apache")
@BenchmarkMetaData(key = "libVendor", value = "Apache")
@BenchmarkMetaData(key = "libUrl", value = "https://activemq.apache.org/")
@BenchmarkMetaData(key = "description", value = "Benchmarks testing ActiveMQ producers")
public class AMQProducerBench {
	
	@Param({"1", "2", "3", "4", "5", "6"})
	int run_number;
	
	private final byte[] bytes_1k = new byte[1_000];
	private final byte[] bytes_2k = new byte[2_000];
	private final byte[] bytes_32k = new byte[32_000];
	private final byte[] bytes_64k = new byte[64_000];
	private final byte[] bytes_128k = new byte[128_000];
	private final byte[] bytes_1000k = new byte[1_000_000];
	
	private BytesMessage message_1k;
	private BytesMessage message_2k;
	private BytesMessage message_32k;
	private BytesMessage message_64k;
	private BytesMessage message_128k;
	private BytesMessage message_1000k;
	
	private TextMessage message_1k_text;
	private TextMessage message_2k_text;
	private TextMessage message_32k_text;
	private TextMessage message_64k_text;
	private TextMessage message_128k_text;
	private TextMessage message_1000k_text;

	private final String QUEUE_NAME  = MockConsumer.QUEUE_NAME;
	private final String URL = MockConsumer.URL;
	
	private ActiveMQConnectionFactory factory;
	private Connection connection;
	private Session session;
	private Destination destination;
	private MessageProducer producer;
	
	private Thread mockConsumerThread;

    @Setup(Level.Trial)
    public void setup() {
    	try {    

    		mockConsumerThread = new Thread(new MockConsumer());
            mockConsumerThread.setDaemon(false);
            mockConsumerThread.start();   
    		    		
    		factory = new ActiveMQConnectionFactory(URL);
    		connection = factory.createConnection();
    		
    		session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    		destination = session.createQueue(QUEUE_NAME);
    		
    		producer = session.createProducer(destination);
            producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
            
            message_1k = session.createBytesMessage();
            message_1k.writeBytes(bytes_1k);
            message_2k = session.createBytesMessage();
            message_2k.writeBytes(bytes_2k);
            message_32k = session.createBytesMessage();
            message_32k.writeBytes(bytes_32k);
            message_64k = session.createBytesMessage();
            message_64k.writeBytes(bytes_64k);
            message_128k = session.createBytesMessage();
            message_128k.writeBytes(bytes_128k);
            message_1000k = session.createBytesMessage();
            message_1000k.writeBytes(bytes_1000k);
            
            message_1k_text = session.createTextMessage(new String(bytes_1k));
            message_2k_text = session.createTextMessage(new String(bytes_2k));
            message_32k_text = session.createTextMessage(new String(bytes_32k));
            message_64k_text = session.createTextMessage(new String(bytes_64k));
            message_128k_text = session.createTextMessage(new String(bytes_128k));
            message_1000k_text = session.createTextMessage(new String(bytes_1000k));
            
            connection.start();
   		}
   		catch (Exception e) {
   			System.out.println("Caught: " + e);
            e.printStackTrace();
   		}
    }

    @Setup(Level.Iteration)
    public void setupIteration() throws InterruptedException {
    	
    }
    

    @Benchmark
    @BenchmarkMode({Mode.Throughput})
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Fork(1)
    @Threads(1)
    @Measurement(iterations = 6, time = 5, timeUnit = TimeUnit.SECONDS)
    @Warmup(iterations = 2, time = 5, timeUnit = TimeUnit.SECONDS)
    @BenchmarkTag(tag = "2208ea38-640d-4780-99fe-e0acb045c06e")
    @BenchmarkMetaData(key = "dataSize", value = "1000")
    @BenchmarkMetaData(key = "actionName", value = "Send message")
    @BenchmarkMetaData(key = "title", value = "Send 1KB BytesMessage")
    @BenchmarkMetaData(key = "description", value = "Send a single non-persistent 1KB BytesMessage to ActiveMQ broker with listener running on separate thread. Using -Xms512M -Xmx8G.")
    public void producerBenchmark_1k(Blackhole bh) throws Exception
    {
    	producer.send(message_1k);
    }
    
    @Benchmark
    @BenchmarkMode({Mode.Throughput})
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Fork(1)
    @Threads(1)
    @Measurement(iterations = 6, time = 5, timeUnit = TimeUnit.SECONDS)
    @Warmup(iterations = 2, time = 5, timeUnit = TimeUnit.SECONDS)
    @BenchmarkTag(tag = "2a121a3d-7e1f-44d9-b940-4d61f0b0570d")
    @BenchmarkMetaData(key = "dataSize", value = "2000")
    @BenchmarkMetaData(key = "actionName", value = "Send message")
    @BenchmarkMetaData(key = "title", value = "Send 2KB BytesMessage")
    @BenchmarkMetaData(key = "description", value = "Send a single non-persistent 2KB BytesMessage to ActiveMQ broker with listener running on separate thread. Using -Xms512M -Xmx8G.")
    public void producerBenchmark_2k(Blackhole bh) throws Exception
    {
    	producer.send(message_2k);
    }
    
    @Benchmark
    @BenchmarkMode({Mode.Throughput})
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Fork(1)
    @Threads(1)
    @Measurement(iterations = 6, time = 5, timeUnit = TimeUnit.SECONDS)
    @Warmup(iterations = 2, time = 5, timeUnit = TimeUnit.SECONDS)
    @BenchmarkTag(tag = "e0eb7f3c-b6b8-4db2-ab27-94e8e520f6d8")
    @BenchmarkMetaData(key = "dataSize", value = "32000")
    @BenchmarkMetaData(key = "actionName", value = "Send message")
    @BenchmarkMetaData(key = "title", value = "Send 32KB BytesMessage")
    @BenchmarkMetaData(key = "description", value = "Send a single non-persistent 32KB BytesMessage to ActiveMQ broker with listener running on separate thread. Using -Xms512M -Xmx8G.")
    public void producerBenchmark_32k(Blackhole bh) throws Exception
    {
    	producer.send(message_32k);
    }
    
    @Benchmark
    @BenchmarkMode({Mode.Throughput})
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Fork(1)
    @Threads(1)
    @Measurement(iterations = 6, time = 5, timeUnit = TimeUnit.SECONDS)
    @Warmup(iterations = 2, time = 5, timeUnit = TimeUnit.SECONDS)
    @BenchmarkTag(tag = "1cd8d548-b02f-4cc9-bd5a-1a93870ac505")
    @BenchmarkMetaData(key = "dataSize", value = "64000")
    @BenchmarkMetaData(key = "actionName", value = "Send message")
    @BenchmarkMetaData(key = "title", value = "Send 64KB BytesMessage")
    @BenchmarkMetaData(key = "description", value = "Send a single non-persistent 64KB BytesMessage to ActiveMQ broker with listener running on separate thread. Using -Xms512M -Xmx8G.")
    public void producerBenchmark_64k(Blackhole bh) throws Exception
    {
    	producer.send(message_64k);
    }
    
    @Benchmark
    @BenchmarkMode({Mode.Throughput})
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Fork(1)
    @Threads(1)
    @Measurement(iterations = 6, time = 5, timeUnit = TimeUnit.SECONDS)
    @Warmup(iterations = 2, time = 5, timeUnit = TimeUnit.SECONDS)
    @BenchmarkTag(tag = "56a9cbd4-7ce0-4e7d-a82f-a6ca71ac85d3")
    @BenchmarkMetaData(key = "dataSize", value = "128000")
    @BenchmarkMetaData(key = "actionName", value = "Send message")
    @BenchmarkMetaData(key = "title", value = "Send 128KB BytesMessage")
    @BenchmarkMetaData(key = "description", value = "Send a single non-persistent 128KB BytesMessage to ActiveMQ broker with listener running on separate thread. Using -Xms512M -Xmx8G.")
    public void producerBenchmark_128k(Blackhole bh) throws Exception
    {
    	producer.send(message_128k);
    }
    
    @Benchmark
    @BenchmarkMode({Mode.Throughput})
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Fork(1)
    @Threads(1)
    @Measurement(iterations = 6, time = 5, timeUnit = TimeUnit.SECONDS)
    @Warmup(iterations = 2, time = 5, timeUnit = TimeUnit.SECONDS)
    @BenchmarkTag(tag = "fdd9cfcd-f646-4763-87c7-ea7c0b3cda3a")
    @BenchmarkMetaData(key = "dataSize", value = "1000000")
    @BenchmarkMetaData(key = "actionName", value = "Send message")
    @BenchmarkMetaData(key = "title", value = "Send 1MB BytesMessage")
    @BenchmarkMetaData(key = "description", value = "Send a single non-persistent 1MB BytesMessage to ActiveMQ broker with listener running on separate thread. Using -Xms512M -Xmx8G.")
    public void producerBenchmark_1000k(Blackhole bh) throws Exception
    {
    	producer.send(message_1000k);
    }
    

    @Benchmark
    @BenchmarkMode({Mode.Throughput})
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Fork(1)
    @Threads(1)
    @Measurement(iterations = 6, time = 5, timeUnit = TimeUnit.SECONDS)
    @Warmup(iterations = 2, time = 5, timeUnit = TimeUnit.SECONDS)
    @BenchmarkTag(tag = "d60c54ba-13d2-4eaa-be11-ab6af7061e09")
    @BenchmarkMetaData(key = "dataSize", value = "1000")
    @BenchmarkMetaData(key = "actionName", value = "Send message")
    @BenchmarkMetaData(key = "title", value = "Send 1KB TextMessage")
    @BenchmarkMetaData(key = "description", value = "Send a single non-persistent 1KB TextMessage to ActiveMQ broker with listener running on separate thread. Using -Xms512M -Xmx8G.")
    public void producerBenchmark_1k_text(Blackhole bh) throws Exception
    {
    	producer.send(message_1k_text);
    }
    
    @Benchmark
    @BenchmarkMode({Mode.Throughput})
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Fork(1)
    @Threads(1)
    @Measurement(iterations = 6, time = 5, timeUnit = TimeUnit.SECONDS)
    @Warmup(iterations = 2, time = 5, timeUnit = TimeUnit.SECONDS)
    @BenchmarkTag(tag = "7823ca6d-c022-4e1d-94d4-bda7276f6f78")
    @BenchmarkMetaData(key = "dataSize", value = "2000")
    @BenchmarkMetaData(key = "actionName", value = "Send message")
    @BenchmarkMetaData(key = "title", value = "Send 2KB TextMessage")
    @BenchmarkMetaData(key = "description", value = "Send a single non-persistent 2KB TextMessage to ActiveMQ broker with listener running on separate thread. Using -Xms512M -Xmx8G.")
    public void producerBenchmark_2k_text(Blackhole bh) throws Exception
    {
    	producer.send(message_2k_text);
    }
    
    @Benchmark
    @BenchmarkMode({Mode.Throughput})
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Fork(1)
    @Threads(1)
    @Measurement(iterations = 6, time = 5, timeUnit = TimeUnit.SECONDS)
    @Warmup(iterations = 2, time = 5, timeUnit = TimeUnit.SECONDS)
    @BenchmarkTag(tag = "764b1e4f-92b4-45ea-92c9-438493bfccc3")
    @BenchmarkMetaData(key = "dataSize", value = "32000")
    @BenchmarkMetaData(key = "actionName", value = "Send message")
    @BenchmarkMetaData(key = "title", value = "Send 32KB TextMessage")
    @BenchmarkMetaData(key = "description", value = "Send a single non-persistent 32KB TextMessage to ActiveMQ broker with listener running on separate thread. Using -Xms512M -Xmx8G.")
    public void producerBenchmark_32k_text(Blackhole bh) throws Exception
    {
    	producer.send(message_32k_text);
    }
    
    @Benchmark
    @BenchmarkMode({Mode.Throughput})
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Fork(1)
    @Threads(1)
    @Measurement(iterations = 6, time = 5, timeUnit = TimeUnit.SECONDS)
    @Warmup(iterations = 2, time = 5, timeUnit = TimeUnit.SECONDS)
    @BenchmarkTag(tag = "f73768c5-4a5e-49c6-85e7-395b833e3f2d")
    @BenchmarkMetaData(key = "dataSize", value = "64000")
    @BenchmarkMetaData(key = "actionName", value = "Send message")
    @BenchmarkMetaData(key = "title", value = "Send 64KB TextMessage")
    @BenchmarkMetaData(key = "description", value = "Send a single non-persistent 64KB TextMessage to ActiveMQ broker with listener running on separate thread. Using -Xms512M -Xmx8G.")
    public void producerBenchmark_64k_text(Blackhole bh) throws Exception
    {
    	producer.send(message_64k_text);
    }
    
    @Benchmark
    @BenchmarkMode({Mode.Throughput})
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Fork(1)
    @Threads(1)
    @Measurement(iterations = 6, time = 5, timeUnit = TimeUnit.SECONDS)
    @Warmup(iterations = 2, time = 5, timeUnit = TimeUnit.SECONDS)
    @BenchmarkTag(tag = "fea84a4d-8f49-4d87-8193-b1a36ceb8407")
    @BenchmarkMetaData(key = "dataSize", value = "128000")
    @BenchmarkMetaData(key = "actionName", value = "Send message")
    @BenchmarkMetaData(key = "title", value = "Send 128KB TextMessage")
    @BenchmarkMetaData(key = "description", value = "Send a single non-persistent 128KB TextMessage to ActiveMQ broker with listener running on separate thread. Using -Xms512M -Xmx8G.")
    public void producerBenchmark_128k_text(Blackhole bh) throws Exception
    {
    	producer.send(message_128k_text);
    }
    
    @Benchmark
    @BenchmarkMode({Mode.Throughput})
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Fork(1)
    @Threads(1)
    @Measurement(iterations = 6, time = 5, timeUnit = TimeUnit.SECONDS)
    @Warmup(iterations = 2, time = 5, timeUnit = TimeUnit.SECONDS)
    @BenchmarkTag(tag = "c63cd130-fdc2-493c-9c08-14e438ca5f0b")
    @BenchmarkMetaData(key = "dataSize", value = "1000000")
    @BenchmarkMetaData(key = "actionName", value = "Send message")
    @BenchmarkMetaData(key = "title", value = "Send 1MB TextMessage")
    @BenchmarkMetaData(key = "description", value = "Send a single non-persistent 1MB TextMessage to ActiveMQ broker with listener running on separate thread. Using -Xms512M -Xmx8G.")
    public void producerBenchmark_1000k_text(Blackhole bh) throws Exception
    {
    	producer.send(message_1000k_text);
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException, JMSException {
    	producer.close();
    	session.close();
        connection.close();
        mockConsumerThread.interrupt();
    }

    @TearDown(Level.Iteration)
    public void tearDownIteration() {
        //TODO Iteration level: write code to be executed after each iteration of the benchmark.
    }

}
