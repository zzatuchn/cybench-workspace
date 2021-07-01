
package org.rabbitmq.app;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

class MockConsumer implements Runnable {
	public final static String QUEUE_NAME = "QUEUE_NAME_ABCDEFGHI";
	
    public void run() {
    	try {
    		ConnectionFactory factory = new ConnectionFactory();
    	    factory.setHost("localhost");
    	    Connection connection = factory.newConnection();
    	    Channel channel = connection.createChannel();
    	    channel.basicQos(0);

    	    channel.queueDeclare(QUEUE_NAME, false, false, false, null);
    	    
    	    DeliverCallback deliverCallback = (consumerTag, delivery) -> {
    	    	//String message = new String(delivery.getBody());
    	    	//System.out.println(message.getBytes()[2]);
    	    };
    	    channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> { });
    	    
    	    while (!Thread.currentThread().isInterrupted()) {}
    	    
    	    
    	    channel.abort();
    	    connection.abort();
    	 
    	}
		catch (Exception e) {
			System.out.println("Caught: " + e);
            e.printStackTrace();
		}
    }
}

@State(Scope.Benchmark)
@BenchmarkMetaData(key = "domain", value = "java")
@BenchmarkMetaData(key = "context", value = "Message brokers")
@BenchmarkMetaData(key = "version", value = "1.0.0")
@BenchmarkMetaData(key = "api", value = "RabbitMQ")
@BenchmarkMetaData(key = "isLibraryBenchmark", value = "true")
@BenchmarkMetaData(key = "libVersion", value = "3.8.11")
@BenchmarkMetaData(key = "libSymbolicName", value = "com.rabbitmq.client")
@BenchmarkMetaData(key = "libDescription", value = "Cross-platform message broker")
@BenchmarkMetaData(key = "libVendor", value = "Pivotal Software")
@BenchmarkMetaData(key = "libUrl", value = "https://www.rabbitmq.com/")
@BenchmarkMetaData(key = "description", value = "Benchmarks testing RabbitMQ producers")
public class RMQProducerBench {
	
	@Param({"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"})
	int run_number;
		
	private ConnectionFactory factory;
	private Connection connection;
	private Channel channel;
	private final String QUEUE_NAME = MockConsumer.QUEUE_NAME;
	private Thread mockConsumerThread = null;
	
	private final byte[] bytes_1k = new byte[1_000];
	private final byte[] bytes_2k = new byte[2_000];
	private final byte[] bytes_32k = new byte[32_000];
	private final byte[] bytes_64k = new byte[64_000];
	private final byte[] bytes_128k = new byte[128_000];
	private final byte[] bytes_1000k = new byte[1_000_000];
	
    @Setup(Level.Trial)
    public void setup() {
    	try {
    		    		
			mockConsumerThread = new Thread(new MockConsumer());
            mockConsumerThread.setDaemon(false);
            mockConsumerThread.start();
            
            
            factory = new ConnectionFactory();
    		factory.setHost("localhost");
    		connection = factory.newConnection();
    		channel = connection.createChannel();
   			channel.queueDeclare(QUEUE_NAME, false, false, false, null);
    		channel.queuePurge(QUEUE_NAME);
    
   		
   		}
   		catch (Exception e) {
   			System.out.println("Caught: " + e);
               e.printStackTrace();
   		}
    }

    @Setup(Level.Iteration)
    public void setupIteration() throws IOException {
    }

    @Benchmark
    @BenchmarkMode({Mode.Throughput})
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Fork(1)
    @Threads(1)
    @Measurement(iterations = 6, time = 5, timeUnit = TimeUnit.SECONDS)
    @Warmup(iterations = 2, time = 5, timeUnit = TimeUnit.SECONDS)
    @BenchmarkTag(tag = "fd28ac11-4a9d-4c13-b3d5-2aa485177f30")
    @BenchmarkMetaData(key = "dataSize", value = "1000")
    @BenchmarkMetaData(key = "actionName", value = "Send message")
    @BenchmarkMetaData(key = "title", value = "Send 1KB message")
    @BenchmarkMetaData(key = "description", value = "Send a single 1KB message to RabbitMQ broker with consumer running on separate thread. Using -Xms512M -Xmx8G.")
    public void producerBenchmark_1k(Blackhole bh)
        throws Exception
    {
    	channel.basicPublish("", QUEUE_NAME, null, bytes_1k);
    }
    
    @Benchmark
    @BenchmarkMode({Mode.Throughput})
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Fork(1)
    @Threads(1)
    @Measurement(iterations = 6, time = 5, timeUnit = TimeUnit.SECONDS)
    @Warmup(iterations = 2, time = 5, timeUnit = TimeUnit.SECONDS)
    @BenchmarkTag(tag = "3b60dc6e-519c-49fc-89be-1550db281f1a")
    @BenchmarkMetaData(key = "dataSize", value = "2000")
    @BenchmarkMetaData(key = "actionName", value = "Send message")
    @BenchmarkMetaData(key = "title", value = "Send 2KB message")
    @BenchmarkMetaData(key = "description", value = "Send a single 2KB message to RabbitMQ broker with consumer running on separate thread. Using -Xms512M -Xmx8G.")
    public void producerBenchmark_2k(Blackhole bh)
        throws Exception
    {
    	channel.basicPublish("", QUEUE_NAME, null, bytes_2k);
    }
    
    @Benchmark
    @BenchmarkMode({Mode.Throughput})
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Fork(1)
    @Threads(1)
    @Measurement(iterations = 6, time = 5, timeUnit = TimeUnit.SECONDS)
    @Warmup(iterations = 2, time = 5, timeUnit = TimeUnit.SECONDS)
    @BenchmarkTag(tag = "a52c1bf7-d424-4841-be6a-5663360c1452")
    @BenchmarkMetaData(key = "dataSize", value = "32000")
    @BenchmarkMetaData(key = "actionName", value = "Send message")
    @BenchmarkMetaData(key = "title", value = "Send 32KB message")
    @BenchmarkMetaData(key = "description", value = "Send a single 32KB message to RabbitMQ broker with consumer running on separate thread. Using -Xms512M -Xmx8G.")
    public void producerBenchmark_32k(Blackhole bh)
        throws Exception
    {
    	channel.basicPublish("", QUEUE_NAME, null, bytes_32k);
    }
    
    @Benchmark
    @BenchmarkMode({Mode.Throughput})
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Fork(1)
    @Threads(1)
    @Measurement(iterations = 6, time = 5, timeUnit = TimeUnit.SECONDS)
    @Warmup(iterations = 2, time = 5, timeUnit = TimeUnit.SECONDS)
    @BenchmarkTag(tag = "a0984f27-6b14-430a-9c21-c6dc8c35884e")
    @BenchmarkMetaData(key = "dataSize", value = "64000")
    @BenchmarkMetaData(key = "actionName", value = "Send message")
    @BenchmarkMetaData(key = "title", value = "Send 64KB message")
    @BenchmarkMetaData(key = "description", value = "Send a single 64KB message to RabbitMQ broker with consumer running on separate thread. Using -Xms512M -Xmx8G.")
    public void producerBenchmark_64k(Blackhole bh)
        throws Exception
    {
    	channel.basicPublish("", QUEUE_NAME, null, bytes_64k);
    }
    
    @Benchmark
    @BenchmarkMode({Mode.Throughput})
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Fork(1)
    @Threads(1)
    @Measurement(iterations = 6, time = 5, timeUnit = TimeUnit.SECONDS)
    @Warmup(iterations = 2, time = 5, timeUnit = TimeUnit.SECONDS)
    @BenchmarkTag(tag = "a0b01288-fc00-41da-8b9f-b1db9cdbbf7d")
    @BenchmarkMetaData(key = "dataSize", value = "128000")
    @BenchmarkMetaData(key = "actionName", value = "Send message")
    @BenchmarkMetaData(key = "title", value = "Send 128KB message")
    @BenchmarkMetaData(key = "description", value = "Send a single 128KB message to RabbitMQ broker with consumer running on separate thread. Using -Xms512M -Xmx8G.")
    public void producerBenchmark_128k(Blackhole bh)
        throws Exception
    {
    	channel.basicPublish("", QUEUE_NAME, null, bytes_128k);
    }
    
    @Benchmark
    @BenchmarkMode({Mode.Throughput})
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Fork(1)
    @Threads(1)
    @Measurement(iterations = 6, time = 5, timeUnit = TimeUnit.SECONDS)
    @Warmup(iterations = 2, time = 5, timeUnit = TimeUnit.SECONDS)
    @BenchmarkTag(tag = "7476b44e-2a1e-4293-978c-67f9b62c9878")
    @BenchmarkMetaData(key = "dataSize", value = "1000000")
    @BenchmarkMetaData(key = "actionName", value = "Send message")
    @BenchmarkMetaData(key = "title", value = "Send 1MB message")
    @BenchmarkMetaData(key = "description", value = "Send a single 1MB message to RabbitMQ broker with consumer running on separate thread. Using -Xms512M -Xmx8G.")
    public void producerBenchmark_1000k(Blackhole bh)
        throws Exception
    {
    	channel.basicPublish("", QUEUE_NAME, null, bytes_1000k);
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException, TimeoutException {
    	mockConsumerThread.interrupt();
    	channel.queuePurge(QUEUE_NAME);
    	channel.abort();
    	connection.abort();
    
    }

    @TearDown(Level.Iteration)
    public void tearDownIteration() {
        //TODO Iteration level: write code to be executed after each iteration of the benchmark.
    }

}
