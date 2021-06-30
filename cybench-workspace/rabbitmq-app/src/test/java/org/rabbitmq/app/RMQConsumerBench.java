
package org.rabbitmq.app;

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
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.Blackhole;

import com.gocypher.cybench.core.annotation.BenchmarkMetaData;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;

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
@BenchmarkMetaData(key = "description", value = "Benchmarks testing RabbitMQ consumers")
public class RMQConsumerBench {
	
	@Param({"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"})
	int run_number;

	private final byte[] bytes_1k = new byte[1_000];
	private final byte[] bytes_2k = new byte[2_000];
	private final byte[] bytes_32k = new byte[32_000];
	private final byte[] bytes_64k = new byte[64_000];
	private final byte[] bytes_128k = new byte[128_000];
	private final byte[] bytes_1000k = new byte[1_000_000];
	
	private byte[] bytes_to_send;
		
	private ConnectionFactory factory;
	private Connection connection;
	private Channel channel;
		
	private final String QUEUE_NAME = "QUEUE_NAME_ABCDEFGH";
	
    @Setup(Level.Trial)
    public void setup(BenchmarkParams params) {
    	try {	
       		factory = new ConnectionFactory();
    		connection = factory.newConnection();
    		channel = connection.createChannel();
   			channel.queueDeclare(QUEUE_NAME, false, false, false, null);
   	    	channel.queuePurge(QUEUE_NAME);
   	    	channel.basicQos(0);
   	    	
   	    	String bench_name = params.getBenchmark();
   	    	
   	    	if (bench_name.equals("org.rabbitmq.app.RMQConsumerBench.consumerBenchmark_1k")) {
   	    		bytes_to_send = bytes_1k;
   	    	}
   	    	else if (bench_name.equals("org.rabbitmq.app.RMQConsumerBench.consumerBenchmark_2k")) {
   	    		bytes_to_send = bytes_2k;
   	    	}
   	    	else if (bench_name.equals("org.rabbitmq.app.RMQConsumerBench.consumerBenchmark_32k")) {
   	    		bytes_to_send = bytes_32k;
   	    	}
   	    	else if (bench_name.equals("org.rabbitmq.app.RMQConsumerBench.consumerBenchmark_64k")) {
   	    		bytes_to_send = bytes_64k;
   	    	}   	
   	    	else if (bench_name.equals("org.rabbitmq.app.RMQConsumerBench.consumerBenchmark_128k")) {
   	    		bytes_to_send = bytes_128k;
   	    	}
   	    	else if (bench_name.equals("org.rabbitmq.app.RMQConsumerBench.consumerBenchmark_1000k")) {
   	    		bytes_to_send = bytes_1000k;
   	    	}
       	}
   		catch (Exception e) {
   			System.out.println("Caught: " + e);
               e.printStackTrace();
   		}
    }
    
    @Setup(Level.Invocation)
    public void setupInvocation() throws InterruptedException, IOException {
    	
    	channel.basicPublish("", QUEUE_NAME, null, bytes_to_send);
    }

    @Benchmark
    @BenchmarkMode({Mode.Throughput})
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Fork(1)
    @Threads(1)
    @Measurement(iterations = 6, time = 5, timeUnit = TimeUnit.SECONDS)
    @Warmup(iterations = 2, time = 5, timeUnit = TimeUnit.SECONDS)
    @BenchmarkMetaData(key = "dataSize", value = "1000")
    @BenchmarkMetaData(key = "actionName", value = "Consume message")
    @BenchmarkMetaData(key = "title", value = "Consume 1KB message")
    @BenchmarkMetaData(key = "description", value = "Consume single 1KB message from the queue with basicGet(). Queue is re-stocked every invocation. Using -Xms512M -Xmx8G.")
    public void consumerBenchmark_1k(Blackhole bh)
        throws Exception
    {
    	GetResponse response = channel.basicGet(QUEUE_NAME, true);
    	bh.consume(response);
    }
    
    @Benchmark
    @BenchmarkMode({Mode.Throughput})
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Fork(1)
    @Threads(1)
    @Measurement(iterations = 6, time = 5, timeUnit = TimeUnit.SECONDS)
    @Warmup(iterations = 2, time = 5, timeUnit = TimeUnit.SECONDS)
    @BenchmarkMetaData(key = "dataSize", value = "2000")
    @BenchmarkMetaData(key = "actionName", value = "Consume message")
    @BenchmarkMetaData(key = "title", value = "Consume 2KB message")
    @BenchmarkMetaData(key = "description", value = "Consume single 1KB message from the queue with basicGet(). Queue is re-stocked every invocation. Using -Xms512M -Xmx8G.")
    public void consumerBenchmark_2k(Blackhole bh)
        throws Exception
    {
    	GetResponse response = channel.basicGet(QUEUE_NAME, true);
    	bh.consume(response);
    }
    
    @Benchmark
    @BenchmarkMode({Mode.Throughput})
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Fork(1)
    @Threads(1)
    @Measurement(iterations = 6, time = 5, timeUnit = TimeUnit.SECONDS)
    @Warmup(iterations = 2, time = 5, timeUnit = TimeUnit.SECONDS)
    @BenchmarkMetaData(key = "dataSize", value = "32000")
    @BenchmarkMetaData(key = "actionName", value = "Consume message")
    @BenchmarkMetaData(key = "title", value = "Consume 32KB message")
    @BenchmarkMetaData(key = "description", value = "Consume single 1KB message from the queue with basicGet(). Queue is re-stocked every invocation. Using -Xms512M -Xmx8G.")
    public void consumerBenchmark_32k(Blackhole bh)
        throws Exception
    {
    	GetResponse response = channel.basicGet(QUEUE_NAME, true);
    	bh.consume(response);
    }
    
    @Benchmark
    @BenchmarkMode({Mode.Throughput})
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Fork(1)
    @Threads(1)
    @Measurement(iterations = 6, time = 5, timeUnit = TimeUnit.SECONDS)
    @Warmup(iterations = 2, time = 5, timeUnit = TimeUnit.SECONDS)
    @BenchmarkMetaData(key = "dataSize", value = "64000")
    @BenchmarkMetaData(key = "actionName", value = "Consume message")
    @BenchmarkMetaData(key = "title", value = "Consume 64KB message")
    @BenchmarkMetaData(key = "description", value = "Consume single 1KB message from the queue with basicGet(). Queue is re-stocked every invocation. Using -Xms512M -Xmx8G.")
    public void consumerBenchmark_64k(Blackhole bh)
        throws Exception
    {
    	GetResponse response = channel.basicGet(QUEUE_NAME, true);
    	bh.consume(response);
    }
    
    @Benchmark
    @BenchmarkMode({Mode.Throughput})
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Fork(1)
    @Threads(1)
    @Measurement(iterations = 6, time = 5, timeUnit = TimeUnit.SECONDS)
    @Warmup(iterations = 2, time = 5, timeUnit = TimeUnit.SECONDS)
    @BenchmarkMetaData(key = "dataSize", value = "128000")
    @BenchmarkMetaData(key = "actionName", value = "Consume message")
    @BenchmarkMetaData(key = "title", value = "Consume 128KB message")
    @BenchmarkMetaData(key = "description", value = "Consume single 1KB message from the queue with basicGet(). Queue is re-stocked every invocation. Using -Xms512M -Xmx8G.")
    public void consumerBenchmark_128k(Blackhole bh)
        throws Exception
    {
    	GetResponse response = channel.basicGet(QUEUE_NAME, true);
    	bh.consume(response);
    }
    
    @Benchmark
    @BenchmarkMode({Mode.Throughput})
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Fork(1)
    @Threads(1)
    @Measurement(iterations = 6, time = 5, timeUnit = TimeUnit.SECONDS)
    @Warmup(iterations = 2, time = 5, timeUnit = TimeUnit.SECONDS)
    @BenchmarkMetaData(key = "dataSize", value = "1000000")
    @BenchmarkMetaData(key = "actionName", value = "Consume message")
    @BenchmarkMetaData(key = "title", value = "Consume 1MB message")
    @BenchmarkMetaData(key = "description", value = "Consume single 1KB message from the queue with basicGet(). Queue is re-stocked every invocation. Using -Xms512M -Xmx8G.")
    public void consumerBenchmark_1000k(Blackhole bh)
        throws Exception
    {
    	GetResponse response = channel.basicGet(QUEUE_NAME, true);
    	bh.consume(response);
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
    	channel.queuePurge(QUEUE_NAME);
    	channel.abort();
	    connection.abort();
    }

    @TearDown(Level.Iteration)
    public void tearDownIteration() throws IOException {
   
    }

}
