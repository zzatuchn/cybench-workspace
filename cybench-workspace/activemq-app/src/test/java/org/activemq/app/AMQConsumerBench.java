
package org.activemq.app;

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
import com.gocypher.cybench.core.annotation.BenchmarkTag;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

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
@BenchmarkMetaData(key = "description", value = "Benchmarks testing ActiveMQ consumers")
public class AMQConsumerBench {
	
	//@Param({"1", "2", "3", "4", "5", "6"})
	//int run_number;
	
	private final String QUEUE_NAME  = "QUEUE_NAME_1234567";
	private final String URL = "tcp://LAPTOP-BIKFQLST:61616";
	
	private ActiveMQConnectionFactory factory;
	private Connection connection;
	private Session session;
	private Destination destination;
	private MessageConsumer consumer;
	private MessageProducer producer;
		
	private final byte[] bytes_1k = new byte[1_000];
	private final byte[] bytes_2k = new byte[2_000];
	private final byte[] bytes_32k = new byte[32_000];
	private final byte[] bytes_64k = new byte[64_000];
	private final byte[] bytes_128k = new byte[128_000];
	private final byte[] bytes_1000k = new byte[1_000_000];
		
	private BytesMessage bytes_message;
	private TextMessage text_message;
	private Message message;
	
	
    @Setup(Level.Trial)
    public void setup(BenchmarkParams params) {
    	try {
    		factory = new ActiveMQConnectionFactory(URL);
    		connection = factory.createConnection();
    		
    		session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    		destination = session.createQueue(QUEUE_NAME);
    		
    		consumer = session.createConsumer(destination);
    		producer = session.createProducer(destination);
    		
    		String bench_name = params.getBenchmark();
   	    	
   	    	if (bench_name.equals("org.activemq.app.AMQConsumerBench.consumerBenchmark_1k")) {
   	    		bytes_message = session.createBytesMessage();
   	    		bytes_message.writeBytes(bytes_1k);
   	    		message = bytes_message;
   	    	}
   	    	else if (bench_name.equals("org.activemq.app.AMQConsumerBench.consumerBenchmark_2k")) {
   	    		bytes_message = session.createBytesMessage();
   	    		bytes_message.writeBytes(bytes_2k);
   	    		message = bytes_message;
   	    	}
   	    	else if (bench_name.equals("org.activemq.app.AMQConsumerBench.consumerBenchmark_32k")) {
   	    		bytes_message = session.createBytesMessage();
   	    		bytes_message.writeBytes(bytes_32k);
   	    		message = bytes_message;
   	    	}
   	    	else if (bench_name.equals("org.activemq.app.AMQConsumerBench.consumerBenchmark_64k")) {
   	    		bytes_message = session.createBytesMessage();
   	    		bytes_message.writeBytes(bytes_64k);
   	    		message = bytes_message;
   	    	}   	
   	    	else if (bench_name.equals("org.activemq.app.AMQConsumerBench.consumerBenchmark_128k")) {
   	    		bytes_message = session.createBytesMessage();
   	    		bytes_message.writeBytes(bytes_128k);
   	    		message = bytes_message;
   	    	}
   	    	else if (bench_name.equals("org.activemq.app.AMQConsumerBench.consumerBenchmark_1000k")) {
   	    		bytes_message = session.createBytesMessage();
   	    		bytes_message.writeBytes(bytes_1000k);
   	    		message = bytes_message;
   	    	}
   	    	else if (bench_name.equals("org.activemq.app.AMQConsumerBench.consumerBenchmark_1k_text")) {
   	    		text_message = session.createTextMessage(new String(bytes_1k));
   	    		message = text_message;
   	    	}
   	    	else if (bench_name.equals("org.activemq.app.AMQConsumerBench.consumerBenchmark_2k_text")) {
   	    		text_message = session.createTextMessage(new String(bytes_2k));
   	    		message = text_message;
   	    	}
   	    	else if (bench_name.equals("org.activemq.app.AMQConsumerBench.consumerBenchmark_32k_text")) {
   	    		text_message = session.createTextMessage(new String(bytes_32k));
   	    		message = text_message;
   	    	}
   	    	else if (bench_name.equals("org.activemq.app.AMQConsumerBench.consumerBenchmark_64k_text")) {
   	    		text_message = session.createTextMessage(new String(bytes_64k));
   	    		message = text_message;
   	    	}   	
   	    	else if (bench_name.equals("org.activemq.app.AMQConsumerBench.consumerBenchmark_128k_text")) {
   	    		text_message = session.createTextMessage(new String(bytes_128k));
   	    		message = text_message;
   	    	}
   	    	else if (bench_name.equals("org.activemq.app.AMQConsumerBench.consumerBenchmark_1000k_text")) {
   	    		text_message = session.createTextMessage(new String(bytes_1000k));
   	    		message = text_message;
   	    	}
    		
    		connection.start();
                
   		}
   		catch (Exception e) {
   			System.out.println("Caught: " + e);
               e.printStackTrace();
   		}
    }

    @Setup(Level.Invocation)
    public void setupInvocation() throws JMSException {
 		producer.send(message);
    }

    @Benchmark
    @BenchmarkMode({Mode.Throughput})
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Fork(1)
    @Threads(1)
    @Measurement(iterations = 6, time = 5, timeUnit = TimeUnit.SECONDS)
    @Warmup(iterations = 2, time = 5, timeUnit = TimeUnit.SECONDS)
    @BenchmarkTag(tag = "dcf9281b-6453-4ce6-9744-523e4b41d639")
    @BenchmarkMetaData(key = "dataSize", value = "1000")
    @BenchmarkMetaData(key = "actionName", value = "Consume message")
    @BenchmarkMetaData(key = "title", value = "Consume 1KB BytesMessage")
    @BenchmarkMetaData(key = "description", value = "Consume single non-persistent 1KB BytesMessage from the queue with receiveNoWait(). Queue is re-stocked every invocation. Using -Xms512M -Xmx8G.")
    public void consumerBenchmark_1k(Blackhole bh)
        throws Exception
    {
    	
    	 Message message = consumer.receiveNoWait();
    	 //if (message instanceof BytesMessage) System.out.println(((BytesMessage)message).getBodyLength());
    	 bh.consume(message);
    }
    
    @Benchmark
    @BenchmarkMode({Mode.Throughput})
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Fork(1)
    @Threads(1)
    @Measurement(iterations = 6, time = 5, timeUnit = TimeUnit.SECONDS)
    @Warmup(iterations = 2, time = 5, timeUnit = TimeUnit.SECONDS)
    @BenchmarkTag(tag = "d9471aad-60fb-4904-ac45-95612eac59e4")
    @BenchmarkMetaData(key = "dataSize", value = "2000")
    @BenchmarkMetaData(key = "actionName", value = "Consume message")
    @BenchmarkMetaData(key = "title", value = "Consume 2KB BytesMessage")
    @BenchmarkMetaData(key = "description", value = "Consume single non-persistent 2KB BytesMessage from the queue with receiveNoWait(). Queue is re-stocked every invocation. Using -Xms512M -Xmx8G.")
    public void consumerBenchmark_2k(Blackhole bh)
        throws Exception
    {
    	 Message message = consumer.receiveNoWait();
    	 bh.consume(message);
    }
    
    @Benchmark
    @BenchmarkMode({Mode.Throughput})
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Fork(1)
    @Threads(1)
    @Measurement(iterations = 6, time = 5, timeUnit = TimeUnit.SECONDS)
    @Warmup(iterations = 2, time = 5, timeUnit = TimeUnit.SECONDS)
    @BenchmarkTag(tag = "44333d49-41e6-4c97-b544-374e711604ce")
    @BenchmarkMetaData(key = "dataSize", value = "32000")
    @BenchmarkMetaData(key = "actionName", value = "Consume message")
    @BenchmarkMetaData(key = "title", value = "Consume 32KB BytesMessage")
    @BenchmarkMetaData(key = "description", value = "Consume single non-persistent 32KB BytesMessage from the queue with receiveNoWait(). Queue is re-stocked every invocation. Using -Xms512M -Xmx8G.")
    public void consumerBenchmark_32k(Blackhole bh)
        throws Exception
    {
    	 Message message = consumer.receiveNoWait();
    	 bh.consume(message);
    }
    
    @Benchmark
    @BenchmarkMode({Mode.Throughput})
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Fork(1)
    @Threads(1)
    @Measurement(iterations = 6, time = 5, timeUnit = TimeUnit.SECONDS)
    @Warmup(iterations = 2, time = 5, timeUnit = TimeUnit.SECONDS)
    @BenchmarkTag(tag = "d5006e45-a70a-44fc-bd9e-61ecc6564686")
    @BenchmarkMetaData(key = "dataSize", value = "64000")
    @BenchmarkMetaData(key = "actionName", value = "Consume message")
    @BenchmarkMetaData(key = "title", value = "Consume 64KB BytesMessage")
    @BenchmarkMetaData(key = "description", value = "Consume single non-persistent 64KB BytesMessage from the queue with receiveNoWait(). Queue is re-stocked every invocation. Using -Xms512M -Xmx8G.")
    public void consumerBenchmark_64k(Blackhole bh)
        throws Exception
    {
    	 Message message = consumer.receiveNoWait();
    	 bh.consume(message);
    }
    
    @Benchmark
    @BenchmarkMode({Mode.Throughput})
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Fork(1)
    @Threads(1)
    @Measurement(iterations = 6, time = 5, timeUnit = TimeUnit.SECONDS)
    @Warmup(iterations = 2, time = 5, timeUnit = TimeUnit.SECONDS)
    @BenchmarkTag(tag = "1f1f4356-3ed6-4b4b-aa35-a413c9e59227")
    @BenchmarkMetaData(key = "dataSize", value = "128000")
    @BenchmarkMetaData(key = "actionName", value = "Consume message")
    @BenchmarkMetaData(key = "title", value = "Consume 128KB BytesMessage")
    @BenchmarkMetaData(key = "description", value = "Consume single non-persistent 128KB BytesMessage from the queue with receiveNoWait(). Queue is re-stocked every invocation. Using -Xms512M -Xmx8G.")
    public void consumerBenchmark_128k(Blackhole bh)
        throws Exception
    {
   
    	 Message message = consumer.receiveNoWait();
    	 bh.consume(message);
    }
    
    @Benchmark
    @BenchmarkMode({Mode.Throughput})
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Fork(1)
    @Threads(1)
    @Measurement(iterations = 6, time = 5, timeUnit = TimeUnit.SECONDS)
    @Warmup(iterations = 2, time = 5, timeUnit = TimeUnit.SECONDS)
    @BenchmarkTag(tag = "927ab1ca-7f0f-4559-b387-64fbed5a7ca9")
    @BenchmarkMetaData(key = "dataSize", value = "1000000")
    @BenchmarkMetaData(key = "actionName", value = "Consume message")
    @BenchmarkMetaData(key = "title", value = "Consume 1MB BytesMessage")
    @BenchmarkMetaData(key = "description", value = "Consume single non-persistent 1MB BytesMessage from the queue with receiveNoWait(). Queue is re-stocked every invocation. Using -Xms512M -Xmx8G.")
    public void consumerBenchmark_1000k(Blackhole bh)
        throws Exception
    {
    	 Message message = consumer.receiveNoWait();
    	 //System.out.println(((BytesMessage)message).getBodyLength());
    	 bh.consume(message);
    }
    
    @Benchmark
    @BenchmarkMode({Mode.Throughput})
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Fork(1)
    @Threads(1)
    @Measurement(iterations = 6, time = 5, timeUnit = TimeUnit.SECONDS)
    @Warmup(iterations = 2, time = 5, timeUnit = TimeUnit.SECONDS)
    @BenchmarkTag(tag = "f4c1d57c-4276-497a-85dc-6d5a9669a506")
    @BenchmarkMetaData(key = "dataSize", value = "1000")
    @BenchmarkMetaData(key = "actionName", value = "Consume message")
    @BenchmarkMetaData(key = "title", value = "Consume 1KB TextMessage")
    @BenchmarkMetaData(key = "description", value = "Consume single non-persistent 1KB TextMessage from the queue with receiveNoWait(). Queue is re-stocked every invocation. Using -Xms512M -Xmx8G.")
    public void consumerBenchmark_1k_text(Blackhole bh)
        throws Exception
    {
    	 Message message = consumer.receiveNoWait();
    	 bh.consume(message);
    }
    
    @Benchmark
    @BenchmarkMode({Mode.Throughput})
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Fork(1)
    @Threads(1)
    @Measurement(iterations = 6, time = 5, timeUnit = TimeUnit.SECONDS)
    @Warmup(iterations = 2, time = 5, timeUnit = TimeUnit.SECONDS)
    @BenchmarkTag(tag = "e45caba3-9d2a-4ab2-b13d-aa96bc60f58e")
    @BenchmarkMetaData(key = "dataSize", value = "2000")
    @BenchmarkMetaData(key = "actionName", value = "Consume message")
    @BenchmarkMetaData(key = "title", value = "Consume 2KB TextMessage")
    @BenchmarkMetaData(key = "description", value = "Consume single non-persistent 2KB TextMessage from the queue with receiveNoWait(). Queue is re-stocked every invocation. Using -Xms512M -Xmx8G.")
    public void consumerBenchmark_2k_text(Blackhole bh)
        throws Exception
    {
    	 Message message = consumer.receiveNoWait();
    	 bh.consume(message);
    }
    
    @Benchmark
    @BenchmarkMode({Mode.Throughput})
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Fork(1)
    @Threads(1)
    @Measurement(iterations = 6, time = 5, timeUnit = TimeUnit.SECONDS)
    @Warmup(iterations = 2, time = 5, timeUnit = TimeUnit.SECONDS)
    @BenchmarkTag(tag = "39ce8284-17f2-400e-9208-67c116d44bdd")
    @BenchmarkMetaData(key = "dataSize", value = "32000")
    @BenchmarkMetaData(key = "actionName", value = "Consume message")
    @BenchmarkMetaData(key = "title", value = "Consume 32KB TextMessage")
    @BenchmarkMetaData(key = "description", value = "Consume single non-persistent 32KB TextMessage from the queue with receiveNoWait(). Queue is re-stocked every invocation. Using -Xms512M -Xmx8G.")
    public void consumerBenchmark_32k_text(Blackhole bh)
        throws Exception
    {
    	 Message message = consumer.receiveNoWait();
    	 bh.consume(message);
    }
    
    @Benchmark
    @BenchmarkMode({Mode.Throughput})
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Fork(1)
    @Threads(1)
    @Measurement(iterations = 6, time = 5, timeUnit = TimeUnit.SECONDS)
    @Warmup(iterations = 2, time = 5, timeUnit = TimeUnit.SECONDS)
    @BenchmarkTag(tag = "59439f18-8002-4bfd-933f-496345a297d0")
    @BenchmarkMetaData(key = "dataSize", value = "64000")
    @BenchmarkMetaData(key = "actionName", value = "Consume message")
    @BenchmarkMetaData(key = "title", value = "Consume 64KB TextMessage")
    @BenchmarkMetaData(key = "description", value = "Consume single non-persistent 64KB TextMessage from the queue with receiveNoWait(). Queue is re-stocked every invocation. Using -Xms512M -Xmx8G.")
    public void consumerBenchmark_64k_text(Blackhole bh)
        throws Exception
    {
    	 Message message = consumer.receiveNoWait();
    	 bh.consume(message);
    }
    
    @Benchmark
    @BenchmarkMode({Mode.Throughput})
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Fork(1)
    @Threads(1)
    @Measurement(iterations = 6, time = 5, timeUnit = TimeUnit.SECONDS)
    @Warmup(iterations = 2, time = 5, timeUnit = TimeUnit.SECONDS)
    @BenchmarkTag(tag = "afa0c3ca-3a4f-4571-9e01-9b225eac9d0a")
    @BenchmarkMetaData(key = "dataSize", value = "128000")
    @BenchmarkMetaData(key = "actionName", value = "Consume message")
    @BenchmarkMetaData(key = "title", value = "Consume 128KB TextMessage")
    @BenchmarkMetaData(key = "description", value = "Consume single non-persistent 128KB TextMessage from the queue with receiveNoWait(). Queue is re-stocked every invocation. Using -Xms512M -Xmx8G.")
    public void consumerBenchmark_128k_text(Blackhole bh)
        throws Exception
    {
    	 
    	 Message message = consumer.receiveNoWait();
    	 bh.consume(message);
    }
    
    @Benchmark
    @BenchmarkMode({Mode.Throughput})
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Fork(1)
    @Threads(1)
    @Measurement(iterations = 6, time = 5, timeUnit = TimeUnit.SECONDS)
    @Warmup(iterations = 2, time = 5, timeUnit = TimeUnit.SECONDS)
    @BenchmarkTag(tag = "bfa58638-7bf9-4849-8ce4-f2bcafe6a86f")
    @BenchmarkMetaData(key = "dataSize", value = "1000000")
    @BenchmarkMetaData(key = "actionName", value = "Consume message")
    @BenchmarkMetaData(key = "title", value = "Consume 1MB TextMessage")
    @BenchmarkMetaData(key = "description", value = "Consume single non-persistent 1MB TextMessage from the queue with receiveNoWait(). Queue is re-stocked every invocation. Using -Xms512M -Xmx8G.")
    public void consumerBenchmark_1000k_text(Blackhole bh)
        throws Exception
    {
    	 Message message = consumer.receiveNoWait();
    	 bh.consume(message);
    }
    

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
    	producer.close();
    	consumer.close();
    	session.close();
        connection.close();
    }

    @TearDown(Level.Iteration)
    public void tearDownIteration() {
        //TODO Iteration level: write code to be executed after each iteration of the benchmark.
    }

}
