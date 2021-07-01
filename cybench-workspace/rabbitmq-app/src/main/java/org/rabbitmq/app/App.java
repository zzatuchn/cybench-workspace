package org.rabbitmq.app;

import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
//import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.GetResponse;

public class App {

	private static final String QUEUE_NAME = "THE_QUEUE_NAME_1";
	
	static int NUM_MESSAGES = 500;
	
	static long producerStart;
	static long producerStop;
	static double producerTimeSum = 0;
	static long producerCalls = 0;

	static long getterStart;
	static long getterStop;
	static double getterTimeSum = 0;
	static long getterCalls = 0;
	
	static long producerFirstMessage;
	static long latency;
	
	static int[] byte_lengths = {1, 2, 32, 64, 128, 1000};
	static byte[] bytes;

	
    public static void main(String[] args) throws Exception {
    	
    	ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");
    	
    	Connection connection = factory.newConnection();
    	
	    Channel channel = connection.createChannel();
		channel.queueDeclare(QUEUE_NAME, false, false, false, null);
		channel.queuePurge(QUEUE_NAME);
		channel.basicQos(0);
   
    	System.out.println("RabbitMQ Demo, Producer + Consumer Concurrent");
    		
		System.out.println("Warmup...");
		bytes = new byte[1000];
		
		Thread p = thread(new ProducerThread());
		Thread c = thread(new ConsumerThread());
		
		Thread.sleep(20000);
		
		p.interrupt();
		c.interrupt();
		
		p.join();
		c.join();
		
		channel.queuePurge(QUEUE_NAME);
		
		producerCalls = 0;
    	getterCalls = 0;
    	producerTimeSum = 0;
    	getterTimeSum = 0;
		
		
		for (int length : byte_lengths) {
			
			System.out.println("LENGTH: " + length + "kB");
    		
    		bytes = new byte[1000 * length];
    		
    		p = thread(new ProducerThread());
    		c = thread(new ConsumerThread());
    		
    		Thread.sleep(20000);
    		
    		p.interrupt();
    		c.interrupt();
    		
    		p.join();
    		c.join();
    		    		    		
    		long producerTime = (producerStop - producerStart);
             
             long getterTime = (getterStop - getterStart);
             
        	double avgProducerTime = (double)producerTime / (double)(producerCalls);
        	avgProducerTime /= 1_000;
        	
        	double avgGetterTime = (double)getterTime / (double)(getterCalls);
        	avgGetterTime /= 1_000;
        	
        	double producerTP = 1 / avgProducerTime;
        	double getterTP = 1 / avgGetterTime;
        	
        	System.out.println("Producer: " + avgProducerTime + "s, " + producerTP + "op/s");
        	//System.out.println("(" + producerStart + " " + producerStop + " " + producerTime + " " + producerCalls + ")");
        	System.out.println("Consumer: " + avgGetterTime + "s, " + getterTP + "op/s");
        	//System.out.println("(" + getterStart + " " + getterStop + " " + getterTime  + " " + getterCalls + ")");
        	
        	producerCalls = 0;
        	getterCalls = 0;
        	producerTimeSum = 0;
        	getterTimeSum = 0;
    		channel.queuePurge(QUEUE_NAME);

    		
		}
		  
		
		/*
    	System.out.println("Warmup...");
    	bytes = new byte[1000];
    	
    	for (int i = 0; i < 100_000; i++) {
    		channel.basicPublish("", QUEUE_NAME, null, bytes);
    		GetResponse response = channel.basicGet(QUEUE_NAME, true);
    		if (response == null) System.out.println("NULL");
		}
    	
    	
    	producerCalls = 0;
    	getterCalls = 0;
    	producerTimeSum = 0;
    	getterTimeSum = 0;
   	
    	for (int length : byte_lengths) {
    		
    		System.out.println("LENGTH: " + length + "kB");
    		
    		bytes = new byte[1000 * length];
    		
    		for (int i = 0; i < 50_000; i++) {
    			producerStart = System.nanoTime();
                channel.basicPublish("", QUEUE_NAME, null, bytes);
                producerStop = System.nanoTime();
                
                getterStart = System.nanoTime();
                GetResponse response = channel.basicGet(QUEUE_NAME, true);
                getterStop = System.nanoTime();
                
                if (response == null) System.out.println("NULL");
                
                producerCalls++;
                getterCalls++;
                double producerTime = (producerStop - producerStart);
                producerTime /= 1_000_000;
                producerTimeSum += producerTime;
                
                double getterTime = (getterStop - getterStart);
                getterTime /= 1_000_000;
                getterTimeSum += getterTime;
    		}
    		
    		channel.queuePurge(QUEUE_NAME);
    		
        	double avgProducerTime = producerTimeSum / (double)(producerCalls);
        	avgProducerTime /= 1000;
        	
        	double avgGetterTime = getterTimeSum / (double)(getterCalls);
        	avgGetterTime /= 1000;
        	
        	double producerTP = 1 / avgProducerTime;
        	double getterTP = 1 / avgGetterTime;
        	
        	System.out.println("Producer: " + avgProducerTime + "s, " + producerTP + "op/s");
        	System.out.println("Consumer: " + avgGetterTime + "s, " + getterTP + "op/s");
        	
        	producerCalls = 0;
        	getterCalls = 0;
        	producerTimeSum = 0;
        	getterTimeSum = 0;
 
    	}
    	*/
		
		channel.abort();
		connection.abort();

	}
    
    public static Thread thread(Runnable r) {
    	Thread t = new Thread(r);
    	t.setDaemon(false);
        t.start();
    	return t;
    }
    
    public static class ProducerThread implements Runnable {

		@Override
		public void run() {
			try {
	    		ConnectionFactory factory = new ConnectionFactory();
	    	    factory.setHost("localhost");
	    	    Connection connection = factory.newConnection();
	    	    Channel channel = connection.createChannel();
	    	    channel.basicQos(0);

	    	    channel.queueDeclare(QUEUE_NAME, false, false, false, null);
	    	    
	    	    producerStart = System.currentTimeMillis();
	    	    while (!Thread.currentThread().isInterrupted()) {
	    	    	channel.basicPublish("", QUEUE_NAME, null, bytes);
	    	    	producerCalls++;
	    	    }
	    	    
	    	    producerStop = System.currentTimeMillis();
	    	    
	    	    channel.abort();
	    	    //System.out.println("producer interrupted");
	    	    connection.abort();
	    	 
	    	}
			catch (Exception e) {
				System.out.println("Caught: " + e);
	            e.printStackTrace();
			}
			
		}
    	
    }
    
    public static class ConsumerThread implements Runnable {

		@Override
		public void run() {
	    	try {
	    		ConnectionFactory factory = new ConnectionFactory();
	    	    factory.setHost("localhost");
	    	    Connection connection = factory.newConnection();
	    	    Channel channel = connection.createChannel();
	    	    channel.basicQos(0);

	    	    channel.queueDeclare(QUEUE_NAME, false, false, false, null);
	    	    
	    	    DeliverCallback deliverCallback = (consumerTag, delivery) -> {
	    	    	getterCalls++;
	    	    };
	    	    
	    	    getterStart = System.currentTimeMillis();
	    	    channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> { });
	    	    
	    	    
	    	    while (!Thread.currentThread().isInterrupted()) {}
	    	    

	    	    getterStop = System.currentTimeMillis();
	    	    
	    	    channel.abort();
	    	    //System.out.println("consumer interrupted");
	    	    connection.abort();
	    	 
	    	}
			catch (Exception e) {
				System.out.println("Caught: " + e);
	            e.printStackTrace();
			}
	    }
    	
    }
        
    
}