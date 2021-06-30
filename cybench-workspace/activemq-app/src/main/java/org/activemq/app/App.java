package org.activemq.app;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;


public class App {

	private static final String QUEUE_NAME = "queue_name_00000";
	
	static long producerStart;
	static long producerStop;
	static double producerTimeSum = 0;
	static long producerCalls = 0;

	static long getterStart;
	static long getterStop;
	static double getterTimeSum = 0;
	static long getterCalls = 0;
	
	static int[] byte_lengths = {1, 2, 32, 64, 128, 1000};
	static byte[] bytes;

	
    public static void main(String[] args) throws Exception {
   
    	System.out.println("ActiveMQ Demo");
    	
    	 ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://LAPTOP-BIKFQLST:61616");

         Connection connection = connectionFactory.createConnection();
         
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         Destination destination = session.createQueue("TEST.FOO");
         
         MessageConsumer consumer = session.createConsumer(destination);
         
         MessageProducer producer = session.createProducer(destination);
         producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
 
         connection.start();
         
         System.out.println("TESTING BytesMessage:");
         System.out.println("Warmup...");
         
         bytes = new byte[1000];
         BytesMessage bm = session.createBytesMessage();
         bm.writeBytes(bytes);
    	
    	 for (int i = 0; i < 100_000; i++) {
    		producer.send(bm);
    		Message m = consumer.receive(1000);
    		if (m == null) System.out.println("NULL");
    	
    	}
    	
    	producerCalls = 0;
    	getterCalls = 0;
    	producerTimeSum = 0;
    	getterTimeSum = 0;
    	
    	for (int length : byte_lengths) {
    		
    		System.out.println("LENGTH: " + length + "kB");
    		
    		bytes = new byte[1000 * length];
    		
    		bm = session.createBytesMessage();
            bm.writeBytes(bytes);
    		
    		for (int i = 0; i < 100_000; i++) {
    			producerStart = System.nanoTime();
                producer.send(bm);
                producerStop = System.nanoTime();
                
                getterStart = System.nanoTime();
                Message m = consumer.receive(1000);
                getterStop = System.nanoTime();
                
                if (m == null) System.out.println("NULL");
                
                producerCalls++;
                getterCalls++;
                double producerTime = (producerStop - producerStart);
                producerTime /= 1_000_000;
                producerTimeSum += producerTime;
                
                double getterTime = (getterStop - getterStart);
                getterTime /= 1_000_000;
                getterTimeSum += getterTime;
                
        	}
    		
    		
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
    	
    	System.out.println("TESTING TextMessage:");
    	System.out.println("Warmup...");
    	
    	bytes = new byte[1000];
        TextMessage tm = session.createTextMessage(new String(bytes));
   	
	   	 for (int i = 0; i < 100_000; i++) {
	   		producer.send(tm);
	   		Message m = consumer.receive(1000);
	   		if (m == null) System.out.println("NULL");
	   	
	   	}
    	
	   	 for (int length : byte_lengths) {
    		
    		System.out.println("LENGTH: " + length + "kB");
    		
    		bytes = new byte[1000 * length];
    		
    		tm = session.createTextMessage(new String(bytes));
    		
    		for (int i = 0; i < 100_000; i++) {
    			producerStart = System.nanoTime();
                producer.send(tm);
                producerStop = System.nanoTime();
                
                getterStart = System.nanoTime();
                Message m = consumer.receive(1000);
                getterStop = System.nanoTime();
                
                if (m == null) System.out.println("NULL");
                
                producerCalls++;
                getterCalls++;
                double producerTime = (producerStop - producerStart);
                producerTime /= 1_000_000;
                producerTimeSum += producerTime;
                
                double getterTime = (getterStop - getterStart);
                getterTime /= 1_000_000;
                getterTimeSum += getterTime;
                
        	}
    		
    		
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
    	
    	producer.close();
    	consumer.close();
    	session.close();
    	connection.close();
    	connection.close();

    }
}