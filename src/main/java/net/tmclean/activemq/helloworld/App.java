package net.tmclean.activemq.helloworld;

import java.util.ArrayList;
import java.util.List;

import org.apache.activemq.ActiveMQConnectionFactory;

public class App 
{
	public static final String URL        = "tcp://localhost:61616";
	public static final String MSG_FMT    = "Hello world from %s : %d";
	public static final String QUEUE_NAME = "TEST.FOO";
	
	private static final List<HelloWorldProducer> producers = new ArrayList<>();
	private static final List<HelloWorldConsumer> consumers = new ArrayList<>();

    public static void main( String[] args ) throws Throwable
    {
		ActiveMQConnectionFactory connFactory = new ActiveMQConnectionFactory( URL );

		int consumerCount = 20;
		int producerCount = 100;
		
		for( int i=0; i<consumerCount; i++ )
		{
			HelloWorldConsumer consumer = new HelloWorldConsumer();
			consumer.init( connFactory );
			consumers.add( consumer );
		}
		
		for( int i=0; i<producerCount; i++ )
		{
			HelloWorldProducer producer = new HelloWorldProducer();
			producer.init( connFactory );
			producers.add( producer );
		}
		
		int count = 20;
		
    	for( int i=0; i<count; i++ )
    	{
    		try
    		{
    			for( int j=0; j<i+1; j++ )
    			{
    				for( int p=0; p<producerCount; p++ )
    				{
    					producers.get( p ).send();
    				}
    			}
    			
    			Thread.sleep( 1000 );
    		}
    		catch( Exception e )
    		{
    			e.printStackTrace();
    		}
    	}
    	
    	Thread.sleep( 1000 );

		for( int p=0; p<producerCount; p++ )
		{
			producers.get( p ).close();
		}
		
		for( int c=0; c<consumerCount; c++ )
		{
			consumers.get( c ).close();
		}
    }
}
