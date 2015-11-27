package net.tmclean.activemq.helloworld;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;

import static net.tmclean.activemq.helloworld.App.*;

import java.util.UUID;

public class HelloWorldProducer implements MessageListener
{
	private Connection conn = null;
	private Session session = null;

	private Destination requestDest  = null;
	private Destination responseDest = null;
	
	private MessageProducer requestProducer  = null;
	private MessageConsumer responseConsumer = null;
	
	public void init( ActiveMQConnectionFactory connFactory ) throws JMSException
	{
    	conn = connFactory.createConnection();
		conn.start();
		
		session = conn.createSession( false, Session.AUTO_ACKNOWLEDGE );
		
		requestDest = session.createQueue( QUEUE_NAME );
		requestProducer = session.createProducer( requestDest );
		requestProducer.setDeliveryMode( DeliveryMode.NON_PERSISTENT );
		
		responseDest = session.createTemporaryQueue();
		responseConsumer = session.createConsumer( responseDest );
		responseConsumer.setMessageListener( this );
	}
	
	public void send() 
	{
		Thread t = new Thread( new Runnable() {
			public void run() {
				try
				{
					String text = String.format( MSG_FMT, Thread.currentThread().getName(), this.hashCode() );
					
					TextMessage message = session.createTextMessage( text );
					message.setJMSReplyTo( responseDest );
					message.setJMSCorrelationID( UUID.randomUUID().toString() );
					
					requestProducer.send( message );
				}
				catch( Exception e )
				{
					e.printStackTrace();
				}
			}
		});
		
		t.start();
	}
	
	@Override
	public void onMessage( Message message )
	{
		try
		{
			if( message == null )
			{
				return;
			}
			
			if( message instanceof TextMessage )
			{
				TextMessage textMessage = (TextMessage)message;
				
				System.out.println( "Got response: " + textMessage.getText() );
			}
		}
		catch( Exception e )
		{
			e.printStackTrace();
		}
	}
	
	public void close()
	{
		try
		{
			requestProducer.close();
		}
		catch( Exception e )
		{
			e.printStackTrace();
		}
		
		try
		{
			responseConsumer.close();
		}
		catch( Exception e )
		{
			e.printStackTrace();
		}
		
		try
		{
			session.close();
		}
		catch( Exception e )
		{
			e.printStackTrace();
		}
		
		try
		{
			conn.close();
		}
		catch( Exception e )
		{
			e.printStackTrace();
		}
	}
}