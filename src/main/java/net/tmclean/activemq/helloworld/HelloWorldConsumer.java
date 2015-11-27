package net.tmclean.activemq.helloworld;

import static net.tmclean.activemq.helloworld.App.*;

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

import org.apache.activemq.ActiveMQConnectionFactory;

public class HelloWorldConsumer implements MessageListener, ExceptionListener
{
	private Connection      conn             = null;
	private Session         session          = null;
	private MessageProducer responseProducer = null;
	private MessageConsumer requestConsumer  = null;
	
	public void init( ActiveMQConnectionFactory connFactory ) throws JMSException
	{
    	conn = connFactory.createConnection();
		conn.start();
		
		session = conn.createSession( false, Session.AUTO_ACKNOWLEDGE );
		
		Destination requestDest = session.createQueue( QUEUE_NAME );
		requestConsumer = session.createConsumer( requestDest );
		requestConsumer.setMessageListener( this );

		responseProducer = session.createProducer( null );
		responseProducer.setDeliveryMode( DeliveryMode.NON_PERSISTENT );
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

			String correlationId = message.getJMSCorrelationID();
			
			TextMessage response = session.createTextMessage();
			response.setJMSCorrelationID( correlationId );
			
			if( message instanceof TextMessage )
			{
				TextMessage textMessage = (TextMessage)message;
				String text = textMessage.getText();
				System.out.println( "Got Message: " + text );
				
				response.setText( "Responding to message '" + text + "' from " + this.hashCode() );
			}
			else
			{
				System.out.println( "Received non text message: " + message.toString() );
				
				response.setText( "Responding non text message from " + this.hashCode() );
			}

			Destination responseDest = message.getJMSReplyTo();
			responseProducer.send( responseDest, response );
		}
		catch( Exception e )
		{
			e.printStackTrace();
		}
	}
	
	@Override
	public void onException( JMSException exception )
	{
		System.out.println( "JMS Exception occured, shutting down client" );
		exception.printStackTrace();
	}
	
	public void close()
	{
		try
		{
			responseProducer.close();
		}
		catch( Exception e )
		{
			e.printStackTrace();
		}
		
		try
		{
			requestConsumer.close();
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
