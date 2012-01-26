package org.apache.flume.source;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.PollableSource;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.Authenticator;
import java.net.PasswordAuthentication;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.protocol.ClientContext;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.protocol.BasicHttpContext;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import com.cloudera.flume.core.Attributes;
//import com.cloudera.flume.core.EventImpl;
import com.google.common.base.Preconditions;

public class GnipNGTwitterSource extends AbstractSource implements EventDrivenSource,
Configurable {



	  private static final Logger logger = LoggerFactory
	      .getLogger(GnipNGTwitterSource.class);

	
	  private String command;

	  private CounterGroup counterGroup;
	  private ExecutorService executor;
	  private Future<?> runnerFuture;
	  
	  private String gnip_user;
	  private String gnip_pass;

	  public GnipNGTwitterSource() {
	 //   sequence = 0;
	    counterGroup = new CounterGroup();
	  }


	  @Override
	  public void start() {
	    logger.info("Exec'ing custom twitter source starting with command:{}", command);

	    executor = Executors.newSingleThreadExecutor();
	    counterGroup = new CounterGroup();

	    ExecRunnable runner = new ExecRunnable();

	    runner.command = command;
	    runner.channel = getChannel();
	    runner.counterGroup = counterGroup;
	    
	    runner.username = this.gnip_user;
	    runner.password = this.gnip_pass;

	    // FIXME: Use a callback-like executor / future to signal us upon failure.
	    runnerFuture = executor.submit(runner);

	    /*
	     * NB: This comes at the end rather than the beginning of the method because
	     * it sets our state to running. We want to make sure the executor is alive
	     * and well first.
	     */
	    super.start();

	    logger.debug("Exec source started");
	  }

	  @Override
	  public void stop() {
	    logger.info("Stopping gnip twitter source with command:{}", command);

	    if (runnerFuture != null) {
	      logger.debug("Stopping exec runner");
	      runnerFuture.cancel(true);
	      logger.debug("Exec runner stopped");
	    }

	    executor.shutdown();

	    while (!executor.isTerminated()) {
	      logger.debug("Waiting for  gnip twitter executor service to stop");
	      try {
	        executor.awaitTermination(500, TimeUnit.MILLISECONDS);
	      } catch (InterruptedException e) {
	        logger
	            .debug("Interrupted while waiting for exec executor service to stop. Just exiting.");
	        Thread.currentThread().interrupt();
	      }
	    }

	    super.stop();

	    logger.debug(" gnip twitter source with command:{} stopped. Metrics:{}", command,
	        counterGroup);
	  }

	  @Override
	  public void configure(Context context) {
	    command = context.get("command", String.class);
	    
	    this.gnip_user = context.get( "gnip_user", String.class );
	    this.gnip_pass = context.get( "gnip_pass", String.class );

	    Preconditions.checkState(command != null,
	        "The parameter command must be specified");
	  }

	  private static class ExecRunnable implements Runnable {

    private String command;
    private Channel channel;
    private CounterGroup counterGroup;

	    String username;
	    String password;
	    String url;

//	    BufferedReader in = null;

		public static BufferedReader Connect( String user, String pass ) {
			
			BufferedReader br = null;
			
			HttpHost host = new HttpHost("stream.gnip.com", 443, "https");
			
			String path = "/accounts/Converseon/publishers/twitter/streams/sample50/HALF.json";
			
			DefaultHttpClient client = new DefaultHttpClient( );
			
			BasicAuthCache auth_cache = new BasicAuthCache();
			
			BasicHttpContext context = new BasicHttpContext();
			
			CredentialsProvider cp = client.getCredentialsProvider();
			
			AuthScope as = new AuthScope( host.getHostName(), host.getPort() );
			
			
				      logger.debug("[GNIP] user: '" + user + "', pass: '" + pass + "'");
			
			UsernamePasswordCredentials userpass = new UsernamePasswordCredentials( user, pass );
			cp.setCredentials(as, userpass);
			
			context.setAttribute(ClientContext.AUTH_CACHE, auth_cache);
			
			auth_cache.put(host, new BasicScheme());


			HttpGet get = new HttpGet( path );
			get.addHeader("Accept-Encoding", "gzip");
			try {
				
				HttpResponse response = client.execute(host, get, context);
				
				GZIPInputStream inputstream = new GZIPInputStream( response.getEntity().getContent() );
				
				br = new BufferedReader( new InputStreamReader( inputstream, "UTF-8" ) );
	/*			
				for (int x = 0; x < 10; x++ ) {
					
					System.out.println( "TWEET:\n\n\n\n" );
					
					System.out.println( br.readLine() );
					
					
				}
	*/			
				//br.close();
				
			} catch (ClientProtocolException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			return br;
			
			
		}  	    
	    
	    @Override
	    public void run() {
	      
	      try {

	    	/*  
	    	  
	    	  String[] commandArgs = command.split("\\s+");
	        Process process = new ProcessBuilder(commandArgs).start();
*/
	        BufferedReader reader = Connect( this.username, this.password );

	        String line = null;

/*// old version
    String line = in.readLine();
    while (line == null || line.length() == 0) {
      line = in.readLine();
    }
    Event e = new EventImpl(line.getBytes());
    Attributes.setString(e, Event.A_SERVICE, "twitter");
    updateEventProcessingStats(e);	        
 */
	        
	        while ((line = reader.readLine()) != null) {
	          counterGroup.incrementAndGet("twitter.lines.read");


	          Transaction transaction = channel.getTransaction();
	          try {
	            transaction.begin();
	            Event event = EventBuilder.withBody(line.getBytes());
	            channel.put(event);
	            transaction.commit();
	          } catch (ChannelException e) {
	            transaction.rollback();
	            throw e;
	          } catch (Exception e) {
	            transaction.rollback();
	            throw e;
	          } 
	          finally {
	            transaction.close();
	          }

	        }


	        reader.close();
	      } catch (Exception e) {
	        logger.error("Failed while running command:" + command + " - Exception follows.", e);
	      }
	    }

	  }




}
