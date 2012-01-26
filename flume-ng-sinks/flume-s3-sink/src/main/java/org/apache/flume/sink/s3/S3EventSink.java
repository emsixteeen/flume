package org.apache.flume.sink.s3;

import java.io.IOException;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSink;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.formatter.output.TextDelimitedOutputFormatter;
import org.apache.flume.sink.AbstractSink;
import org.jets3t.service.model.S3Object;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class S3EventSink extends AbstractSink implements PollableSink,
    Configurable {
  private static final Logger LOG = LoggerFactory
    .getLogger(S3EventSink.class);

  private static enum S3BufferType { MEMORY, FILE };
  
  private S3BufferType bufferType;
  private S3Configuration s3Configuration;

  private Context context;
  private S3BufferedWriter bufferedWriter;
  private TextDelimitedOutputFormatter formatter;
  
  @Override
  public void configure(Context context) {
    this.context = context;
    
    String s3AwsBucket = context.getString("s3sink.awsBucket");
    String s3AwsAccessKey = context.getString("s3sink.awsAccessKey");
    String s3AwsSecretKey = context.getString("s3Sink.awsSecretKey");
    
    Preconditions.checkState(s3AwsBucket != null, "s3Sink.awsBucket not specified");
    Preconditions.checkState(s3AwsAccessKey != null, "s3Sink.awsAccessKey not specified");
    Preconditions.checkState(s3AwsSecretKey != null, "s3Sink.awsSecretKey not specified");
    
    String bufferType = context.getString("s3Sink.bufferType");
    if (bufferType == null) {
      this.bufferType = S3BufferType.MEMORY;
    } else {
      if (bufferType.toLowerCase().equals("memory")) {
        this.bufferType = S3BufferType.MEMORY;
      } else if (bufferType.toLowerCase().equals("file")) {
        this.bufferType = S3BufferType.FILE;
      } else {
        this.bufferType = S3BufferType.MEMORY;
      }
    }
    
    LOG.debug("Using AWS Bucket \"{}\"", s3AwsBucket);
    LOG.debug("Using AWS Access Key \"{}\"", s3AwsAccessKey);
    LOG.debug("Using AWS Secret Key \"{}\"", s3AwsSecretKey);
    
    this.s3Configuration = new S3Configuration(s3AwsAccessKey, s3AwsSecretKey,
        s3AwsBucket, context.getString("s3Sink.awsPrefix"),
        context.getString("s3Sink.awsFilename"));
    
    this.formatter = new TextDelimitedOutputFormatter();
  }

  @Override
  public Status process() throws EventDeliveryException {
    Channel channel = getChannel();
    Transaction transaction = channel.getTransaction();
    Status result = Status.READY;
    Event event = null;
    
    try {
      transaction.begin();
      event = channel.take();
      
      if (event != null) {
        bufferedWriter.append(event, formatter);
      }
      
      transaction.commit();
    } catch (IOException ex) {
      transaction.rollback();
      return Status.BACKOFF;
    } catch (Exception ex) {
      transaction.rollback();
      throw new EventDeliveryException("Unable to process event: " + event, ex);
    } finally {
      transaction.close();
    }
    
    return result;
  }
  
  @Override
  public void start() {
    switch (bufferType) {
      case MEMORY:
        bufferedWriter = new S3MemoryBufferedWriter();
        LOG.debug("Configured with a memory buffer");
        break;
        
      case FILE:
        bufferedWriter = new S3FileBufferedWriter(s3Configuration);
        LOG.debug("Configured with a file buffer");
        break;
    }
    
    bufferedWriter.configure(context);
    
    try {
      bufferedWriter.open();
    } catch (IOException ex) {
      LOG.warn("Got IOException while trying to open writer: ", ex);
    }
    super.start();
  }
  
  @Override
  public void stop() {
    try {
      bufferedWriter.close();
    } catch (IOException ex) {
      LOG.warn("Got IOException while trying to close bufferedWriter");
    }
    super.stop();
  }
}
