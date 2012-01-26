package org.apache.flume.sink.s3;

import java.io.IOException;

import org.apache.flume.Event;
import org.apache.flume.conf.Configurable;
import org.apache.flume.formatter.output.EventFormatter;
import org.apache.flume.sink.FlumeFormatter;

public interface S3BufferedWriter extends Configurable {
  public void open() throws IOException;
  
  public void append(Event e, EventFormatter fmt) throws IOException, InterruptedException;
  
  public void close() throws IOException;
}
