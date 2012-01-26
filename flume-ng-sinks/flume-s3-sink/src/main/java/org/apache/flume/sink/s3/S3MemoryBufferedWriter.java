package org.apache.flume.sink.s3;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.Configurable;
import org.apache.flume.formatter.output.EventFormatter;
import org.apache.flume.sink.FlumeFormatter;

public class S3MemoryBufferedWriter implements S3BufferedWriter {

  static final long defaultBufferDepth = 1024;
  static final long defaultOverflowBuffers = 2;
  
  private long bufferDepth = 1024;
  private long overflowBuffers = 2;

  @Override
  public void configure(Context context) {
    String bufferDepth = context.get("s3sink.memoryBufferDepth", String.class);
    String overflowBuffers = context.get("s3sink.memoryOverflowBuffers", String.class);
    
    this.bufferDepth = (bufferDepth == null) ? defaultBufferDepth : Long.valueOf(bufferDepth);
    this.overflowBuffers = (overflowBuffers == null) ? defaultOverflowBuffers : Long.valueOf(bufferDepth);
  }

  @Override
  public void open() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void append(Event e, EventFormatter fmt) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void close() {
    // TODO Auto-generated method stub
    
  }

}
