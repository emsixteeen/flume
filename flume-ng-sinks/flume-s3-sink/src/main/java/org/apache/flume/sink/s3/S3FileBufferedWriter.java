package org.apache.flume.sink.s3;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.formatter.output.EventFormatter;
import org.jets3t.service.S3Service;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.ServiceException;
import org.jets3t.service.model.S3Object;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3FileBufferedWriter implements S3BufferedWriter {
  private static final Logger LOG = LoggerFactory
    .getLogger(S3FileBufferedWriter.class);

  static final long defaultRollInterval = 30000;
  static final long defaultRollSize = 5 * 1024 * 1024; // 5MB
  static final int defaultRollCount = 1024;
  static final int defaultOverflowLimit = 2;
  static final String tmpFilePrefix = "s3Sink-";
  
  private File tempPath;
  private long rollInterval;
  private long rollSize;
  private int rollCount;
  private int overflowLimit;
  
  private long bytesWritten = 0;
  private long millisLastRoll = System.currentTimeMillis();
  private int eventCount = 0;
  
  private long rollCounter = 0;
  private FileOutputStream out;
  private File file;
  
  private S3Configuration s3Configuration;
  private Semaphore streams;
  private ExecutorService executor;
  private Event lastEvent;
  
  public S3FileBufferedWriter(S3Configuration s3Configuration) {
    this.s3Configuration = s3Configuration;
  }
  
  @Override
  public void configure(Context context) {
    String tempPath = context.get("s3sink.fileTempPath", String.class);
    String rollInterval = context.get("s3sink.fileRollInterval", String.class);
    String rollSize = context.get("s3sink.fileRollSize", String.class);
    String rollCount = context.get("s3sink.fileRollCount", String.class);
    String overflowLimit = context.get("s3Sink.fileOverflowLimit", String.class);
  
    this.tempPath = (tempPath == null) ? new File(System.getProperty("java.io.tmpdir")) : new File(tempPath);
    this.rollInterval = (rollInterval == null) ? defaultRollInterval : Long.valueOf(rollInterval);
    this.rollSize = (rollSize == null) ? defaultRollSize : Long.valueOf(rollSize);
    this.rollCount = (rollCount == null) ? defaultRollCount : Integer.valueOf(rollCount);
    this.overflowLimit = (overflowLimit == null) ? defaultOverflowLimit : Integer.valueOf(overflowLimit);
    
    this.streams = new Semaphore(this.overflowLimit);
  }

  @Override
  public void open() throws IOException {
    executor = Executors.newFixedThreadPool(overflowLimit);
    file = File.createTempFile(tmpFilePrefix, null, tempPath);
    out = new FileOutputStream(file);
    LOG.info("Opened new temporary file {}", file);
  }

  @Override
  public void close() throws IOException {
    out.flush();
    out.close();
    
    try {
      initiateUpload(file);
    } catch (InterruptedException ex) {
      throw new IOException("Interrupted while trying to upload file", ex);
    }
    
    executor.shutdown();
      
    try {
      while (executor.isTerminated() == false) {
        LOG.warn("Waiting for thread pool to complete and shutdown");
        executor.awaitTermination(1, TimeUnit.SECONDS);
      }
    } catch (InterruptedException ex) {
      LOG.warn("Shutdown interrupted: ", ex);
    }
  }
  
  @Override
  public void append(Event e, EventFormatter fmt) throws IOException, InterruptedException {
    // Save last event for output bucketing
    lastEvent = e;
    doRotate();

    byte[] b = fmt.format(e);
    out.write(b);
    out.flush();
    
    bytesWritten += b.length;
    eventCount++;
  }

  protected void initiateUpload(final File file) throws InterruptedException {
    LOG.debug("Initiating upload to Amazon S3");
    LOG.debug("{} of permits available in semaphore", streams.availablePermits());
    
    while(!streams.tryAcquire(1000, TimeUnit.MILLISECONDS)) {
      LOG.warn("Waiting to acquire semaphore for uploading a file. This will block appends.");
    }
    
    executor.execute(new Runnable() {
      public void run() {
        try {
          S3Service s3Service = s3Configuration.getService();
          S3Object s3Object = new S3Object(file);
          String objectKey = s3Configuration.getPrefix(lastEvent) + "/" + s3Configuration.getFilename() + "-" + System.currentTimeMillis() + "." + rollCounter;
          long startTime = System.currentTimeMillis();
          
          LOG.info("Attempting to upload {} to s3: {}", file, objectKey);
          // rollCounter Thread safety?
          s3Object.setKey(objectKey);
          s3Service.putObject(s3Configuration.getBucket(), s3Object);
          LOG.info("Succesfully uploaded " + file + " to s3: \"{}\" in {}ms", s3Object.getKey(), System.currentTimeMillis() - startTime);

          file.delete();
          LOG.info("Deleted temporary file {}", file);
          
          // Return semaphore
          streams.release();
          
        } catch (S3ServiceException ex) {
          LOG.error("Unable to upload object to S3: ", ex);
        } catch (ServiceException ex) {
          LOG.error("Unable to acquire S3Service: " + ex);
        } catch (IOException ex) {
          LOG.error("Got exception while trying to create new object: " + ex);
        } catch (NoSuchAlgorithmException ex) {
          LOG.error("Got NoSuchAlgorithimException: " + ex);
        }
      }
    });
  }
  
  protected void doRotate() throws InterruptedException, IOException {
    boolean needsRotate = false;
    
    if (bytesWritten != 0 && bytesWritten >= rollSize) {
      needsRotate = true;
    } else if (rollInterval != 0 && (System.currentTimeMillis() - millisLastRoll) >= rollInterval) {
      needsRotate = true;
    } else if(rollCount != 0 && eventCount >= rollCount) {
      needsRotate = true;
    }
    
    if (!needsRotate)
      return;

    // Rotate
    LOG.debug("Need to roll");
    out.flush();
    out.close();
    
    // Upload file
    initiateUpload(file);
    
    // New file
    file = File.createTempFile(tmpFilePrefix, null, tempPath);
    out = new FileOutputStream(file);
    LOG.info("Opened new temporary file {}", file);

    // Reset/bump counters
    rollCounter++;
    bytesWritten = 0;
    millisLastRoll = System.currentTimeMillis();
    eventCount = 0;
  }
}
