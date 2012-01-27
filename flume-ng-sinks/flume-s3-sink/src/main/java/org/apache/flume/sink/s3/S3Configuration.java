package org.apache.flume.sink.s3;
import org.apache.flume.Event;
import org.apache.flume.formatter.output.BucketPath;
import org.jets3t.service.S3Service;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.multi.s3.ThreadedS3Service;
import org.jets3t.service.security.AWSCredentials;


public class S3Configuration {

  private AWSCredentials awsCredentials;
  private String accessKey;
  private String secretKey;
  private S3Service service;
  private String bucket;
  private String prefix;
  private String filename;
  
  public S3Configuration(String accessKey, String secretKey, String bucket, String prefix, String filename)  {
    this.accessKey = accessKey;
    this.secretKey = secretKey;
    this.bucket = bucket;
    this.prefix = (prefix != null) ? prefix : "FlumeS3Sink/%Y-%m-%d/%H"; 
    this.filename = (filename != null) ? filename : "S3SinkData";
  }
  
  public S3Service getService() throws ServiceException {
    if(awsCredentials == null)
      awsCredentials = new AWSCredentials(accessKey, secretKey);
    
    if(service == null)
      service = new RestS3Service(awsCredentials);
    
    return service;
  }
  
  public String getBucket() {
    return bucket;
  }
  
  public String getPrefix(Event e) {
    return BucketPath.escapeString(prefix, e.getHeaders());
  }
  
  public String getFilename() {
    return filename;
  }
}
