package backup.store.s3;

import org.apache.hadoop.conf.Configured;

import com.amazonaws.auth.AWSCredentialsProvider;

public abstract class S3AWSCredentialsProviderFactory extends Configured {

  public abstract AWSCredentialsProvider getCredentials() throws Exception;

}
