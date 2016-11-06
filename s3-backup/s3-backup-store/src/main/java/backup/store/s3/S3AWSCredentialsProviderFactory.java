package backup.store.s3;

import com.amazonaws.auth.AWSCredentialsProvider;

import backup.store.Configured;

public abstract class S3AWSCredentialsProviderFactory extends Configured {

  public abstract AWSCredentialsProvider getCredentials() throws Exception;

}
