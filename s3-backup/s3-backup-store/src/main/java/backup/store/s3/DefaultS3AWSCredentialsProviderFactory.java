package backup.store.s3;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;

public class DefaultS3AWSCredentialsProviderFactory extends S3AWSCredentialsProviderFactory {

  private final AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();

  @Override
  public AWSCredentialsProvider getCredentials() throws Exception {
    return credentialsProvider;
  }

}
