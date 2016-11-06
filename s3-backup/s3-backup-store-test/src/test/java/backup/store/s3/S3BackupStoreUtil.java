package backup.store.s3;

import java.util.List;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class S3BackupStoreUtil {

  public static void removeBucket(String bucketName) throws Exception {
    removeAllObjects(bucketName);
    AmazonS3Client client = new AmazonS3Client(new DefaultAWSCredentialsProviderChain());
    client.deleteBucket(bucketName);
  }

  public static void removeAllObjects(String bucketName) throws Exception {
    AmazonS3Client client = new AmazonS3Client(new DefaultAWSCredentialsProviderChain());
    ObjectListing listObjects = client.listObjects(bucketName);
    List<S3ObjectSummary> objectSummaries = listObjects.getObjectSummaries();
    for (S3ObjectSummary objectSummary : objectSummaries) {
      String key = objectSummary.getKey();
      client.deleteObject(bucketName, key);
    }
  }

  public static void removeAllObjects(String bucketName, String prefix) throws Exception {
    AmazonS3Client client = new AmazonS3Client(new DefaultAWSCredentialsProviderChain());
    ObjectListing listObjects = client.listObjects(bucketName);
    List<S3ObjectSummary> objectSummaries = listObjects.getObjectSummaries();
    for (S3ObjectSummary objectSummary : objectSummaries) {
      String key = objectSummary.getKey();
      if (key.startsWith(prefix)) {
        client.deleteObject(bucketName, key);
      }
    }
  }

  public static boolean exists(String bucketName) throws Exception {
    AmazonS3Client client = new AmazonS3Client(new DefaultAWSCredentialsProviderChain());
    return client.doesBucketExist(bucketName);
  }

  public static void createBucket(String bucketName) throws Exception {
    AmazonS3Client client = new AmazonS3Client(new DefaultAWSCredentialsProviderChain());
    client.createBucket(bucketName);
  }
}
