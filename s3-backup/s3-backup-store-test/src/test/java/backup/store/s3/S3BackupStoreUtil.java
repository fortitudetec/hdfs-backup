/*
 * Copyright 2016 Fortitude Technologies LLC
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 *     
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
