package backup.store.s3;

import java.io.InputStream;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.LengthInputStream;
import org.apache.hadoop.util.ReflectionUtils;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;

import backup.store.BackupStore;

@SuppressWarnings("unchecked")
public class S3BackupStore extends BackupStore {

  public static final String DFS_BACKUP_S3_OBJECT_PREFIX = "dfs.backup.s3.object.prefix";
  public static final String DFS_BACKUP_S3_BUCKET_NAME = "dfs.backup.s3.bucket.name";
  public static final String DFS_BACKUP_S3_CREDENTIALS_PROVIDER_FACTORY = "dfs.backup.s3.credentials.provider.factory";

  private static final String DATA = ".data";
  private static final String META = ".meta";
  private static final String NUM_BYTES = "numBytes";
  private static final String GEN_STAMP = "genStamp";
  private static final String BLOCK_ID = "blockId";
  private static final String BLOCK_POOL_ID = "blockPoolId";
  private static final String BLOCK_NAME = "blockName";

  private static final Joiner JOINER = Joiner.on('/');

  private String bucketName;
  private String objectPrefix;
  private S3AWSCredentialsProviderFactory credentialsProviderFactory;
  private AmazonS3Client s3Client;

  @Override
  public void init() throws Exception {
    Configuration conf = getConf();

    Class<? extends S3AWSCredentialsProviderFactory> clazz = (Class<? extends S3AWSCredentialsProviderFactory>) conf.getClass(
        DFS_BACKUP_S3_CREDENTIALS_PROVIDER_FACTORY, DefaultS3AWSCredentialsProviderFactory.class);
    credentialsProviderFactory = ReflectionUtils.newInstance(clazz, conf);
    bucketName = conf.get(DFS_BACKUP_S3_BUCKET_NAME);
    objectPrefix = conf.get(DFS_BACKUP_S3_OBJECT_PREFIX);
    s3Client = new AmazonS3Client(credentialsProviderFactory.getCredentials());
  }

  protected AmazonS3Client getAmazonS3Client() throws Exception {
    return s3Client;
  }

  private void releaseAmazonS3Client(AmazonS3Client client) {

  }

  @Override
  public void backupBlock(ExtendedBlock extendedBlock, LengthInputStream data, LengthInputStream metaData)
      throws Exception {
    upload(metaData, getMetaDataKey(extendedBlock), extendedBlock);
    upload(data, getDataKey(extendedBlock), extendedBlock);
  }

  protected void upload(LengthInputStream input, String key, ExtendedBlock extendedBlock) throws Exception {
    ObjectMetadata objectMetaData = new ObjectMetadata();
    objectMetaData.setUserMetadata(toUserMetadata(extendedBlock));
    objectMetaData.setContentLength(input.getLength());
    AmazonS3Client client = getAmazonS3Client();
    try {
      if (client.doesObjectExist(bucketName, key)) {
        // Remove an already existing object and upload
        client.deleteObject(bucketName, key);
      }
      client.putObject(new PutObjectRequest(bucketName, key, input, objectMetaData));
    } finally {
      releaseAmazonS3Client(client);
    }
  }

  protected Map<String, String> toUserMetadata(ExtendedBlock extendedBlock) {
    Builder<String, String> builder = ImmutableMap.builder();
    builder.put(BLOCK_NAME, extendedBlock.getBlockName());
    builder.put(BLOCK_POOL_ID, extendedBlock.getBlockPoolId());
    builder.put(BLOCK_ID, Long.toString(extendedBlock.getBlockId()));
    builder.put(GEN_STAMP, Long.toString(extendedBlock.getGenerationStamp()));
    builder.put(NUM_BYTES, Long.toString(extendedBlock.getNumBytes()));
    return builder.build();
  }

  protected String getMetaDataKey(ExtendedBlock extendedBlock) {
    return getBaseKey(extendedBlock) + META;
  }

  protected String getDataKey(ExtendedBlock extendedBlock) {
    return getBaseKey(extendedBlock) + DATA;
  }

  protected String getBaseKey(ExtendedBlock extendedBlock) {
    if (objectPrefix == null) {
      return JOINER.join(extendedBlock.getBlockPoolId(),
          extendedBlock.getBlockId() + "." + extendedBlock.getGenerationStamp());
    } else {
      return JOINER.join(objectPrefix, extendedBlock.getBlockPoolId(),
          extendedBlock.getBlockId() + "." + extendedBlock.getGenerationStamp());
    }
  }

  @Override
  public boolean hasBlock(ExtendedBlock extendedBlock) throws Exception {
    AmazonS3Client client = getAmazonS3Client();
    try {
      if (client.doesObjectExist(bucketName, getMetaDataKey(extendedBlock))) {
        // Check content length of metadata with number of bytes in data???
        if (client.doesObjectExist(bucketName, getDataKey(extendedBlock))) {
          ObjectMetadata objectMetadata = client.getObjectMetadata(bucketName, getDataKey(extendedBlock));
          long contentLength = objectMetadata.getContentLength();
          return extendedBlock.getNumBytes() == contentLength;
        }
      }
      return false;
    } finally {
      releaseAmazonS3Client(client);
    }
  }

  @Override
  public InputStream getMetaDataInputStream(ExtendedBlock extendedBlock) throws Exception {
    String metaDataKey = getMetaDataKey(extendedBlock);
    AmazonS3Client client = getAmazonS3Client();
    try {
      S3Object s3Object = client.getObject(bucketName, metaDataKey);
      return s3Object.getObjectContent();
    } finally {
      releaseAmazonS3Client(client);
    }
  }

  @Override
  public InputStream getDataInputStream(ExtendedBlock extendedBlock) throws Exception {
    String dataKey = getDataKey(extendedBlock);
    AmazonS3Client client = getAmazonS3Client();
    try {
      S3Object s3Object = client.getObject(bucketName, dataKey);
      return s3Object.getObjectContent();
    } finally {
      releaseAmazonS3Client(client);
    }
  }

}
