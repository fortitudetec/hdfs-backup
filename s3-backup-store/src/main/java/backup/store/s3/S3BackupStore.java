package backup.store.s3;

import java.io.InputStream;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.LengthInputStream;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
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
  public static final String DFS_BACKUP_S3_CREDENTIALS_PROVIDER = "dfs.backup.s3.credentials.provider";

  private static final String DATA = ".data";
  private static final String META = ".meta";
  private static final String NUM_BYTES = "numBytes";
  private static final String GEN_STAMP = "genStamp";
  private static final String BLOCK_ID = "blockId";
  private static final String BLOCK_POOL_ID = "blockPoolId";
  private static final String BLOCK_NAME = "blockName";

  private static final Joiner JOINER = Joiner.on('/');

  private AmazonS3Client s3client;
  private String bucketName;
  private String objectPrefix;

  @Override
  public void init() throws Exception {
    Configuration conf = getConf();
    Class<? extends AWSCredentialsProvider> clazz = (Class<? extends AWSCredentialsProvider>) conf
        .getClass(DFS_BACKUP_S3_CREDENTIALS_PROVIDER, DefaultAWSCredentialsProviderChain.class);
    s3client = new AmazonS3Client(clazz.newInstance());
    bucketName = conf.get(DFS_BACKUP_S3_BUCKET_NAME);
    objectPrefix = conf.get(DFS_BACKUP_S3_OBJECT_PREFIX);
  }

  @Override
  public void backupBlock(ExtendedBlock extendedBlock, LengthInputStream data, LengthInputStream metaData)
      throws Exception {
    upload(metaData, getMetaDataKey(extendedBlock), extendedBlock);
    upload(data, getDataKey(extendedBlock), extendedBlock);
  }

  private void upload(LengthInputStream input, String key, ExtendedBlock extendedBlock) {
    ObjectMetadata objectMetaData = new ObjectMetadata();
    objectMetaData.setUserMetadata(toUserMetadata(extendedBlock));
    objectMetaData.setContentLength(input.getLength());
    if (s3client.doesObjectExist(bucketName, key)) {
      // Remove an already existing object and upload
      s3client.deleteObject(bucketName, key);
    }
    s3client.putObject(new PutObjectRequest(bucketName, key, input, objectMetaData));
  }

  private Map<String, String> toUserMetadata(ExtendedBlock extendedBlock) {
    Builder<String, String> builder = ImmutableMap.builder();
    builder.put(BLOCK_NAME, extendedBlock.getBlockName());
    builder.put(BLOCK_POOL_ID, extendedBlock.getBlockPoolId());
    builder.put(BLOCK_ID, Long.toString(extendedBlock.getBlockId()));
    builder.put(GEN_STAMP, Long.toString(extendedBlock.getGenerationStamp()));
    builder.put(NUM_BYTES, Long.toString(extendedBlock.getNumBytes()));
    return builder.build();
  }

  private String getMetaDataKey(ExtendedBlock extendedBlock) {
    return getBaseKey(extendedBlock) + META;
  }

  private String getDataKey(ExtendedBlock extendedBlock) {
    return getBaseKey(extendedBlock) + DATA;
  }

  private String getBaseKey(ExtendedBlock extendedBlock) {
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
    if (s3client.doesObjectExist(bucketName, getMetaDataKey(extendedBlock))) {
      // Check content length of metadata with number of bytes in data???
      if (s3client.doesObjectExist(bucketName, getDataKey(extendedBlock))) {
        ObjectMetadata objectMetadata = s3client.getObjectMetadata(bucketName, getDataKey(extendedBlock));
        long contentLength = objectMetadata.getContentLength();
        return extendedBlock.getNumBytes() == contentLength;
      }
    }
    return false;
  }

  @Override
  public InputStream getMetaDataInputStream(ExtendedBlock extendedBlock) throws Exception {
    String metaDataKey = getMetaDataKey(extendedBlock);
    S3Object s3Object = s3client.getObject(bucketName, metaDataKey);
    return s3Object.getObjectContent();
  }

  @Override
  public InputStream getDataInputStream(ExtendedBlock extendedBlock) throws Exception {
    String dataKey = getDataKey(extendedBlock);
    S3Object s3Object = s3client.getObject(bucketName, dataKey);
    return s3Object.getObjectContent();
  }
}
