package backup.store.s3;

import static backup.store.s3.S3BackupStoreContants.DFS_BACKUP_S3_BUCKET_NAME_KEY;
import static backup.store.s3.S3BackupStoreContants.DFS_BACKUP_S3_CREDENTIALS_PROVIDER_FACTORY_DEFAULT;
import static backup.store.s3.S3BackupStoreContants.DFS_BACKUP_S3_CREDENTIALS_PROVIDER_FACTORY_KEY;
import static backup.store.s3.S3BackupStoreContants.DFS_BACKUP_S3_ENDPOINT;
import static backup.store.s3.S3BackupStoreContants.DFS_BACKUP_S3_LISTING_MAXKEYS_DEFAULT;
import static backup.store.s3.S3BackupStoreContants.DFS_BACKUP_S3_LISTING_MAXKEYS_KEY;
import static backup.store.s3.S3BackupStoreContants.DFS_BACKUP_S3_OBJECT_PREFIX_KEY;

import java.io.InputStream;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.configuration.Configuration;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;

import backup.store.BackupStore;
import backup.store.ExtendedBlock;
import backup.store.ExtendedBlockEnum;
import backup.store.LengthInputStream;
import backup.store.ReflectionUtils;

public class S3BackupStore extends BackupStore {

  enum FileType {
    meta, data
  }

  private static final String NUM_BYTES = "numBytes";
  private static final String GEN_STAMP = "genStamp";
  private static final String BLOCK_ID = "blockId";
  private static final String BLOCK_POOL_ID = "blockPoolId";
  private static final Joiner JOINER = Joiner.on('/');
  private static final Splitter SPLITTER = Splitter.on('/');

  private String bucketName;
  private String objectPrefix;
  private S3AWSCredentialsProviderFactory credentialsProviderFactory;
  private AmazonS3Client s3Client;
  private int maxKeys;

  @Override
  public void init() throws Exception {
    Configuration conf = getConf();

    String classname = conf.getString(DFS_BACKUP_S3_CREDENTIALS_PROVIDER_FACTORY_KEY,
        DFS_BACKUP_S3_CREDENTIALS_PROVIDER_FACTORY_DEFAULT);

    Class<? extends S3AWSCredentialsProviderFactory> clazz = getCredentialsProviderFactory(classname);

    credentialsProviderFactory = ReflectionUtils.newInstance(clazz, conf);
    bucketName = conf.getString(DFS_BACKUP_S3_BUCKET_NAME_KEY);
    objectPrefix = conf.getString(DFS_BACKUP_S3_OBJECT_PREFIX_KEY);
    maxKeys = conf.getInt(DFS_BACKUP_S3_LISTING_MAXKEYS_KEY, DFS_BACKUP_S3_LISTING_MAXKEYS_DEFAULT);
    s3Client = new AmazonS3Client(credentialsProviderFactory.getCredentials());
    s3Client.setEndpoint(conf.getString(DFS_BACKUP_S3_ENDPOINT, s3Client.getEndpointPrefix()));
  }

  @SuppressWarnings("unchecked")
  private Class<? extends S3AWSCredentialsProviderFactory> getCredentialsProviderFactory(String classname)
      throws ClassNotFoundException {
    return (Class<? extends S3AWSCredentialsProviderFactory>) getClass().getClassLoader().loadClass(classname);
  }

  protected AmazonS3Client getAmazonS3Client() throws Exception {
    return s3Client;
  }

  private void releaseAmazonS3Client(AmazonS3Client client) {

  }

  @Override
  public ExtendedBlockEnum<Void> getExtendedBlocks() throws Exception {
    AmazonS3Client client = getAmazonS3Client();
    try {

      return new S3ExtendedBlockEnum(client, bucketName, getExtendedBlocksPrefix());
    } finally {
      releaseAmazonS3Client(client);
    }
  }

  class S3ExtendedBlockEnum implements ExtendedBlockEnum<Void> {

    private final AmazonS3Client client;
    private final String bucketName;
    private final String extendedBlocksPrefix;

    private ExtendedBlock current;
    private ListObjectsV2Result listObjectsV2;
    private Iterator<S3ObjectSummary> iterator;
    private String startAfter;

    public S3ExtendedBlockEnum(AmazonS3Client client, String bucketName, String extendedBlocksPrefix) {
      this.client = client;
      this.bucketName = bucketName;
      this.extendedBlocksPrefix = extendedBlocksPrefix;
    }

    private ListObjectsV2Request createObjectRequest(String bucketName, String extendedBlocksPrefix,
        String startAfter) {
      return new ListObjectsV2Request().withBucketName(bucketName).withPrefix(extendedBlocksPrefix)
          .withStartAfter(startAfter).withMaxKeys(maxKeys);
    }

    @Override
    public ExtendedBlock next() throws Exception {
      if (iterator == null) {
        nextListing();
      }
      while (true) {
        if (iterator.hasNext()) {
          S3ObjectSummary summary = iterator.next();
          startAfter = summary.getKey();
          return current = getExtendedBlockFromKey(FileType.meta, objectPrefix, summary.getKey());
        } else if (listObjectsV2.isTruncated()) {
          nextListing();
        } else {
          return current = null;
        }
      }
    }

    private void nextListing() {
      ListObjectsV2Request request = createObjectRequest(bucketName, extendedBlocksPrefix, startAfter);
      listObjectsV2 = client.listObjectsV2(request);
      iterator = listObjectsV2.getObjectSummaries().iterator();
    }

    @Override
    public ExtendedBlock current() {
      return current;
    }
  }

  @Override
  public void deleteBlock(ExtendedBlock extendedBlock) throws Exception {
    String metaDataKey = getMetaDataKey(extendedBlock);
    String dataKey = getDataKey(extendedBlock);
    AmazonS3Client client = getAmazonS3Client();
    try {
      client.deleteObject(bucketName, metaDataKey);
      client.deleteObject(bucketName, dataKey);
    } finally {
      releaseAmazonS3Client(client);
    }
  }

  @Override
  public void backupBlock(ExtendedBlock extendedBlock, LengthInputStream data, LengthInputStream metaData)
      throws Exception {
    upload(metaData, getMetaDataKey(extendedBlock), extendedBlock);
    upload(data, getDataKey(extendedBlock), extendedBlock);
  }

  @Override
  public boolean hasBlock(ExtendedBlock extendedBlock) throws Exception {
    String metaDataKey = getMetaDataKey(extendedBlock);
    String dataKey = getDataKey(extendedBlock);
    AmazonS3Client client = getAmazonS3Client();
    try {
      if (client.doesObjectExist(bucketName, metaDataKey)) {
        // Check content length of metadata with number of bytes in data???
        if (client.doesObjectExist(bucketName, dataKey)) {
          ObjectMetadata objectMetadata = client.getObjectMetadata(bucketName, dataKey);
          long contentLength = objectMetadata.getContentLength();
          return extendedBlock.getLength() == contentLength;
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
    builder.put(BLOCK_POOL_ID, extendedBlock.getPoolId());
    builder.put(BLOCK_ID, Long.toString(extendedBlock.getBlockId()));
    builder.put(GEN_STAMP, Long.toString(extendedBlock.getGenerationStamp()));
    builder.put(NUM_BYTES, Long.toString(extendedBlock.getLength()));
    return builder.build();
  }

  protected String getMetaDataKey(ExtendedBlock extendedBlock) {
    return getBaseKey(extendedBlock, FileType.meta, objectPrefix);
  }

  protected String getDataKey(ExtendedBlock extendedBlock) {
    return getBaseKey(extendedBlock, FileType.data, objectPrefix);
  }

  public static String getBaseKey(ExtendedBlock extendedBlock, FileType fileType, String objectPrefix) {
    if (objectPrefix == null) {
      return JOINER.join(fileType.name(), extendedBlock.getPoolId(), extendedBlock.getBlockId(),
          extendedBlock.getGenerationStamp(), extendedBlock.getLength());
    } else {
      return JOINER.join(objectPrefix, fileType.name(), extendedBlock.getPoolId(), extendedBlock.getBlockId(),
          extendedBlock.getGenerationStamp(), extendedBlock.getLength());
    }
  }

  public static ExtendedBlock getExtendedBlockFromKey(FileType fileType, String objectPrefix, String key) {
    Iterable<String> iterable = SPLITTER.split(key);
    Iterator<String> iterator = iterable.iterator();
    if (objectPrefix != null) {
      iterator.next();// objectPrefix
    }
    iterator.next();// filetype
    String poolId = iterator.next();// poolid
    long blockId = Long.parseLong(iterator.next());// blockid
    long genstamp = Long.parseLong(iterator.next());// genstamp
    long len = Long.parseLong(iterator.next());// numbytes
    return new ExtendedBlock(poolId, blockId, len, genstamp);
  }

  private String getExtendedBlocksPrefix() {
    if (objectPrefix == null) {
      return FileType.meta.name();
    } else {
      return JOINER.join(objectPrefix, FileType.meta.name());
    }
  }

}