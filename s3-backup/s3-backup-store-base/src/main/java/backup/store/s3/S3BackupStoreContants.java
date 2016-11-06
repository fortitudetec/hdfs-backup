package backup.store.s3;

public class S3BackupStoreContants {
  public static final String DFS_BACKUP_S3_BUCKET_NAME_KEY = "dfs.backup.s3.bucket.name";
  public static final String DFS_BACKUP_S3_OBJECT_PREFIX_KEY = "dfs.backup.s3.object.prefix";
  public static final String DFS_BACKUP_S3_LISTING_MAXKEYS_KEY = "dfs.backup.s3.listing.maxkeys";
  public static final String DFS_BACKUP_S3_ENDPOINT = "dfs.backup.s3.endpoint";
  public static final int DFS_BACKUP_S3_LISTING_MAXKEYS_DEFAULT = 10000;
  public static final String DFS_BACKUP_S3_CREDENTIALS_PROVIDER_FACTORY_KEY = "dfs.backup.s3.credentials.provider.factory";
  public static final String DFS_BACKUP_S3_CREDENTIALS_PROVIDER_FACTORY_DEFAULT = "backup.store.s3.DefaultS3AWSCredentialsProviderFactory";
}
