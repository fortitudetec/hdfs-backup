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

public class S3BackupStoreContants {
  public static final String DFS_BACKUP_S3_BUCKET_NAME_KEY = "dfs.backup.s3.bucket.name";
  public static final String DFS_BACKUP_S3_OBJECT_PREFIX_KEY = "dfs.backup.s3.object.prefix";
  public static final String DFS_BACKUP_S3_LISTING_MAXKEYS_KEY = "dfs.backup.s3.listing.maxkeys";
  public static final String DFS_BACKUP_S3_ENDPOINT_KEY = "dfs.backup.s3.endpoint";
  public static final String DFS_BACKUP_S3_REGION_KEY = "dfs.backup.s3.region";
  public static final int DFS_BACKUP_S3_LISTING_MAXKEYS_DEFAULT = 10000;
  public static final String DFS_BACKUP_S3_CREDENTIALS_PROVIDER_FACTORY_KEY = "dfs.backup.s3.credentials.provider.factory";
  public static final String DFS_BACKUP_S3_CREDENTIALS_PROVIDER_FACTORY_DEFAULT = "backup.store.s3.DefaultS3AWSCredentialsProviderFactory";
}
