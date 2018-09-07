# S3 Backup Configuration

## Basic Setup

Required for every backup store:
```
<property>
  <name>dfs.datanode.fsdataset.factory</name>
  <value>backup.datanode.BackupFsDatasetSpiFactory</value>
</property>
<property>
  <name>dfs.datanode.plugins</name>
  <value>backup.datanode.DataNodeBackupServicePlugin</value>
</property>
<property>
  <name>dfs.namenode.plugins</name>
  <value>backup.namenode.NameNodeBackupServicePlugin</value>
</property>
```

## S3 Required Setup

Required for s3 backup store:

NOTE: Assumes that bucket has already been created with proper policies.

```
<property>
  <name>dfs.backup.store.key</name>
  <value>backup.store.s3.S3BackupStore</value>
</property>
<property>
  <name>dfs.backup.s3.bucket.name</name>
  <value>BUCKET_NAME</value>
</property>
```

## Optional Properties

### Object Prefix

If the prefix is not provided the keys created will be of the format:
```
<block pool id>/<block id>/<generation stamp>/<block length>
```

If the prefix is provided the keys created will be of the format:
```
<prefix>/<block pool id>/<block id>/<generation stamp>/<block length>
```

Example:
```
<property>
  <name>dfs.backup.s3.object.prefix</name>
  <value>hdfs-backup</value>
</property>
```

### Credentials Provider Factory

The backup.store.s3.DefaultS3AWSCredentialsProviderFactory is used by default as the credentials provider.  The DefaultS3AWSCredentialsProviderFactory uses the AWS com.amazonaws.auth.DefaultAWSCredentialsProviderChain internally to determine the credentials.  

This provider uses the following providers in order:

* EnvironmentVariableCredentialsProvider
* SystemPropertiesCredentialsProvider
* ProfileCredentialsProvider
* EC2ContainerCredentialsProviderWrapper

Example:
```
<property>
  <name>dfs.backup.s3.credentials.provider.factory</name>
  <value>backup.store.s3.DefaultS3AWSCredentialsProviderFactory</value>
</property>
```

### S3 Listing Maximum Keys

Maximum number of keys to fetch per iteration while walking the blocks stored
in S3.

Example:
```
<property>
  <name>dfs.backup.s3.listing.maxkeys</name>
  <value>10000</value>
</property>

```
