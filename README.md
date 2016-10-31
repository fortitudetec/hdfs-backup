# HDFS Backup

This project actively backs up the blocks created in the HDFS cluster to an external source. The primary use case for this feature is to utilize the ephemeral storage on EC2 instances for HDFS and during a disaster recovery situation the missing blocks could be pulled from an external data source (S3).  This will still require the NameNode to have EBS backed storage.

The primary reasoning for using HDFS in this way over a straight S3 access is that with HDFS you have certain file system guarantees (atomic renames, snapshots, etc).

In the projects current state the blocks are replicated from the DataNodes to the backup store and if the NameNode detects a missing block it will request that one of the DataNodes restore the block from the backup store.  After the block has been finalized on the DataNode the NameNode is contacted with the updated block information.

## S3 Backup Setup

### Install

Execute:
```
mvn clean install
```

This will execute all tests and build all of the binaries.

#### CDH Parcel

Execute:
```
cd s3-backup-store-parcel
./run_parcel_server.sh
```

In Cloudera Manager add your computer as a parcel server (e.g. http://hostname:8000/).

Now you will need to add the parcel to all the nodes running NameNode and DataNode processes.

### Configure

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
<property>
  <name>dfs.backup.zookeeper.connection</name>
  <value><zookeeper connection>/backup</value>
</property>
<property>
  <name>dfs.backup.store.key</name>
  <value>backup.store.s3.S3BackupStore</value>
</property>
<property>
  <name>dfs.backup.s3.bucket.name</name>
  <value>BUCKET_NAME</value>
</property>

<!-- Optional -->
<!--

If the prefix is not provided the keys generated will be of the format:
  <block pool id>/<block id>.<generation stamp>

If the prefix is provided the keys generated will be of the format:
  <prefix>/<block pool id>/<block id>.<generation stamp>
-->

<property>
  <name>dfs.backup.s3.object.prefix</name>
  <value>hdfs-backup</value>
</property>

<!--

The backup.store.s3.DefaultS3AWSCredentialsProviderFactory is used by default
as the credentials provider.  The DefaultS3AWSCredentialsProviderFactory uses
the AWS com.amazonaws.auth.DefaultAWSCredentialsProviderChain internally to
determine the credentials.  

This provider uses the following providers in order:

* EnvironmentVariableCredentialsProvider
* SystemPropertiesCredentialsProvider
* ProfileCredentialsProvider
* EC2ContainerCredentialsProviderWrapper
-->
<property>
  <name>dfs.backup.s3.credentials.provider.factory</name>
  <value>backup.store.s3.DefaultS3AWSCredentialsProviderFactory</value>
</property>
```
