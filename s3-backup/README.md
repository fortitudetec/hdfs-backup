# S3 Backup Setup

## Install

Execute:
```
mvn clean install
```

This will execute all tests and build all of the binaries.

### CDH Parcel

Execute:
```
cd s3-backup-store
./run_parcel_server.sh
```

In Cloudera Manager add your computer as a parcel server (e.g. http://hostname:8000/).

Now you will need to add the parcel to all the nodes running NameNode and DataNode processes.

## Configure

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

<!-- Required S3 Backup Store Properties -->

<property>
  <name>dfs.backup.store.key</name>
  <value>backup.store.s3.S3BackupStore</value>
</property>
<property>
  <name>dfs.backup.s3.bucket.name</name>
  <value>BUCKET_NAME</value>
</property>

<!-- Optional Properties -->
<!--

If the prefix is not provided the keys generated will be of the format:
  <block pool id>/<block id>/<generation stamp>/<block length>

If the prefix is provided the keys generated will be of the format:
  <prefix>/<block pool id>/<block id>/<generation stamp>/<block length>
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

<!--
Maximum number of keys to fetch per iteration while walking the blocks stored
in S3
-->
<property>
  <name>dfs.backup.s3.listing.maxkeys</name>
  <value>10000</value>
</property>

```
