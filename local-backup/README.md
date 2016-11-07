# Local Backup Configuration

NOTE: This project is likely only to be used for testing.  Though it should work for a HDFS cluster that has the same network mount point (or something similar) on each machine.

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
<property>
  <name>dfs.backup.zookeeper.connection</name>
  <value><zookeeper connection>/backup</value>
</property>
```

## Local Required Setup

Required for s3 backup store:

NOTE: Assumes that bucket has already been created with proper policies.

```
<property>
  <name>dfs.backup.store.key</name>
  <value>backup.store.local.LocalBackupStore</value>
</property>
<property>
  <name>dfs.backup.localbackupstore.path</name>
  <value>/opt/backup</value>
</property>
```
