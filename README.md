# HDFS Backup

This project actively backs up the blocks created in the HDFS cluster to an external source. The primary use case for this feature is to utilize the ephemeral storage on EC2 instances for HDFS and during a disaster recovery situation the missing blocks could be pulled from an external data source (S3).  This will still require the NameNode to have EBS backed storage.

The primary reasoning for using HDFS in this way over a straight S3 access is that with HDFS you have certain file system guarantees (atomic renames, snapshots, etc).  HDFS can also have lower latency when it comes to read performance.

In the projects current state the blocks are replicated from the DataNodes to the backup store and if the NameNode detects a missing block it will request that one of the DataNodes restore the block from the backup store.  After the block has been finalized on the DataNode the NameNode is contacted with the updated block information.

Also the NameNode will run block reports to ensure that all blocks are replicated to the backup store.  During this process the NameNode will delete blocks from the backup store that BlockManager no longer references.

## Backup Install

- S3 backup install and setup. See [S3 README](s3-backup/README.md).
- Local backup install and setup. See [Local README](local-backup/README.md).

This will execute all tests and build all of the binaries.

## Basic Configure

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
  <value><backup store class></value>
</property>
```
