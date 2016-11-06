# Local Backup Setup

NOTE: This project is likely only to be used for testing.  Though it should work for a HDFS cluster that has the same network mount point (or something similar) on each machine.

## Install

Execute:
```
mvn clean install
```

This will execute all tests and build all of the binaries.

### CDH Parcel

Execute:
```
cd local-backup-store
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

<!-- Required Local Backup Store Properties -->

<property>
  <name>dfs.backup.store.key</name>
  <value>backup.store.local.LocalBackupStore</value>
</property>
<property>
  <name>dfs.backup.localbackupstore.path</name>
  <value>/opt/backup</value>
</property>

```
