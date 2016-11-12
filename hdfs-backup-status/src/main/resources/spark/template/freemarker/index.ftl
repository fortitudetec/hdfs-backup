<html>
<head>
<title>Backup Status</title>
<meta charset="utf-8">

<link rel="stylesheet" href="css/bootstrap.min.css">
<link rel="stylesheet" href="css/bootstrap-theme.min.css">
<script src="js/bootstrap.min.js"></script>

<style>
.table-nonfluid {
   width: auto !important;
}
</style>

</head>

<body>
<h1>Backup Status</h1>
<table class="table table-nonfluid">
<tr><th>Type</th><th>Counts</th></tr>
<tr><th>Backup MB/s</th><td>${backupBytesPerSecond}</td></tr>
<tr><th>Backup in Progress</th><td>${backupsInProgressCount}</td></tr>
<tr><th>Backup Finalized Blocks Size</th><td>${finalizedBlocksSizeCount}</td></tr>
<tr><th>Backup Future Checks Size</th><td>${futureChecksSizeCount}</td></tr>
<tr><th>Restore Blocks</th><td>${restoreBlocks}</td></tr>
<tr><th>Restore MB/s</th><td>${restoreBytesPerSecond}</td></tr>
<tr><th>Restore in Progress</th><td>${restoresInProgressCount}</td></tr>
</table>
</body>
</html>
