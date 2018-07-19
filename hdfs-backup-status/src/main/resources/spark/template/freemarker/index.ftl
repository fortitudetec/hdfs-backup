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
<tr><th>Restore Blocks</th><td>${restoreBlocks}</td></tr>
<tr><th>Restore MB/s</th><td>${restoreBytesPerSecond}</td></tr>
<tr><th>Restore in Progress</th><td>${restoresInProgressCount}</td></tr>
</table>

<h1>Backup Reports</h1>
<table class="table table-nonfluid">
<#list reportIds as reportId>
<tr><td><a href="report/${reportId}">${reportId}</a></td></tr>
</#list>
</table>

<input type="button" onclick="location.href='runreport';" value="Run Report" />
<input type="button" onclick="location.href='runreport-details';" value="Run Report (w/Details)" />
</body>
</html>
