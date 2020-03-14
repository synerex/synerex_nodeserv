# power shell script

$sha1 = (git rev-parse HEAD).Trim()
$gitver = (git describe --tag).Trim()
$now = Get-Date -UFormat "%Y-%m-%d_%T"
echo "Building go binary with GitInfo $gitver $now $sha1"
go build -ldflags "-X main.sha1ver=$sha1 -X main.buildTime=$now -X main.gitver=$gitver"
