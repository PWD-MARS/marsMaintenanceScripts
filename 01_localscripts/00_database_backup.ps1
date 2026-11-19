##########Global parameters
#Binary locations
$pgdump_exe = 'C:/Program Files/PostgreSQL/14/bin/pg_dump.exe'
$pgrestore_exe = 'C:/Program Files/PostgreSQL/14/bin/pg_restore.exe'
$psql_exe = 'C:/Program Files/PostgreSQL/14/bin/psql.exe'
$createdb_exe = 'C:/Program Files/PostgreSQL/14/bin/createdb.exe'

#Folder to host backups
$folderpath = $env:MARSBACKUPDIR

#Date string to be used in dump filenames and test restores
$datestring = $(Get-Date).ToString('yyyyMMdd')

#Don't prompt for a password
  #Passwords can not be supplied to pg_dump/restore in shell commands.
  #They are hosted in the PGPASSFILE, located at the path stored in the environment variable of the same name.
  #See the admin docs for more details
$arg_pass = "--no-password"

##########Retention parameters
#Set to $true to log what WOULD be deleted without deleting anything.
#KEEP $true UNTIL THE DRY-RUN OUTPUT HAS BEEN REVIEWED ON THE REAL BACKUP FOLDER.
$pruneDryRun = $false
#If a past quarter has no backup from its final day (e.g. a missed run),
#$false = delete everything from that quarter (strict reading of the policy)
#$true  = keep the latest backup available in that quarter instead
$pruneFallbackToLatest = $true

##########Transcript (captures console output of unattended runs)
$transcriptDir = $PSScriptRoot
if (-not $transcriptDir) { $transcriptDir = (Get-Location).Path }
$transcriptPath = Join-Path $transcriptDir ("backup_transcript_" + $datestring + ".log")
try { Start-Transcript -Path $transcriptPath -Append | Out-Null } catch { Write-Host "Could not start transcript: $_" }
#Delete transcripts older than 30 days
Get-ChildItem -LiteralPath $transcriptDir -Filter 'backup_transcript_*.log' -File -ErrorAction SilentlyContinue |
    Where-Object { $_.LastWriteTime -lt (Get-Date).AddDays(-30) } |
    Remove-Item -Force -ErrorAction SilentlyContinue

##########Log Parameters
#Log hash (from https://gist.github.com/dalton-cole/4b9b77db108c554999eb)
$time = $((Get-Date).ToString())
$date = $((Get-Date -Format "yyyy-MM-dd").ToString())
$md5 = new-object -TypeName System.Security.Cryptography.MD5CryptoServiceProvider
$utf8 = new-object -TypeName System.Text.UTF8Encoding
$rawhash = [System.BitConverter]::ToString($md5.ComputeHash($utf8.GetBytes($time)))
$loghash = $rawhash.ToLower() -replace '-', ''

#Log table parameters
$scripttable = 'log.tbl_script_backup'
$writetable = 'log.tbl_writes_backup'

##########Server parameters
#Database host (--host)
$hostname = 'PWDMARSDBS1'
$arg_host = '--host=' + $hostname

#Database server port (--port)
$port = '5434'
$arg_port = '--port=' + $port

#Database to back up (--dbname)
$dbname = 'mars_prod'
$arg_dbname = '--dbname=' + $dbname

#Backup role (--username and --role)
$userrole = 'mars_admin'
$arg_user = '--username=' + $userrole
$arg_role = '--role=' + $userrole

##########pg_dump arguments
#Archive format (--format)
$format = "c" #Custom format
$arg_format = "--format=" + $format

#Compression level (--compress)
$compress = 6
$arg_compress = "--compress=" + $compress

#File extension and names
$extension = '.pgdump'
$fileprefix = $datestring + '_' + $dbname
$filename = $fileprefix + $extension

#Dump's full filepath (Join-Path handles a missing trailing slash)
$filepath = if ($folderpath) { Join-Path $folderpath $filename } else { $null }
$arg_file = '--file=' + $filepath

##########pg_restore arguments
$testdbname = 'z_backup_' + $fileprefix
$arg_template = '-Ttemplate0'
$arg_testdb = '-d' + $testdbname

##########Helper functions
function Write-Log($milestone, $exit, $note) {
    $exitVal = if ($null -eq $exit) { 'NULL' } else { $exit }
    $sql = "insert into $scripttable (date, milestone, exit_code, note, hash) VALUES ('$date', $milestone, $exitVal, '$note', '$loghash');"
    Write-Host "LOG: milestone=$milestone exit=$exitVal note=$note"
    & $psql_exe $arg_host $arg_port $arg_user $arg_pass $arg_dbname "-c $sql"
}

function Fail($milestone, $note, $code) {
    Write-Host "FAILED: $note (exit $code)"
    Write-Log $milestone 2 "$note failed"
    try { Stop-Transcript | Out-Null } catch {}
    exit 1
}

function Remove-OldBackups {
    param(
        [string]$Folder,
        [string]$Suffix,          # e.g. '_mars_prod.pgdump'
        [bool]$DryRun,
        [bool]$FallbackToLatest
    )

    $today = (Get-Date).Date
    $currentQStart = [datetime]::new($today.Year, 3 * [math]::Floor(($today.Month - 1) / 3) + 1, 1)

    #Only consider files named yyyyMMdd_<db>.pgdump
    $pattern = '^(\d{8})' + [regex]::Escape($Suffix) + '$'
    $files = foreach ($f in Get-ChildItem -LiteralPath $Folder -File) {
        if ($f.Name -match $pattern) {
            $d = [datetime]::MinValue
            if ([datetime]::TryParseExact($Matches[1], 'yyyyMMdd', $null, 'None', [ref]$d)) {
                $qStart = [datetime]::new($d.Year, 3 * [math]::Floor(($d.Month - 1) / 3) + 1, 1)
                [pscustomobject]@{
                    File   = $f
                    Date   = $d.Date
                    QStart = $qStart
                    QEnd   = $qStart.AddMonths(3).AddDays(-1)
                }
            }
        }
    }

    #Current quarter: keep everything. Previous quarters: keep only the final day.
    $past = $files | Where-Object { $_.QStart -lt $currentQStart }

    foreach ($grp in ($past | Group-Object QStart)) {
        $qEnd = $grp.Group[0].QEnd
        $keepDate = $qEnd
        if (-not ($grp.Group | Where-Object { $_.Date -eq $qEnd })) {
            if ($FallbackToLatest) {
                $keepDate = ($grp.Group | Measure-Object Date -Maximum).Maximum
                Write-Host "WARNING: no backup for $($qEnd.ToString('yyyy-MM-dd')); keeping latest in quarter ($($keepDate.ToString('yyyy-MM-dd')))"
            } else {
                Write-Host "WARNING: no backup for $($qEnd.ToString('yyyy-MM-dd')); no backup will remain for that quarter"
            }
        }

        foreach ($item in ($grp.Group | Where-Object { $_.Date -ne $keepDate })) {
            if ($DryRun) {
                Write-Host "DRY RUN: would delete $($item.File.FullName)"
            } else {
                Write-Host "Deleting $($item.File.FullName)"
                Remove-Item -LiteralPath $item.File.FullName -Force -ErrorAction Stop
            }
        }
    }
}

##########Startup checks (fail before touching anything)
Write-Host "Running as: $(whoami)  |  Folder: $folderpath  |  Dry run: $pruneDryRun"

if ([string]::IsNullOrWhiteSpace($folderpath)) {
    Write-Host "ABORT: MARSBACKUPDIR is not set for this account"
    try { Stop-Transcript | Out-Null } catch {}
    exit 1
}
if (-not (Test-Path -LiteralPath $folderpath)) {
    Write-Host "ABORT: backup folder not found or not reachable: $folderpath"
    try { Stop-Transcript | Out-Null } catch {}
    exit 1
}
if ([string]::IsNullOrWhiteSpace($env:PGPASSFILE) -and -not (Test-Path (Join-Path $env:APPDATA 'postgresql\pgpass.conf'))) {
    Write-Host "ABORT: no PGPASSFILE set and no default pgpass.conf for this account"
    try { Stop-Transcript | Out-Null } catch {}
    exit 1
}
#Confirm the account can write to the backup folder before the (long) dump starts
$probe = Join-Path $folderpath ("_writetest_" + $datestring + ".tmp")
try {
    "test" | Out-File -LiteralPath $probe -ErrorAction Stop
    Remove-Item -LiteralPath $probe -Force -ErrorAction SilentlyContinue
} catch {
    Write-Host "ABORT: cannot write to $folderpath : $_"
    try { Stop-Transcript | Out-Null } catch {}
    exit 1
}

##########Purge any old test restores (not just yesterday's, and including a same-day rerun)
$old = & $psql_exe $arg_host $arg_port $arg_user $arg_pass $arg_dbname -t -A `
    -c "select datname from pg_database where datname like 'z\_backup\_%';"
foreach ($db in $old) {
    if ($db) {
        Write-Host "Dropping old test DB $db"
        & $psql_exe $arg_host $arg_port $arg_user $arg_pass $arg_dbname "-c drop database if exists `"$db`" with (force);"
        if ($LASTEXITCODE -ne 0) { Fail 1 "drop test DB $db" $LASTEXITCODE }
    }
}

##########Dump
Write-Log 1 $null 'Initiating DB Backup'
& $pgdump_exe $arg_file $arg_host $arg_port $arg_user $arg_pass $arg_role $arg_format $arg_dbname $arg_compress
if ($LASTEXITCODE -ne 0) { Fail 1 'pg_dump' $LASTEXITCODE }
if (-not (Test-Path -LiteralPath $filepath)) { Fail 1 'pg_dump produced no file' 0 }

##########Create test DB
Write-Log 2 $null 'Creating test DB'
& $createdb_exe $arg_user $arg_host $arg_port $arg_pass $arg_template $testdbname
if ($LASTEXITCODE -ne 0) { Fail 2 'createdb' $LASTEXITCODE }

##########Restore into test DB
Write-Log 3 $null 'Restoring test DB'
& $pgrestore_exe $arg_host $arg_port $arg_user $arg_pass $arg_testdb $filepath
if ($LASTEXITCODE -ne 0) { Fail 3 'pg_restore' $LASTEXITCODE }

#Sanity check: restored DB actually contains tables
$n = & $psql_exe $arg_host $arg_port $arg_user $arg_pass "--dbname=$testdbname" -t -A `
    -c "select count(*) from information_schema.tables where table_schema not in ('pg_catalog','information_schema');"
if ($LASTEXITCODE -ne 0 -or [int]$n -lt 1) { Fail 3 'restore verification' $LASTEXITCODE }

##########Prune old backup files (only runs after a verified backup)
Write-Log 4 $null 'Pruning old backups'
try {
    Remove-OldBackups -Folder $folderpath -Suffix ("_" + $dbname + $extension) `
        -DryRun $pruneDryRun -FallbackToLatest $pruneFallbackToLatest
} catch {
    Write-Host "Prune error: $_"
    Fail 4 'backup pruning' 0
}

Write-Log 0 1 'Execution Successful'
try { Stop-Transcript | Out-Null } catch {}