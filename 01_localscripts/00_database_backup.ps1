##########Global parameters
#Binary locations
$pgdump_exe = 'C:/Program Files/PostgreSQL/14/bin/pg_dump.exe'
$pgrestore_exe = 'C:/Program Files/PostgreSQL/14/bin/pg_restore.exe'
$psql_exe = 'C:/Program Files/PostgreSQL/14/bin/psql.exe'
$createdb_exe = 'C:/Program Files/PostgreSQL/14/bin/createdb.exe'

#Folder to host backups
$folderpath = $env:MARSBACKUPDIR

#Date string to be used in dump filenames and archivetest restores
$datestring = $(Get-Date -UFormat '%Y%m%dT%H%M')

#Don't prompt for a password
  #Passwords can not be supplied to pg_dump/restore in shell commands. 
  #They are hosted in the PGPASSFILE, located at the path stored in the environment variable of the same name.
  #See the admin docs for more details
$arg_pass = "--no-password"

##########Log Parameters
#Log hash
  #From here https://gist.github.com/dalton-cole/4b9b77db108c554999eb
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
# 
#Compression level (--compress)
  $compress = 6
  $arg_compress = "--compress=" + $compress

#Filename
  #File extension for dumps
  $extension = '.pgdump'

  #Dump's resultant filename
  $filename = $datestring + '_' + $dbname + $extension

  #Dump's full filepath (--file)
  $filepath = $folderpath + $filename
  $arg_file = '--file='+ $filepath

###########pg_restore arguments
 $testdbname = $filename
 $arg_template = '-Ttemplate0'
 $arg_testdb = '-d' + $filename

#Backup the DB
Write-Host $psql_exe $arg_host $arg_port $arg_user $arg_pass $arg_dbname "-c insert into $scripttable (date, milestone, exit_code, note, hash) VALUES ('$date', 1, NULL, 'Initiating DB Backup', '$loghash');"
& $psql_exe $arg_host $arg_port $arg_user $arg_pass $arg_dbname "-c insert into $scripttable (date, milestone, exit_code, note, hash) VALUES ('$date', 1, NULL, 'Initiating DB Backup', '$loghash');"

Write-Host $pgdump_exe "$arg_file" $arg_host $arg_port $arg_user $arg_pass $arg_role $arg_format $arg_dbname $arg_compress
& $pgdump_exe "$arg_file" $arg_host $arg_port $arg_user $arg_pass $arg_role $arg_format $arg_dbname $arg_compress

Write-Host "The time is $time and the hash is $loghash"

#Create the test db
Write-Host $psql_exe $arg_host $arg_port $arg_user $arg_pass $arg_dbname "-c insert into $scripttable (date, milestone, exit_code, note, hash) VALUES ('$date', 2, NULL, 'Creating test DB', '$loghash');"
& $psql_exe $arg_host $arg_port $arg_user $arg_pass $arg_dbname "-c insert into $scripttable (date, milestone, exit_code, note, hash) VALUES ('$date', 2, NULL, 'Creating test DB', '$loghash');"

Write-Host $createdb_exe $arg_host $arg_port $arg_user $arg_template $arg_testdb
& $createdb_exe $arg_user $arg_host $arg_port $arg_template $testdbname

#Restore the DB into the test db
Write-Host $psql_exe $arg_host $arg_port $arg_user $arg_pass $arg_dbname "-c insert into $scripttable (date, milestone, exit_code, note, hash) VALUES ('$date', 3, NULL, 'Restoring test DB', '$loghash');"
& $psql_exe $arg_host $arg_port $arg_user $arg_pass $arg_dbname "-c insert into $scripttable (date, milestone, exit_code, note, hash) VALUES ('$date', 2, NULL, 'Restoring test DB', '$loghash');"

Write-Host $pgrestore_exe $arg_host $arg_port $arg_user $arg_testdb "$filepath"
& $pgrestore_exe $arg_host $arg_port $arg_user $arg_testdb "$filepath"

Write-Host $psql_exe $arg_host $arg_port $arg_user $arg_pass $arg_dbname "-c insert into $scripttable (date, milestone, exit_code, note, hash) VALUES ('$date', 0, 1, 'Execution Successful', '$loghash');"
& $psql_exe $arg_host $arg_port $arg_user $arg_pass $arg_dbname "-c insert into $scripttable (date, milestone, exit_code, note, hash) VALUES ('$date', 0, 1, 'Execution Successful', '$loghash');"