# ChangeLog for the MySQL Enterprise Backup Wrapper Script 

## 1.2.0 (unreleased)

### Backwards Incompatible Changes
  - mysqlbackup 3.12.0 or greater is now required.
  - Restore will not continue if the restore directory is non-empty.
  - ib_buffer_pool is now restored to ib_buffer_pool instead of saved-ib_buffer_pool

### Improvements
  - Backup type is now printed
  - Added --force option to allow overwriting non-empty directories.
  
### Bug Fixes
  - Fix purging of backups when retention value is 0

## 1.1.0 (March 23, 2015)

### Backwards Incompatible Changes
  - Disabled history logging by default

### Improvements
  - Added the `--mysqlbackup option` to allow the location of the mysqlbackup binary to be set.
  - Added the `--history-logging` to turn on history logging.
  - Backup /etc/my.cnf by default

## 1.0.0 (December 16, 2014)

  - Initial Release
