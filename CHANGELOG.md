# ChangeLog for the MySQL Enterprise Backup Wrapper Script 

## 2.0.1

### Bug Fixes
  - Fixed backup dir validation test when doing a restore
  - Don't always print usage on error

## 2.0.0 (June 5, 2015)

### Backwards Incompatible Changes
  - mysqlbackup 3.12.0 or greater is now required.
  - Restore will not continue if the restore directory is non-empty.
  - ib_buffer_pool is now restored to ib_buffer_pool instead of saved-ib_buffer_pool
  - Removed `--mode` option

### Improvements
  - Backup type is now printed
  - Added `--force` option to allow overwriting non-empty directories.
  
### Bug Fixes
  - Fixed purging of backups when retention value was 0

## 1.1.0 (March 23, 2015)

### Backwards Incompatible Changes
  - Disabled history logging by default

### Improvements
  - Added the `--mysqlbackup option` to allow the location of the mysqlbackup binary to be set.
  - Added the `--history-logging` to turn on history logging.
  - Backup /etc/my.cnf by default

## 1.0.0 (December 16, 2014)

  - Initial Release
