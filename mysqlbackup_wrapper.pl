#!/usr/bin/perl

use strict;
use warnings;
use Getopt::Long;
use DBI;
use Sys::Hostname;
use Net::Domain qw(hostfqdn);
use File::Temp qw(tempdir);
use File::Basename;
use File::Copy;
use File::Find;
no warnings 'File::Find';
use POSIX qw(strftime mktime);
use Time::HiRes qw(tv_interval gettimeofday time);

my $mysqlbackup = 'mysqlbackup';
my $backupname_regex = "[0-9]{4}-[0-9]{2}-[0-9]{2}_[0-9]{2}-[0-9]{2}-[0-9]{2}";

my $LOG_DEBUG = 1;
my $LOG_INFO = 2;
my $LOG_WARNING = 3;
my $LOG_ERR = 4;

my $script_path = $0;
my $script = substr($script_path, rindex($script_path, '/') + 1, length($script_path));
my $version = "2.0.0";
my $pid_file = "/tmp/mysqlbackup_wrapper.pid";

my $exit_code = 0;
my $hostname;

my %options;
my $num_options;

my @email_msg;
my $sendmail="/usr/sbin/sendmail -t";
my $email_line_end = "\r\n";
my $retention_type = 'time';

my $chmod_dir = 0700;
my $chmod_file = 0660;

my $mode = 'backup';

sub get_hostname{
  $hostname = hostfqdn;
  if(! defined $hostname){
    $hostname = hostname;
  }
}

sub timestamp{
  my ($sec,$min,$hour,$mday,$mon,$year) = localtime();
 
  return sprintf("%04d%02d%02d %02d:%02d:%02d", $year+1900, $mon+1, $mday, $hour, $min, $sec);
}

sub _print_msg{
  my $log_level = pop;
  my @messages = @_;
  my $fmt = "%-17s %8s %s\n";
  my $fmt2 = "%26s %s\n";    
  
  my $msg_txt = timestamp();
  my $msg_type;

  if($log_level eq $LOG_DEBUG){
    $msg_type = "Debug:";
  }
  elsif ($log_level eq $LOG_INFO){
    $msg_type = "Info:";
  }
  elsif ($log_level eq $LOG_WARNING){
    $msg_type = "Warning:";
  } 
  elsif ($log_level eq $LOG_ERR){
    $msg_type = "Error:";
  }

  foreach my $msg (@messages){
    my @msg_lines = split(/\n/,$msg);
    my $first_line = 1;
    foreach my $msg_line (@msg_lines){
      if($first_line){
        printf($fmt, $msg_txt, $msg_type, $msg_line);
      }
      else{
        printf($fmt2, '', $msg_line);
      }
      $first_line = 0;
    }      
    
  }
}

sub _email_msg{
  my $log_level = pop;
  my @messages = @_;
  my $fmt = "%s%s";
  my $fmt2 = "%9s%s";

  my $msg_type;

  if($log_level eq $LOG_DEBUG){
    $msg_type = "Debug: ";
  }
  elsif ($log_level eq $LOG_INFO){
    $msg_type = "";
  }
  elsif ($log_level eq $LOG_WARNING){
    $msg_type = "Warning: ";
  } 
  elsif ($log_level eq $LOG_ERR){
    $msg_type = "Error: ";
  }

  foreach my $msg (@messages){
    my @msg_lines = split(/\n/,$msg);
    my $first_line = 1;
    foreach my $msg_line (@msg_lines){
      if($first_line){
        push(@email_msg, sprintf($fmt,$msg_type,$msg_line));
      }
      else{
        push(@email_msg, sprintf($fmt2,'',$msg_line));
      }
      $first_line = 0;
    }
  }
}

sub log_msg{
  my $log_level = pop;
  my @msg = @_;
  
  # Print debug messages only if in debug mode
  return if (! $options{'debug'}) and ($log_level eq $LOG_DEBUG);
  
  if($log_level eq $LOG_ERR) { $exit_code = 1 };
  
  _print_msg(@msg, $log_level);
  
  # Add to email only if we are emailing
  return if (! $options{'email'});
  
  _email_msg(@msg, $log_level);
}

sub run_command{
  my $stderr;
  my $stdout;
  my @return = (1,'','');
  
  my $fh_stdout = File::Temp->new();
  my $tmp_stdout = $fh_stdout->filename;

  my $fh_stderr = File::Temp->new();
  my $tmp_stderr = $fh_stderr->filename;
    
  log_msg("Running: " . join(" ", @_), $LOG_DEBUG);
  
  my $return_code = system("@_ 1>$tmp_stdout 2>$tmp_stderr");
  
  my $program_status = $?;
  my $program_error = $!;
  
  if(-e $tmp_stderr){
    $stderr = `cat $tmp_stderr`;
    chomp($stderr);
    unlink $tmp_stderr;
  }
  
  if(-e $tmp_stdout){
    $stdout = `cat $tmp_stdout`;
    chomp($stdout);
    unlink $tmp_stdout;
  }
  
  if($return_code == 0 && $program_status == 0){
    $return[0] = 1;
  }
  else{
    if(! $stderr){
      if($program_status & 0xff){
        $stderr = "received signal " . ($program_status & 0xff);
      } 
      elsif($program_status >> 8){
       $stderr = "exit status " . ($program_status >> 8);
      } 
      else{
        $stderr = $program_error;
      }
    }
    
    chomp($stderr);
    
    $return[0] = 0;
  }
  
  $return[1] = $stdout;
  $return[2] = $stderr;
  
  if(wantarray){
    return @return;
  }

  return $return[0];
}

sub version{
  print $script ." version $version\n";
  exit 0;
}

sub usage{
$exit_code = $_[0];

my $txt = <<END;
$script - Wrapper script for mysqlbackup. Requires mysqlbackup version 3.12.0 or
greater.

Usage: 
  Backup:  $script --backup-dir=PATH [STD-OPTIONS] [BACKUP-OPTIONS]
  
  Restore: $script --backup-dir=PATH --restore-dir=PATH [STD-OPTIONS] [RESTORE-OPTIONS]
  
  Standard Options [STD-OPTIONS]:
  -------------------------------
  --config-file=PATH    
                    Configuration file. Processes option group 
                    [mysqlbackup-wrapper]. You can also have option group
                    [mysqlbackup], which will be processed by mysqlbackup.

  --limit-memory=MB     
                    This option determines the memory available for the MEB
                    operation. The default value for apply-log is 100 which
                    implies 100MB. For all other operations the default value is
                    300 and it implies 300MB. If required, the number of memory 
                    buffers is adjusted according to this value.

  --email=S         Email address to send audit report

  --mysqlbackup=MYSQLBACKUP
                    The mysqlbackup binary location. Useful if mysqlbackup
                    binary is not in your path.
                    
  --skip-binlog
                    Skips copying of binlogs on backup, or skips copying the
                    binlogs onto the server on restore.
                    
  --skip-relaylog
                    Skips copying of relaylogs on backup, or skips copying the
                    relay logs onto the server on restore.

  --help            Show help

  --version         Show version

  --debug           Enable verbose debugging output
  
  Backup Options [BACKUP-OPTIONS]:
  -------------------------------
  --backup-dir=PATH     
                    Location to save backup directory. Must be absolute path.
                    
  --backup-type=TYPE    
                    The type of backup to perform. Possible values are:
                      full (Default)
                      incremental
  --retention=NT|N      
                    How long to keep backups. 
  
                    When format is N, the last N previous full backups are kept. 
                        
                    When format is NT, N is a number and T is one of y (year), 
                    m (month), w (week), d (day), h (hour), j (minute), or s 
                    (second). If an increment backup is not old enough all 
                    previous incrementals up to an including the most recent 
                    full are also kept even if they are older than the specified
                    retention time.
                    
  --user=USER           
                    User for login if not current user
                    
  --password[=PASSWORD] 
                    Password to use when connecting. If not given, it is 
                    prompted from std input terminal.
                    
  --slave-info          
                    Backup slave info
                    
  --read-threads=N      
                    Specifies the number of read-threads for the backup 
                    operation.
                    
  --process-threads=N   
                    Specifies the number of process-threads for the backup
                    operation.
                    
  --optimistic-time=S   
                    Optimistic time to pass to mysqlbackup
                    
  --my-file
                    The configuration file to include in backup. It is saved and
                    restored as saved-my.cnf. If not set and /etc/my.cnf is 
                    present and readable, /etc/my.cnf is backed up by default.
                    
  --buffer-pool-file=PATH
                    The location of the InnoDB Buffer Pool to backup.
  --history-logging Enable history logging if connection is available. This is
                    disabled by default.

  Restore Options [BACKUP-OPTIONS]:
  -------------------------------
  --backup-dir=PATH     
                    Location of backup directory to restore. Must be absolute 
                    path.
                        
  --restore-dir=PATH    
                    Location to restore backup to. Must be absolute path.
  
  --force
                    By default, the restore operation will refuse to overwrite
                    files in the --restore-dir. The --force option will cause
                    files to be overwritten.

END

print $txt;
exit($exit_code);
}

sub get_options{
  log_msg("Getting options", $LOG_DEBUG);

  my $ret = GetOptions( \%options,
    "mysqlbackup=s",
    "backup-dir=s",
    "backup-type=s",
    "restore-dir=s",
    "retention=s",
    "user=s",
    "password:s",
    "email=s",
		"config-file=s",
    "slave-info!",
    "read-threads=i",
    "process-threads=i",
    "optimistic-time=i",
    "skip-binlog!",
    "skip-relaylog!",
    "limit-memory=i",
    "my-file=s",
    "buffer-pool-file=s",
    "history-logging!",
    "force!",
    "debug!",
    "help",
    "version"
  );
  
  unless( $ret ){
    usage(1);
  }
  
  $num_options = scalar keys %options;
}

sub parse_config_file{
  my $mysqlbackup_wrapper_group;
  my $succeeded = 1;
  
  if($options{'config-file'}){
    log_msg("Parsing configuration file ".$options{'config-file'},$LOG_DEBUG);
    
    if (!open (CONFIG, "<$options{'config-file'}")){
      log_msg("Can not open $options{'config-file'}: $!", $LOG_ERR);
      $succeeded = 0;
    }
    else{
      while(<CONFIG>){
        chomp;
        s/#.*//;
        s/^\s*//;
        s/\s*$//;
        
        next unless length;        
        
        # Check for option group
        if(/\[\s*(.+)\s*\]/){
          log_msg("Option Group [".$1."]",$LOG_DEBUG);
          if($1 eq "mysqlbackup-wrapper"){
            $mysqlbackup_wrapper_group = 1;
          }
          else{
            $mysqlbackup_wrapper_group = 0;
          }
        }
        # Check that we have found at least on option group
        elsif(! defined $mysqlbackup_wrapper_group){
          log_msg("Found option without preceding option group in config file: ".$options{'config-file'},$LOG_ERR);
          $succeeded = 0;
          last;
        }
        # Check that we are in the correct option group
        elsif($mysqlbackup_wrapper_group == 1){
          my ($var, $value) = split(/\s*=\s*/, $_, 2);
    
          if(!defined($options{$var}) || $options{$var} eq ""){
            # config option can be turned on or off
            if(
              $var =~ /^(no-?|)slave-info$/
              || $var =~ /^(no-?|)skip-binlog$/
              || $var =~ /^(no-?|)skip-relaylog$/
              || $var =~ /^(no-?|)debug$/ 
              || $var =~ /^(no-?|)history-logging$/
              || $var =~ /^(no-?|)force$/
            ){
              # value is not defined so figure out what it needs to be
              if(!defined($value)){
                if($var =~ /^no./){
                  $var =~ s/^no-?//;
                  $value = 0;
                }
                else{
                  $value = 1;
                }
              }
              # value is defined
              else{
                print "Option ".$var." does not take an argument\n";
                usage(0);                
              }
              log_msg("Setting Option [".$var."] to value [".$value."]", $LOG_DEBUG);
              $options{$var} = $value;
            }
            elsif(defined $value){
              log_msg("Setting Option [".$var."] to value [".$value."]", $LOG_DEBUG);
              $options{$var} = $value;
            }
            else{
              log_msg("Option [".$var."] has no value set", $LOG_WARNING);
            }
          }
        }
        else{
          log_msg("Skipping option on line $.", $LOG_DEBUG);
        }
      }
      
      close(CONFIG);
    }
  }
  
  return $succeeded;
}

sub validate_options{
  log_msg("Validating options", $LOG_DEBUG);

  if(exists($options{'help'})){
    usage(0);
  }
  elsif(exists($options{'version'})){
    version();
  }
  else{    
    check_mysqlbackup_binary();
    
    # Check Backup Dir
    if(!$options{'backup-dir'}){
      print "Required option --backup-dir is missing\n";
      usage(0);
    }
    # Check writable
    elsif(! -w $options{'backup-dir'}){
      print "Backup directory is not writable or does not exist\n";
      usage(0);
    }
    
    # Check for Ending Slash
    if($options{'backup-dir'} =~ /.+\/$/){
      chop($options{'backup-dir'});
    }
    
    # Check for relative path
    if( $options{'backup-dir'} !~ /^\//){
      print "Option --backup-dir must be an absolute path\n";
      usage(0);
    }
    
    # Check for restore directory
    if($options{'restore-dir'}){
      # Check for relative path
      if($options{'restore-dir'} !~ /^\//){
        print "Option --restore-dir must be an absolute path\n";
        usage(0);
      }
      elsif($options{'backup-dir'} eq $options{'restore-dir'}){
        print "--backup-dir and --restore-dir can not be the same location\n";
        usage(0);
      }
      elsif(! -w $options{'restore-dir'}){
        print "Restore directory is not writable or does not exist\n";
        usage(0);
      }
      # Check for Ending Slash
      elsif($options{'restore-dir'} =~ /.+\/$/){
        chop($options{'restore-dir'});
      }
      
      $mode = 'restore';
    }
    
    # Taking a backup
    if($mode eq 'backup'){
      if(! exists $options{'backup-type'}){
        $options{'backup-type'} = 'full';
      }
      elsif($options{'backup-type'} ne 'full' && $options{'backup-type'} ne 'incremental'){
        print "Incorrect --backup-type specified\n";
        usage(0);
      }

      # Check retention
      if(defined $options{'retention'} && !valid_retention()){
        usage(0);
      }
      
      # Check my-file
      if(defined $options{'my-file'} && ! -r $options{'my-file'}){
        print $options{'my-file'}." set by option --my-file, is not a readable file\n";
        usage(0);
      }
      elsif(! defined $options{'my-file'} && -r '/etc/my.cnf'){
        $options{'my-file'} = '/etc/my.cnf';
      }
      
      # Check buffer-pool-file
      if(defined $options{'buffer-pool-file'} && ! -r $options{'buffer-pool-file'}){
        print "--buffer-pool-file is not a readable file\n";
        usage(0);
      }
    }
  }
}

sub is_integer{
  my $input = shift;

  if(defined $input){
    if($input =~ /^[0-9]+$/){
      return 1;
    }
  }

  return 0;
}

sub valid_retention(){
  my ($number, $time, $time_range);
  
  log_msg("Validating retention option", $LOG_DEBUG);
  
  # Check for format n
  if(is_integer(substr($options{'retention'}, -1, 1))){
    if(!is_integer($options{'retention'})){
      print "Invalid retention policy\n";
      return 0;
    }
    elsif($options{'retention'} < 0){
      print "Retention policy must be greater than or equal to 0\n";
      return 0;
    }
    else{
      $retention_type = 'number';
    }
  }
  # Format is nt
  else{
    if(length($options{'retention'}) < 2){
      print "Invalid retention policy\n";
      return 0;
    }
    else{
      $number = substr($options{'retention'}, 0, length($options{'retention'}) - 1);
      $time = lc(substr($options{'retention'}, -1, 1));
  
      if(!is_integer($number)){
        print "Retention policy must be an integer followed by one of y, m, w, d, h, j, or s\n";
        return 0;
      }
      elsif($number < 0){
        print "Retention policy must be greater than 0\n";
        return 0;
      }
  
      if($time ne "y" && $time ne "m" && $time ne "w" && $time ne "d" && $time ne "h" && $time ne "j" && $time ne "s"){
        print "Retention policy must be an integer followed by one of y, m, w, d, h, j, or s\n";
        return 0;
      }
  
      if($time eq "y"){
        $time_range = "year";
      }
      elsif($time eq "m"){
        $time_range = "month";
      }
      elsif($time eq "w"){
        $time_range = "week";
      }
      elsif($time eq "d"){
        $time_range = "day";
      }
      elsif($time eq "h"){
        $time_range = "hour";
      }
      elsif($time eq "j"){
        $time_range = "minute";
      }
      else{
        $time_range = "second";
      }
  
      $options{'retention'} = $number." ".$time_range." ago";   
    }
  }
  
  log_msg("Retention Type is $retention_type", $LOG_DEBUG);

  return 1;
}

sub get_backup_dir_list{
  my $dir = shift;
  my $reverse = shift;
  my @dirs;
  
  if(! defined $reverse){
    $reverse = 0;
  }
  
  # Get Backup List
  log_msg("Getting the list of available backup directories", $LOG_DEBUG);
  my @program = ('ls');

  if($reverse){
    push(@program, '-r');
  }

  push(@program, 
    ($dir."/*/backup*.mbi",
    "|",
    'grep',
    '-E',
    '"'.$backupname_regex.'"')
  );
  
  my ($success, $stdout, $stderr) = run_command(@program);
  if($success){
    @dirs = split(/\n/, $stdout);
    return (1,@dirs);
  }
  else{
    log_msg($stderr, $LOG_DEBUG);
    return (0,@dirs);
  }
}

sub dir_is_empty{
  my $dir = shift;
  
  opendir DIR, $dir;
  
  while(my $entry = readdir DIR) {  
    next if($entry =~ /^\.\.?$/);
    
    closedir DIR;
    
    return 0;
  }

  closedir DIR;

  return 1;
}

sub remove_dir{
  my $dir = shift;
  
  my @program = ('rm', '-rf', $dir);
  my ($success, $stdout, $stderr) = run_command(@program);
  if(! $success){
    log_msg("Failed to remove dir $dir", $LOG_WARNING);
    log_msg($stderr, $LOG_DEBUG);
    return 0;
  }
  else{
    return 1;
  }
}

sub get_date{
  my $time_range = shift;

  my @program = (
    'date',
    "--date='".$time_range."'",
    '+"%Y-%m-%d_%H-%M-%S"'
  );
  
  log_msg("Getting date string", $LOG_DEBUG);
  my ($success, $stdout, $stderr) = run_command(@program);
  
  if(!$success){
    log_msg("Failed to generate date", $LOG_WARNING);
    return 0;
  }
  else{
    return $stdout;
  }
}

sub purge_by_time{
  my $return = 1;
  my $full_backup_found = 0;
  my $past_date;
  
  log_msg("Purging by time", $LOG_DEBUG);
  
  if(! ($past_date = get_date($options{'retention'}))){
    log_msg("Failed to get retention date. Can not purge old backups", $LOG_WARNING);
    return 0;
  }
  
  # Get list of backups
  (my $success, my @dirs) = get_backup_dir_list($options{'backup-dir'}, 1);
  if($success){
    # loop through backup dirs to see if we should purge them
    foreach my $dir (@dirs){
      my($backup_name, $backup_dir) = fileparse($dir);
      
      # Backup is older than retention time
      if($backup_dir lt $options{'backup-dir'}."/".$past_date){
        # Full Backup
        if(is_full_backup($backup_name)){
          log_msg("Found full backup ".$backup_dir, $LOG_DEBUG);
          if($full_backup_found == 1){
            log_msg("Removing backup ".$backup_dir, $LOG_DEBUG);
            remove_dir($backup_dir) or $return = 0;
          }
          else{
            $full_backup_found = 1;
          }
        }
        # Incremental Backup
        else{
          log_msg("Found incremental backup ".$backup_dir, $LOG_DEBUG);
          # if we have found a previous full backup, we can remove this 
          # incremental backup
          if($full_backup_found == 1){
            log_msg("Removing backup ".$backup_dir, $LOG_DEBUG);
            remove_dir($backup_dir) or $return = 0;
          }
        }
      }
      else{
        # Full Backup
        if(is_full_backup($backup_name)){
          log_msg("Found full backup ".$backup_dir, $LOG_DEBUG);
        }
        else{
          log_msg("Found incremental backup ".$backup_dir, $LOG_DEBUG);
        }
      }
    }
  }
  else{
    log_msg("Failed to purge old backups", $LOG_WARNING);
    $return = 0;
  }
  
  return $return;
}

sub purge_by_number{
  my $return = 1;

  log_msg("Purging by number", $LOG_DEBUG);
  
  # need to adjust number for backup that just happen before purge
  $options{'retention'}++;
  
  my ($success, @dirs) = get_backup_dir_list($options{'backup-dir'}, 1);
  if($success){
    # Loop through dirs and remove the old ones
    my $number = 1;
    foreach my $dir (@dirs){
      my($backup_name, $backup_dir) = fileparse($dir);
      
      # Full Backup
      if(is_full_backup($backup_name)){
        log_msg("Found full backup ".$backup_dir, $LOG_DEBUG);
      }
      # Incremental Backup
      else{
        log_msg("Found incremental backup ".$backup_dir, $LOG_DEBUG);
      }

      # Check that we have the required number of backups to save
      if($number > $options{'retention'} && defined $backup_dir){
        log_msg("Removing backup ".$backup_dir, $LOG_DEBUG);
        remove_dir($backup_dir) or $return = 0;
      }
      else{
        log_msg("Skipping backup ".$backup_dir, $LOG_DEBUG);
      }
      
      # We found a full backup so count it
      if(is_full_backup($backup_name)){
        $number++;
      }
    }
  }
  else{
    log_msg("Failed to purge old backups", $LOG_WARNING);
    $return = 0;
  }
  
  return $return;
}

sub purge_old_backups{
  log_msg("Purging Old Backups", $LOG_DEBUG);
  
  # Purging by time
  if($retention_type eq "time"){
    return purge_by_time();
  }
  # Purging by number
  else{
    return purge_by_number();
  }
}

sub prompt_password{
  my $prompt = shift;
  my $password;
  
  print $prompt;
  system("stty -echo");
  $password = <STDIN>;
  system("stty echo");
  print "\n";
  chomp($password);
  
  return $password;
}

sub email_report{
  my $headers = "";
  
  log_msg("Sending Email", $LOG_DEBUG);
  
  $headers=$headers.'From: MySQL Backup Wrapper Script <mysql@prairiesys.com>'.$email_line_end;
  $headers=$headers."To: ".$options{email}.$email_line_end;
  $headers=$headers."Subject: MySQL ".($mode eq 'backup' ? ucfirst($options{'backup-type'}).' ' : '').ucfirst($mode)." for $hostname".$email_line_end;
  
  if($exit_code == 1){
    $headers=$headers."X-Priority: 1 (Highest)".$email_line_end;
    $headers=$headers."X-MSMail-Priority: High".$email_line_end;
    $headers=$headers."Importance: High".$email_line_end;
  }
  
  $headers=$headers.$email_line_end;
  
  if(open(SENDMAIL, "|$sendmail")){
    print SENDMAIL $headers;
  
    foreach my $email_line (@email_msg){
      print SENDMAIL "  ".$email_line.$email_line_end;
    }
  }
  else{
    log_msg("Cannot open $sendmail: $!", $LOG_ERR);
  }
}

sub is_full_backup{
  my $backup = shift;
  
  my($backup_name, $backup_dir) = fileparse($backup);
  
  if($backup_name =~ "backup.mbi"){
    return 1;
  }
  else{
    return 0;
  }
}

sub get_backup_dir{
  my @backup_time = @_;
  my $count = 1;
  
  # Try looking for a backup dir that was just created at backup time
  my $created_backup_dir = $options{'backup-dir'}."/".strftime("%Y-%m-%d_%H-%M-%S",@backup_time);;
  if(! -d $created_backup_dir){
    # Backup dir could have been created a couple of seconds after backup script 
    # start. Try looking a couple of seconds ahead
    while ($count <= 30){
      my $possible_time = mktime(@backup_time)+$count;
      $created_backup_dir = $options{'backup-dir'}."/".strftime("%Y-%m-%d_%H-%M-%S",localtime($possible_time));
      
      if(! -d $created_backup_dir){
        $created_backup_dir = "";
      }
      else{
        last;
      }
      
      $count++;
    }    
  }
  
  log_msg("Possible created backup dir: ". $created_backup_dir, $LOG_DEBUG);
  return $created_backup_dir;
}

sub get_dir_size{
  my $dir = shift;
  
  my @program = (
    'du',
    '--max-depth=0',
    '-h',
    $dir
  );
  
  my ($success, $stdout, $stderr) = run_command(@program);
  if($success){
    my @size = split(' ', $stdout);
    return $size[0];
  }
  else{
    log_msg("Failed to get size of dir [$dir]", $LOG_WARNING);
    return "0";
  }
}

sub get_last_backup{
  log_msg("Getting last backup dir", $LOG_DEBUG);
  my @program = (
    'ls',
    '-r',
    "'".$options{'backup-dir'}."/'",
    "|",
    'grep',
    '-E',
    '"'.$backupname_regex.'"',
    "|",
    "head",
    "-1"
  );
  
  my ($success, $stdout, $stderr) = run_command(@program);
  if($success){
    my @lines = split(/\n/, $stdout);
    
    if(@lines){
      log_msg("Last backup found is ".$options{'backup-dir'}."/".$lines[0],$LOG_DEBUG);
      return $lines[0];
    }
    
  }
  else{
    log_msg($stderr, $LOG_ERR);
  }
  
  return '';
}

sub take_backup{
  my $last_backup = '';
  
  my @now_string = localtime();
  log_msg("Backup Server: $hostname", $LOG_INFO);
  log_msg("Backup Date: ". strftime("%A, %B %d %Y %H:%M:%S", @now_string), $LOG_INFO);
  log_msg("Backup Type: ".ucfirst($options{'backup-type'}), $LOG_INFO);
  
  if($options{'backup-type'} eq 'incremental'){
    $last_backup = get_last_backup();
    
    if($last_backup eq ''){
      log_msg("Previous backup not found. Unable to take increment backup", $LOG_ERR);
      return 0;
    }
  }
  
  if(exists $options{'password'} && $options{'password'} eq ''){
    $options{'password'} = prompt_password("Enter password: ");
  }
  
  my @program = ($mysqlbackup);
  
  if($options{'config-file'}){
    push(@program, "--defaults-extra-file=".$options{'config-file'});
  }
  
  push(@program,
    ("--disable-manifest",
    "--with-timestamp",
    ($options{'backup-type'} eq 'full' ? '--compress' : '--incremental'),
    "--backup-image=".($options{'backup-type'} eq 'full' ? 'backup.mbi' : 'backup-incremental.mbi'))
  );
  
  if($options{'user'}){
    push(@program, "--user=".$options{'user'});
  }
  
  if($options{'password'}){
    push(@program, "--password=".$options{'password'});
  }
  
  if($options{'read-threads'}){
    push(@program, "--read-threads=".$options{'read-threads'});
  }
  
  if($options{'process-threads'}){
    push(@program, "--process-threads=".$options{'process-threads'});
  }

  if($options{'limit-memory'}){
    push(@program, "--limit-memory=".$options{'limit-memory'});
  }
  
  if($options{'slave-info'}){
    push(@program, "--slave-info");
  }
  
  if($options{'skip-binlog'}){
    push(@program, "--skip-binlog");
  }
  
  if($options{'skip-relaylog'}){
    push(@program, "--skip-relaylog");
  }
  
  if(! $options{'history-logging'}){
    push(@program, "--no-history-logging");
  }
  
  if($options{'backup-dir'}){
    push(@program, "--backup-dir=".$options{'backup-dir'});
  }
  
  if($options{'backup-type'} eq 'full' && $options{'optimistic-time'}){
    push(@program, "--optimistic-time=".$options{'optimistic-time'});
  }
  
  if($options{'backup-type'} eq 'incremental'){
    push(@program, "--incremental-base=dir:".$options{'backup-dir'}."/".$last_backup);
  }
  
  push(@program,"backup-to-image");  

  log_msg("Starting ".($options{'backup-type'} eq 'incremental' ? 'Incremental ' : '')."Backup", $LOG_DEBUG);
  my $backup_start = [gettimeofday];

  my ($success, $stdout, $stderr) = run_command(@program);
  if($success){
    my $created_back_dir = get_backup_dir(@now_string);

    # Check that we figured out the backup dir    
    if($created_back_dir ne ""){
      # Copy my-file if applicable
      if(defined $options{'my-file'} && ! copy($options{'my-file'},$created_back_dir."/saved-my.cnf")){
        log_msg("Failed to copy ".$options{'my-file'}." $!", $LOG_WARNING);
      }
      
      # Copy buffer-pool-file if applicable
      if(defined $options{'buffer-pool-file'} && ! copy($options{'buffer-pool-file'},$created_back_dir."/saved-ib_buffer_pool")){
        log_msg("Failed to copy ".$options{'buffer-pool-file'}." $!", $LOG_WARNING);
      }
    
      log_msg("Backup Size: ".get_dir_size($created_back_dir), $LOG_INFO);
    }
    
    log_msg("Backup Time: ".strftime("%H:%M:%S",gmtime(tv_interval($backup_start))), $LOG_INFO);
    log_msg("Backup Status: Completed Successfully", $LOG_INFO);
    return 1;
  }
  else{
    log_msg($stderr, $LOG_ERR);
    log_msg("Backup Status: Failed", $LOG_INFO);
    return 0;
  }
}

sub get_backup_type{
  my $backup_path = shift;
  
  log_msg("Getting backup type of ".$backup_path, $LOG_DEBUG);

  my @program = (
    'ls',
    $backup_path."/backup*.mbi",
    "|",
    'grep',
    '-E',
    '"'.$backupname_regex.'"'
  );
  
  my ($success, $stdout, $stderr) = run_command(@program);
  if($success){
    # Full Backup
    if(is_full_backup($stdout)){
      return 'full';
    }
    else{
      return 'incremental';
    }
  }
  else{
    log_msg($stderr, $LOG_DEBUG);
    return 0;
  }  
}

sub get_list_of_backups_to_restore{
  my $staring_backup = shift;
  my @backups_to_restore = ();
  
  log_msg("Getting list of backup directories to restore", $LOG_DEBUG);
  
  # Get base backup dir
  my($starting_backup_dir, $base_backup_dir) = fileparse($staring_backup);
  
  # Check for Ending Slash
  if($base_backup_dir =~ /.+\/$/){
    chop($base_backup_dir);
  }
  
  # Get list of backup dirs
  (my $success, my @dirs) = get_backup_dir_list($base_backup_dir, 1);
  if($success){
    my $found_staring_backup = 0;
    my $found_full_backup = 0;
    
    # Loop through all of the backup dirs
    foreach my $dir (@dirs){
      my($backup_name, $backup_dir) = fileparse($dir);
      
      # Check for Ending Slash
      if($backup_dir =~ /.+\/$/){
        chop($backup_dir);
      }
      
      log_msg("Checking: $backup_dir", $LOG_DEBUG);
      
      # Look for backup that was requested to be restored
      if($backup_dir eq $staring_backup || $found_staring_backup){
        if($backup_dir eq $staring_backup){
          log_msg("Found starting backup", $LOG_DEBUG);
          $found_staring_backup = 1;
        }

        log_msg("Adding ".$backup_dir." to list of backup to restore", $LOG_DEBUG);
        push(@backups_to_restore, $backup_dir);
        
        # Found full backup we can stop
        if(is_full_backup($dir)){
          $found_full_backup = 1;
          log_msg("Found full backup. Stopping", $LOG_DEBUG);
          last;
        }
      }
    }
    
    if($found_full_backup){
      log_msg("Backups to restore: \n".join("\n", @backups_to_restore), $LOG_DEBUG);
    }
    else{
      log_msg("Unable to find original full backup", $LOG_ERR);
      @backups_to_restore = ();
    }
  }
  else{
    log_msg("Failed to get full list of backups to restore", $LOG_ERR);
  }
  
  return @backups_to_restore;
}

sub find_in_dir{
  my $dir = shift;
  my %options = @_;

  my @found_items;

  if(! exists $options{'dirs'}){
    $options{'dirs'} = 1;
  }

  if(! exists $options{'files'}){
    $options{'files'} = 1;
  }

  find(
    sub {
      if( -d && $options{'dirs'} == 1 && $File::Find::name ne $dir){
        push(@found_items, $File::Find::name);
      }

      if( -f && $options{'files'} == 1){
        push(@found_items, $File::Find::name);
      }
    },
    ($dir)
  );

  return @found_items;
}

sub restore_cleanup_single{
  my $restore_dir = shift;
  my $restore_tmp_dir = shift;
  
  my $return_code = 1;  
  
  log_msg("Cleaning up after single restore", $LOG_DEBUG);
  
  # Check for ibbackup_slav_info file
  if(-e $restore_dir."/meta/ibbackup_slave_info"){
    # Try to copy it to the root of the restore dir
    log_msg("Copying ".$restore_dir."/meta/ibbackup_slave_info to ".$restore_dir, $LOG_DEBUG);
    if(! copy($restore_dir."/meta/ibbackup_slave_info", $restore_dir)){
      log_msg("Failed to copy ".$restore_dir."/meta/ibbackup_slave_info to ".$restore_dir." ".$!, $LOG_WARNING);
      $return_code = 0;
    }
  }
   
  # Move restore log files to root level of restored dir
  log_msg("Moving restore log files to root of backup dir", $LOG_DEBUG);
  my @restore_log_files = glob $restore_tmp_dir."/meta/MEB_*.log";
  foreach my $log_file (@restore_log_files){
    if(! move($log_file, $restore_dir)){
      log_msg("Failed to move ".$log_file." to ".$restore_dir, $LOG_WARNING);
      $return_code = 0;
    }
  }

  # Remove restore temp dir
  if(-e $restore_tmp_dir && ! remove_dir($restore_tmp_dir)){
    log_msg("Failed to remove dir ".$restore_tmp_dir, $LOG_WARNING);
    $return_code = 0;
  }
  
  # Remove meta dir
  if(-e $restore_dir."/meta" && ! remove_dir($restore_dir."/meta")){
    log_msg("Failed to remove dir ".$restore_dir."/meta", $LOG_WARNING);
    $return_code = 0;
  }

  return $return_code;
}

sub restore_cleanup{
  my $restore_dir = shift;
  
  my $return_code = 1;
  
  log_msg("Final Clean up", $LOG_DEBUG);
  
  # Remove server-all.cnf
  log_msg("Removing ".$restore_dir."/server-all.cnf if it exists", $LOG_DEBUG);
  if(-e $restore_dir."/server-all.cnf" && ! unlink($restore_dir."/server-all.cnf")){
    log_msg("Failed to unlink ".$restore_dir."/server-all.cnf : $!", $LOG_WARNING);
    $return_code = 0;
  }
  
  # Remove server-my.cnf if we have saved-my.cnf
  if(-e $restore_dir."/saved-my.cnf" && -e $restore_dir."/server-my.cnf" && ! unlink($restore_dir."/server-my.cnf")){
    log_msg("Failed to unlink ".$restore_dir."/server-my.cnf : $!", $LOG_WARNING);
    $return_code = 0;
  }

  # Remove backup_variables.txt
  log_msg("Removing ".$restore_dir."/backup_variables.txt if it exists", $LOG_DEBUG);
  if(-e $restore_dir."/backup_variables.txt" && ! unlink($restore_dir."/backup_variables.txt")){
    log_msg("Failed to unlink ".$restore_dir."/backup_variables.txt : $!", $LOG_WARNING);
    $return_code = 0;
  }
  
  # Get list of dirs restored
  my @restored_dirs = find_in_dir($restore_dir, ('files', 0, 'dirs', 1));
  
  # Change permissions on the dirs found
  log_msg("Changing permissions on the restored directores", $LOG_DEBUG);
  my $num_dirs_changed = chmod $chmod_dir, @restored_dirs;
  
  if($num_dirs_changed != @restored_dirs){
    log_msg("Failed to change permissions on the restored directores: $!", $LOG_WARNING);
    $return_code = 0;
  }
  
  # Get list of files restored
  my @restored_files = find_in_dir($restore_dir, ('files', 1, 'dirs', 0));
  
  # Change permissions on the files found
  log_msg("Changing permissions on the restored files", $LOG_DEBUG);
  my $num_files_changed = chmod $chmod_file, @restored_files;
  
  if($num_files_changed != @restored_files){
    log_msg("Failed to change permissions on the restored files: $!", $LOG_WARNING);
    $return_code = 0;
  }

  return $return_code;
}

sub restore_single_backup{
  my $backup_dir = shift;
  my $restore_dir = shift;
  my $tmp_dir = shift;
  
  my $backup_type;
  
  log_msg("Restoring backup ".$backup_dir, $LOG_DEBUG);
  
  if(! ($backup_type = get_backup_type($backup_dir))){
    log_msg("Failed to get backup type for ".$options{'backup-dir'}, $LOG_ERR);
    return 0;
  }
  
  log_msg("Backup type is ".$backup_type, $LOG_DEBUG);
  
  my @program = ($mysqlbackup, "--defaults-file=".$backup_dir."/backup-my.cnf");  
  
  if($options{'config-file'}){
    push(@program, "--defaults-extra-file=".$options{'config-file'});
  }

  if($options{'process-threads'}){
    push(@program, "--process-threads=".$options{'process-threads'});
  }

  if($options{'limit-memory'}){
    push(@program, "--limit-memory=".$options{'limit-memory'});
  }
  
  if($options{'skip-binlog'}){
    push(@program, "--skip-binlog");
  }
  
  if($options{'skip-relaylog'}){
    push(@program, "--skip-relaylog");
  }

  push(@program, 
    ("--backup-image=".$backup_dir."/".($backup_type eq 'full' ? 'backup.mbi' : 'backup-incremental.mbi'),
    "--backup-dir=".$tmp_dir,
    "--datadir=".$restore_dir,
    ($backup_type eq 'full' ? '--uncompress' : '--incremental'),
    "--force",
    "copy-back-and-apply-log")
  );
  
  my ($success, $stdout, $stderr) = run_command(@program);
  if($success){
    # If we backed up the my.cnf file, restore it as well
    if(-r $backup_dir."/saved-my.cnf" && ! copy($backup_dir."/saved-my.cnf",$restore_dir."/saved-my.cnf")){
      log_msg("Failed to restore ".$backup_dir."/saved-my.cnf $!", $LOG_WARNING);
    }
    
    # If we backed up the ib_buffer_pool file, restore it as well
    if(-r $backup_dir."/saved-ib_buffer_pool" && ! copy($backup_dir."/saved-ib_buffer_pool",$restore_dir."/ib_buffer_pool")){
      log_msg("Failed to restore ".$backup_dir."/saved-ib_buffer_pool $!", $LOG_WARNING);
    }
    
    return 1;
  }
  else{
    log_msg($stderr, $LOG_ERR);
    return 0;
  }
}

sub restore_backup{
  my $backup_type;
  my @now_string = localtime();
  
  log_msg("Restore Server: $hostname", $LOG_INFO);
  log_msg("Restore Date: ". strftime("%A, %B %d %Y %H:%M:%S", @now_string), $LOG_INFO);
  
  my @backups_to_restore = get_list_of_backups_to_restore($options{'backup-dir'});
  if(@backups_to_restore > 0){
    # Check if restore dir is empty
    if(!dir_is_empty($options{'restore-dir'}) && ! $options{'force'}){
      log_msg($options{'restore-dir'}.' is a non-empty directory. Use the "--force" option to overwrite.', $LOG_ERR);
      return 0;
    }
    
    @backups_to_restore = reverse @backups_to_restore;
    
    my $restore_start = [gettimeofday];
    
    foreach my $backup_to_restore (@backups_to_restore){
      # Create tmp dir for tmp restore items
      my $backup_tmp_dir = tempdir("mysqlbackup_wrapper_XXXXXX", DIR => $options{'restore-dir'});

      if(! restore_single_backup($backup_to_restore, $options{'restore-dir'}, $backup_tmp_dir)){
        log_msg("Restore Status: Failed", $LOG_INFO);
        return 0;
      }
      elsif(! restore_cleanup_single($options{'restore-dir'}, $backup_tmp_dir)){
        log_msg("Failed Clean up after restoring ". $backup_to_restore, $LOG_ERR);
        return 0;
      }
    }
    
    restore_cleanup($options{'restore-dir'});
    
    log_msg("Restore Time: ".strftime("%H:%M:%S",gmtime(tv_interval($restore_start))), $LOG_INFO);
    log_msg("Restore Status: Completed Successfully", $LOG_INFO);
    return 1;
  }
  else{
    log_msg("Unable to restore backup", $LOG_ERR);
    return 0;
  }
}

sub process_running{
  my $pid = shift;
  
  log_msg("Check if process is running", $LOG_DEBUG);
  my @program = (
    'ps',
    '--no-header',
    '-p',
    $pid,
    '|',
    'wc',
    '-l'
  );
  
  my ($success, $stdout, $stderr) = run_command(@program);
  if($success){
    if($stdout eq "1"){
      log_msg("There is currently a $script process running", $LOG_ERR);
      return 1;
    }
  }
  else{
    log_msg("Failed to check if process in pid file is running", $LOG_ERR);
    log_msg($stderr, $LOG_DEBUG);
    return 1;
  }  
  
  return 0;
}

sub can_run{
  log_msg("Checking for pid file", $LOG_DEBUG);
  
  if(-e $pid_file){
    log_msg("Checking that pid file is readable", $LOG_DEBUG);
    if(-r $pid_file){
      log_msg("Opening pid file", $LOG_DEBUG);
      if(open(PID_FILE, "$pid_file")){       
        while(<PID_FILE>){
          chomp;
          s/^\s*//;
          s/\s*$//;
          
          next unless length;
          
          if(is_integer($_)){
            log_msg("Found pid [$_]", $LOG_DEBUG);
            if(process_running($_)){
              close(PID_FILE);
              return 0;
            }
          } 
          
          last;
        }
        close(PID_FILE);
      }
      else{
        log_msg("Failed to open pid file: $!", $LOG_ERR);
        return 0;
      }      
    }
    else{
      log_msg("Can't open pid file $pid_file for reading", $LOG_ERR);
      return 0;
    }
  }
  
  if(create_pidfile()){  
    return 1;
  }
  else{
    return 0;
  }
}

sub create_pidfile{
  log_msg("Creating pid file", $LOG_DEBUG);
  if(open(PID_FILE, ">$pid_file")){
    print PID_FILE $$."\n";
    close(PID_FILE);
    
    if(! chmod(0644,$pid_file)){
      log_msg("Failed to chmod pid file: $!", $LOG_DEBUG);
      remove_pidfile();
      return 0;
    }
    
    return 1;
  }
  else{
    if(-e $pid_file){
      log_msg("$pid_file exits and is not writable. Please make sure there isn't another $script running and then remove pid file.", $LOG_ERR);
      log_msg("Failed to open for write pid file: $!", $LOG_DEBUG);
    }
    else{
      log_msg("Failed to create pid file: $!", $LOG_ERR);
    }
    
    return 0;
  }
}

sub remove_pidfile{
  log_msg("Removing pid file", $LOG_DEBUG);
  if(unlink($pid_file)){
    return 1;
  }
  else{
    log_msg("Failed to unlink pid file: $!", $LOG_ERR);
    return 0;
  }
}

sub check_mysqlbackup_binary{
  # check mysqlbackup binary set by option
  if($options{'mysqlbackup'}){
    if(! -e $options{'mysqlbackup'}){
      print "'$options{'mysqlbackup'}' is not found\n";
      exit(0);
    } 
    elsif(! -x $options{'mysqlbackup'}){
      print "'$options{'mysqlbackup'}' is not executable\n";
      exit(0);
    }
    else{
      $mysqlbackup = $options{'mysqlbackup'};
    }
  }
  # check for mysqlbackup in current dir or working path
  else{
    log_msg("Check for mysqlbackup in path", $LOG_DEBUG);
    my @program = ("which", "mysqlbackup");    
    my ($success, $stdout, $stderr) = run_command(@program);
    
    log_msg("Success: $success", $LOG_DEBUG);
    log_msg("Stdout ".$stdout, $LOG_DEBUG);
    log_msg("Stderr ".$stderr, $LOG_DEBUG);
    
    log_msg("Check for mysqlbackup in working directory", $LOG_DEBUG);
    if(!$success){
      my $mysqlbackup_tmp = "./".$mysqlbackup;
      if(! -e $mysqlbackup_tmp){
        print "$mysqlbackup command not found in working directory or path\n";
        exit(0);
      }
      elsif(! -x $mysqlbackup_tmp){
        print "'$mysqlbackup_tmp' is not executable\n";
        exit(0);
      }
      else{
        $mysqlbackup = $mysqlbackup_tmp;
      }
    }
  }
  
  # Check mysqlbackup version
  if(!correct_mysqlbackup_version()){
    print "Incorrect version of mysqlbackup detected. Version must be 3.12.0 or greater\n";
    exit(0);
  }
}

sub correct_mysqlbackup_version{
  log_msg("Checking version of mysqlbackup command.", $LOG_DEBUG);
  my @program = ($mysqlbackup, "--version");
  
  my ($success, $stdout, $stderr) = run_command(@program);
  
  if($success){
    if($stderr =~ /^.*MySQL Enterprise Backup version ((\d+)(\.(\d+)(\.(\d+))?)?).*$/m){
      my $version = $1;
      my $major = $2;
      my $minor = 0;
      my $revision = 0;
      # Grab minor if set
      if($4){
        $minor = $4;
      }
      # Grab revision if set
      if($6){
        $revision = $6;
      }
      
      log_msg("Version string is $version major: $major minor: $minor revision: $revision", $LOG_DEBUG); 
      
      if($major < 3){
        return 0;
      }
      elsif($major > 3){
        return 1;
      }
      # major is 3
      else{
        if($minor >= 12){
          return 1;
        }
        else{
          return 0;
        }
      }    
      return 0;
    }
    else{
      log_msg("Failed to get version string from mysqlbackup", $LOG_ERR);
      log_msg($stderr, $LOG_DEBUG);
      return 0;
    }
  }
  else{
    log_msg("Failed to check version of mysqlbackup", $LOG_ERR);
    log_msg($stderr, $LOG_DEBUG);
    return 0;
  }
}

sub main{
  get_options();
  parse_config_file() or exit(1);
  validate_options();
  get_hostname();
  
  if(can_run()){    
    if($mode eq 'backup'){
      if(take_backup() && defined $options{'retention'}){
        purge_old_backups();
      }
    }
    else{
      restore_backup();
    }

    remove_pidfile();
  }

  if($options{'email'}){
    email_report();
  }
}

main();
exit($exit_code);