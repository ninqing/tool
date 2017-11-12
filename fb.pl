#!/usr/local/bin/perl -w
# (C) 2007-2012 Alibaba Group Holding Limited

# Time: 2012.9

# Authors:
# zhisheng.cfg@taobao.com
# xiyu.lh@taobao.com
#
#  Copyright (C) 2012 TaoBao Co.,Ltd.
#
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation; either version 2 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.  #
#  You should have received a copy of the GNU General Public License
#   along with this program; if not, write to the Free Software
#  Foundation, Inc.,
#  51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA

#  2012.10.11 "add Special filter" zhisheng.cjf 

use strict;
use warnings FATAL => 'all';
use FindBin qw($Bin);
use lib "$Bin/../lib";

use Carp qw(croak);
use Getopt::Long;
use Pod::Usage;
use DBI;
use RollbackComm;
use RollbackConst;

use constant Binlog_Format              => "select \@\@global.binlog_format as Value";

sub print_usage{
        print <<EOF;

============================================================
Usage :
Command line options :
    --rb_file           This binlog need rollback and need full path.
    --local_port        MySql port and default 3306.
    --start_position    Start reading the binlog at position N. Applies to the
                      first binlog passed on the command line.
    --stop_position     Stop reading the binlog at position N. Applies to the
                      last binlog passed on the command line.
    --start_datetime    Start reading the binlog at first event having a datetime
                      equal or posterior to the argument; the argument must be
                      a date and time in the local time zone, in any format
                      accepted by the MySQL server for DATETIME and TIMESTAMP
                      types, for example: 2004-12-25 11:25:56 (you should
                      probably use quotes for your shell to set it properly).
    --stop_datetime    Stop reading the binlog at first event having a datetime
                      equal or posterior to the argument; the argument must be
                      a date and time in the local time zone, in any format
                      accepted by the MySQL server for DATETIME and TIMESTAMP
                      types, for example: 2004-12-25 11:25:56 (you should
                      probably use quotes for your shell to set it properly).
    --database          just rollback the database and default all database, 
                      not support multi databases.
    --table             just rollback the table under database, and --database 
                      must be set, not support multi databases.
    --dry_run           rollback switch and default on
     |- off mean just generate the rollback binlog, and execute
     |- on  mean just generate the rollback binlog, but not execute
Sample :
     shell> sudo perl ./rollback.pl --rb_file=/u01/mysql/log/mysql-bin.000001
============================================================

EOF
        exit;
}

GetOptions(
    \my %opt, qw/
    help
    version
    rb_file=s
    start_position=i
    stop_position=i
    start_datetime=s
    stop_datetime=s
    database=s
    table=s
    local_port=i
    dry_run=s
    /,
    ) or print_usage();


####################### option help

if ( $opt{help} ) {
  print_usage();
}

if ( $opt{version} ) {
  print "rollback version 1.0 \n";
  exit 0;
}

####################### option handle

unless ( $opt{rb_file} ) {
  croak "--rb_file must be set.\n";
}

unless ( $opt{local_port} ) {
  $opt{local_port} = 3306
}

unless ( $opt{dry_run} ) {
  $opt{dry_run} = "on";
}

my ($binlog_name) = $opt{rb_file};
$binlog_name =~ s/(.*\/)//;
$binlog_name = $binlog_name . "_" . $opt{local_port};
print "[DEBUG] binlog_name = $binlog_name\n";

&main();

#######################################################################

sub get_dbh () {
  my $db_name = "test";
  my $port    = shift;
  my $ip      = shift;

  my $dsn = "DBI:mysql:$db_name:$ip:$port";
  my $dbh = DBI->connect($dsn,"$RollbackConst::Local_User","$RollbackConst::Local_Pwd",{RaiseError => 1});

  return $dbh;
} 

####################### check binlog format to ensure it's ROW.
sub check_binlog_formmat($) {
  my $dbh = shift;
  my ( $query,$sth,$ret,$href );
  $query  = Binlog_Format;
  $sth    = $dbh->prepare($query);
  $ret    = $sth->execute();

  if ( !defined($ret) || $ret != 1 ) {
    croak "Can not get binlog format!\n";
  }
  elsif ( defined( $sth->errstr ) ) {
    croak "Got error when executing binlog format, the error is ",$sth->errstr," \n";
  }

  $href = $sth->fetchrow_hashref;

  if ($href->{Value} !~ /ROW/ and $href->{Value} !~ /row/) {
    croak "Not support for $href->{Value} format binlog.\n";
  }
}

sub main () {
  
system("date \'+%s.%N\'");

  my $ffdbh = &get_dbh($opt{local_port}, $RollbackConst::Local_Host);
  check_binlog_formmat ($ffdbh);
  set_db_conn_info($RollbackConst::Local_Host, $opt{local_port});

  my $binlog_options = "--no-defaults -f -v --base64-output=decode-rows";
  my $cmd_suffix = "";
  if ($opt{database}) {
    $cmd_suffix = $cmd_suffix . " --database=$opt{database}";
  }
  if ($opt{start_position}) {
    $cmd_suffix = $cmd_suffix . " --start-position=$opt{start_position}";
  }
  if ($opt{stop_position}) {
    $cmd_suffix = $cmd_suffix . " --stop-position=$opt{stop_position}";
  }
  if ($opt{start_datetime}) {
    $cmd_suffix = $cmd_suffix . " --start-datetime='$opt{start_datetime}'";
  }
  if ($opt{stop_datetime}) {
    $cmd_suffix = $cmd_suffix . " --stop-datetime='$opt{stop_datetime}'";
  }

  my $log =  "$Bin/../tmp/$binlog_name.log";
  printf("\n$Bin/mysqlbinlog $binlog_options $cmd_suffix $opt{rb_file} > $log\n");
  system("$Bin/mysqlbinlog $binlog_options $cmd_suffix $opt{rb_file} > $log");

  parse_sql($log, $binlog_name);
  reverse_transaction($binlog_name);
  reverse_sql($binlog_name);
  if ($opt{table}) {
    my $filter_name = $opt{database} . "." . $opt{table};
    print "$filter_name\n";
    filter($binlog_name, $filter_name);
  }
  if ($opt{special}) {
    my $filter_name = $opt{special};
    print "$filter_name\n";
    filter_special($binlog_name, $filter_name);
  }
  if ($opt{dry_run} eq "on") {
    printf("=== DRY_RUN mode. ===\n");
    printf("Please manually execute the file!\n");
  }
  else{
    my $cmd ="/u01/mysql/bin/mysql -h$RollbackConst::Local_Host -p$RollbackConst::Local_Pwd -u$RollbackConst::Local_User -P$opt{local_port} -f";#-v -v -v --show-warnings";
    print("Rollback with command: $cmd < $Bin/../log/rollback_$binlog_name.sql > $Bin/../log/report_$binlog_name.log 2>&1\n");
    system(" $cmd < $Bin/../log/rollback_$binlog_name.sql"); #> $Bin/../log/report_$binlog_name.log 2>&1");
    #printf ("apply binlog: rollback_$binlog_name\n");
    #apply_sql($binlog_name);
  }

  print "ROLLBACK DONE.\n";

system("date \'+%s.%N\'");

}
