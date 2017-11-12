#!/u01/dbaperl/bin/perl -w
# (C) 2007-2012 Alibaba Group Holding Limited

# Authors:
# thomas.wangyj@alibaba-inc.com
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
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#   along with this program; if not, write to the Free Software
#  Foundation, Inc.,
#  51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA

#  2012.10.11 "add Special filter" zhisheng.cjf 
package RollbackConst;

our $Local_Host='127.0.0.1';
our $Local_User='root';
our $Local_Pwd='';

1;



package RollbackComm;
require Exporter;

use DBI;
use FindBin qw($Bin);

our @ISA = qw(Exporter);
# export functions

our @EXPORT = qw(
  set_db_conn_info
  parse_sql
  reverse_transaction
  reverse_sql
  filter
  filter_special
  apply_sql
  exist_ddl
);

our $g_line_buf = "";
our $g_out_buf = "";
our $g_flag_and = 0;
our $g_DBname_TBname;
our @g_schema;
our $g_id;          
our $g_flag_read;  

our $host;
our $port;
our $user;
our $password;

our %DDL_white=();
our %QUERY_white=();

our ($ddsn);
our ($ddbh, $dsth);
our (@dary);
our ($iid);
our ($dsql);

our (@memPool);
our ($poolSize);

our (%g_hash_schema);

sub GetTableField{
  my ($aaa, $bbb) = @_;
  $iid = 0;
  $dsql = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS where table_name = \'$bbb\' and table_schema = \'$aaa\'";
  $dsth = $ddbh->prepare($dsql);
  $dsth->execute();
  while(@dary = $dsth->fetchrow_array()){
    $g_schema[$iid] = $dary[0];
    $iid++;
  }
  $dsth->finish();
}

sub Process_Insert{
  $g_out_buf = "DELETE FROM ";
  $g_line_buf =~ /(?<=^### INSERT INTO ).*/;
  $g_DBname_TBname = $&;
  $g_out_buf = $g_out_buf . $g_DBname_TBname . " WHERE";

  my (@t_n) = split(/\./, $g_DBname_TBname);

  if(!exists($g_hash_schema{$g_DBname_TBname})){
    &GetTableField($t_n[0], $t_n[1]);
    @{$g_hash_schema{$g_DBname_TBname}} = @g_schema;
  }
  else{
    @g_schema = @{$g_hash_schema{$g_DBname_TBname}};
  }

  $g_line_buf = <INPUT>;
  $g_id = 1;
  $g_flag_and = 0;

  while($g_line_buf = <INPUT>){
    if($g_line_buf =~ /^###.*/ && $g_line_buf =~ /^###   .*/){
      $g_line_buf =~ /(?<=^###   ).*/;
      if($g_flag_and == 1){
        $g_out_buf = $g_out_buf . " AND";
      }
      $ans = $&;
      $ans =~ s/@($g_id)/$g_schema[$g_id - 1]/;
      if($ans =~ /.*=NULL/){
        $ans =~ s/=NULL/ is NULL/;
      }
      $g_out_buf = $g_out_buf . ' ' . $ans;
      $g_flag_and = 1;
      $g_id++;
    }
    else{
      #print ("$g_out_buf;\n");
      $memPool[$poolSize] = $g_out_buf . ";\n";
      $poolSize++;
      $g_flag_read = 1;
      last;
    }
  }
}

sub Process_Delete{
  $g_out_buf = "INSERT INTO ";
  $g_line_buf =~ /(?<=^### DELETE FROM ).*/;
  $g_DBname_TBname = $&;
  $g_out_buf = $g_out_buf . $g_DBname_TBname;
  $g_line_buf = <INPUT>;

  my ($tmp1) = "";
  my ($tmp2) = "";
  $g_id = 1;
  $g_flag_and = 0;

  my (@t_n) = split(/\./, $g_DBname_TBname);
  
  if(!exists($g_hash_schema{$g_DBname_TBname})){
    &GetTableField($t_n[0], $t_n[1]);
    @{$g_hash_schema{$g_DBname_TBname}} = @g_schema;
  }
  else{
    @g_schema = @{$g_hash_schema{$g_DBname_TBname}};
  }

  while($g_line_buf = <INPUT>){
    if($g_line_buf =~ /^###.*/ && $g_line_buf =~ /^###   .*/){
      $g_line_buf =~ /(?<=^###   ).*/;
      $g_line_buf = $&;

      $g_line_buf =~ /@\d*(?==.*)/;
      if($g_flag_and == 1){
        $tmp1 = $tmp1 . ', ';
        $tmp2 = $tmp2 . ', ';
      }
      $ans = $&;
      $tmp1 = $tmp1 . $ans;

      $g_line_buf =~ /(?<=@($g_id)=).*/;
      $ans = $&;
      $tmp2 = $tmp2 . $ans;
      $g_flag_and = 1;
      $tmp1 =~ s/@($g_id)/$g_schema[$g_id-1]/;
      $g_id++;
    }
    else{
      $g_out_buf = $g_out_buf . '(' . $tmp1 . ') values(' . $tmp2 . ')';
      #print OUTPUT ("$g_out_buf;\n");
      $memPool[$poolSize] = $g_out_buf . ";\n";
      $poolSize++;
      $g_flag_read = 1;
      last;
    }
  }
}

sub Process_Update{
  $g_line_buf =~ /(?<=^### ).*/;
  $g_out_buf = $& . " SET ";
  $g_line_buf =~ /(?<=^### UPDATE ).*/;
  $g_DBname_TBname = $&;
  $g_line_buf = <INPUT>;
	
  my (@t_n) = split(/\./, $g_DBname_TBname);

  if(!exists($g_hash_schema{$g_DBname_TBname})){
    &GetTableField($t_n[0], $t_n[1]);
    @{$g_hash_schema{$g_DBname_TBname}} = @g_schema;
  }
  else{
    @g_schema = @{$g_hash_schema{$g_DBname_TBname}};
  }

  $g_flag_and = 0;
  $g_id = 1;
  while($g_line_buf = <INPUT>){
    if($g_line_buf =~ /(?<=^###   ).*/){
      if($g_flag_and == 1){
        $g_out_buf = $g_out_buf . ", "
      }
      $ans = $&;
      $ans =~ s/@($g_id)/$g_schema[$g_id-1]/;
      $g_out_buf = $g_out_buf . $ans;
      $g_flag_and = 1;
      $g_id++;
    }
    else{
      $g_out_buf = $g_out_buf . " WHERE ";
      last;
    }
  }

  $g_flag_and = 0;
  $g_id = 1;
  while($g_line_buf = <INPUT>){
    if($g_line_buf =~ /(?<=^###   ).*/){
      if($g_flag_and == 1){
        $g_out_buf = $g_out_buf . " AND "
      }
      $ans = $&;
      $ans =~ s/@($g_id)/$g_schema[$g_id-1]/;
      if($ans =~ /.*=NULL/){
        $ans =~ s/=NULL/ is NULL/;
      }
      $g_out_buf = $g_out_buf . $ans;
      $g_flag_and = 1;
      $g_id++;
    }
    else{
      #print OUTPUT ("$g_out_buf;\n");
      $memPool[$poolSize] = $g_out_buf . ";\n";
      $poolSize++;
      $g_flag_read = 1;
      last;
    }
  }
}

sub get_white_cnf() {
  open(INPUT, "$Bin/../conf/white.ini") || die("failed");
  my $cnf_flag=-1;

  while($g_line_buf = <INPUT>) {
    if (!defined $g_line_buf or $g_line_buf =~ /^\s*$/) {
	next;
    }
    if ($g_line_buf =~ /^### IGNORE DDL*/) {
        $cnf_flag = 0;
	next;
    } elsif ($g_line_buf =~ /^### IGNORE SQL*/) {
        $cnf_flag = 1;
	next;
    }
    if ($cnf_flag == 0) {
	 $DDL_white{$g_line_buf} = 1;
    } elsif ($cnf_flag == 1) {
	 $QUERY_white{$g_line_buf} = 1;
    }
  }
  close(INPUT);
}

sub set_db_conn_info {
   ($host, $port) = @_;
    
   get_white_cnf();
}

sub parse_sql {
  my ($file_tmp, $cnt) = @_;
  $g_flag_read = 0;
  open(INPUT, "$file_tmp") || die("failed");
  open(OUTPUT, ">$Bin/../tmp/tmp1_$cnt.sql");
$ddsn="DBI:mysql:test:$host:$port";  
$ddbh = DBI->connect($ddsn, $RollbackConst::Local_User, $RollbackConst::Local_Pwd,  {RaiseError => 1});

  $poolSize = 0;
  undef %g_hash_schema;
  undef @memPool;

  while(1){
    if($g_flag_read != 1){
      $g_line_buf = <INPUT>;
    }
    if($g_line_buf =~ /^###.*/){
      if($g_line_buf =~ /^### INSERT.*/){
        &Process_Insert();
      }
      elsif($g_line_buf =~ /^### DELETE.*/){
        &Process_Delete();
      }
      elsif($g_line_buf =~ /^### UPDATE.*/){
        &Process_Update();
      }
    }
    else{
      #print OUTPUT ("$g_line_buf");
      $memPool[$poolSize] = $g_line_buf;
      $poolSize++;
      $g_flag_read = 0;
    }

    if(eof){
      print ("| ParseSQL |\n");
      last;
    }
  }

  print OUTPUT @memPool;

  close(INPUT);
  close(OUTPUT);
$ddbh->disconnect();
undef %g_hash_schema;
undef @memPool;
}


sub reverse_transaction {
  my ($cnt) = @_;
  open(INPUT, "$Bin/../tmp/tmp1_$cnt.sql") || die("failed");
  open(OUTPUT, ">$Bin/../tmp/tmp2_$cnt.sql");

  my (@g_stack);
  my ($top_point) = 0;

  $g_stack[0] = "";
  my ($g_charset) = "";

  while($g_line_buf = <INPUT>){
    if($g_line_buf =~ /^BEGIN.*/){
      if($g_stack[$top_point] =~ /^#.*/){
        $g_stack[$top_point] = $g_stack[$top_point] . $g_line_buf;
      }
      else{
        $g_stack[$top_point] = $g_charset . $g_stack[$top_point];
        $top_point++;
        $g_stack[$top_point] = $g_line_buf;
      }
    }
    elsif($g_line_buf =~ /^COMMIT.*/){
      # in case of COMMIT and /*!*/; not in the same line
      if ($g_line_buf !~ /^COMMIT.+/) {
        $g_line_buf = $g_line_buf . "/*!*/;\n";
      }

      $g_stack[$top_point] = $g_stack[$top_point] . $g_line_buf;
      $g_stack[$top_point] = $g_charset . $g_stack[$top_point];
      $top_point++;
      $g_stack[$top_point] = "";
    }
    else{
      if($g_line_buf =~ /^SET \@\@session.character_set_client.*/){
        $g_charset = $g_line_buf;
      }
      else{
        $g_stack[$top_point] = $g_stack[$top_point] . $g_line_buf;
      }
    }
    if(eof){
      last;
    }
  }

  my ($tmp);

  $tmp = $top_point;
  print OUTPUT ("$g_stack[0]");
  $top_point--;
  while($top_point > 0){
    print OUTPUT ("$g_stack[$top_point]");
    $top_point--;
  }
  if($tmp != 0){
    print OUTPUT ("$g_stack[$tmp]");
  }
  close(INPUT);
  close(OUTPUT);
  
  printf("| ReverseTransction |\n");
undef @g_stack;;

}

sub reverse_sql {
  my ($cnt) = @_;

  open(INPUT, "$Bin/../tmp/tmp2_$cnt.sql") || die("failed");
  open(OUTPUT, ">$Bin/../log/rollback_$cnt.sql");

  my (@g_stack);
  my ($top_point) = 0;
  $g_stack[0] = "";
  my ($g_flag) = 0;


  # disable binlog of rollback for session
  print OUTPUT "SET sql_log_bin=0;\n";

  while($g_line_buf = <INPUT>){
    if($g_line_buf =~ /^BEGIN.*/){
      $g_flag = 1;
      print OUTPUT ("$g_line_buf");
      $g_line_buf = <INPUT>;
      print OUTPUT ("$g_line_buf");

      $g_stack[$top_point = 0] = "";
    }
    elsif($g_line_buf =~ /^COMMIT.*/){
      $g_flag = 0;
      while($top_point >= 0){
        print OUTPUT ("$g_stack[$top_point]");
        $top_point--;
      }
      print OUTPUT ("$g_line_buf");
    }
    else{
	    for (keys %QUERY_white) {
		    if ($g_line_buf =~ /^$_*/) {
			    next;
		    }
	    }
      if($g_flag == 1){
        if($g_line_buf =~ /^#.*/){
          $g_stack[$top_point] = $g_stack[$top_point] . $g_line_buf;
        }
        else{
          $g_stack[$top_point] = $g_stack[$top_point] . $g_line_buf;
          $top_point++;
          $g_stack[$top_point] = "";
        }
      }
      else{
        print OUTPUT ("$g_line_buf");
      }
    }
    if(eof){
      last;
    }
  }

  close(INPUT);
  close(OUTPUT);

  printf("| ReverseSQL |\n");
undef @memPool;
}

sub filter{
  my ($cnt, $name) = @_;

  open(INPUT, "$Bin/../log/rollback_$cnt.sql") || die("failed");
  open(OUTPUT, ">$Bin/../log/rollback_${cnt}_2.sql");

  while($g_line_buf = <INPUT>){
    if ($g_line_buf =~ /^(INSERT|DELETE|UPDATE).*$name.*/){
      ;
    }
    else{
      $g_line_buf = "";
    }
    print OUTPUT ("$g_line_buf");
  }
  close(INPUT);
  close(OUTPUT);
  system("rm $Bin/../log/rollback_$cnt.sql");
  system("mv $Bin/../log/rollback_${cnt}_2.sql $Bin/../log/rollback_$cnt.sql");
  printf("| FilterTable |\n");
}

sub filter_special{
  my ($cnt, $name) = @_;

  open(INPUT, "$Bin/../log/rollback_$cnt.sql") || die("failed");
  open(OUTPUT, ">$Bin/../log/rollback_${cnt}_2.sql");

  while($g_line_buf = <INPUT>){
    if ($g_line_buf =~ /^$name.*/){
      ;
    }
    else{
      $g_line_buf = "";
    }
    print OUTPUT ("$g_line_buf");
  }
  close(INPUT);
  close(OUTPUT);
  system("rm $Bin/../log/rollback_$cnt.sql");
  system("mv $Bin/../log/rollback_${cnt}_2.sql $Bin/../log/rollback_$cnt.sql");
  printf("| FilterSpecial |\n");
}

sub apply_sql{
  my ($cnt) = @_;
  $cmd = 'sed -i "s/\/\*\!\*\///g"';
  system(" $cmd $Bin/../log/rollback_$cnt.sql");

  open(INPUT, "$Bin/../log/rollback_$cnt.sql") || die("failed");
  #open(ERRORPUT, "$Bin/../log/report_$cnt.log");

$ddsn="DBI:mysql:test:$host:$port";
$ddbh = DBI->connect($ddsn, $user, $password,  {PrintError => 1});

  while($g_line_buf = <INPUT>){
    chomp($g_line_buf);
    if($g_line_buf =~ /^#.*/){
      next;
    }
    if($g_line_buf eq ";"){
      next;
    }
    if($g_line_buf =~ /^DELIMITER.*/){
      next;
    }
    if($g_line_buf =~ /^\/\*\!.*/){
      next;
    }

    my $drv = $ddbh->do($g_line_buf);
    if( not defined $drv){
      next;
    }
    $drv = $drv + 0;
    if ($drv == 0){
      if($g_line_buf =~ /^BEGIN|^COMMIT|^SET/){
        next;
      }
      print ERRORPUT "ROW affected 0 : $g_line_buf\n\n";
    }
  }
$ddbh->disconnect();
  printf("| ApplySQL |\n");
  close(INPUT);
  close(ERRORPUT);
}

sub exist_ddl {
  my ($cnt) = @_;
  my ($i);

  open(INPUT, "$Bin/../log/rollback_$cnt.sql") || die("Can not findd rollback_$cnt.sql!");

  while($g_line_buf = <INPUT>){
    if($g_line_buf =~ /^create/i || $g_line_buf =~ /^alter/i
      || $g_line_buf =~ /^drop/i || $g_line_buf =~ /^truncate/i){
      if (!exists($DDL_white{$g_line_buf})) {
	      close(INPUT);
        print "Exist DDL query: $g_line_buf";
	      return 1;
      }
    }
    if(eof){
      last;
    }
  }
  close(INPUT);

  return 0;
}


1;





