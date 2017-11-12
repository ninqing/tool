#!/usr/bin/perl -w 
# Creator  : zhuxu@taobao.com  
# My Blog  : http://chenxu.yo2.cn/
# Func     : get system info (such as: LOAD/CPU/IO/MEMORY)
# Version  :

use strict;
use Getopt::Long;                             # Usage Info URL:  http://perldoc.perl.org/Getopt/Long.html
use POSIX qw(strftime);                       # Usage Info URL:  http://perldoc.perl.org/functions/localtime.html
use Term::ANSIColor;                          # Usage Info URL:  http://perldoc.perl.org/Term/ANSIColor.html
use Socket;                                   # Get IP info
#use DBI;

# ?? God!!! I can't install it using the way behind! HELP ??
# ----------------------------------------------------------------------------------------
# How To Install "GD::Graph::lines" :
# first : need to install "GD::Text"
#         (1) wget http://search.cpan.org/CPAN/authors/id/L/LD/LDS/GD-2.45.tar.gz
#         (2) tar -zxvf GD-2.45.tar.gz
#         (3) cd GD-2.45
#         (4) perl Makefile.PL && make && make test && make install
# second: need to install "GD::Text"
#         wget http://search.cpan.org/CPAN/authors/id/M/MV/MVERB/GDTextUtil-0.86.tar.gz
# third : need to install "GD::Graph::lines" 
#         wget http://search.cpan.org/CPAN/authors/id/B/BW/BWARFIELD/GDGraph-1.44.tar.gz
# ----------------------------------------------------------------------------------------
#use GD::Graph::lines;
Getopt::Long::Configure qw(no_ignore_case);   #

# ----------------------------------------------------------------------------------------
# Variables
# ----------------------------------------------------------------------------------------
our  %opt;               # Get options info
our  $headline1 = '';
our  $headline2 = '';
our  $mysql_headline1 = '';
our  $mysql_headline2 = '';
our  $mycount = 0;      # to control the print of headline
# Options Flag
#----->
our $timeFlag = 0;      # -t   : print current time
our $interval = 1;      # -i   : time(second) interval  
our $load     = 0;      # -l   : print load info
our $cpu      = 0;      # -c   : print cpu  info
our $swap     = 0;      # -s   : print swap info
our $disk  ;            # -d   : print disk info
our $mysql = 0;         # -m   : print mysql status
our $count ;            # -C   : times
#<-----

# Variables For :
#-----> Get SysInfo (from /proc/stat): CPU 
our @sys_cpu1   = (0)x8;
our $total_1    = 0;
#
our $user_diff   ; 
our $system_diff ; 
our $idle_diff   ; 
our $iowait_diff ; 
#<----- Get SysInfo (from /proc/stat): CPU 

#-----> Get SysInfo (from /proc/diskstats): IO
our @sys_io1   = (0)x15;
#our $not_first  = 0;                                   # no print first value
our $ncpu = `grep processor /proc/cpuinfo | wc -l`;     #/* Number of processors */
# grep "HZ" -R /usr/include/*
# /usr/include/asm-x86_64/param.h:#define HZ 100  
our $HZ = 100;           
#<----- Get SysInfo (from /proc/diskstats): IO

#-----> Get SysInfo (from /proc/vmstat): SWAP 
our %swap1 =
(
        "pswpin"  => 0,
        "pswpout" => 0
);
our $swap_not_first = 0;
#<----- Get SysInfo (from /proc/vmstat): SWAP 

#-----> Get Mysql Status
our %mystat1 =
(
        "Com_select" => 0 , 
        "Com_delete" => 0 , 
        "Com_update" => 0 , 
        "Com_insert" => 0,
        "Innodb_buffer_pool_read_requests" => 0
);
our $not_first  = 0;
#<----- Get Mysql Status

# autoflush
$| = 1;

# ----------------------------------------------------------------------------------------
# 0.
# Main()
# ----------------------------------------------------------------------------------------

# Get options info
&get_options();

while(1) {
	# -C;Times to exits
	if( defined ($count) and $mycount > $count ) {
		exit;
	}
        # Print Headline
        if ( $mycount%15 == 0 ) {
                print color("blue bold"),"$headline1",color("reset");
                print color("ON_BLUE green"),"$mysql_headline1",color("reset") if $mysql;
                print "\n";
                print color("blue underline bold"),"$headline2",color("reset");
                print color("green underline"),"$mysql_headline2",color("blue bold"),"|",color("reset") if $mysql;
                print "\n";
        }

        $mycount += 1;

        # (1) Print Current Time
        if($timeFlag){
                print color 'yellow';
                my $nowTime = strftime "%H:%M:%S", localtime;
                print "$nowTime",color("blue bold"),"|",color("reset");
        }

        # (2) Print SysInfo  
        &get_sysinfo();

        # (3) Print MySQL Status  
        &get_mysqlstat();

        #
        print "\n";
        sleep($interval);
}


# ----------------------------------------------------------------------------------------
# 1.
# Func :  print usage
# ----------------------------------------------------------------------------------------
sub print_usage{
        print color("blue bold"),<<EOF,color("reset");

==================================================
Info  :
        Created By ZhuXu
Usage :
Command line options :
   -i,--interval   Time(second) Interval.  
   -h,--help       Print Help Info. 
   -t,--time       Print The Current Time.
   -sys            Print SysInfo (include -l,-c) .
   -l,--load       Print Load Info.
   -c,--cpu        Print Cpu  Info.
   -d,--disk       Print Disk Info.
   -s,--swap       Print Swap Info.

   -m,--mysql      Print MySQL Status.
   -C,--count      Times. 

   -lazy           Print Info (include -t,-l,-c,-m,-s). 
Sample :
        shell> ./orzdba -sys -d sda -t -m -i 2
==================================================

EOF
        exit;   
}

# ----------------------------------------------------------------------------------------
# 2.
# Func : get options and set option flag 
# ----------------------------------------------------------------------------------------
sub get_options{
        # Get options info
        GetOptions(\%opt,
                        'h|help',           # OUT : print help info   
                        'i|interval=i',     # IN  : time(second) interval  
                        't|time',           # OUT : print current time
                        'sys',              # OUT : print SysInfo (include -l,-c,-s) 
                        'l|load',           # OUT : print load info
                        'c|cpu',            # OUT : print cpu  info
                        'd|disk=s',         # IN  : print disk info
                        's|swap',           # OUT : print swap info
                        'm|mysql',          # OUT : print mysql status
                        'C|count=i',        # IN  : times 
                        'lazy'                  # OUT : Print Info (include -t,-l,-c,-m,-s). 
                  ) or print_usage();

        #-----> Just to print option informations for debugging
#       my $key;
#       my $value;
#       while ( ($key,$value) = each %opt ){
#               print color("blue");
#               printf "%-3s => %s\n",$key,$value;
#               print color("reset");
#       }
        #<----- Just to print option informations for debugging
        if (!scalar(%opt)) {
                &print_usage();
        }

        #-----> Just to Print 
        print color("green");
        print <<EOF;

.=================================================.
|       Welcome to use the orzdba tool !          | 
|          Yep...Chinese English~                 |
EOF
        print color("green"),"'=============== ";
        print color("red"),"Date : ",strftime ("%Y-%m-%d", localtime);
        print color("green")," ==============='"."\n\n";
        print color("reset");
        #<----- Just to print

        # Handle for options
        # -h
        $opt{'h'} and print_usage();
        # -i
        $opt{'i'} and $interval = $opt{'i'};
        # -t
        #$timeFlag = 1 if $opt{'T'};
        $opt{'t'} and $timeFlag = 1;
        # -sys (include -l,-c) 
        #$opt{'sys'} and $sysinfo = 1;
        $opt{'sys'} and $load= 1 and $cpu=1;
        # -i
        $opt{'l'} and $load = 1;
        # -c
        $opt{'c'} and $cpu = 1;
        # -d
        $opt{'d'} and $disk = $opt{'d'};
        # -m
        $opt{'m'} and $mysql = 1;
        # -s
        $opt{'s'} and $swap = 1;
        # -lazy (include -t,-l,-c,-m,-s)
        $opt{'lazy'} and $timeFlag = 1 and $load=1 and $cpu=1 and $swap = 1 and $mysql=1;
        # -C
        $opt{'C'} and $count = $opt{'C'};

        # Get Hostname and IP 
        chomp (my $hostname = `hostname` ); 
        my $ip = inet_ntoa((gethostbyname($hostname))[4]);
        print color("red"), "HOST: ",color("yellow"),$hostname,color("red"),"   IP: ",color("yellow"),$ip,color("reset")."\n";
        # Get MYSQL DB Name and Variables
        if ($mysql) {
                my $mysqldb_sql = qq{mysql -s --skip-column-names -uroot -e 'show databases' | grep -ivE "information_schema|mysql|test" | tr "\n" "|"};
                my $db_name = `$mysqldb_sql`;
                chop($db_name);
                print color("red"),"DB  : ",color("yellow"),$db_name,color("reset")."\n";

                # Get MySQL Variables
                my $mysql = qq{mysql -s --skip-column-names -uroot -e 'show variables where Variable_name in ("innodb_flush_log_at_trx_commit","innodb_flush_method","innodb_buffer_pool_size")'};
                open MYSQL_VARIABLES,"$mysql|" or die "Can't connect to mysql!";
                print color("red"),"Var : ",color("reset");
                while (my $line = <MYSQL_VARIABLES>) {
                        chomp($line);
                        my($key,$value) = split(/\s+/,$line);
                        if ($key eq 'innodb_buffer_pool_size') {
                                print color("MAGENTA"),"$key",color("white"),"[";
                                $value/1024/1024/1024>=1 ? print $value/1024/1024/1024,"G" : ($value/1024/1024>1 ? print $value/1024/1024,"M" : print $value) ; 
                                print "] ",color("reset");
                        } else {
                                print color("MAGENTA"),"$key",color("white"),"[$value] ",color("reset");
                        }   
                }   
                close MYSQL_VARIABLES or die "Can't close!";
                print "\n";

        }
        print "\n";


        # Init Headline
        if($timeFlag){
                $headline1 = "-------- ";
                $headline2 = "  time  |"; 
        }
        if($load){
                $headline1 .= "-----load-avg---- ";
                $headline2 .= "  1m    5m   15m |";
        }
        if($cpu){
                $headline1 .= "---cpu-usage--- ";
                $headline2 .= "usr sys idl iow|";
        }
        if($swap){
                $headline1 .= "---swap--- ";
                $headline2 .= "   si   so|";
        }
        if($disk){
                $headline1 .= "-----------------io-usage--------------- ";
                $headline2 .= "  rkB/s   wkB/s  queue await svctm \%util|";
        }
        if($mysql){
                $mysql_headline1 .= "                   -QPS- -TPS-        -Hit%- ";
                $mysql_headline2 .= "  ins   upd   del   sel   iud     lor  hit  ";
        }
}

# ----------------------------------------------------------------------------------------
# 3.
# Func : get sys performance info
# ----------------------------------------------------------------------------------------
sub get_sysinfo{
        # 1. Get SysInfo (from /proc/loadavg): Load
        if($load){
                open PROC_LOAD,"</proc/loadavg" or die "Can't open file(/proc/loadavg)!";
                if ( defined (my $line = <PROC_LOAD>) ){ 
                        chomp($line);
                        #print $line;
                        my @sys_load = split(/\s+/,$line); 
                        # print "$sys_load[0] $sys_load[1] $sys_load[2]",color("blue bold"),"|",color,("reset");
                        $sys_load[0]>$ncpu ? print color("red") : print color("white");
                        printf "%5.2f",$sys_load[0] and print color("reset");
                        $sys_load[1]>$ncpu ? print color("red") : print color("white");
                        printf " %5.2f",$sys_load[1] and print color("reset");
                        $sys_load[2]>$ncpu ? print color("red") : print color("white");
                        printf " %5.2f",$sys_load[2] and print color("reset");
                        print color("blue bold"),"|",color("reset");
                }
                close PROC_LOAD or die "Can't close file(/proc/loadavg)!";
        }
        # 2. Get SysInfo (from /proc/stat): CPU 
        if($cpu or $disk) {
                open PROC_CPU,"</proc/stat" or die "Can't open file(/proc/stat)!";
                if ( defined (my $line = <PROC_CPU>) ){ # use "if" instead of "while" to read first line 
                        chomp($line);
                        my @sys_cpu2 = split(/\s+/,$line); 
                        # line format :     (http://blog.csdn.net/nineday/archive/2007/12/11/1928847.aspx)
                        # cpu   1-user  2-nice  3-system 4-idle   5-iowait  6-irq   7-softirq  
                        # cpu   628808  1642    61861    24978051 22640     349     3086        0
                        my $total_2 =$sys_cpu2[1]+$sys_cpu2[2]+$sys_cpu2[3]+$sys_cpu2[4]+$sys_cpu2[5]+$sys_cpu2[6]+$sys_cpu2[7];

                        # my $user_diff   = int ( ($sys_cpu2[1] - $sys_cpu1[1]) / ($total_2 - $total_1) * 100 + 0.5 );
                        # my $system_diff = int ( ($sys_cpu2[3] - $sys_cpu1[3]) / ($total_2 - $total_1) * 100 + 0.5 );
                        # my $idle_diff   = int ( ($sys_cpu2[4] - $sys_cpu1[4]) / ($total_2 - $total_1) * 100 + 0.5 );
                        # my $iowait_diff = int ( ($sys_cpu2[5] - $sys_cpu1[5]) / ($total_2 - $total_1) * 100 + 0.5 );
                        #printf "%3d %3d %3d %3d",$user_diff,$system_diff,$idle_diff,$iowait_diff;

                        $user_diff        = $sys_cpu2[1] + $sys_cpu2[2] - $sys_cpu1[1] - $sys_cpu1[2] ;
                        $system_diff = $sys_cpu2[3] + $sys_cpu2[6] + $sys_cpu2[7] - $sys_cpu1[3] - $sys_cpu1[6] - $sys_cpu1[7];
                        $idle_diff        = $sys_cpu2[4] - $sys_cpu1[4] ;
                        $iowait_diff      = $sys_cpu2[5] - $sys_cpu1[5] ;
                        my $user_diff_1   = int ( $user_diff / ($total_2 - $total_1) * 100 + 0.5 );                
                        my $system_diff_1 = int ( $system_diff / ($total_2 - $total_1) * 100 + 0.5 );
                        my $idle_diff_1   = int ( $idle_diff / ($total_2 - $total_1) * 100 + 0.5 );
                        my $iowait_diff_1 = int ( $iowait_diff / ($total_2 - $total_1) * 100 + 0.5 );

                        if ($cpu) {
                                # printf "%3d %3d %3d %3d",$user_diff_1,$system_diff_1,$idle_diff_1,$iowait_diff_1;
                                $user_diff_1>10   ? print color("red") : print color("green");
                                printf "%3d",$user_diff_1 and print color("reset");
                                $system_diff_1>10   ? print color("red") : print color("white");
                                printf " %3d",$system_diff_1 and print color("reset");
                                print color("white") ;
                                printf " %3d",$idle_diff_1;
                                $iowait_diff_1>10 ? print color("red") : print color("green");
                                printf " %3d",$iowait_diff_1;
                                # if ($iowait_diff_1>10) {
                                #       print color("red");
                                #       printf "%3d",$iowait_diff_1;
                                # } else {
                                #       print color("green");
                                #       printf "%3d",$iowait_diff_1;
                                # }
                                print color("blue bold"),"|",color("reset");
                        }

                        # Keep Last Status
                        # print @sys_cpu1; print '<->';
                        @sys_cpu1 = @sys_cpu2;
                        $total_1  = $total_2;
                        # print @sys_cpu2;
                }
                close PROC_CPU or die "Can't close file(/proc/stat)!";
        }

        # 3. Get SysInfo (from /proc/vmstat): SWAP
        # Detail Info : http://www.linuxinsight.com/proc_vmstat.html
        if($swap) {
                my %swap2;
                open PROC_VMSTAT,"cat /proc/vmstat | grep -E \"pswpin|pswpout\" |" or die "Can't open file(/proc/vmstat)!";
                while (my $line = <PROC_VMSTAT>) {
                        chomp($line);
                        my($key,$value) = split(/\s+/,$line);
                        $swap2{"$key"}= $value;
                }   
                if ($swap_not_first) {
                        ($swap2{"pswpin"} - $swap1{"pswpin"})>0  ? print color("red") : print color("white");
                        printf " %4d",($swap2{"pswpin"} - $swap1{"pswpin"})/$interval;
                        ($swap2{"pswpout"} - $swap1{"pswpout"})>0 ? print color("red") : print color("white");
                        printf " %4d",($swap2{"pswpout"} - $swap1{"pswpout"})/$interval;
                        print color("blue bold"),"|",color("reset");
                } else {
                        print color("white");
                        printf " %4d %4d",0,0;
                        print color("blue bold"),"|",color("reset");
                }   

                # Keep Last Status
                %swap1 = %swap2;
                $swap_not_first += 1;
        }


        # 4. Get SysInfo (from /proc/diskstats): IO 
        if($disk) {
                # Detail IO Info :
                # (1) http://www.mjmwired.net/kernel/Documentation/iostats.txt
                # (2) http://www.linuxinsight.com/iostat_utility.html
                # (3) source code --> http://www.linuxinsight.com/files/iostat-2.2.tar.gz
                my $deltams = 1000.0 * ( $user_diff + $system_diff + $idle_diff + $iowait_diff ) / $ncpu / $HZ ;
                # Shell Command : cat /proc/diskstats  | grep "\bsda\b"
                open PROC_IO,"cat /proc/diskstats  | grep \"\\b$disk\\b\" |" or die "Can't open file(/proc/diskstats)!";
                if ( defined (my $line = <PROC_IO>) ) {
                        chomp($line);
                        # iostat --> line format :
                        # 0               1        2        3     4      5        6     7        8          9      10     11   
                        # Device:         rrqm/s   wrqm/s   r/s   w/s    rkB/s    wkB/s avgrq-sz avgqu-sz   await  svctm  %util
                        # sda               0.05    12.44  0.42  7.60     5.67    80.15    21.42     0.04    4.63   0.55   0.44
                        my @sys_io2 = split(/\s+/,$line);

                        my $rd_ios     = $sys_io2[4]  - $sys_io1[4];  #/* Read I/O operations */
                        my $rd_merges  = $sys_io2[5]  - $sys_io1[5];  #/* Reads merged */ 
                        my $rd_sectors = $sys_io2[6]  - $sys_io1[6];  #/* Sectors read */ 
                        my $rd_ticks   = $sys_io2[7]  - $sys_io1[7];  #/* Time in queue + service for read */ 
                        my $wr_ios     = $sys_io2[8]  - $sys_io1[8];  #/* Write I/O operations */ 
                        my $wr_merges  = $sys_io2[9]  - $sys_io1[9];  #/* Writes merged */ 
                        my $wr_sectors = $sys_io2[10] - $sys_io1[10]; #/* Sectors written */
                        my $wr_ticks   = $sys_io2[11] - $sys_io1[11]; #/* Time in queue + service for write */
                        my $ticks      = $sys_io2[13] - $sys_io1[13]; #/* Time of requests in queue */
                        my $aveq       = $sys_io2[14] - $sys_io1[14]; #/* Average queue length */

                        my $n_ios;        #/* Number of requests */
                        my $n_ticks;      #/* Total service time */
                        my $n_kbytes;     #/* Total kbytes transferred */
                        my $busy;         #/* Utilization at disk       (percent) */
                        my $svc_t;        #/* Average disk service time */
                        my $wait;         #/* Average wait */
                        my $size;         #/* Average request size */
                        my $queue;        #/* Average queue */
                        $n_ios    = $rd_ios + $wr_ios;
                        $n_ticks  = $rd_ticks + $wr_ticks;
                        $n_kbytes = ( $rd_sectors + $wr_sectors) / 2.0;
                        $queue    = $aveq/$deltams;
                        $size     = $n_ios ? $n_kbytes / $n_ios : 0.0;
                        $wait     = $n_ios ? $n_ticks / $n_ios : 0.0;
                        $svc_t    = $n_ios ? $ticks / $n_ios : 0.0;
                        $busy     = 100.0 * $ticks / $deltams;  #/* percentage! */
                        if ($busy > 100.0) {
                                $busy = 100.0;
                        }
                        #
                        my $rkbs     = (1000.0 * $rd_sectors/$deltams /2) ;
                        my $wkbs     = (1000.0 * $wr_sectors/$deltams /2) ;

                        # printf "%7.1f %7.1f %5.1f %6.1f %5.1f %5.1f",$rkbs,$wkbs,$queue,$wait,$svc_t,$busy ;
                        # color print wait/svc_t/busy info
                        $rkbs > 1024 ? print color("red") : print color("white");
                        printf "%7.1f",$rkbs and print color("reset");
                        $wkbs > 1024 ? print color("red") : print color("white");
                        printf " %7.1f",$wkbs and print color("reset");
                        print color("white") ;
                        printf " %5.1f",$queue;
                        $wait>5  ? print color("red") : print color("green");
                        printf " %6.1f",$wait and print color("reset");
                        $svc_t>5 ? print color("red") : print color("white");
                        printf " %5.1f",$svc_t and print color("reset");
                        $busy>80 ? print color("red") : print color("green");
                        printf " %5.1f",$busy and print color("reset");
                        print color("blue bold"),"|",color("reset");

                        # Keep Last Status  
                        @sys_io1 = @sys_io2;

                        close PROC_IO or die "Can't close file(/proc/diskstats)!";
                } else {
                        print color("red");
                        print "\nERROR! Please set the right disk info!\n"; 
                        print color("reset");
                        exit;
                }
        }


        # END !
}


# ----------------------------------------------------------------------------------------
# 4.
# Func : get mysql status
# ----------------------------------------------------------------------------------------
sub get_mysqlstat{
        if($mysql) {
                my %mystat2 ;
                my $mysql = qq{mysql -s --skip-column-names -uroot -e 'show global status where Variable_name in ("Com_select","Com_insert","Com_update","Com_delete","Innodb_buffer_pool_read_requests","Innodb_buffer_pool_reads")'};
                #print color("yellow"),$mysql,color("reset");
                open MYSQL_STAT,"$mysql|" or die "Can't connect to mysql!";
                while (my $line = <MYSQL_STAT>) {
                        chomp($line);
                        my($key,$value) = split(/\s+/,$line);
                        $mystat2{"$key"}=$value;
                }
                close MYSQL_STAT or die "Can't close!";

                if ($not_first) {
                        my $insert_diff = ( $mystat2{"Com_insert"} - $mystat1{"Com_insert"} ) / $interval;
                        my $update_diff = ( $mystat2{"Com_update"} - $mystat1{"Com_update"} ) / $interval;
                        my $delete_diff = ( $mystat2{"Com_delete"} - $mystat1{"Com_delete"} ) / $interval;
                        my $select_diff = ( $mystat2{"Com_select"} - $mystat1{"Com_select"} ) / $interval;
                        my $read_request = ( $mystat2{"Innodb_buffer_pool_read_requests"} - $mystat1{"Innodb_buffer_pool_read_requests"} ) / $interval;
                        my $read         = ( $mystat2{"Innodb_buffer_pool_reads"} - $mystat1{"Innodb_buffer_pool_reads"} ) / $interval;

                        print color ("white");
                        # Com_insert # Com_update # Com_delete
                        printf "%5d %5d %5d",$insert_diff,$update_diff,$delete_diff;
                        print color("yellow");
                        # Com_select
                        printf " %5d",$select_diff;
                        # Total TPS
                        printf " %5d",$insert_diff+$update_diff+$delete_diff;
                        # Innodb_buffer_pool_read_requests
                        print color ("white");
                        printf " %7d",$read_request;
                        # Hit% : (Innodb_buffer_pool_read_requests - Innodb_buffer_pool_reads) / Innodb_buffer_pool_read_requests * 100%
                        if ($read_request) {
                                my $hit = ($read_request-$read)/$read_request*100;
                                $hit>99 ? print color("green") : print color("red");
                                printf " %6.2f",$hit;
                        } else {
                                print color("green")," 100.00";
                        }

                        print color("blue bold"),"|",color("reset");
                } else{
                        print color("white"),"    0     0     0     0     0       0 100.00";
                        print color("blue bold"),"|",color("reset");
                }

                # Keep Last Status      
                %mystat1 = %mystat2;
                $not_first += 1;
        }
}









