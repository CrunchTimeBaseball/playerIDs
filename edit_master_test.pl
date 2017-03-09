#!/usr/bin/perl

use strict;
use DBI();
use DBD::mysql;
use filehandle;

my @row; my $row; my @mlb_id; my @mlb_name; my @mlb_pos; my @mlb_team; my @mlb_team_long; my @bats; my @throws; my @birth_year; my @bp_id; my @bref_id; my @bref_name; my @cbs_id; my @cbs_name; my @cbs_pos; my @espn_id; my @espn_name; my @espn_pos; my @fg_id; my @fg_name; my @lahman_id; my @nfbc_id; my @nfbc_name; my @nfbc_pos; my @retro_id; my @retro_name; my @debut; my @yahoo_id; my @yahoo_name; my @yahoo_pos; my @mlb_depth; my $mlb_id; my $mlb_name; my $mlb_pos; my $mlb_team; my $mlb_team_long; my $bats; my $throws; my $birth_year; my $bp_id; my $bref_id; my $bref_name; my $cbs_id; my $cbs_name; my $cbs_pos; my $espn_id; my $espn_name; my $espn_pos; my $fg_id; my $fg_name; my $lahman_id; my $nfbc_id; my $nfbc_name; my $nfbc_pos; my $retro_id; my $retro_name; my $debut; my $yahoo_id; my $yahoo_name; my $yahoo_pos; my $mlb_depth;
my @y_mlb_id; my @y_mlb_name; my @y_yahoo_id; my @y_yahoo_name; my @y_yahoo_pos; my $y_mlb_id; my $y_mlb_name; my $y_yahoo_id; my $y_yahoo_name; my $y_yahoo_pos; my @new_yahoo_id; my @new_yahoo_name; my @new_yahoo_pos; my $new_yahoo_id; my $new_yahoo_name; my $new_yahoo_pos;my $count1=0; my $count2=0, my $i; my $j; my $new_master_query; 

my $sth7; my $sth4; my $sth3; my $sth10; my $sth13; my $sth16; my $sth19; my $sth22; my $sth25;

my @new_mlb_id; my @new_mlb_name; my @new_mlb_team; my @new_mlb_team_long; my @new_mlb_pos; my @new_hands_id; my @new_hands_name; my @new_bats; my @new_throws; my @new_birth_year;
 my $count3=0; my $count4=0; my $count5=0; my $count6=0; my $count7=0; my $count8=0; my $count9=0; my $count10=0; my $count11=0; my $count12=0; my $count13=0; my $count14=0; my $count15=0; my $count16=0; my $count17=0; my $count18=0; my $replace;

my @new2_mlb_id; my @new2_mlb_name; my @new2_bref_id; my @new2_bref_name; 
my @new3_mlb_id; my @new3_mlb_name; my @new3_cbs_id; my @new3_cbs_name; my @new3_cbs_pos; 
my @new4_mlb_id; my @new4_mlb_name; my @new4_espn_id; my @new4_espn_name; my @new4_espn_pos;
my @new5_mlb_id; my @new5_mlb_name; my @new5_fg_id; my @new5_fg_name;
my @new6_mlb_id; my @new6_mlb_name; my @new6_nfbc_id; my @new6_nfbc_name; my @new6_nfbc_pos;
my $sec; my $min; my $hour; my $mday; my $mon; my $year; my $wday; my $yday; my $isdst;

my $dbh = DBI->connect("DBI:mysql:database='schema_name';host=localhost","username", "password", {'RaiseError' => 1});

MLB_HANDS:
# retrieve data from the master table.
my $sth1 = $dbh->prepare("SELECT mlb_id, mlb_name, mlb_pos, mlb_team, mlb_team_long, bats, throws, birth_year, bp_id, bref_id, bref_name, cbs_id, cbs_name, cbs_pos, espn_id, espn_name, espn_pos, fg_id, fg_name, lahman_id, nfbc_id, nfbc_name, nfbc_pos, retro_id, retro_name, debut, yahoo_id, yahoo_name, yahoo_pos, mlb_depth FROM master");
$sth1->execute();

while (@row = $sth1->fetchrow_array) {
   push (@mlb_id, $row[0]) ;
   push (@mlb_name, $row[1]) ;
   push (@mlb_pos, $row[2]) ;
   push (@mlb_team, $row[3]) ; 
   push (@mlb_team_long, $row[4]) ; 
   push (@bats, $row[5]) ;
   push (@throws, $row[6]) ; 
   push (@birth_year, $row[7]) ;
   push (@bp_id, $row[8]) ;
   push (@bref_id, $row[9]) ;
   push (@bref_name, $row[10]) ;
   push (@cbs_id, $row[11]) ;
   push (@cbs_name, $row[12]) ;
   push (@cbs_pos, $row[13]) ;
   push (@espn_id, $row[14]) ;
   push (@espn_name, $row[15]) ;
   push (@espn_pos, $row[16]) ;
   push (@fg_id, $row[17]) ;
   push (@fg_name, $row[18]) ;
   push (@lahman_id, $row[19]) ;
   push (@nfbc_id, $row[20]) ;
   push (@nfbc_name, $row[21]) ;
   push (@nfbc_pos, $row[22]) ;
   push (@retro_id, $row[23]) ;
   push (@retro_name, $row[24]) ;
   push (@debut, $row[25]) ;
   push (@yahoo_id, $row[26]) ;
   push (@yahoo_name, $row[27]) ;
   push (@yahoo_pos, $row[28]) ;
   push (@mlb_depth, $row[29]) ;
   $count5 = $count5 + 1;
}
$sth1->finish();

# retrieve data from the mlb_hands_compare table.
my $sth2 = $dbh->prepare("SELECT mlb_id, mlb_name, bats, throws, birth_year FROM mlb_hands_compare WHERE flag = 0");
$sth2->execute();
while (@row = $sth2->fetchrow_array) {
   push (@new_hands_id, $row[0]) ;
   push (@new_hands_name, $row[1]) ;
   push (@new_bats, $row[2]) ;
   push (@new_throws, $row[3]) ;
   push (@new_birth_year, $row[4]) ; 
   $count2 = $count2 + 1;
}
$sth2->finish();

if ( $count2 == 0 ) { goto MASTER_NEW; }

for $j ($count5 .. $count5 + $count2 - 1) {
    @mlb_id[$j] = @new_hands_id[$count4];
    @mlb_name[$j] = @new_hands_name[$count4];
    @bats[$j] = @new_bats[$count4];
    @throws[$j] = @new_throws[$count4];
    @birth_year[$j] = @new_birth_year[$count4];
    $count4 = $count4 + 1;
} 

#for $i (0 .. $count2 - 1) {
#   for $j  (0 .. $count5 - 1) {
#      if (@new_hands_id[$i] == @mlb_id[$j]) { 
#         @bats[$j] = @new_bats[$i];
#         @throws[$j] = @new_throws[$i];
#         @birth_year[$j] = @new_birth_year[$i];
#      }
#   }
#}

MASTER_NEW:

eval { $dbh->do("DROP TABLE if exists master_new") };
print "Dropping master_new failed: $$\n" if $@;
$dbh->do("CREATE TABLE master_new (mlb_id INT(6), mlb_name VARCHAR(31), mlb_pos VARCHAR(2), mlb_team VARCHAR(3), mlb_team_long VARCHAR(30), bats VARCHAR(1), throws VARCHAR(1), birth_year INT(4), bp_id VARCHAR(6), bref_id VARCHAR(9), bref_name VARCHAR(24), cbs_id VARCHAR(7), cbs_name VARCHAR(26), cbs_pos VARCHAR(2), espn_id VARCHAR(5), espn_name VARCHAR(27), espn_pos VARCHAR(2), fg_id VARCHAR(8), fg_name VARCHAR(24), lahman_id VARCHAR(9), nfbc_id VARCHAR(5), nfbc_name VARCHAR(24), nfbc_pos VARCHAR(2), retro_id VARCHAR(8), retro_name VARCHAR(24), debut VARCHAR(10), yahoo_id VARCHAR(5), yahoo_name VARCHAR(24), yahoo_pos VARCHAR(5), mlb_depth VARCHAR(4))");

#for $i (0 .. $count5 - 1) {
for $i (0 .. $count5 + $count2 - 1) {
 $new_master_query = "INSERT INTO master_new (mlb_id, mlb_name, mlb_pos, mlb_team, mlb_team_long, bats, throws, birth_year, bp_id, bref_id, bref_name, cbs_id, cbs_name, cbs_pos, espn_id, espn_name, espn_pos, fg_id, fg_name, lahman_id, nfbc_id, nfbc_name, nfbc_pos, retro_id, retro_name, debut, yahoo_id, yahoo_name, yahoo_pos, mlb_depth) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
$sth3 = $dbh->prepare($new_master_query);
 $sth3->execute($mlb_id[$i],$mlb_name[$i],$mlb_pos[$i],$mlb_team[$i],$mlb_team_long[$i],$bats[$i],$throws[$i],$birth_year[$i],$bp_id[$i],$bref_id[$i],$bref_name[$i],$cbs_id[$i],$cbs_name[$i],$cbs_pos[$i],$espn_id[$i],$espn_name[$i],$espn_pos[$i],$fg_id[$i],$fg_name[$i],$lahman_id[$i],$nfbc_id[$i],$nfbc_name[$i],$nfbc_pos[$i],$retro_id[$i],$retro_name[$i],$debut[$i],$yahoo_id[$i],$yahoo_name[$i],$yahoo_pos[$i],$mlb_depth[$i]) or die ("MySQL Error: $DBI::errstr\n$new_master_query\n");
}
$sth3->finish();

my @replace = ("bats", "throws", "birth_year", "bp_id", "bref_id", "bref_name", "cbs_id", "cbs_name", "cbs_pos", "espn_id", "espn_name", "espn_pos", "fg_id", "fg_name", "lahman_id", "nfbc_id", "nfbc_name", "nfbc_pos", "retro_id", "retro_name", "debut", "yahoo_id", "yahoo_name", "yahoo_pos", "mlb_depth");
foreach $replace (@replace) {
my $sth4 = $dbh->prepare("update master_new set $replace = replace($replace, NULL, '')");
$sth4->execute();
}

MLB:

# retrieve data from the master_new table.
my $sth5 = $dbh->prepare("SELECT mlb_id, mlb_name, mlb_pos, mlb_team, mlb_team_long, bats, throws, birth_year, bp_id, bref_id, bref_name, cbs_id, cbs_name, cbs_pos, espn_id, espn_name, espn_pos, fg_id, fg_name, lahman_id, nfbc_id, nfbc_name, nfbc_pos, retro_id, retro_name, debut, yahoo_id, yahoo_name, yahoo_pos, mlb_depth FROM master_new");
$sth5->execute();
while (@row = $sth5->fetchrow_array) {
   push (@mlb_id, $row[0]) ;
   push (@mlb_name, $row[1]) ;
   push (@mlb_pos, $row[2]) ;
   push (@mlb_team, $row[3]) ; 
   push (@mlb_team_long, $row[4]) ; 
   push (@bats, $row[5]) ;
   push (@throws, $row[6]) ; 
   push (@birth_year, $row[7]) ;
   push (@bp_id, $row[8]) ;
   push (@bref_id, $row[9]) ;
   push (@bref_name, $row[10]) ;
   push (@cbs_id, $row[11]) ;
   push (@cbs_name, $row[12]) ;
   push (@cbs_pos, $row[13]) ;
   push (@espn_id, $row[14]) ;
   push (@espn_name, $row[15]) ;
   push (@espn_pos, $row[16]) ;
   push (@fg_id, $row[17]) ;
   push (@fg_name, $row[18]) ;
   push (@lahman_id, $row[19]) ;
   push (@nfbc_id, $row[20]) ;
   push (@nfbc_name, $row[21]) ;
   push (@nfbc_pos, $row[22]) ;
   push (@retro_id, $row[23]) ;
   push (@retro_name, $row[24]) ;
   push (@debut, $row[25]) ;
   push (@yahoo_id, $row[26]) ;
   push (@yahoo_name, $row[27]) ;
   push (@yahoo_pos, $row[28]) ;
   push (@mlb_depth, $row[29]) ;
   $count1 = $count1 + 1;
}
$sth5->finish();

# retrieve data from the mlb_compare table.
my $sth6 = $dbh->prepare("SELECT mlb_id, mlb_name, mlb_pos, mlb_team, mlb_team_long FROM mlb_compare");
$sth6->execute();
while (@row = $sth6->fetchrow_array) {
   push (@new_mlb_id, $row[0]) ;
   push (@new_mlb_name, $row[1]) ;
   push (@new_mlb_pos, $row[2]) ;
   push (@new_mlb_team, $row[3]) ; 
   push (@new_mlb_team_long, $row[4]) ; 
   $count3 = $count3 + 1;
}
$sth6->finish();

if ( $count3 == 0 ) { goto BREF; }

for $i (0 .. $count3 - 1) {
   for $j  (0 .. $count1 - 1) {
      if (@new_mlb_id[$i] == @mlb_id[$j]) { 
         @mlb_id[$j] = @new_mlb_id[$i];
         @mlb_name[$j] = @new_mlb_name[$i];
         @mlb_pos[$j] = @new_mlb_pos[$i];
         @mlb_team[$j] = @new_mlb_team[$i];
         @mlb_team_long[$j] = @new_mlb_team_long[$i];
      }
   }
} 

eval { $dbh->do("DROP TABLE if exists master_new") };
print "Dropping master_new failed: $$\n" if $@;
$dbh->do("CREATE TABLE master_new (mlb_id INT(6), mlb_name VARCHAR(31), mlb_pos VARCHAR(2), mlb_team VARCHAR(3), mlb_team_long VARCHAR(30), bats VARCHAR(1), throws VARCHAR(1), birth_year INT(4), bp_id VARCHAR(6), bref_id VARCHAR(9), bref_name VARCHAR(24), cbs_id VARCHAR(7), cbs_name VARCHAR(26), cbs_pos VARCHAR(2), espn_id VARCHAR(5), espn_name VARCHAR(27), espn_pos VARCHAR(2), fg_id VARCHAR(8), fg_name VARCHAR(24), lahman_id VARCHAR(9), nfbc_id VARCHAR(5), nfbc_name VARCHAR(24), nfbc_pos VARCHAR(2), retro_id VARCHAR(8), retro_name VARCHAR(24), debut VARCHAR(10), yahoo_id VARCHAR(5), yahoo_name VARCHAR(24), yahoo_pos VARCHAR(5), mlb_depth VARCHAR(4))");

for $i (0 .. $count1 - 1 ) {
 $new_master_query = "INSERT INTO master_new (mlb_id, mlb_name, mlb_pos, mlb_team, mlb_team_long, bats, throws, birth_year, bp_id, bref_id, bref_name, cbs_id, cbs_name, cbs_pos, espn_id, espn_name, espn_pos, fg_id, fg_name, lahman_id, nfbc_id, nfbc_name, nfbc_pos, retro_id, retro_name, debut, yahoo_id, yahoo_name, yahoo_pos, mlb_depth) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
$sth7 = $dbh->prepare($new_master_query);
 $sth7->execute($mlb_id[$i],$mlb_name[$i],$mlb_pos[$i],$mlb_team[$i],$mlb_team_long[$i],$bats[$i],$throws[$i],$birth_year[$i],$bp_id[$i],$bref_id[$i],$bref_name[$i],$cbs_id[$i],$cbs_name[$i],$cbs_pos[$i],$espn_id[$i],$espn_name[$i],$espn_pos[$i],$fg_id[$i],$fg_name[$i],$lahman_id[$i],$nfbc_id[$i],$nfbc_name[$i],$nfbc_pos[$i],$retro_id[$i],$retro_name[$i],$debut[$i],$yahoo_id[$i],$yahoo_name[$i],$yahoo_pos[$i],$mlb_depth[$i]) or die ("MySQL Error: $DBI::errstr\n$new_master_query\n");
}
$sth7->finish();   

BREF:
# retrieve data from the master_new table.
my $sth8 = $dbh->prepare("SELECT mlb_id, mlb_name, mlb_pos, mlb_team, mlb_team_long, bats, throws, birth_year, bp_id, bref_id, bref_name, cbs_id, cbs_name, cbs_pos, espn_id, espn_name, espn_pos, fg_id, fg_name, lahman_id, nfbc_id, nfbc_name, nfbc_pos, retro_id, retro_name, debut, yahoo_id, yahoo_name, yahoo_pos, mlb_depth FROM master_new");
$sth8->execute();

while (@row = $sth8->fetchrow_array) {
   push (@mlb_id, $row[0]) ;
   push (@mlb_name, $row[1]) ;
   push (@mlb_pos, $row[2]) ;
   push (@mlb_team, $row[3]) ; 
   push (@mlb_team_long, $row[4]) ; 
   push (@bats, $row[5]) ;
   push (@throws, $row[6]) ; 
   push (@birth_year, $row[7]) ;
   push (@bp_id, $row[8]) ;
   push (@bref_id, $row[9]) ;
   push (@bref_name, $row[10]) ;
   push (@cbs_id, $row[11]) ;
   push (@cbs_name, $row[12]) ;
   push (@cbs_pos, $row[13]) ;
   push (@espn_id, $row[14]) ;
   push (@espn_name, $row[15]) ;
   push (@espn_pos, $row[16]) ;
   push (@fg_id, $row[17]) ;
   push (@fg_name, $row[18]) ;
   push (@lahman_id, $row[19]) ;
   push (@nfbc_id, $row[20]) ;
   push (@nfbc_name, $row[21]) ;
   push (@nfbc_pos, $row[22]) ;
   push (@retro_id, $row[23]) ;
   push (@retro_name, $row[24]) ;
   push (@debut, $row[25]) ;
   push (@yahoo_id, $row[26]) ;
   push (@yahoo_name, $row[27]) ;
   push (@yahoo_pos, $row[28]) ;
   push (@mlb_depth, $row[29]) ;
   $count7 = $count7 + 1;
}
$sth8->finish();

# retrieve data from the bref_subs table.
my $sth9 = $dbh->prepare("SELECT mlb_id, mlb_name, bref_id, bref_name FROM bref_subs");
$sth9->execute();
while (@row = $sth9->fetchrow_array) {
   push (@new2_mlb_id, $row[0]) ;
   push (@new2_mlb_name, $row[1]) ;
   push (@new2_bref_id, $row[2]) ;
   push (@new2_bref_name, $row[3]) ; 
   $count8 = $count8 + 1;
}
$sth9->finish();

if ( $count8 == 0 ) { goto CBS; }

for $i (0 .. $count8 - 1) {
   for $j  (0 .. $count7 - 1) {
      if (@new2_mlb_id[$i] == @mlb_id[$j]) { 
         @bref_id[$j] = @new2_bref_id[$i];
         @bref_name[$j] = @new2_bref_name[$i];
      }
   }
} 

eval { $dbh->do("DROP TABLE if exists master_new") };
print "Dropping master_new failed: $$\n" if $@;
$dbh->do("CREATE TABLE master_new (mlb_id INT(6), mlb_name VARCHAR(31), mlb_pos VARCHAR(2), mlb_team VARCHAR(3), mlb_team_long VARCHAR(30), bats VARCHAR(1), throws VARCHAR(1), birth_year INT(4), bp_id VARCHAR(6), bref_id VARCHAR(9), bref_name VARCHAR(24), cbs_id VARCHAR(7), cbs_name VARCHAR(26), cbs_pos VARCHAR(2), espn_id VARCHAR(5), espn_name VARCHAR(27), espn_pos VARCHAR(2), fg_id VARCHAR(8), fg_name VARCHAR(24), lahman_id VARCHAR(9), nfbc_id VARCHAR(5), nfbc_name VARCHAR(24), nfbc_pos VARCHAR(2), retro_id VARCHAR(8), retro_name VARCHAR(24), debut VARCHAR(10), yahoo_id VARCHAR(5), yahoo_name VARCHAR(24), yahoo_pos VARCHAR(5), mlb_depth VARCHAR(4))");

for $i (0 .. $count7 - 1) {
 $new_master_query = "INSERT INTO master_new (mlb_id, mlb_name, mlb_pos, mlb_team, mlb_team_long, bats, throws, birth_year, bp_id, bref_id, bref_name, cbs_id, cbs_name, cbs_pos, espn_id, espn_name, espn_pos, fg_id, fg_name, lahman_id, nfbc_id, nfbc_name, nfbc_pos, retro_id, retro_name, debut, yahoo_id, yahoo_name, yahoo_pos, mlb_depth) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
$sth10 = $dbh->prepare($new_master_query);
 $sth10->execute($mlb_id[$i],$mlb_name[$i],$mlb_pos[$i],$mlb_team[$i],$mlb_team_long[$i],$bats[$i],$throws[$i],$birth_year[$i],$bp_id[$i],$bref_id[$i],$bref_name[$i],$cbs_id[$i],$cbs_name[$i],$cbs_pos[$i],$espn_id[$i],$espn_name[$i],$espn_pos[$i],$fg_id[$i],$fg_name[$i],$lahman_id[$i],$nfbc_id[$i],$nfbc_name[$i],$nfbc_pos[$i],$retro_id[$i],$retro_name[$i],$debut[$i],$yahoo_id[$i],$yahoo_name[$i],$yahoo_pos[$i],$mlb_depth[$i]) or die ("MySQL Error: $DBI::errstr\n$new_master_query\n");
}
$sth10->finish();   

CBS:
# retrieve data from the master_new table.
my $sth11 = $dbh->prepare("SELECT mlb_id, mlb_name, mlb_pos, mlb_team, mlb_team_long, bats, throws, birth_year, bp_id, bref_id, bref_name, cbs_id, cbs_name, cbs_pos, espn_id, espn_name, espn_pos, fg_id, fg_name, lahman_id, nfbc_id, nfbc_name, nfbc_pos, retro_id, retro_name, debut, yahoo_id, yahoo_name, yahoo_pos, mlb_depth FROM master_new");
$sth11->execute();

while (@row = $sth11->fetchrow_array) {
   push (@mlb_id, $row[0]) ;
   push (@mlb_name, $row[1]) ;
   push (@mlb_pos, $row[2]) ;
   push (@mlb_team, $row[3]) ; 
   push (@mlb_team_long, $row[4]) ; 
   push (@bats, $row[5]) ;
   push (@throws, $row[6]) ; 
   push (@birth_year, $row[7]) ;
   push (@bp_id, $row[8]) ;
   push (@bref_id, $row[9]) ;
   push (@bref_name, $row[10]) ;
   push (@cbs_id, $row[11]) ;
   push (@cbs_name, $row[12]) ;
   push (@cbs_pos, $row[13]) ;
   push (@espn_id, $row[14]) ;
   push (@espn_name, $row[15]) ;
   push (@espn_pos, $row[16]) ;
   push (@fg_id, $row[17]) ;
   push (@fg_name, $row[18]) ;
   push (@lahman_id, $row[19]) ;
   push (@nfbc_id, $row[20]) ;
   push (@nfbc_name, $row[21]) ;
   push (@nfbc_pos, $row[22]) ;
   push (@retro_id, $row[23]) ;
   push (@retro_name, $row[24]) ;
   push (@debut, $row[25]) ;
   push (@yahoo_id, $row[26]) ;
   push (@yahoo_name, $row[27]) ;
   push (@yahoo_pos, $row[28]) ;
   push (@mlb_depth, $row[29]) ;
   $count10 = $count10 + 1;
}
$sth11->finish();

# retrieve data from the cbs_subs table.
my $sth12 = $dbh->prepare("SELECT mlb_id, mlb_name, cbs_id, cbs_name, cbs_pos FROM cbs_subs");
$sth12->execute();
while (@row = $sth12->fetchrow_array) {
   push (@new3_mlb_id, $row[0]) ;
   push (@new3_mlb_name, $row[1]) ;
   push (@new3_cbs_id, $row[2]) ;
   push (@new3_cbs_name, $row[3]) ; 
   push (@new3_cbs_pos, $row[4]) ; 
   $count11 = $count11 + 1;
}
$sth12->finish();

if ( $count11 == 0 ) { goto ESPN; }

for $i (0 .. $count11 - 1) {
   for $j  (0 .. $count10 - 1) {
      if (@new3_mlb_id[$i] == @mlb_id[$j]) { 
         @cbs_id[$j] = @new3_cbs_id[$i];
         @cbs_name[$j] = @new3_cbs_name[$i];
         @cbs_pos[$j] = @new3_cbs_pos[$i];
      }
   }
} 

eval { $dbh->do("DROP TABLE if exists master_new") };
print "Dropping master_new failed: $$\n" if $@;
$dbh->do("CREATE TABLE master_new (mlb_id INT(6), mlb_name VARCHAR(31), mlb_pos VARCHAR(2), mlb_team VARCHAR(3), mlb_team_long VARCHAR(30), bats VARCHAR(1), throws VARCHAR(1), birth_year INT(4), bp_id VARCHAR(6), bref_id VARCHAR(9), bref_name VARCHAR(24), cbs_id VARCHAR(7), cbs_name VARCHAR(26), cbs_pos VARCHAR(2), espn_id VARCHAR(5), espn_name VARCHAR(27), espn_pos VARCHAR(2), fg_id VARCHAR(8), fg_name VARCHAR(24), lahman_id VARCHAR(9), nfbc_id VARCHAR(5), nfbc_name VARCHAR(24), nfbc_pos VARCHAR(2), retro_id VARCHAR(8), retro_name VARCHAR(24), debut VARCHAR(10), yahoo_id VARCHAR(5), yahoo_name VARCHAR(24), yahoo_pos VARCHAR(5), mlb_depth VARCHAR(4))");

for $i (0 .. $count10 - 1) {
 $new_master_query = "INSERT INTO master_new (mlb_id, mlb_name, mlb_pos, mlb_team, mlb_team_long, bats, throws, birth_year, bp_id, bref_id, bref_name, cbs_id, cbs_name, cbs_pos, espn_id, espn_name, espn_pos, fg_id, fg_name, lahman_id, nfbc_id, nfbc_name, nfbc_pos, retro_id, retro_name, debut, yahoo_id, yahoo_name, yahoo_pos, mlb_depth) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
$sth13 = $dbh->prepare($new_master_query);
 $sth13->execute($mlb_id[$i],$mlb_name[$i],$mlb_pos[$i],$mlb_team[$i],$mlb_team_long[$i],$bats[$i],$throws[$i],$birth_year[$i],$bp_id[$i],$bref_id[$i],$bref_name[$i],$cbs_id[$i],$cbs_name[$i],$cbs_pos[$i],$espn_id[$i],$espn_name[$i],$espn_pos[$i],$fg_id[$i],$fg_name[$i],$lahman_id[$i],$nfbc_id[$i],$nfbc_name[$i],$nfbc_pos[$i],$retro_id[$i],$retro_name[$i],$debut[$i],$yahoo_id[$i],$yahoo_name[$i],$yahoo_pos[$i],$mlb_depth[$i]) or die ("MySQL Error: $DBI::errstr\n$new_master_query\n");
}
$sth13->finish();   

ESPN:
# retrieve data from the master_new table.
my $sth14 = $dbh->prepare("SELECT mlb_id, mlb_name, mlb_pos, mlb_team, mlb_team_long, bats, throws, birth_year, bp_id, bref_id, bref_name, cbs_id, cbs_name, cbs_pos, espn_id, espn_name, espn_pos, fg_id, fg_name, lahman_id, nfbc_id, nfbc_name, nfbc_pos, retro_id, retro_name, debut, yahoo_id, yahoo_name, yahoo_pos, mlb_depth FROM master_new");
$sth14->execute();

while (@row = $sth14->fetchrow_array) {
   push (@mlb_id, $row[0]) ;
   push (@mlb_name, $row[1]) ;
   push (@mlb_pos, $row[2]) ;
   push (@mlb_team, $row[3]) ; 
   push (@mlb_team_long, $row[4]) ; 
   push (@bats, $row[5]) ;
   push (@throws, $row[6]) ; 
   push (@birth_year, $row[7]) ;
   push (@bp_id, $row[8]) ;
   push (@bref_id, $row[9]) ;
   push (@bref_name, $row[10]) ;
   push (@cbs_id, $row[11]) ;
   push (@cbs_name, $row[12]) ;
   push (@cbs_pos, $row[13]) ;
   push (@espn_id, $row[14]) ;
   push (@espn_name, $row[15]) ;
   push (@espn_pos, $row[16]) ;
   push (@fg_id, $row[17]) ;
   push (@fg_name, $row[18]) ;
   push (@lahman_id, $row[19]) ;
   push (@nfbc_id, $row[20]) ;
   push (@nfbc_name, $row[21]) ;
   push (@nfbc_pos, $row[22]) ;
   push (@retro_id, $row[23]) ;
   push (@retro_name, $row[24]) ;
   push (@debut, $row[25]) ;
   push (@yahoo_id, $row[26]) ;
   push (@yahoo_name, $row[27]) ;
   push (@yahoo_pos, $row[28]) ;
   push (@mlb_depth, $row[29]) ;
   $count13 = $count13 + 1;
}
$sth14->finish();

# retrieve data from the espn_subs table.
my $sth15 = $dbh->prepare("SELECT mlb_id, mlb_name, espn_id, espn_name, espn_pos FROM espn_subs");
$sth15->execute();
while (@row = $sth15->fetchrow_array) {
   push (@new4_mlb_id, $row[0]) ;
   push (@new4_mlb_name, $row[1]) ;
   push (@new4_espn_id, $row[2]) ;
   push (@new4_espn_name, $row[3]) ; 
   push (@new4_espn_pos, $row[4]) ; 
   $count14 = $count14 + 1;
}
$sth15->finish();

if ( $count14 == 0 ) { goto FG; }

for $i (0 .. $count14 - 1) {
   for $j  (0 .. $count13 - 1) {
      if (@new4_mlb_id[$i] == @mlb_id[$j]) { 
         @espn_id[$j] = @new4_espn_id[$i];
         @espn_name[$j] = @new4_espn_name[$i];
         @espn_pos[$j] = @new4_espn_pos[$i];
      }
   }
} 

eval { $dbh->do("DROP TABLE if exists master_new") };
print "Dropping master_new failed: $$\n" if $@;
$dbh->do("CREATE TABLE master_new (mlb_id INT(6), mlb_name VARCHAR(31), mlb_pos VARCHAR(2), mlb_team VARCHAR(3), mlb_team_long VARCHAR(30), bats VARCHAR(1), throws VARCHAR(1), birth_year INT(4), bp_id VARCHAR(6), bref_id VARCHAR(9), bref_name VARCHAR(24), cbs_id VARCHAR(7), cbs_name VARCHAR(26), cbs_pos VARCHAR(2), espn_id VARCHAR(5), espn_name VARCHAR(27), espn_pos VARCHAR(2), fg_id VARCHAR(8), fg_name VARCHAR(24), lahman_id VARCHAR(9), nfbc_id VARCHAR(5), nfbc_name VARCHAR(24), nfbc_pos VARCHAR(2), retro_id VARCHAR(8), retro_name VARCHAR(24), debut VARCHAR(10), yahoo_id VARCHAR(5), yahoo_name VARCHAR(24), yahoo_pos VARCHAR(5), mlb_depth VARCHAR(4))");

for $i (0 .. $count13 - 1) {
 $new_master_query = "INSERT INTO master_new (mlb_id, mlb_name, mlb_pos, mlb_team, mlb_team_long, bats, throws, birth_year, bp_id, bref_id, bref_name, cbs_id, cbs_name, cbs_pos, espn_id, espn_name, espn_pos, fg_id, fg_name, lahman_id, nfbc_id, nfbc_name, nfbc_pos, retro_id, retro_name, debut, yahoo_id, yahoo_name, yahoo_pos, mlb_depth) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
$sth16 = $dbh->prepare($new_master_query);
 $sth16->execute($mlb_id[$i],$mlb_name[$i],$mlb_pos[$i],$mlb_team[$i],$mlb_team_long[$i],$bats[$i],$throws[$i],$birth_year[$i],$bp_id[$i],$bref_id[$i],$bref_name[$i],$cbs_id[$i],$cbs_name[$i],$cbs_pos[$i],$espn_id[$i],$espn_name[$i],$espn_pos[$i],$fg_id[$i],$fg_name[$i],$lahman_id[$i],$nfbc_id[$i],$nfbc_name[$i],$nfbc_pos[$i],$retro_id[$i],$retro_name[$i],$debut[$i],$yahoo_id[$i],$yahoo_name[$i],$yahoo_pos[$i],$mlb_depth[$i]) or die ("MySQL Error: $DBI::errstr\n$new_master_query\n");
}
$sth16->finish();   

FG:
# retrieve data from the master_new table.
my $sth17 = $dbh->prepare("SELECT mlb_id, mlb_name, mlb_pos, mlb_team, mlb_team_long, bats, throws, birth_year, bp_id, bref_id, bref_name, cbs_id, cbs_name, cbs_pos, espn_id, espn_name, espn_pos, fg_id, fg_name, lahman_id, nfbc_id, nfbc_name, nfbc_pos, retro_id, retro_name, debut, yahoo_id, yahoo_name, yahoo_pos, mlb_depth FROM master_new");
$sth17->execute();

while (@row = $sth17->fetchrow_array) {
   push (@mlb_id, $row[0]) ;
   push (@mlb_name, $row[1]) ;
   push (@mlb_pos, $row[2]) ;
   push (@mlb_team, $row[3]) ; 
   push (@mlb_team_long, $row[4]) ; 
   push (@bats, $row[5]) ;
   push (@throws, $row[6]) ; 
   push (@birth_year, $row[7]) ;
   push (@bp_id, $row[8]) ;
   push (@bref_id, $row[9]) ;
   push (@bref_name, $row[10]) ;
   push (@cbs_id, $row[11]) ;
   push (@cbs_name, $row[12]) ;
   push (@cbs_pos, $row[13]) ;
   push (@espn_id, $row[14]) ;
   push (@espn_name, $row[15]) ;
   push (@espn_pos, $row[16]) ;
   push (@fg_id, $row[17]) ;
   push (@fg_name, $row[18]) ;
   push (@lahman_id, $row[19]) ;
   push (@nfbc_id, $row[20]) ;
   push (@nfbc_name, $row[21]) ;
   push (@nfbc_pos, $row[22]) ;
   push (@retro_id, $row[23]) ;
   push (@retro_name, $row[24]) ;
   push (@debut, $row[25]) ;
   push (@yahoo_id, $row[26]) ;
   push (@yahoo_name, $row[27]) ;
   push (@yahoo_pos, $row[28]) ;
   push (@mlb_depth, $row[29]) ;
   $count15 = $count15 + 1;
}
$sth17->finish();

# retrieve data from the fg_subs table.
my $sth18 = $dbh->prepare("SELECT mlb_id, mlb_name, fg_id, fg_name FROM fg_subs");
$sth18->execute();
while (@row = $sth18->fetchrow_array) {
   push (@new5_mlb_id, $row[0]) ;
   push (@new5_mlb_name, $row[1]) ;
   push (@new5_fg_id, $row[2]) ;
   push (@new5_fg_name, $row[3]) ; 
   $count16 = $count16 + 1;
}
$sth18->finish();

if ( $count16 == 0 ) { goto NFBC; }

for $i (0 .. $count16 - 1) {
   for $j  (0 .. $count15 - 1) {
      if (@new5_mlb_id[$i] == @mlb_id[$j]) { 
         @fg_id[$j] = @new5_fg_id[$i];
         @fg_name[$j] = @new5_fg_name[$i];
      }
   }
} 

eval { $dbh->do("DROP TABLE if exists master_new") };
print "Dropping master_new failed: $$\n" if $@;
$dbh->do("CREATE TABLE master_new (mlb_id INT(6), mlb_name VARCHAR(31), mlb_pos VARCHAR(2), mlb_team VARCHAR(3), mlb_team_long VARCHAR(30), bats VARCHAR(1), throws VARCHAR(1), birth_year INT(4), bp_id VARCHAR(6), bref_id VARCHAR(9), bref_name VARCHAR(24), cbs_id VARCHAR(7), cbs_name VARCHAR(26), cbs_pos VARCHAR(2), espn_id VARCHAR(5), espn_name VARCHAR(27), espn_pos VARCHAR(2), fg_id VARCHAR(8), fg_name VARCHAR(24), lahman_id VARCHAR(9), nfbc_id VARCHAR(5), nfbc_name VARCHAR(24), nfbc_pos VARCHAR(2), retro_id VARCHAR(8), retro_name VARCHAR(24), debut VARCHAR(10), yahoo_id VARCHAR(5), yahoo_name VARCHAR(24), yahoo_pos VARCHAR(5), mlb_depth VARCHAR(4))");

for $i (0 .. $count15 - 1) {
 $new_master_query = "INSERT INTO master_new (mlb_id, mlb_name, mlb_pos, mlb_team, mlb_team_long, bats, throws, birth_year, bp_id, bref_id, bref_name, cbs_id, cbs_name, cbs_pos, espn_id, espn_name, espn_pos, fg_id, fg_name, lahman_id, nfbc_id, nfbc_name, nfbc_pos, retro_id, retro_name, debut, yahoo_id, yahoo_name, yahoo_pos, mlb_depth) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
$sth19 = $dbh->prepare($new_master_query);
 $sth19->execute($mlb_id[$i],$mlb_name[$i],$mlb_pos[$i],$mlb_team[$i],$mlb_team_long[$i],$bats[$i],$throws[$i],$birth_year[$i],$bp_id[$i],$bref_id[$i],$bref_name[$i],$cbs_id[$i],$cbs_name[$i],$cbs_pos[$i],$espn_id[$i],$espn_name[$i],$espn_pos[$i],$fg_id[$i],$fg_name[$i],$lahman_id[$i],$nfbc_id[$i],$nfbc_name[$i],$nfbc_pos[$i],$retro_id[$i],$retro_name[$i],$debut[$i],$yahoo_id[$i],$yahoo_name[$i],$yahoo_pos[$i],$mlb_depth[$i]) or die ("MySQL Error: $DBI::errstr\n$new_master_query\n");
}
$sth19->finish();   

NFBC:
# retrieve data from the master_new table.
my $sth20 = $dbh->prepare("SELECT mlb_id, mlb_name, mlb_pos, mlb_team, mlb_team_long, bats, throws, birth_year, bp_id, bref_id, bref_name, cbs_id, cbs_name, cbs_pos, espn_id, espn_name, espn_pos, fg_id, fg_name, lahman_id, nfbc_id, nfbc_name, nfbc_pos, retro_id, retro_name, debut, yahoo_id, yahoo_name, yahoo_pos, mlb_depth FROM master_new");
$sth20->execute();

while (@row = $sth20->fetchrow_array) {
   push (@mlb_id, $row[0]) ;
   push (@mlb_name, $row[1]) ;
   push (@mlb_pos, $row[2]) ;
   push (@mlb_team, $row[3]) ; 
   push (@mlb_team_long, $row[4]) ; 
   push (@bats, $row[5]) ;
   push (@throws, $row[6]) ; 
   push (@birth_year, $row[7]) ;
   push (@bp_id, $row[8]) ;
   push (@bref_id, $row[9]) ;
   push (@bref_name, $row[10]) ;
   push (@cbs_id, $row[11]) ;
   push (@cbs_name, $row[12]) ;
   push (@cbs_pos, $row[13]) ;
   push (@espn_id, $row[14]) ;
   push (@espn_name, $row[15]) ;
   push (@espn_pos, $row[16]) ;
   push (@fg_id, $row[17]) ;
   push (@fg_name, $row[18]) ;
   push (@lahman_id, $row[19]) ;
   push (@nfbc_id, $row[20]) ;
   push (@nfbc_name, $row[21]) ;
   push (@nfbc_pos, $row[22]) ;
   push (@retro_id, $row[23]) ;
   push (@retro_name, $row[24]) ;
   push (@debut, $row[25]) ;
   push (@yahoo_id, $row[26]) ;
   push (@yahoo_name, $row[27]) ;
   push (@yahoo_pos, $row[28]) ;
   push (@mlb_depth, $row[29]) ;
   $count17 = $count17 + 1;
}
$sth20->finish();

# retrieve data from the nfbc_subs table.
my $sth21 = $dbh->prepare("SELECT mlb_id, mlb_name, nfbc_id, nfbc_name, nfbc_pos FROM nfbc_subs");
$sth21->execute();
while (@row = $sth21->fetchrow_array) {
   push (@new6_mlb_id, $row[0]) ;
   push (@new6_mlb_name, $row[1]) ;
   push (@new6_nfbc_id, $row[2]) ;
   push (@new6_nfbc_name, $row[3]) ; 
   push (@new6_nfbc_pos, $row[4]) ; 
   $count18 = $count18 + 1;
}
$sth21->finish();

if ( $count18 == 0 ) { goto YAHOO; }

for $i (0 .. $count18 - 1) {
   for $j  (0 .. $count17 - 1) {
      if (@new6_mlb_id[$i] == @mlb_id[$j]) { 
         @nfbc_id[$j] = @new6_nfbc_id[$i];
         @nfbc_name[$j] = @new6_nfbc_name[$i];
         @nfbc_pos[$j] = @new6_nfbc_pos[$i];
      }
   }
} 

eval { $dbh->do("DROP TABLE if exists master_new") };
print "Dropping master_new failed: $$\n" if $@;
$dbh->do("CREATE TABLE master_new (mlb_id INT(6), mlb_name VARCHAR(31), mlb_pos VARCHAR(2), mlb_team VARCHAR(3), mlb_team_long VARCHAR(30), bats VARCHAR(1), throws VARCHAR(1), birth_year INT(4), bp_id VARCHAR(6), bref_id VARCHAR(9), bref_name VARCHAR(24), cbs_id VARCHAR(7), cbs_name VARCHAR(26), cbs_pos VARCHAR(2), espn_id VARCHAR(5), espn_name VARCHAR(27), espn_pos VARCHAR(2), fg_id VARCHAR(8), fg_name VARCHAR(24), lahman_id VARCHAR(9), nfbc_id VARCHAR(5), nfbc_name VARCHAR(24), nfbc_pos VARCHAR(2), retro_id VARCHAR(8), retro_name VARCHAR(24), debut VARCHAR(10), yahoo_id VARCHAR(5), yahoo_name VARCHAR(24), yahoo_pos VARCHAR(5), mlb_depth VARCHAR(4))");

for $i (0 .. $count17 - 1) {
 $new_master_query = "INSERT INTO master_new (mlb_id, mlb_name, mlb_pos, mlb_team, mlb_team_long, bats, throws, birth_year, bp_id, bref_id, bref_name, cbs_id, cbs_name, cbs_pos, espn_id, espn_name, espn_pos, fg_id, fg_name, lahman_id, nfbc_id, nfbc_name, nfbc_pos, retro_id, retro_name, debut, yahoo_id, yahoo_name, yahoo_pos, mlb_depth) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
$sth22 = $dbh->prepare($new_master_query);
 $sth22->execute($mlb_id[$i],$mlb_name[$i],$mlb_pos[$i],$mlb_team[$i],$mlb_team_long[$i],$bats[$i],$throws[$i],$birth_year[$i],$bp_id[$i],$bref_id[$i],$bref_name[$i],$cbs_id[$i],$cbs_name[$i],$cbs_pos[$i],$espn_id[$i],$espn_name[$i],$espn_pos[$i],$fg_id[$i],$fg_name[$i],$lahman_id[$i],$nfbc_id[$i],$nfbc_name[$i],$nfbc_pos[$i],$retro_id[$i],$retro_name[$i],$debut[$i],$yahoo_id[$i],$yahoo_name[$i],$yahoo_pos[$i],$mlb_depth[$i]) or die ("MySQL Error: $DBI::errstr\n$new_master_query\n");
}
$sth22->finish();   

YAHOO:
# retrieve data from the master_new table.
my $sth23 = $dbh->prepare("SELECT mlb_id, mlb_name, mlb_pos, mlb_team, mlb_team_long, bats, throws, birth_year, bp_id, bref_id, bref_name, cbs_id, cbs_name, cbs_pos, espn_id, espn_name, espn_pos, fg_id, fg_name, lahman_id, nfbc_id, nfbc_name, nfbc_pos, retro_id, retro_name, debut, yahoo_id, yahoo_name, yahoo_pos, mlb_depth FROM master_new");
$sth23->execute();

while (@row = $sth23->fetchrow_array) {
   push (@mlb_id, $row[0]) ;
   push (@mlb_name, $row[1]) ;
   push (@mlb_pos, $row[2]) ;
   push (@mlb_team, $row[3]) ; 
   push (@mlb_team_long, $row[4]) ; 
   push (@bats, $row[5]) ;
   push (@throws, $row[6]) ; 
   push (@birth_year, $row[7]) ;
   push (@bp_id, $row[8]) ;
   push (@bref_id, $row[9]) ;
   push (@bref_name, $row[10]) ;
   push (@cbs_id, $row[11]) ;
   push (@cbs_name, $row[12]) ;
   push (@cbs_pos, $row[13]) ;
   push (@espn_id, $row[14]) ;
   push (@espn_name, $row[15]) ;
   push (@espn_pos, $row[16]) ;
   push (@fg_id, $row[17]) ;
   push (@fg_name, $row[18]) ;
   push (@lahman_id, $row[19]) ;
   push (@nfbc_id, $row[20]) ;
   push (@nfbc_name, $row[21]) ;
   push (@nfbc_pos, $row[22]) ;
   push (@retro_id, $row[23]) ;
   push (@retro_name, $row[24]) ;
   push (@debut, $row[25]) ;
   push (@yahoo_id, $row[26]) ;
   push (@yahoo_name, $row[27]) ;
   push (@yahoo_pos, $row[28]) ;
   push (@mlb_depth, $row[29]) ;
   $count6 = $count6 + 1;
}
$sth23->finish();

# retrieve data from the yahoo_subs table.
my $sth24 = $dbh->prepare("SELECT mlb_id, mlb_name, yahoo_id, yahoo_name, yahoo_pos FROM yahoo_subs");
$sth24->execute();
while (@row = $sth24->fetchrow_array) {
   push (@y_mlb_id, $row[0]) ;
   push (@y_mlb_name, $row[1]) ;
   push (@y_yahoo_id, $row[2]) ;
   push (@y_yahoo_name, $row[3]) ; 
   push (@y_yahoo_pos, $row[4]) ;
   $count12 = $count12 + 1;
}
$sth24->finish();

if ( $count12 == 0 ) { goto THENEXTSET; }

for $i (0 .. $count12-1) {
   for $j  (0 .. $count6 - 1) {
      if (@y_mlb_id[$i] == @mlb_id[$j]) { 
         @yahoo_id[$j] = @y_yahoo_id[$i];
         @yahoo_name[$j] = @y_yahoo_name[$i];
         @yahoo_pos[$j] = @y_yahoo_pos[$i];
      }
   }
} 

eval { $dbh->do("DROP TABLE if exists master_new") };
print "Dropping master_new failed: $$\n" if $@;
$dbh->do("CREATE TABLE master_new (mlb_id INT(6), mlb_name VARCHAR(31), mlb_pos VARCHAR(2), mlb_team VARCHAR(3), mlb_team_long VARCHAR(30), bats VARCHAR(1), throws VARCHAR(1), birth_year INT(4), bp_id VARCHAR(6), bref_id VARCHAR(9), bref_name VARCHAR(24), cbs_id VARCHAR(7), cbs_name VARCHAR(26), cbs_pos VARCHAR(2), espn_id VARCHAR(5), espn_name VARCHAR(27), espn_pos VARCHAR(2), fg_id VARCHAR(8), fg_name VARCHAR(24), lahman_id VARCHAR(9), nfbc_id VARCHAR(5), nfbc_name VARCHAR(24), nfbc_pos VARCHAR(2), retro_id VARCHAR(8), retro_name VARCHAR(24), debut VARCHAR(10), yahoo_id VARCHAR(5), yahoo_name VARCHAR(24), yahoo_pos VARCHAR(5), mlb_depth VARCHAR(4))");

for $i (0 .. $count6 - 1) {
 $new_master_query = "INSERT INTO master_new (mlb_id, mlb_name, mlb_pos, mlb_team, mlb_team_long, bats, throws, birth_year, bp_id, bref_id, bref_name, cbs_id, cbs_name, cbs_pos, espn_id, espn_name, espn_pos, fg_id, fg_name, lahman_id, nfbc_id, nfbc_name, nfbc_pos, retro_id, retro_name, debut, yahoo_id, yahoo_name, yahoo_pos, mlb_depth) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
$sth25 = $dbh->prepare($new_master_query);
 $sth25->execute($mlb_id[$i],$mlb_name[$i],$mlb_pos[$i],$mlb_team[$i],$mlb_team_long[$i],$bats[$i],$throws[$i],$birth_year[$i],$bp_id[$i],$bref_id[$i],$bref_name[$i],$cbs_id[$i],$cbs_name[$i],$cbs_pos[$i],$espn_id[$i],$espn_name[$i],$espn_pos[$i],$fg_id[$i],$fg_name[$i],$lahman_id[$i],$nfbc_id[$i],$nfbc_name[$i],$nfbc_pos[$i],$retro_id[$i],$retro_name[$i],$debut[$i],$yahoo_id[$i],$yahoo_name[$i],$yahoo_pos[$i],$mlb_depth[$i]) or die ("MySQL Error: $DBI::errstr\n$new_master_query\n");
}
$sth25->finish();   

THENEXTSET:

($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime();
my $changes = new FileHandle ">changes_" . ($year + 1900) . ($mon + 1) . $mday . ".txt";
my $datestring = localtime();
print $changes "$datestring\n";
print $changes "\n";
print $changes "mlb_names_changes " . $count3 . " " . $count1 . "\n";
print $changes "mlb_hands_changes " . $count2 . " " . $count5 . " " . $count4 . "\n";
print $changes "bref_changes " . $count8 . " " . $count7 . "\n";
print $changes "cbs_changes " . $count11 . " " . $count10 . "\n";
print $changes "espn_changes " . $count14 . " " . $count13 . "\n";
print $changes "fg_changes " . $count16 . " " . $count15 . "\n";
print $changes "nfbc " . $count18 . " " . $count17 . "\n";
print $changes "yahoo " . $count12 . " " . $count6 . "\n";

# Disconnect from the database.
$dbh->disconnect();
