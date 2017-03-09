#!/usr/bin/perl

use strict;
use DBI();
use DBD::mysql;
use filehandle;

my @row; my $row; my @mlb_id; my @mlb_name; my @mlb_pos; my @mlb_team; my @mlb_team_long; my @bats; my @throws; my @birth_year; my @bp_id; my @bref_id; my @bref_name; my @cbs_id; my @cbs_name; my @cbs_pos; my @espn_id; my @espn_name; my @espn_pos; my @fg_id; my @fg_name; my @lahman_id; my @nfbc_id; my @nfbc_name; my @nfbc_pos; my @retro_id; my @retro_name; my @debut; my @yahoo_id; my @yahoo_name; my @yahoo_pos; my @mlb_depth; my $mlb_id; my $mlb_name; my $mlb_pos; my $mlb_team; my $mlb_team_long; my $bats; my $throws; my $birth_year; my $bp_id; my $bref_id; my $bref_name; my $cbs_id; my $cbs_name; my $cbs_pos; my $espn_id; my $espn_name; my $espn_pos; my $fg_id; my $fg_name; my $lahman_id; my $nfbc_id; my $nfbc_name; my $nfbc_pos; my $retro_id; my $retro_name; my $debut; my $yahoo_id; my $yahoo_name; my $yahoo_pos; my $mlb_depth; my $j; my $count1;

my $dbh = DBI->connect("DBI:mysql:database='schema_name';host=localhost","root", "password", {'RaiseError' => 1, 'mysql_enable_utf8'=> 1});

# retrieve data from the master_final table.
my $sth1 = $dbh->prepare("SELECT mlb_id, mlb_name, mlb_pos, mlb_team, mlb_team_long, bats, throws, birth_year, bp_id, bref_id, bref_name, cbs_id, cbs_name, cbs_pos, espn_id, espn_name, espn_pos, fg_id, fg_name, lahman_id, nfbc_id, nfbc_name, nfbc_pos, retro_id, retro_name, debut, yahoo_id, yahoo_name, yahoo_pos, mlb_depth FROM master_final");
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
   $count1 = $count1 + 1;
}
$sth1->finish();

my $fh = new FileHandle ">master_final.csv";
print $fh "mlb_id,mlb_name,mlb_pos,mlb_team,mlb_team_long,bats,throws,birth_year,bp_id,bref_id,bref_name,cbs_id,cbs_name,cbs_pos,espn_id,espn_name,espn_pos,fg_id,fg_name,lahman_id,nfbc_id,nfbc_name,nfbc_pos,retro_id,retro_name,debut,yahoo_id,yahoo_name,yahoo_pos,mlb_depth\n";

for $j (0 .. $count1) {
        print $fh @mlb_id[$j] . "," . @mlb_name[$j] . "," . @mlb_pos[$j] . "," . @mlb_team[$j] . "," . @mlb_team_long[$j] . "," . @bats[$j] . "," . @throws[$j] . "," . @birth_year[$j] . "," . @bp_id[$j] . "," . @bref_id[$j] . "," . @bref_name[$j] . "," . @cbs_id[$j] . "," . @cbs_name[$j] . "," . @cbs_pos[$j] . "," . @espn_id[$j] . "," . @espn_name[$j] . "," . @espn_pos[$j] . "," . @fg_id[$j] . "," . @fg_name[$j] . "," . @lahman_id[$j] . "," . @nfbc_id[$j] . "," . @nfbc_name[$j] . "," . @nfbc_pos[$j] . "," . @retro_id[$j] . "," . @retro_name[$j] . "," . @debut[$j] . "," . @yahoo_id[$j] . "," . @yahoo_name[$j] . "," . @yahoo_pos[$j] . "," . @mlb_depth[$j] . "\n";
}

# Disconnect from the database.
$dbh->disconnect();
