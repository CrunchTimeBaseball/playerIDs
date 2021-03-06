#!/usr/bin/perl

#use strict;
use LWP;
use DBI();
use DBD::mysql;

my $position; my $url; my $response; my $html; my @lines; my $count; my $line; my @mlb_id; my @mlb_name; my@mlb_pos; my @mlb_team;
my $i; my $mlb_names_query; my $sth7;

my $browser = LWP::UserAgent->new;
my $dbh = DBI->connect("DBI:mysql:database=dbname;host=localhost","user", "password", {'RaiseError' => 1});

my @positions = (1, 2, 3, 4, 5, 6, O, D);

$count = 0;

foreach $position (@positions) {
    $url = "http://mlb.mlb.com/lookup/json/named.search_player_all_pos.bam?sport_code='mlb'&active_sw='Y'&position='$position'";
    $response = $browser->get($url);
    die "Couldn't get $url:", $response->status_line, "\n"
	  unless $response->is_success;
    $html = $response->content;
    @lines = split "\},[{]", $html;
    foreach $line (@lines) {
	    if ($line =~ /\"position\"\:\"([0-9]{0,1}[A-Z]{1,2})\"\,\"team\_abbrev\"\:\"([A-Z]{2,3}).+first\_last\"\:\"(.+)\"\,\".+roster\"\:\".+player\_id\"\:\"([0-9]{6})\".+active\_sw\"\:\"Y\"\,\"team\_full\"\:\"(.+)\"/) {$mlb_id[$count] = $4; $mlb_name[$count] = $3; $mlb_pos[$count] = $1; $mlb_team[$count] = $2; $mlb_team_long[$count] = $5; $count = $count + 1;}
	  }
    sleep(1);
}

# Drop table.
$dbh->do("DROP TABLE if exists mlb_names");
# Create a new table.
$dbh->do("CREATE TABLE mlb_names (mlb_id INTEGER(6), mlb_name VARCHAR(30), mlb_pos VARCHAR(2), mlb_team VARCHAR(3), mlb_team_long VARCHAR(30))");

for $i (0 .. $count-1) {
 $mlb_names_query = "INSERT INTO mlb_names (mlb_id, mlb_name, mlb_pos, mlb_team, mlb_team_long) VALUES (?, ?, ?, ?, ?)";
 $sth7 = $dbh->prepare($mlb_names_query);
 $sth7->execute($mlb_id[$i],$mlb_name[$i],$mlb_pos[$i],$mlb_team[$i],$mlb_team_long[$i]) or die ("MySQL Error: $DBI::errstr\n$mlb_names_query\n");
}
$sth7->finish();   

# Disconnect from the database.
$dbh->disconnect();
