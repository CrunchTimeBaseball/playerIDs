#!/usr/bin/perl

#use strict;
use LWP;
use FileHandle;

my $position; my $url; my $response; my $html; my @lines; my $count; my $line; my @mlb_id; my @mlb_name; my@mlb_pos; my @mlb_team;
my $i; my $mlb_names_query; my $sth7;

my $browser = LWP::UserAgent->new;
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

#write the results to file
my $fh = new FileHandle ">mlb_names.csv";
print $fh "mlb_id,mlb_name,mlb_pos,mlb_team,mlb_team_long" . "\n";
for $k (0 .. $count - 1) { 
	print $fh $mlb_id[$k] . "\," . $mlb_name[$k] . "\," . $mlb_pos[$k] . "\," . $mlb_team[$k] . "\," . $mlb_team_long[$k] . "\n"; 
	}
$fh->close();
