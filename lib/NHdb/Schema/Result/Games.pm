#!/usr/bin/env perl

package NHdb::Schema::Result::Games;
use base qw/DBIx::Class::Core/;

__PACKAGE__->table('games');
__PACKAGE__->add_columns(qw/
rowid
line
logfiles_i
name
name_orig
role
race
gender
gender0
align
align0
starttime
starttime_raw
endtime
endtime_raw
birthdate
deathdate
death
deathdnum
deathlev
deaths
hp
maxhp
maxlvl
points
conduct
elbereths
turns
achieve
realtime
version
ascended
quit
scummed
dumplog
/);
__PACKAGE__->set_primary_key('rowid');
__PACKAGE__->belongs_to(
  'logfiles', 'NHdb::Schema::Result::Logfiles', 'logfiles_i'
);

1;
