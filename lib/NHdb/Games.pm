#!/usr/bin/env perl

#=============================================================================
# This class encapsulates interaction with the 'games' table.
#=============================================================================

package NHdb::Games;

use Moo;
use Carp;
use Ref::Util qw(is_blessed_hashref is_hashref);


#=============================================================================
#=== ATTRIBUTES ==============================================================
#=============================================================================

# DBIx::Class resultset representing the 'games' table

has db => (
  is => 'ro',
  required => 1,
  isa => sub {
    croak __PACKAGE__ . ' constructor needs a DBIx::Class resultset as argument'
    if !is_blessed_hashref($_[0]) || !$_[0]->isa('DBIx::Class::ResultSet');
  },
);

# NHdb::Config instance ref

has nhdb => (
  is => 'ro',
  required => 1,
  isa => sub {
    croak __PACKAGE__ . q{ constructor needs a NHdb::Config as argument 'nhdb'}
    if !is_blessed_hashref($_[0]) || !$_[0]->isa('NHdb::Config');
  },
);

# NetHack::Config instance ref

has nh => (
  is => 'ro',
  required => 1,
  isa => sub {
    croak __PACKAGE__ . q{ constructor needs a NetHack::Config as argument 'nh'}
    if !is_blessed_hashref($_[0]) || !$_[0]->isa('NetHack::Config');
  },
);

# NHdb::Logfile instance ref

has log => (
  is => 'ro',
  required => 1,
  isa => sub {
    croak __PACKAGE__ . q{ constructor needs a NHdb::Logfile as argument 'nh'}
    if !is_blessed_hashref($_[0]) || !$_[0]->isa('NHdb::Logfile');
  },
);

# translations hashref

has translations => (
  is => 'ro',
  required => 1,
  isa => sub {
    croak __PACKAGE__ . q{ constructor needs a hashref as argument 'translations'}
    if !is_hashref($_[0]);
  },
);


#=============================================================================
#=== METHODS =================================================================
#=============================================================================

#=============================================================================
# Add new game.
#=============================================================================

sub add_new
{
  my ($self, $l, $line_no) = @_;
  my $log = $self->log;
  my $nhdb = $self->nhdb;
  my $nh = $self->nh;
  my $variant = $log->get('variant');
  my $server = $log->get('server');
  my %d;

  #--- reject too old log entries without necessary info
  return undef unless $nhdb->require_fields(keys %$l);

  #--- reject wizmode games, paxed test games
  return undef if $nhdb->reject_name($l->{'name'});

  #--- reject "special" modes of NH4 and its kin
  #--- Fourk challenge mode is okay, though
  if(
    exists $l->{'mode'} &&
    !($l->{'mode'} eq 'normal' || $l->{'mode'} eq 'challenge')
  ) {
    return undef;
  }

  #--- reject entries with empty name
  if(!exists $l->{'name'} || !$l->{'name'}) { return undef; }

  #--- death (reason)
  my $death = $l->{'death'};
  $death =~ tr[\x{9}\x{A}\x{D}\x{20}-\x{D7FF}\x{E000}-\x{FFFD}\x{10000}-\x{10FFFF}][]cd;
  $d{'death'} = substr($death, 0, 128);

  #--- ascended flag
  $d{'ascended'} = ( $death =~ /^(ascended|defied the gods)\b/ ? 'true': 'false' );

  #--- dNetHack combo mangling workaround
  # please refer to comment in NetHack.pm; this is only done to two specific
  # winning games!
  if($variant eq 'dnh' && $l->{'ascended'} eq 'true') {
    ($d{'role'}, $d{'race'})
    = $nh->variant('dnh')->dnethack_map($l->{'role'}, $l->{'race'});
  }

  #--- regular fields
  for my $k ($nhdb->regular_fields()) {
    $d{$k} = $l->{$k} if exists $l->{$k}
  }

  #--- name (before translation)
  $d{'name_orig'} = $l->{'name'};

  #--- name
  if(exists($self->translations->{$server}{$l->{'name'}})) {
    $d{'name'} = $self->translations->{$server}{$l->{'name'}};
  } else {
    $d{'name'} = $l->{'name'};
  }

  #--- logfiles_i
  $d{'logfiles_i'} = $log->get('logfiles_i');

  #--- line number
  $d{'line'} = $line_no;

  #--- conduct
  $d{'conduct'} = eval($l->{'conduct'});

  #--- achieve
  if(exists $l->{'achieve'}) {
    $d{'achieve'} = eval($l->{'achieve'});
  }

  #--- start time
  if(exists $l->{'starttime'}) {
    $d{'starttime'} = \[
      q{timestamp with time zone 'epoch' + ? * interval '1 second'}, $l->{'starttime'}
    ];
    $d{'starttime_raw'} = $l->{'starttime'};
  }

  #--- end time
  if(exists $l->{'endtime'}) {
    $d{'endtime'} = \[
      q{timestamp with time zone 'epoch' + ? * interval '1 second'}, $l->{'endtime'}
    ];
    $d{'endtime_raw'} = $l->{'endtime'};
  }

  #--- birth date
  if(exists $l->{'birthdate'} && !exists $l->{'starttime'}) {
    $d{'birthdate'} = $l->{'birthdate'};
    $d{'starttime'} = $l->{'birthdate'};
  }

  #--- death date
  if(exists $l->{'deathdate'} && !exists $l->{'endtime'}) {
    $d{'deathdate'} = $l->{'deathdate'};
    $d{'endtime'}   = $l->{'deathdate'};
  }

  #--- quit flag (escaped also counts)
  my $flag_quit = 'false';
  $flag_quit = 'true' if $death =~ /^quit\b/;
  $flag_quit = 'true' if $death =~ /^escaped\b/;
  $d{'quit'} = $flag_quit;

  #--- scummed flag
  my $flag_scummed = 'false';
  if($flag_quit eq 'true' && $l->{'points'} < 1000) {
    $flag_scummed = 'true';
  }
  $d{'scummed'} = $flag_scummed;

  #--- perform database insert

  my $rs = $self->db->create(\%d);
  return $rs;
}


#=============================================================================

1;
