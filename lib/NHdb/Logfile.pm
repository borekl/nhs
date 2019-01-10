#!/usr/bin/env perl

#=============================================================================
# Interface to a single logfile.
#=============================================================================

package NHdb::Logfile;

use Moo;
use NHdb::Config;
use Path::Tiny;
use Ref::Util;
use Carp;
use MIME::Base64 qw(decode_base64);


#=============================================================================
#=== ATTRIBUTES ==============================================================
#=============================================================================

# DBIx::Class row object

has db => (
  is => 'ro',
  required => 1,
);

has localfile => (
  is => 'ro',
  builder => 1,
  lazy => 1,
);


#=============================================================================
#=== METHODS =================================================================
#=============================================================================

#=============================================================================
# Universal getter that pulls the fields from the DBIx::Class object mapped to
# the database.
#=============================================================================

sub get
{
  my ($self, $field) = @_;

  return $self->db->$field();
}


#=============================================================================
# Get local xlogfile copy pathname.
#=============================================================================

sub _build_localfile
{
  my ($self) = @_;
  my $nhdb = NHdb::Config->instance->config;

  return path(
    $nhdb->{'logs'}{'localpath'},
    $self->get('localfile')
  );
}


#=============================================================================
# Return true when the source has given option.
#=============================================================================

sub has_option
{
  my ($self, $option) = @_;

  ($self->get('options')
  && grep { $option eq $_ } @{$self->get('options')})
  ? 1 : 0;
}


#=============================================================================
# Attempt to retrieve the remote xlogfile. Returns -1 on failure and number
# of new bytes retrieved on success (this can be 0)
#=============================================================================

sub retrieve_xlogfile
{
  my ($self) = @_;
  my $nhdb = NHdb::Config->instance;
  my $localfile = $self->localfile;

  #--- fail if wget invocation not defined

  die 'wget invocation template not configured' if !$nhdb->config()->{'wget'};

  #--- do nothing for xlogfiles marked as 'static' or the logurl is not
  #--- defined

  return 0 if $self->get('static');
  return 0 if !$self->get('logurl');

  #--- record the starting size of the file

  my $fsize_before = int(-s $localfile);

  #--- get the xlogfile

  my $r = system(
    sprintf($nhdb->config()->{'wget'}, $localfile, $self->get('logurl'))
  );

  #--- return how many new bytes were retrieved

  return $r ? -1 : (-s $localfile) - $fsize_before;
}


#=============================================================================
# Parse the log. This is internal helper used by the 'read' method.
#=============================================================================

sub _parse
{
  my ($self, $l) = @_;
  my %l;

  #--- there are two field separators in use: comma and horizontal tab;
  #--- we use simple heuristics to find out the one that is used for given
  #--- xlogfile row

  my @a1 = split(/:/, $l);
  my @a2 = split(/\t/, $l);
  my $a0 = @a1 > @a2 ? \@a1 : \@a2;

  #--- split keys and values

  for my $field (@$a0) {
    $field =~ /^(.+?)=(.+)$/;
    $l{$1} = $2 unless exists $l{$1};
  }

  #--- if this is enabled for a source (through "logfiles.options"), check
  #--- whether base64 fields exist and decode them

  if($self->has_option('base64xlog')) {
    for my $field (keys %l) {
      next if $field !~ /^(.+)64$/;
      $l{$1} = decode_base64($l{$field});
    }
  }

  #--- finish returning hashref

  return \%l
}


#=============================================================================
# Read the new part of the local xlogfile copy.
#=============================================================================

sub read
{
  my ($self, $cb) = @_;
  my $fpos = $self->get('fpos');

  #--- make sure we got a callback

  croak 'No callback supplied' unless is_coderef($cb);

  #--- open local xlogfile

  open(my $logf, $self->localfile)
  or die 'Failed to open xlogfile local copy';

  #--- seek into the xlogfile based on position stored in the database

  if($fpos) {
    my $r = seek($logf, $fpos, 0)
    or die 'Failed to seek into xlogfile local copy';
  }

  #--- devnull logfiles are slightly modified by having a server id
  #--- prepended to the usual xlogfile line

  my $devnull = $self->has_option('devnull');

  #--- main read loop

  my $lc = 0;
  while(my $l = <$fpos>) {
    chomp $l;
    if($devnull) { $l =~ s/^\S+\s(.*)$/$1/; }
    $cb->($self->_parse($l), $lc);
    $lc++;
  }

  #--- close the xlogfile

  close($logf);

  #--- update backend database

  $self->db->update({
    fpos => -s $self->localfile,
    lastchk => \['current_timestamp'],
    lines => $self->get('lines') + $lc,
  });

}


#=============================================================================
# Reset the 'fpos' and 'lines' fields.
#=============================================================================

sub reset
{
  my ($self) = @_;

  $self->db->update({
    fpos => undef,
    lines => 0,
  });
}


#=============================================================================

1;
