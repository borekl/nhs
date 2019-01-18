#!/usr/bin/env perl

#=============================================================================
# Logfiles
#=============================================================================

package NHdb::Logfiles;

use Moo;
use Carp;
use Ref::Util qw(is_blessed_hashref is_arrayref);

use NHdb::Logfile;


#=============================================================================
#=== ATTRIBUTES ==============================================================
#=============================================================================

# DBIx::Class resultset representing the 'logfiles' table

has db => (
  is => 'ro',
  required => 1,
  isa => sub {
    croak __PACKAGE__ . ' constructor needs a DBIx::Class resultset as argument'
    if !is_blessed_hashref($_[0]) || !$_[0]->isa('DBIx::Class::ResultSet');
  },
);

# result set with applied filtering conditions

has _rs => (
  is => 'ro',
  lazy => 1,
  builder => '_build_rs',
),

# list of NHdb::Logfile instances, these are created based on query from
# database

has logfiles => (
  is => 'ro',
  lazy => 1,
  builder => '_load_logfiles',
);

# input condition, these constrain logfiles that are loaded on instantiation

has variants => (
  is => 'ro',
  predicate => 1,
);

has servers => (
  is => 'ro',
  predicate => 1,
);

has logids => (
  is => 'ro',
  predicate => 1,
);


#=============================================================================
#=== METHODS =================================================================
#=============================================================================

#=============================================================================
# Result set filter
#=============================================================================

sub _build_rs
{
  my ($self) = @_;
  my %where;

  #--- the condition

  if(
    $self->has_variants
    && is_arrayref($self->variants)
    && @{$self->variants}
  ) {
    $where{'variant'} = $self->variants();
  }
  if(
    $self->has_servers
    && is_arrayref($self->servers)
    && @{$self->servers}
  ) {
    $where{'server'} = $self->servers();
  }
  if(
    $self->has_logids
    && is_arrayref($self->logids)
    && @{$self->logids}
  ) {
    $where{'logfiles_i'} = $self->logids();
  }

  return $self->db->search(\%where);
}


#=============================================================================
# Builder function for the 'logfiles' attribute
#=============================================================================

sub _load_logfiles
{
  my ($self) = @_;
  my @logfiles;
  my $rs = $self->_rs;

  #--- load the logfiles

  while(my $log = $rs->next) {
    push(@logfiles, NHdb::Logfile->new(db => $log));
  }

  #--- finish

  return
    [ sort { $a->get('logfiles_i') <=> $b->get('logfiles_i') } @logfiles ];
}


#=============================================================================
# Method for setting the 'oper' and 'static' fields.
#=============================================================================

sub set_state
{
  my ($self) = shift;
  my %arg = @_;

  for my $key (keys %arg) {
    if($key ne 'oper' && $key ne 'static') {
      croak qq{Invalid argument '$key' in NHdb::Logfiles::set_state};
    }
    if(!defined $arg{$key}) { delete $arg{$key}; }
  }

  return %arg ? $self->_rs->update(\%arg) : 0;
}


#=============================================================================
# Return count of selected logfiles
#=============================================================================

sub count
{
  my ($self) = shift;

  return $self->_rs->count;
}


#=============================================================================
# Return count of selected logfiles
#=============================================================================

sub count_all
{
  my ($self) = shift;

  return $self->db->count;
}


#=============================================================================

1;
