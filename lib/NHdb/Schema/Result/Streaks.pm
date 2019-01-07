#!/usr/bin/env perl

package NHdb::Schema::Result::Streaks;
use base qw/DBIx::Class::Core/;

__PACKAGE__->table('streaks');
__PACKAGE__->add_columns(
  qw/streaks_i logfiles_i name name_orig open num_games/
);
__PACKAGE__->set_primary_key('streaks_i');

1;
