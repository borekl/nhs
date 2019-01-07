#!/usr/bin/env perl

package NHdb::Schema::Result::MapGamesStreaks;
use base qw/DBIx::Class::Core/;

__PACKAGE__->table('map_games_streaks');
__PACKAGE__->add_columns(qw/rowid streaks_i/);
__PACKAGE__->set_primary_key(qw/rowid streaks_i/);

1;
