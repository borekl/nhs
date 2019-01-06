#!/usr/bin/env perl

package NHdb::Schema::Result::Translations;
use base qw/DBIx::Class::Core/;

__PACKAGE__->table('translations');
__PACKAGE__->add_columns(qw/server name_from name_to/);
__PACKAGE__->set_primary_key(qw/server name_from/);

1;

