#!/usr/bin/env perl

package NHdb::Schema::Result::Update;
use base qw/DBIx::Class::Core/;

__PACKAGE__->table('update');
__PACKAGE__->add_columns(qw/variant name upflag/);
__PACKAGE__->set_primary_key(qw/variant name/);

1;

