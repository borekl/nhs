#!/usr/bin/env perl

package NHdb::Schema::Result::Logfiles;
use base qw/DBIx::Class::Core/;

__PACKAGE__->table('logfiles');
__PACKAGE__->add_columns(qw/
  logfiles_i descr server variant logurl localfile dumpurl rcfileurl options
  oper static tz fpos lines lastchk
/);
__PACKAGE__->set_primary_key('logfiles_i');

1;

