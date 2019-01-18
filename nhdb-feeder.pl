#!/usr/bin/env perl

#============================================================================
# NHDB Feeder
# """""""""""
# (c) 2013-2018 Borek Lupomesky
#
# This program scrapes logs from pre-defined NetHack servers and inserts
# game entries into database.
#============================================================================

#--- pragmas ----------------------------------------------------------------

use strict;
use utf8;

#--- external modules -------------------------------------------------------

use DBI;
use Getopt::Long;
use Log::Log4perl qw(get_logger);
use MIME::Base64 qw(decode_base64);
use Text::Pluralize;
use Path::Tiny;
use Try::Tiny;

#--- internal modules -------------------------------------------------------

use FindBin qw($Bin);
use lib "$Bin/lib";
use NetHack::Config;
use NetHack::Variant;
use NHdb::Config;
use NHdb::Db;
use NHdb::Schema;
use NHdb::Utils;
use NHdb::Feeder::Cmdline;
use NHdb::Logfiles;
use NHdb::Games;


#--- additional perl runtime setup ------------------------------------------

$| = 1;


#============================================================================
#=== definitions ============================================================
#============================================================================

my $lockfile = '/tmp/nhdb-feeder.lock';


#============================================================================
#=== globals ================================================================
#============================================================================

my %translations;               # name-to-name translations
my $translations_cnt = 0;       # number of name translation
my $logger;                     # log4perl instance
my $nh = new NetHack::Config(config_file => 'cfg/nethack_def.json');
my $nhdb = NHdb::Config->instance;
my $db;                         # NHdb::Db instance
my $dbic;                       # DBIx::Class schema instance


#============================================================================
#=== functions =============================================================
#============================================================================

#============================================================================
# Split a line along field separator, parse it into hash and return it as
# a hashref.
#============================================================================

sub parse_log
{
  my $log = shift;
  my $l = shift;
  my %l;
  my (@a1, @a2, $a0);

  #--- there are two field separators in use: comma and horizontal tab;
  #--- we use simple heuristics to find out the one that is used for given
  #--- xlogfile row

  @a1 = split(/:/, $l);
  @a2 = split(/\t/, $l);
  $a0 = scalar(@a1) > scalar(@a2) ? \@a1 : \@a2;

  #--- split keys and values

  for my $field (@$a0) {
    $field =~ /^(.+?)=(.+)$/;
    $l{$1} = $2 unless exists $l{$1};
  }

  #--- if this is enabled for a source (through "logfiles.options"), check
  #--- whether base64 fields exist and decode them

  if($log->has_option('base64xlog')) {
    for my $field (keys %l) {
      next if $field !~ /^(.+)64$/;
      $l{$1} = decode_base64($l{$field});
    }
  }

  #--- finish returning hashref

  return \%l
}


#============================================================================
# Create new streak entry, add one game to it and return [ streaks_i ] on
# success or error msg.
#============================================================================

sub sql_streak_create_new
{
  my $logfiles_i = shift;
  my $name = shift;
  my $name_orig = shift;
  my $rowid = shift;
  my $logger = get_logger('Streaks');

  #--- create new streak entry

  my $rs = $dbic->resultset('Streaks')->create({
    logfiles_i => $logfiles_i,
    name => $name,
    name_orig => $name_orig
  });
  my $streaks_i = $rs->id;

  $logger->debug(
    sprintf(
      'Started new streak %d (logfiles_i=%d, name=%s, rowid=%d)',
      $streaks_i, $logfiles_i, $name, $rowid
    )
  );

  #--- add the game to the new streak

  my $r = sql_streak_append_game($streaks_i, $rowid);
  return $r if !ref($r);

  #--- return

  return [ $streaks_i ];
}


#============================================================================
# Add a game to a streak.
#============================================================================

sub sql_streak_append_game
{
  my $streaks_i = shift;     # 1. streak to be appended to
  my $rowid = shift;         # 2. the game to be appended
  my $logger = get_logger('Streaks');

  #--- create mapping entry

  $dbic->resultset('MapGamesStreaks')->create({
    rowid => $rowid,
    streaks_i => $streaks_i
  });

  $logger->debug(
    sprintf(
      'Appended game %d to streak %d',
      $rowid, $streaks_i
    )
  );

  #--- finish

  return [];
}


#============================================================================
# This will close streak, ie. set streaks.open to FALSE. If the streak has
# num_games = 1; it will be deleted by database trigger.
#============================================================================

sub sql_streak_close
{
  my $streaks_i = shift;
  my $logger = get_logger('Streaks');

  #--- close streak entity and get its current state

  my $streak = $dbic->resultset('Streaks')->find($streaks_i);
  $streak->open('false');
  $streak->update;

  $logger->debug(sprintf('Closed streak %d', $streaks_i));

  #--- finish

  return [];
}


#============================================================================
# This will close all streaks for a given source.
#============================================================================

sub sql_streak_close_all
{
  my $logfiles_i = shift;
  my $logger = get_logger('Streaks');

  my $r = $dbic->resultset('Streaks')->search(
    { logfiles_i => $logfiles_i }
  )->update(
    { open => 'false' }
  );

  #--- finish

  return [ $r ];
}


#============================================================================
# This function gets last game in a streak entry.
#============================================================================

sub sql_streak_get_tail
{
  my $streaks_i = shift;
  my $logger = get_logger('Streaks');
  my $dbh = $db->handle();

  my $qry = q{SELECT * FROM streaks };
  $qry .= q{JOIN map_games_streaks USING (streaks_i) };
  $qry .= q{JOIN games USING (rowid) };
  $qry .= q{WHERE streaks_i = ? ORDER BY endtime DESC, line DESC LIMIT 1};
  my $sth = $dbh->prepare($qry);
  my $r = $sth->execute($streaks_i);
  if(!$r) {
    $logger->fatal(
      sprintf(
        'sql_streak_get_tail(%d) failed, errdb=%s',
        $streaks_i, $dbh->errstr()
      )
    );
    return $sth->errstr();
  }
  my $result = $sth->fetchrow_hashref();
  $sth->finish();

  #--- finish

  return $result ? $result : "Last game in streak $streaks_i not found";
}


#============================================================================
# Get streaks_i for given logfiles_i and name.
#============================================================================

sub sql_streak_find
{
  #--- arguments

  my $logfiles_i = shift;
  my $name = shift;

  #--- other init

  my $logger = get_logger('Streaks');
  my $dbh = $db->handle();

  #--- db query

  my $qry =
    q{SELECT streaks_i FROM streaks } .
    q{WHERE logfiles_i = ? AND name = ? AND open IS TRUE};
  my $sth = $dbh->prepare($qry);
  my $r = $sth->execute($logfiles_i, $name);
  if(!$r) {
    $logger->fatal(
      sprintf(
        'sql_streak_find(%d, %s) failed, errdb=%s',
        $logfiles_i, $name, $dbh->errstr()
      )
    );
    return $sth->errstr();
  }
  my $streaks_i = $sth->fetchrow_array();
  $sth->finish();
  $logger->debug(
    sprintf(
      'Pre-existing streak %d for (%d,%s) found',
      $streaks_i, $logfiles_i, $name
    )
  ) if $streaks_i;

  #--- finish

  return [ $streaks_i ];
}


#============================================================================
# Write update info into "update" table.
#============================================================================

sub sql_update_info
{
  my $update_variant = shift;
  my $update_name    = shift;
  my ($qry, $re);
  my $dbh = $db->handle();

  #--- write updated variants

  if(scalar(keys %$update_variant)) {
    for my $var (keys %$update_variant, 'all') {
      $re = $dbh->do(
        q{UPDATE update SET upflag = TRUE WHERE variant = ? AND name = ''},
        undef, $var
      );
      if(!$re) {
        return $dbh->errstr();
      }

      # if no entry was updated, we have to create one instead
      elsif($re == 0) {
        $re = $dbh->do(
          q{INSERT INTO update VALUES (?,'',TRUE)},
          undef, $var
        );
        if(!$re) { return $dbh->errstr(); }
      }
    }
  }

  #--- write update player names

  for my $name (keys %$update_name) {
    for my $var (keys %{$update_name->{$name}}, 'all') {
      $re = $dbh->do(
        q{UPDATE update SET upflag = TRUE WHERE variant = ? AND name = ?},
        undef, $var, $name
      );
      if(!$re) { return $dbh->errstr(); }
      if($re == 0) {
        $re = $dbh->do(
          q{INSERT INTO update VALUES (?, ?, TRUE)},
          undef, $var, $name
        );
        if(!$re) {
          return $dbh->errstr();
        }
      }
    }
  }

  #--- finish successfully

  return undef;
}


#============================================================================
# This function performs database purge for given servers/variants (or all
# of them if none are specified).
#============================================================================

sub sql_purge_database
{
  #--- arguments

  my ($logfiles) = @_;

  #--- other variables

  my $dbh = $db->handle();

  #--- init logging

  my $logger = get_logger('Feeder::Purge_db');
  $logger->info(
    pluralize('Requested database purge of %d source(s)', $logfiles->count)
  );

    #--- iterate over logfiles

  for my $log (@{$logfiles->logfiles}) {
    my $r;
    my ($srv, $var) = ($log->get('server'), $log->get('variant'));
    my $logfiles_i = $log->get('logfiles_i');
    $logger->info("[$srv/$var] ", $log->get('descr'));

  #--- eval begin

    eval {

  #--- start transaction

      $r = $dbh->begin_work();
      if(!$r) {
        $logger->fatal(
          sprintf(
            "[%s/%s] Transaction begin failed (%s), aborting batch",
            $srv, $var, $dbh->errstr()
          )
        );
        die "TRFAIL\n";
      }

  #--- delete the games

      $logger->info("[$srv/$var] Deleting from games");
      $r = $dbh->do('DELETE FROM games WHERE logfiles_i = ?', undef, $logfiles_i);
      if(!$r) {
        $logger->fatal(
          sprintf(
            '[%s/%s] Deleting from games failed (%s)',
            $srv, $var, $dbh->errstr()
          )
        );
        die "ABORT\n";
      } else {
        $logger->info(
          sprintf('[%s/%s] Deleted %d entries', $srv, $var, $r)
        );
      }

  #--- reset 'fpos' field in 'logfiles' table

      $log->reset;

  #--- eval end

    };
    chomp $@;
    if(!$@) {
      $r = $dbh->commit();
      if(!$r) {
        $logger->fatal(
          sprintf(
            "[%s/%s] Failed to commit transaction (%s)",
            $srv, $var, $dbh->errstr()
          )
        );
      } else {
        $logger->info("[$srv/$var] Transaction commited");
      }
    } elsif($@ eq 'ABORT') {
      $r = $dbh->rollback();
      if(!$r) {
        $logger->fatal(
          sprintf(
            "[%s/%s] Failed to abort transaction (%s)",
            $srv, $var, $dbh->errstr()
          )
        );
      } else {
        $logger->info("[$srv/$var] Transaction aborted");
      }
    }

  #--- end of iteration over logfiles

  }

}


#============================================================================
# Function for listing/adding/removing player name mappings using the --pmap
# options (--pmap-list, --pmap-add, --pmap-remove).
#
# If no argument is given, existing mappings are listed.
# Otherwise, the arguments have the form: SRCNAME/SRV=DSTNAME. When DSTNAME
# is present, a mapping is added. If DSTNAME is missing, a mapping is
# removed. The removes are performed before the additions.
#
# Returns undef on success of error message.
#============================================================================

sub sql_player_name_map
{
  #--- init

  my $logger = get_logger('Feeder::Admin');
  my $dbh = $db->handle();
  my $in_transaction = 0;
  my $r;

  #--- eval loop

  eval {

  #--- listing all configured mappings

    if(!@_) {
      $logger->info('Listing configured player name mappings');
      my $cnt = 0;
      my $sth = $dbh->prepare('SELECT * FROM translations ORDER BY name_to');
      $r = $sth->execute();
      if(!$r) {
        die 'Failed to query database (' . $sth->errstr() . ") \n";
      } else {
        $logger->info('source               | destination');
        $logger->info('-' x (20+16+3));
        while(my $row = $sth->fetchrow_hashref()) {
          $logger->info(
            sprintf(
              "%-20s | %-16s\n",
              $row->{'server'} . '/' . $row->{'name_from'},
              $row->{'name_to'}
            )
          );
          $cnt++;
        }
        $logger->info('-' x (20+16+3));
        $logger->info(
          sprintf('%d mappings configured', $cnt)
        );
      }
      return undef;
    }

  #--- start transaction

    $r = $dbh->begin_work();
    if(!$r) {
      die sprintf("Cannot begin database transaction (%s)\n", $dbh->errstr());
    }
    $in_transaction = 1;

  #--- loop over arguments and create update plan

  # We are creating separate plans for adding and removing so that removing
  # can go before adding.

    my (@plan_add, @plan_remove);
    for my $arg (@_) {
      if($arg =~ /
        ^
        (?<src>[a-zA-Z0-9]+)         # 1. source (server-specific) name
        \/                           #    slash (separator)
        (?<srv>[a-zA-Z0-9]{3})       # 2. server id
        (?:
          =                          #    = sign (separator)
          (?<dst>[a-zA-Z0-9]+)       # 3. destination (aggregate) name
        )?
        $
      /x) {
        if($+{'dst'}) {
          push(@plan_add, {
            src => $+{'src'}, srv => $+{'srv'}, dst => $+{'dst'}
          });
        } else {
          push(@plan_remove, {
            src => $+{'src'}, srv => $+{'srv'}
          });
        }
      }
    }

  #--- perform removals

    for my $row (@plan_remove) {
      my $s;
      $r = $dbh->do(
        'DELETE FROM translations WHERE server = ? AND name_from = ?',
        undef, $row->{'srv'}, $row->{'src'}
      );
      if($r) {
        $r = $dbh->do(
          'UPDATE games g SET name = name_orig FROM logfiles l ' .
          'WHERE g.logfiles_i = l.logfiles_i AND name_orig = ? AND server = ?',
          undef, $row->{'src'}, $row->{'srv'}
        );
        if($r) {
          $s = $dbh->do(
            'UPDATE streaks s SET name = name_orig FROM logfiles l ' .
            'WHERE s.logfiles_i = l.logfiles_i AND name_orig = ? AND server = ?',
            undef, $row->{'src'}, $row->{'srv'}
          );
        }
      }
      if(!$r || !$s) {
        die sprintf "Failed to update database (%s)\n", $dbh->errstr();
      }
      $logger->info(sprintf(
        'Removed mapping %s/%s, updated %d games, %d streaks',
        $row->{'srv'}, $row->{'src'}, $r, $s
      ));
    }

  #--- perform additions

    for my $row (@plan_add) {
      my $s;
      $r = $dbh->do(
        'INSERT INTO translations ( server,name_from,name_to ) ' .
        'VALUES ( ?,?,? )',
        undef, $row->{'srv'}, $row->{'src'}, $row->{'dst'}
      );
      if($r) {
        $r = $dbh->do(
          'UPDATE games g SET name = ? FROM logfiles l ' .
          'WHERE g.logfiles_i = l.logfiles_i AND name_orig = ? AND server = ?',
          undef, $row->{'dst'}, $row->{'src'}, $row->{'srv'}
        );
        if($r) {
          $s = $dbh->do(
            'UPDATE streaks s SET name = ? FROM logfiles l ' .
            'WHERE s.logfiles_i = l.logfiles_i AND name_orig = ? AND server = ?',
            undef, $row->{'dst'}, $row->{'src'}, $row->{'srv'}
          );
        }
      }
      if(!$r || !$s) {
        die sprintf "Failed to update database (%s)\n", $dbh->errstr();
      }
      $logger->info(sprintf(
        'Added mapping %s/%s to %s, updated %d games, %d streaks',
        $row->{'srv'}, $row->{'src'}, $row->{'dst'}, $r, $s
      ));
    }

  #--- eval end

  };
  if($@) {
    my $err = $@;
    chomp($err);
    $logger->error($err);
    if($in_transaction) {
      $r = $dbh->rollback();
      if(!$r) {
        $logger->error(
          sprintf('Failed to abort transaction (%s)', $dbh->errstr())
        );
        $err = $err . sprintf(', transaction not aborted (%s)', $dbh->errstr());
      } else {
        $logger->error('Transaction aborted, no changes made');
        $err = $err . ', transaction aborted';
      }
    }
    return $err;
  }

  #--- commit transaction

  if($in_transaction) {
    $r = $dbh->commit();
    if(!$r) {
      return sprintf(
        'Failed to commit database transaction (%s)', $dbh->errstr()
      );
    }
    $logger->info('Changes commited');
  }

  #--- finish successfully

  return undef;
}


#============================================================================
#===================  _  ====================================================
#===  _ __ ___   __ _(_)_ __  ===============================================
#=== | '_ ` _ \ / _` | | '_ \  ==============================================
#=== | | | | | | (_| | | | | | ==============================================
#=== |_| |_| |_|\__,_|_|_| |_| ==============================================
#===                           ==============================================
#============================================================================
#============================================================================

#--- initialize logging

Log::Log4perl->init('cfg/logging.conf');
$logger = get_logger('Feeder');

#--- title

$logger->info('NetHack Scoreboard / Feeder');
$logger->info('(c) 2013-17 Borek Lupomesky');
$logger->info('---');

#--- process commandline options

my $cmd = NHdb::Feeder::Cmdline->instance(lockfile => $lockfile);

#--- lock file check/open

try {
  $cmd->lock;
} catch {
  chomp;
  $logger->warn($_);
  exit(1);
};

#--- connect to database

$db = NHdb::Db->new(id => 'nhdbfeeder', config => $nhdb);
my $dbh = $db->handle();
die "Undefined database handle" if !$dbh;

$dbic = NHdb::Schema->connect(sub { $dbh; });
#$dbic = NHdb::Schema->connect(
#  'dbi:Pg:dbname=nhdb',
#  'nhdbfeeder',
#  $nhdb->config()->{'auth'}{'nhdbfeeder'},
#  { pg_enable_utf8 => 1 }
#);

#--- process --pmap options

my (@cmd_pmap_add, @cmd_pmap_remove);

if($cmd->pmap_list()) {
  my $r = sql_player_name_map();
  exit($r ? 1 : 0);
}

if($cmd->pmap_add() || $cmd->pmap_remove()) {
  @cmd_pmap_add =
    grep { /^[a-zA-Z0-9]+\/[a-zA-Z0-9]+=[a-zA-Z0-9]+$/ } @{$cmd->pmap_add()}
    if $cmd->pmap_add();
  @cmd_pmap_remove =
    grep { /^[a-zA-Z0-9]+\/[a-zA-Z0-9]+$/ } @{$cmd->pmap_remove()}
    if $cmd->pmap_remove();
  my $r = 1;
  if(scalar(@cmd_pmap_add) + scalar(@cmd_pmap_remove)) {
    $r = sql_player_name_map(@cmd_pmap_add, @cmd_pmap_remove);
  } else {
    $logger->fatal('No valid maps');
  }
  exit($r ? 1 : 0);
}

#--- initialize logfiles processing

my $logfiles = NHdb::Logfiles->new(
  db => $cmd->oper_filter
    ? $dbic->resultset('Logfiles')->search_rs({ oper => 'true'})
    : $dbic->resultset('Logfiles'),
  servers => $cmd->servers,
  variants => $cmd->variants,
  # FIXME: We really should make --logid accept multiple values,
  # current state of things is inconsistent
  logids => $cmd->logid ? [ $cmd->logid ] : undef,
);

$logger->info(
  pluralize(
    '%d source(s) out of %d selected for processing',
    $logfiles->count, $logfiles->count_all
  )
);

if(!$logfiles->count) {
  $cmd->unlock; exit(1);
}

#--- process --oper and --static options

if(defined($cmd->operational()) || defined($cmd->static())) {
  $logfiles->set_state(
    oper => $cmd->operational,
    static => $cmd->static,
  );
  $logger->info('Operational/static flags set, exiting');
  exit(0);
}

#--- display logfiles, if requested with --logfiles

if($cmd->show_logfiles()) {
  $logfiles->display_list;
  exit(0);
}

#--- database purge

if($cmd->purge()) {
  sql_purge_database($logfiles);
  $cmd->unlock;
  exit(0);
}

#--- load list of translations

my $translations_rs = $dbic->resultset('Translations');
while(my $tl = $translations_rs->next()) {
  $translations{$tl->server}{$tl->name_from} = $tl->name_to;
  $translations_cnt++;
}
$logger->info(
  sprintf(
    "Loaded %d name translation%s\n",
    $translations_cnt,
    ($translations_cnt != 1 ? 's' : '')
  )
);

#--- check update table
# this code checks if update table has any rows in it;
# when finds none, it assumes it is uninitialized and
# initializes it

my ($r, $sth);
$logger->info('Checking update table');
my $cnt_update = $dbic->resultset('Update')->count();
if($cnt_update == 0) {
  $logger->info('No entries in the update table');
  $logger->info('Initializing update table, step 1');

  $r = $dbic->storage->dbh_do(sub {
    my ($storage, $dbh) = @_;
    $dbh->do(
      'INSERT INTO update ' .
      'SELECT variant, name ' .
      'FROM games LEFT JOIN logfiles USING (logfiles_i) ' .
      'GROUP BY variant, name'
    );
  });
  $logger->info(sprintf('Update table initialized with %d entries (step 1)', $r));

  $logger->info('Initializing update table, step 2');
  $r = $dbic->storage->dbh_do(sub {
    my ($storage, $dbh) = @_;
    $dbh->do(
      q{INSERT INTO update } .
      q{SELECT 'all', name, FALSE } .
      q{FROM games LEFT JOIN logfiles USING (logfiles_i) } .
      q{GROUP BY name}
    )
  });
  $logger->info(sprintf('Update table initialized with %d entries (step 2)', $r));
} else {
  $logger->info(sprintf('Update table has %d entries', $cnt_update));
}

#--- iterate over logfiles

for my $log (@{$logfiles->logfiles}) {

  my $transaction_in_progress = 0;
  my $logfiles_i = $log->get('logfiles_i');
  my $lbl = sprintf('[%s/%s] ', $log->get('variant'), $log->get('server'));

  try { # <--- try block starts here -----------------------------------------

    #--- prepare, print info

    my $localfile = $log->localfile;
    my @fsize;
    my $fpos = $log->get('fpos');
    $fsize[0] = -s $localfile;
    $logger->info('---');
    $logger->info($lbl, 'Processing started');
    $logger->info($lbl, 'Local file is ', $localfile);
    $logger->info($lbl,
      'Logfile URL is ', $log->get('logurl') ? $log->get('logurl') : 'N/A'
    );

    #--- retrieve file
    # FIXME: Feedback to the user needs improving

    $logger->debug($lbl,
      sprintf('Current fpos = %s',
      defined $log->get('fpos') ? $log->get('fpos') : 'undef')
    );

    $r = $log->retrieve_xlogfile;
    if($r == -1) {
      $logger->warn($lbl, 'Failed to retrieve the xlogfile');
      die;
    } elsif($r == 0 && defined $log->get('fpos')) {
      $logger->warn($lbl, 'No new data, skipping further processing');
      die "OK\n";
    } else {
      $logger->info($lbl,
        sprintf(
          'Logfile retrieved successfully, got %d bytes',
          (-s $localfile) - $log->get('fpos')
        )
      );
    }

    #--- open the file

    if(!open(F, $localfile)) {
      $logger->error($lbl, 'Failed to open local file ', $localfile);
      die;
    }

    #--- seek into the file (if position is known)

    if($fpos) {
      $logger->info($lbl, sprintf('Seeking to %d', $fpos));
      $r = seek(F, $fpos, 0);
      if(!$r) {
        $logger->error($lbl, sprintf('Failed to seek to $fpos', $fpos));
        die;
      }
    }

    #--- initialize the interface into the 'games' table

    my $games = NHdb::Games->new(
      db => $dbic->resultset('Games'),
      nhdb => $nhdb,
      nh => $nh,
      log => $log,
      translations => \%translations,
    );

    #--- begin transaction

    $logger->info($lbl, 'Starting database transaction');
    $r = $dbh->begin_work();
    if(!$r) {
      $logger->info($lbl, 'Failed to start database transaction');
      die;
    }
    $transaction_in_progress = 1;

    #--- now read content of the file

    my $lc = 0;           # line counter
    my $tm = time();      # timer
    my $ll = 0;           # time of last info
    my %update_name;      # updated names
    my %update_variant;   # updated variants
    my %streak_open;      # indicates open streak for

    $logger->info($lbl, 'Processing file ', $localfile);

    while(my $l = <F>) { #<<< read loop beings here

      chomp($l);

    #--- devnull logfiles are slightly modified by having a server id
    #--- prepended to the usual xlogfile line

      if($log->has_option('devnull')) {
        $l =~ s/^\S+\s(.*)$/$1/;
      }

    #--- parse log

      my $pl = parse_log($log, $l);

    #--- insert row into database

      my $row = $games->add_new($pl, $log->get('lines') + $lc);
      if(1) {

    #--- mark updates
    # FIXME: There's subtle potential issue with this, since
    # scummed games do trigger these updates; I haven't decided
    # if we want this or not.

        $update_variant{$log->get('variant')} = 1;
        $update_name{$row->name}{$log->get('variant')} = 1;

    #-------------------------------------------------------------------------
    #--- streak processing starts here ---------------------------------------
    #-------------------------------------------------------------------------

    #--- initialize streak status for name

    # if the streak status is not yet stored in memory, which happens when
    # we first encounter (logfiles_i, name) pair, it is loaded from database
    # (table "streaks"); if the streak is not found (ie. the player has no
    # streaks) 0 is returned

        if(!exists($streak_open{$logfiles_i}{$row->name})) {
          $r = sql_streak_find($logfiles_i, $row->name);
          die $r if !ref($r);
          $streak_open{$logfiles_i}{$row->name} = $r->[0];
        }

    #--- game is ASCENDED

        if($row->ascended) {

    #--- game is ASCENDED / streak is NOT OPEN

          if(!$streak_open{$logfiles_i}{$row->name}) {
            my $streaks_i = sql_streak_create_new(
              $logfiles_i,
              $row->name,
              $row->name_orig,
              $row->id
            );
            die $streaks_i if !ref($streaks_i);
            $streak_open{$logfiles_i}{$row->name} = $streaks_i->[0]
          }

    #--- game is ASCENDED / streak is OPEN
    # we are checking for overlap between the last game of the streak
    # and the current game; if there is overlap, the streak is broken;
    # NOTE: overlap can only be checked when starttime/endtime fields
    # actually exist! This is not fulfilled for NAO games before
    # March 19, 2018.

          else {
            my $last_game = sql_streak_get_tail(
              $streak_open{$logfiles_i}{$row->name}
            );
            if(!ref($last_game)) {
              die sprintf(
                'sql_streak_get_tail(%s) failed with msg "%s"',
                $streak_open{$logfiles_i}{$row->name}, $last_game
              );
            }
            #printf("--> last_game.endtime_raw=%d current_game.endtime_raw=%d, ovr=%d\n",
            #  $last_game->{'endtime_raw'},
            #  $row->starttime_raw,
            #  $last_game->{'endtime_raw'} >= $row->starttime_raw
            #);
            if(
              $last_game->{'endtime_raw'}
              && $row->starttime_raw
              && $last_game->{'endtime_raw'} >= $row->starttime_raw
            ) {
              # close current streak
              $logger->info($lbl,
                sprintf(
                  'Closing overlapping streak %d',
                  $streak_open{$logfiles_i}{$row->name})
              );
              $r = sql_streak_close(
                $streak_open{$logfiles_i}{$row->name}
              );
              die $r if !ref($r);
              # open new
              $r = sql_streak_create_new(
                $logfiles_i,
                $row->name,
                $row->name_orig,
                $row->id
              );
              die $r if !ref($r);
              $streak_open{$logfiles_i}{$row->name} = $r->[0];
            } else {
              $r = sql_streak_append_game(
                $streak_open{$logfiles_i}{$row->name},
                $row->id
              );
              die $r if !ref($r);
            }
          }
        }

    #--- game is not ASCENDED

        else {

    #--- game is not ASCENDED / streak is OPEN

          if($streak_open{$logfiles_i}{$row->name}) {
            $r = sql_streak_close(
              $streak_open{$logfiles_i}{$row->name}
            );
            die $r if !ref($r);
            $streak_open{$logfiles_i}{$row->name} = undef;
          }

        }

      }

    #--- display progress info

      if((time() - $tm) > 5) {
        $tm = time();
        $logger->info($lbl,
          sprintf('Processing (%d lines, %d l/sec)', $lc, ($lc-$ll)/5 )
        );
        $ll = $lc;
      }
      $lc++;

    } #<<< read loop ends here

    $logger->info($lbl,
      sprintf('Finished reading %d lines', $lc)
    );

    #--- close streak for 'static' sources

    if($log->get('static')) {
      my $re = sql_streak_close_all($logfiles_i);
      if(!ref($re)) {
        $logger->error($lbl, q{Failed to close all streaks});
        die;
      }
      if($re->[0]) {
        $logger->info($lbl, sprintf('Closed %d streak(s)', $re->[0]));
      }
    }

    #--- write update info

    my $re = sql_update_info(\%update_variant, \%update_name);
    if($re) { die $re; }

    #--- update database with new position in the file

    my @logupdate = (
      'fpos = ?',
      'lastchk = current_timestamp',
      'lines = ?'
    );

    if($log->get('static')) { push(@logupdate, 'oper = false'); }
    my $qry = sprintf(
      'UPDATE logfiles SET %s WHERE logfiles_i = ?', join(', ', @logupdate)
    );
    $sth = $dbh->prepare($qry);
    $r = $sth->execute(
      $fsize[1],
      $log->get('lines') + $lc,
      $log->get('logfiles_i')
    );
    if(!$r) {
      $logger->error($lbl, q{Failed to update table 'servers'});
      die;
    }

    #--- commit transaction

    $r = $dbh->commit();
    $transaction_in_progress = 0;
    if(!$r) {
      $logger->error($lbl, 'Failed to commit transaction');
      die;
    }
    $logger->info($lbl, 'Transaction commited');

  } # <--- eval ends here -------------------------------------------------

  #--- handle failure

  catch {

    # log exception message, if any
    if($_ ne "OK\n") {
      $logger->warn($lbl, 'Eval ended with error: ', $_);
    }

    # rollback if needed
    if($transaction_in_progress) {
      $logger->warn($lbl, 'Transaction rollback');
      $dbh->rollback();
    }
  };

  #--- finish

  $logger->info($lbl, 'Processing finished');
}

#--- release lock file

$cmd->unlock;
