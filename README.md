# NETHACK SCOREBOARD

**Note**: This original version of the NetHack Scoreboard is no longer maintained as I have stopped working on it. Further development is happening in [this](https://github.com/aoeixsz4/nhs-fork) fork.

-----

This is the code used to run [NetHack Scoreboard](https://scoreboard.xd.cm/) web site. The code consists of two main components: *feeder* and *stats generator*. The feeder retrieves [xlogfiles](http://nethackwiki.com/wiki/Xlogfile) from public NetHack servers, parses them and stores the parsed log entries in a back-end database. The stats generator then uses this data to generate static HTML pages with various statistics, including personal pages.

The NetHack Scoreboard is written using:

* **perl** as the programming language
* **PostgreSQL** as backend database
* **Template Toolkit** as templating engine
* **Log4Perl** as logging system
* **Moo** as OOP framework

-----

## Command-line parameters

All the options that suply variants, servers or player names can be either used multiple times on the command-line, or they can have aggregate multiple strings by joining them with commas. Example:

     nhdb-feeder --variant=all --variant=nh --variant=nh4
     nhdb-feeder --variant=all,nh,nh4

### nhdb-feeder.pl

**--logfiles**  
This will list all configured data sources and exit without doing anything else.

**--server**=*server*  
Only sources on specified server will be processed. "srv" is three letter
server acronym such as "nao", "nxc" etc. Using this option will override the
source server being defined as unoperational in the database (table
'logfiles'), but it will not override the server being defined as static. This
behaviour enables reloading inoperational servers without needing to go to the
database to temporarily switch their 'oper' field. Please note, that one
server can host multiple variants (and therefore have multiple logs associated
with it), use `--variant` to further limit processing to single source.

**--variant**=*variant*  
Limit processing only to variant specified by its short-code (such as "nh", "unh" etc.)

**--logid**=*id*  
Limit processing only to logfiles specified by their log ids. Log id is NHS's internal identification of a configured logfile. The `--logfiles` option will display these id's.

**--purge**  
Erase all database entries that match `--logid`, `--server` and `--variant` options. If used alone without any specification, all the entries are deleted.

**--oper**, **--nooper**  
Enable/disable all processing of selected sources.

**--static**, **--nostatic**  
Make selected sources static (or non-static), ie. never try to download the source's configured xlogfile, but still
process it if it grows or its database entries are purged.

**--pmap-list**  
Display list of current player name translations.

**--pmap-add**=*SRCNAME*/*SERVER*=*DSTNAME*  
Add new translation, playername *srcname* on server *server* will be considered
*dstname*. Multiple translations can be added at the same time and this can be combined with `--pmap-remove`.

**--pmap-remove**=*SRCNAME*/*SERVER*  
Remove existing translation. Multiple translations can be removed at the same time and this can be combined with `--pmap-add`.

### nhdb-stats.pl

**--noaggr**  
Disable generation of aggregate pages (such as list of recent ascensions, streak list and so on).

**--force**  
Force processing of all variants and players, even if they do not need updating. Note, that regenerating all players' pages takes very long time. If you just want force regenerating aggregate pages only, use the `--noplayers` option along with `--force`.

**--variant**=*variant*  
Limit processing only to specified variant. This can be used multiple times. Variant can also be "all".

**--noplayers**  
Disable generating player pages.

**--player**=*player*  
Use this to limit processing player pages to specific player or players.
