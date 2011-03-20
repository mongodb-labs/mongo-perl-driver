package MongoDB_TestUtils;

use strict;
use warnings;

use MongoDB;

use Exporter 'import';
our @EXPORT = qw(port start_mongod stop_mongod mconnect restart_mongod );

sub port        { 27272 }
sub dbpath      { '/tmp' }
sub host        { 'localhost' }
sub pidfilepath { dbpath() . '/md.pid' }

sub restart_mongod {

    stop_mongod() && start_mongod() && return 1;
    return 0;
}

sub start_mongod {

    my $port    = shift || port();
    my $dbpath  = shift || dbpath();
    my $pidfile = shift || pidfilepath();

    my $cmd = "mongod "
        . "--dbpath $dbpath "
        . "--port $port "
        . "--pidfilepath $pidfile "
        . "--fork "
        . "--logpath $dbpath/mongod.log";

    #print $cmd;
    system $cmd;

    sleep 3;

    return !$?;
}

sub stop_mongod {

    my $pidfilepath = shift || pidfilepath();

    open(my $fh, '<', $pidfilepath) || return 0;
    my $pid = <$fh>;
    if ($pid) {
        system "kill $pid";
        sleep 3;
    }
    return 1;
}

sub mconnect {

    my $port = shift || port();
    return eval {
        MongoDB::Connection->new(
            host => $ENV{MONGOD} || host(),
            port => $port,
            auto_reconnect => 1
        );
    };
}

1;
