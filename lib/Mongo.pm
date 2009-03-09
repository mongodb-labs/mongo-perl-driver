use strict;
use warnings;

package Mongo;
# ABSTRACT: A Mongo Driver for Perl

=head1 SYNOPSIS

    use Mongo;

    my $connection = Mongo::Connection->new(host => 'localhost, port => 27017);
    my $database   = $connection->get_database('foo');
    my $collection = $database->get_collection('bar');
    my $id         = $collection->insert({ some => 'data' });
    my $data       = $collection->find_one({ _id => $id });

=cut

our $VERSION = '0.01';

use XSLoader;
use Mongo::Connection;

XSLoader::load(__PACKAGE__, $VERSION);

1;
