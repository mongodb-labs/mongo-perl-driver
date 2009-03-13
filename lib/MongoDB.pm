use strict;
use warnings;

package MongoDB;
# ABSTRACT: A Mongo Driver for Perl

=head1 SYNOPSIS

    use MongoDB;

    my $connection = MongoDB::Connection->new(host => 'localhost, port => 27017);
    my $database   = $connection->get_database('foo');
    my $collection = $database->get_collection('bar');
    my $id         = $collection->insert({ some => 'data' });
    my $data       = $collection->find_one({ _id => $id });

=cut

our $VERSION = '0.01';

use XSLoader;
use MongoDB::Connection;

XSLoader::load(__PACKAGE__, $VERSION);

1;
