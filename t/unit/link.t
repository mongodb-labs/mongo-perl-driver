use strict;
use warnings;
use Test::More 0.88;
use Test::Fatal;

use MongoDB::_Server;
use Time::HiRes qw/gettimeofday/;

my $class = "MongoDB::_Link";

require_ok( $class );

my $obj = new_ok( $class, [ address => 'localhost:27017'] );

my $dummy_server = MongoDB::_Server->new(
    address => 'localhost:27017',
    last_update_time => [ gettimeofday ],
);

$obj->set_metadata( $dummy_server );

is( $obj->max_bson_object_size, 4*1024*1024, "default max bson object size" );
is( $obj->max_message_size_bytes, 2*4*1024*1024, "default max message size" );

{
    # monkeypatch to let length check fire
    no warnings 'redefine', 'once';

    local *MongoDB::_Link::assert_valid_connection = sub { 1 };
    like(
        exception { $obj->write( "a" x ($obj->max_message_size_bytes + 1) ) },
        qr/Message.*?exceeds maximum/,
        "over long message throws error",
    );
}

done_testing;
# vim: ts=4 sts=4 sw=4 et:
