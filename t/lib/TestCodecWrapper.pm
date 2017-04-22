use strict;
use warnings;

use MongoDB::BSON;
use MongoDB::OID;

package TestCodecWrapper;

our @ISA = qw/MongoDB::BSON/;

sub create_oid {
    my $oid = MongoDB::OID->new();
    return bless $oid, "TestCodecWrapper::OID";
}

package TestCodecWrapper::OID;

our @ISA = qw/MongoDB::OID/;

1;

