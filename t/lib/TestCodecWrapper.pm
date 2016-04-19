use strict;
use warnings;

use BSON;
use BSON::OID;

package TestCodecWrapper;

our @ISA = qw/BSON/;

sub create_oid {
    my $oid = BSON::OID->new();
    return bless $oid, "TestCodecWrapper::OID";
}

package TestCodecWrapper::OID;

our @ISA = qw/BSON::OID/;

1;

