#
#  Copyright 2018 - present MongoDB, Inc.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

use strict;
use warnings;
use Test::More 0.96;
use Test::Fatal;

use MongoDB;

use lib "t/lib";
use MongoDBTest qw/skip_unless_mongod build_client get_test_db
  get_unique_collection server_type server_version/;

skip_unless_mongod();

my $conn           = build_client( bson_codec => { wrap_strings => 1 } );
my $server_type    = server_type($conn);
my $server_version = server_version($conn);

my $testdb = get_test_db($conn);
my $coll = get_unique_collection( $testdb, "types" );

sub roundtrip {
    my ($obj) = @_;
    my $type = ref($obj);
    my $doc = { _id => $type, x => $obj };
    $coll->insert_one($doc);
    my $rt = $coll->find_one( { _id => $type } );
    return $rt->{x};
}

#--------------------------------------------------------------------------#
# Manually-blessed types have no behavior so they can't be a subtype
# of a corresponding BSON type wrapper, but they will round-trip into
# that type (if wrap_strings is true);
#--------------------------------------------------------------------------#

my %manual_types = (
    'MongoDB::BSON::String' => 'BSON::String',
    'MongoDB::MaxKey'       => 'BSON::MaxKey',
    'MongoDB::MinKey'       => 'BSON::MinKey',
);

subtest "manually-blessed types" => sub {
    for my $k ( sort keys %manual_types ) {
        my $str = "dummy";
        my $obj = bless \$str, $k;
        ok( !$obj->isa( $manual_types{$k} ), "$k: is not a $manual_types{$k}" );
        is( ref( roundtrip($obj) ),
            $manual_types{$k}, "$k: round trip is type $manual_types{$k}" );
    }
};

#--------------------------------------------------------------------------#
# Deprecated class types should be empty subclasses of a BSON typewrapper,
# and must preserve their prior API to the greatest extent possible
#--------------------------------------------------------------------------#

my %class_types = (
    'MongoDB::BSON::Binary' => 'BSON::Bytes',
    'MongoDB::BSON::Regexp' => 'BSON::Regex',
    'MongoDB::Code'         => 'BSON::Code',
    'MongoDB::DBRef'        => 'BSON::DBRef',
    'MongoDB::OID'          => 'BSON::OID',
    'MongoDB::Timestamp'    => 'BSON::Timestamp',
);

subtest 'MongoDB::BSON::Binary' => sub {
    require MongoDB::BSON::Binary;
    my $class = "MongoDB::BSON::Binary";

    # API
    my @api = qw(
      data
      subtype
      SUBTYPE_GENERIC
      SUBTYPE_FUNCTION
      SUBTYPE_GENERIC_DEPRECATED
      SUBTYPE_UUID_DEPRECATED
      SUBTYPE_UUID
      SUBTYPE_MD5
      SUBTYPE_USER_DEFINED
    );

    for my $k (@api) {
        can_ok( $class, $k );
    }

    # Construction
    my $obj = $class->new( data => "123" );
    is( $obj->data,    "123", "data" );
    is( $obj->subtype, 0,     "subtype" );
    is( "$obj",        "123", "overload string" );

    $obj = $class->new( data => "123", subtype => 128 );
    is( $obj->subtype, 128, "subtype 128" );

    # ISA and roundtrip
    isa_ok( $class, $class_types{$class}, $class );
    is( ref( roundtrip($obj) ),
        $class_types{$class}, "round trip is type $class_types{$class}" );
};

subtest 'MongoDB::BSON::Regexp' => sub {
    require MongoDB::BSON::Regexp;
    my $class = "MongoDB::BSON::Regexp";

    # API
    my @api = qw(
      pattern
      flags
      try_compile
    );

    for my $k (@api) {
        can_ok( $class, $k );
    }

    # Construction
    my $obj = $class->new( pattern => "123" );
    is( $obj->pattern, "123", "pattern" );

    # MongoDB::BSON::Regexp defaulted to undef; BSON::Regex defaults to ""
    ok( !$obj->flags, "flags" );

    $obj = $class->new( pattern => "123", flags => "i" );

    ok( exception { $class->new( pattern => "123", flags => "a" ) },
        "unsupported flag errors" );

    # ISA and roundtrip
    isa_ok( $class, $class_types{$class}, $class );
    is( ref( roundtrip($obj) ),
        $class_types{$class}, "round trip is type $class_types{$class}" );
};

subtest 'MongoDB::Code' => sub {
    require MongoDB::Code;
    my $class = "MongoDB::Code";

    # API
    my @api = qw(
      code
      scope
    );

    for my $k (@api) {
        can_ok( $class, $k );
    }

    # Construction
    my $obj = $class->new( code => "123" );
    is( $obj->code,  "123", "code" );
    is( $obj->scope, undef, "scope (undef)" );

    $obj = $class->new( code => "123", scope => { x => 1 } );
    is_deeply( $obj->scope, { x => 1 }, "scope (hashref)" );

    # ISA and roundtrip
    isa_ok( $class, $class_types{$class}, $class );
    is( ref( roundtrip($obj) ),
        $class_types{$class}, "round trip is type $class_types{$class}" );
};

subtest 'MongoDB::DBRef' => sub {
    require MongoDB::DBRef;
    my $class = "MongoDB::DBRef";

    # API
    my @api = qw(
      id
      ref
      db
      extra
      _ordered
    );

    for my $k (@api) {
        can_ok( $class, $k );
    }

    # Construction
    my $input = { id => "123", 'ref' => "ref", db => 'db' };
    my $obj = $class->new($input);
    for my $k ( sort keys %$input ) {
        is( $obj->$k, $input->{$k}, $k );
    }

    # Construction, dollar sign version, with extras
    $input = { '$id' => "123", '$ref' => "ref", '$db' => 'db' };
    $obj = $class->new($input);
    for my $k ( sort keys %$input ) {
        ( my $new_k = $k ) =~ s/\$//;
        is( $obj->$new_k, $input->{$k}, $k );
    }

    # Construction with extras
    my %extra = ( x => 1, y => 2 );
    $obj = $class->new( %$input, %extra );
    is( $obj->extra->{x},   1, "extra: x" );
    is( $obj->extra->{'y'}, 2, "extra: y" );

    # Tie::IxHash
    isa_ok( $obj->_ordered, "Tie::IxHash", "_ordered" );

    # ISA and roundtrip
    isa_ok( $class, $class_types{$class}, $class );
    is( ref( roundtrip($obj) ),
        $class_types{$class}, "round trip is type $class_types{$class}" );
};

subtest 'MongoDB::OID' => sub {
    require MongoDB::OID;
    my $class = "MongoDB::OID";

    # API
    my @api = qw(
      value
      to_string
      get_time
      _get_pid
    );

    for my $k (@api) {
        can_ok( $class, $k );
    }

    # Construction
    my $aa = "a" x 24;
    my $obj = $class->new( value => $aa );
    is( $obj->value,     $aa, "value" );
    is( $obj->to_string, $aa, "to_string" );
    is( "$obj",          $aa, "overload string" );

    # Constructor variations
    $obj = $class->new($aa);
    is( $obj->value, $aa, "value" );
    $obj = $class->new();
    is( $obj->_get_pid, ($$ & 0xffff), "_get_pid" );
    $obj = $class->_new_oid;
    is( $obj->_get_pid, ($$ & 0xffff), "_get_pid" );
    $obj = $class->new( value => "00000000" . ( "a" x 16 ) );
    is( $obj->get_time, 0, "get_time" );

    # ISA and roundtrip
    isa_ok( $class, $class_types{$class}, $class );
    is( ref( roundtrip($obj) ),
        $class_types{$class}, "round trip is type $class_types{$class}" );
};

subtest 'MongoDB::Timestamp' => sub {
    require MongoDB::Timestamp;
    my $class = "MongoDB::Timestamp";

    # API
    my @api = qw(
      sec
      inc
    );

    for my $k (@api) {
        can_ok( $class, $k );
    }

    # Construction
    my %input = ( sec => 12345, inc => 0 );
    my $obj = $class->new(%input);
    for my $k ( sort keys %input ) {
        is( $obj->$k, $input{$k}, $k );
    }

    # Overloading
    my $low  = 1;
    my $high = 2;

    my $ts_low_sec_low_inc   = $class->new( sec => $low,  inc => $low );
    my $ts_low_sec_low_inc2  = $class->new( sec => $low,  inc => $low );
    my $ts_high_sec_low_inc  = $class->new( sec => $high, inc => $low );
    my $ts_high_sec_high_inc = $class->new( sec => $high, inc => $high );
    my $ts_low_sec_high_inc  = $class->new( sec => $low,  inc => $high );

    ok $ts_low_sec_low_inc < $ts_high_sec_low_inc, '<';
    ok $ts_low_sec_low_inc <= $ts_low_sec_high_inc, '<=';
    ok $ts_low_sec_low_inc <= $ts_low_sec_low_inc2, '<= identical';
    ok $ts_high_sec_low_inc > $ts_low_sec_high_inc, '>';
    ok $ts_high_sec_high_inc >= $ts_high_sec_low_inc, '>=';
    ok $ts_low_sec_low_inc >= $ts_low_sec_low_inc2,   '>= identical';
    ok $ts_low_sec_low_inc == $ts_low_sec_low_inc2, '==';
    ok $ts_low_sec_low_inc != $ts_high_sec_low_inc, '!=';

    # ISA and roundtrip
    isa_ok( $class, $class_types{$class}, $class );
    is( ref( roundtrip($obj) ),
        $class_types{$class}, "round trip is type $class_types{$class}" );
};

done_testing;
