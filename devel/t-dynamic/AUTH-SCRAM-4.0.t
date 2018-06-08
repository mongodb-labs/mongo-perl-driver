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

use strict;
use warnings;
use utf8;
use Test::More 0.88;
use Test::Fatal;
use Test::Deep qw/!blessed/;
use Scalar::Util qw/refaddr/;
use Tie::IxHash;
use boolean;

use MongoDB;
use MongoDB::Error;

use lib "t/lib";
use lib "devel/lib";

use if $ENV{MONGOVERBOSE}, qw/Log::Any::Adapter Stderr/;

use MongoDBTest::Orchestrator;
use MongoDBTest qw/build_client get_test_db clear_testdbs uri_escape/;
use MongoDB::_URI;

binmode( Test::More->builder->$_, ":utf8" )
  for qw/output failure_output todo_output/;

# Monkey patch MongoDB::Credential to verify which auth is called
my @scram_args;
{
    no warnings 'redefine';
    my $fcn = \&MongoDB::_Credential::_scram_auth;
    *MongoDB::_Credential::_scram_auth = sub {
        @scram_args = @_;
        goto $fcn;
      }
}

sub create_user {
    my ( $db, $user, $pass, $mechs ) = @_;
    note "Creating user credential for $user";
    $db->run_command(
        [
            createUser => $user,
            pwd        => $pass,
            roles      => [ { role => 'readWrite', db => $db->name } ],
            ( $mechs ? ( mechanisms => $mechs ) : () )
        ]
    );
}

sub auth_ok {
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    my ( $label, $dbname, %options ) = @_;

    my %cases = (
        "via code" => \%options,
        "via uri" => _options_to_uri( %options )
    );

    for my $k ( sort keys %cases ) {
        my $conn = build_client(%{$cases{$k}});
        my $coll = $conn->db($dbname)->get_collection("test_collection");
        is( exception { $coll->count_documents }, undef, "$label ($k)" );
    }

}

sub auth_not_ok {
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    my ( $label, $dbname, @options ) = @_;
    my $conn = build_client(@options);
    my $coll = $conn->db($dbname)->get_collection("test_collection");
    like( exception { $coll->count_documents }, qr/MongoDB::AuthError/, $label );
}

my $orc =
  MongoDBTest::Orchestrator->new(
    config_file => "devel/config/mongod-4.0-scram.yml" );
$orc->start;
$ENV{MONGOD} = $orc->as_uri;

my $uri = MongoDB::_URI->new( uri => $ENV{MONGOD} );
my $no_auth_string = "mongodb://" . $uri->hostids->[0];
my $roman_four = "\x{2163}"; # ROMAN NUMERAL FOUR -> prepped is "IV"
my $roman_nine = "\x{2168}"; # ROMAN NUMERAL NINE -> prepped is "IX"
my $admin_client     = build_client();
my $testdb = get_test_db($admin_client);

sub _options_to_uri {
    my %options = @_;
    my $u = uri_escape(delete $options{username});
    my $p = uri_escape(delete $options{password});
    my $s = delete $options{db_name};
    my $uri = "$no_auth_string/$s";
    $uri =~ s{mongodb://}{mongodb://$u:$p\@};
    $options{host} = $uri;
    return \%options;
}

my %users = (
    # Test steps 1-3
    sha1        => [ 'sha1',      ['SCRAM-SHA-1'] ],
    sha256      => [ 'sha256',    ['SCRAM-SHA-256'] ],
    both        => [ 'both',      [ 'SCRAM-SHA-1', 'SCRAM-SHA-256' ] ],
    # Test step 4 ( extra array ref are alternate passwd forms )
    IX          => [ 'IX',        ['SCRAM-SHA-256'], ["I\x{00AD}X"] ],
    $roman_nine => [ $roman_four, ['SCRAM-SHA-256'], ["I\x{00AD}V"] ],
);

for my $user ( sort keys %users ) {
    my ( $pwd, $mechs, undef ) = @{ $users{$user} };
    create_user( $testdb, $user, $pwd, $mechs );
}

subtest "no authentication" => sub {
    my $conn = build_client( host => $no_auth_string, dt_type => undef );
    my $coll = $conn->db( $testdb->name )->get_collection("test_collection");

    like(
        exception { $coll->count_documents },
        qr/not authorized/,
        "can't read collection when not authenticated"
    );
};

subtest "invalid user" => sub {
    my $conn = build_client(
        host     => $no_auth_string,
        username => 'doesntexist',
        password => 'trustno1',
        db_name  => $testdb->name,
        dt_type  => undef,
    );
    my $coll = $conn->db( $testdb->name )->get_collection("test_collection");

    like(
        exception { $coll->count_documents },
        qr/MongoDB::AuthError.*mechanism negotiation error/,
        "unknown user is an auth error"
    );
};

for my $user ( sort keys %users ) {
    my @pwds = ( $users{$user}[0] );
    push @pwds, @{$users{$user}[2]}
        if $users{$user}[2];

    for my $pass ( @pwds ) {
        subtest "auth user $user, pwd $pass" => sub {
            my @options = (
                host     => $no_auth_string,
                username => $user,
                password => $pass,
                db_name  => $testdb->name,
                dt_type  => undef,
            );

            my $user_has_mech = { map { $_ => 1 } @{ $users{$user}[1] } };

            # auth with explicit mechanisms
            for my $mech (qw/SCRAM-SHA-1 SCRAM-SHA-256/) {
                if ( $user_has_mech->{$mech} ) {
                    auth_ok( "auth via $mech", $testdb->name, @options, auth_mechanism => $mech );
                    is( $scram_args[-1], $mech, "correct internal call for $mech" );
                }
                else {
                    auth_not_ok( "auth via $mech", $testdb->name, @options, auth_mechanism => $mech );
                }
            }

            # auth with negotiation
            auth_ok( "auth via negotiation", $testdb->name, @options );
            my $expected_mech =
            ( grep { $_ eq 'SCRAM-SHA-256' } @{ $users{$user}[1] } )
            ? 'SCRAM-SHA-256'
            : 'SCRAM-SHA-1';
            is( $scram_args[-1], $expected_mech, "correct internal call for negotiated mech" );
        };
    }
}

clear_testdbs;

done_testing;
