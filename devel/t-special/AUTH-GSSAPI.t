#  Copyright 2014 - present MongoDB, Inc.
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
use boolean;

# uncomment one of these to force a particular back-end
# use Authen::SASL 'XS';
# use Authen::SASL 'Perl';

use MongoDB;

# REQUIRES DRIVER WITH SASL SUPPORT: Authen::SASL and either
# the GSSAPI module or the Authen::SASL::XS module
#
# Test setup requires various environment variables.
#
#    export PERL_MONGO_TEST_KRB_HOST=hostname.example.com
#    export PERL_MONGO_TEST_KRB_GOOD_USER=goodone@EXAMPLE.COM
#    export PERL_MONGO_TEST_KRB_BAD_USER=badone@EXAMPLE.COM
#
# Be sure to run kinit for the valid username

my $krb_uri  = $ENV{PERL_MONGO_TEST_KRB_HOST};
my $good_user = $ENV{PERL_MONGO_TEST_KRB_GOOD_USER};
my $bad_user  = $ENV{PERL_MONGO_TEST_KRB_BAD_USER};

plan skip_all => "PERL_MONGO_TEST_KRB_* environment vars incomplete"
  unless $krb_uri && $good_user && $bad_user;

plan skip_all => "No SASL library available"
  unless eval { require Authen::SASL; 1 }
  && ( eval { require GSSAPI; 1 } || eval { require Authen::SASL::XS } );

subtest "no auth" => sub {
    my $mc = MongoDB::MongoClient->new(
        host                        => "mongodb://$krb_uri/",
        server_selection_timeout_ms => 1000,
    );

    my $coll = $mc->get_database("kerberos")->get_collection("test");

    like(
        exception { ok( $coll->find_one( {} ), "find_one" ) },
        qr/not authorized/,
        "find_one failed due to auth error",
    );
};

subtest "auth fails" => sub {
    my $mc = MongoDB::MongoClient->new(
        host                        => "mongodb://$krb_uri/",
        username                    => $bad_user,
        auth_mechanism              => 'GSSAPI',
        server_selection_timeout_ms => 1000,
    );
    like( exception { $mc->connect; }, qr/MongoDB::AuthError/, "authentication fails", );

};

subtest "auth OK via attributes" => sub {
    my $mc = MongoDB::MongoClient->new(
        host                        => "mongodb://$krb_uri/",
        username                    => $good_user,
        auth_mechanism              => 'GSSAPI',
        server_selection_timeout_ms => 1000,
    );

    my $coll = $mc->get_database("kerberos")->get_collection("test");
    ok( $coll->find_one( {} ), "find" );
};

subtest "auth OK via connect string" => sub {
    (my $escaped_user = $good_user) =~ s/@/%40/;

    my $mc = MongoDB::MongoClient->new(
        host => "mongodb://$escaped_user\@$krb_uri/?authMechanism=GSSAPI",
        server_selection_timeout_ms => 1000,
    );

    my $coll = $mc->get_database("kerberos")->get_collection("test");
    ok( $coll->find_one( {} ), "find" );
};

subtest "auth fails via connect string to wrong realm" => sub {
    (my $escaped_user = $good_user) =~ s/@.*/%40WRONG.REALM.COM/;

    my $mc = MongoDB::MongoClient->new(
        host => "mongodb://$escaped_user\@$krb_uri/?authMechanism=GSSAPI",
        server_selection_timeout_ms => 1000,
    );
    like( exception { $mc->connect; }, qr/MongoDB::AuthError/, "authentication fails", );
};

done_testing;
