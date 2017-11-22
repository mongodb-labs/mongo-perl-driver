#
#  Copyright 2014 MongoDB, Inc.
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
package MongoDB::_Credential;

use version;
our $VERSION = 'v1.999.0';

use Moo;
use MongoDB::Error;
use MongoDB::Op::_Command;
use MongoDB::_Types qw(
    AuthMechanism
    NonEmptyStr
);

use Digest::MD5 qw/md5_hex/;
use Encode qw/encode/;
use MIME::Base64 qw/encode_base64 decode_base64/;
use Tie::IxHash;
use Try::Tiny;
use Types::Standard qw(
    Bool
    HashRef
    InstanceOf
    Str
);

use namespace::clean -except => 'meta';

has mechanism => (
    is       => 'ro',
    isa      => AuthMechanism,
    required => 1,
);

has username => (
    is      => 'ro',
    isa     => Str,
    default => '',
);

has source => (
    is      => 'lazy',
    isa     => NonEmptyStr,
    builder => '_build_source',
);

has password => (
    is      => 'ro',
    isa     => Str,
    default => '',
);

has pw_is_digest => (
    is  => 'ro',
    isa => Bool,
);

has mechanism_properties => (
    is      => 'ro',
    isa     => HashRef,
    default => sub { {} },
);

has _digested_password => (
    is      => 'lazy',
    isa     => Str,
    builder => '_build__digested_password',
);

has _scram_client => (
    is      => 'lazy',
    isa     => InstanceOf['Authen::SCRAM::Client'],
    builder => '_build__scram_client',
);

sub _build__scram_client {
    my ($self) = @_;
    # loaded only demand as it has a long load time relative to other
    # modules
    require Authen::SCRAM::Client;
    Authen::SCRAM::Client->VERSION(0.006);
    return Authen::SCRAM::Client->new(
        username      => $self->username,
        password      => $self->_digested_password,
        skip_saslprep => 1,
    );
}

sub _build__digested_password {
    my ($self) = @_;
    return $self->password if $self->pw_is_digest;
    return md5_hex( encode( "UTF-8", $self->username . ":mongo:" . $self->password ) );
}

sub _build_source {
    my ($self) = @_;
    my $mech = $self->mechanism;
    return $mech eq 'DEFAULT' || $mech eq 'MONGODB-CR' || $mech eq 'SCRAM-SHA-1'
      ? 'admin'
      : '$external';
}

#<<< No perltidy
my %CONSTRAINTS = (
    'MONGODB-CR' => {
        username             => sub { length },
        password             => sub { length },
        source               => sub { length },
        mechanism_properties => sub { !keys %$_ },
    },
    'MONGODB-X509' => {
        password             => sub { ! length },
        source               => sub { $_ eq '$external' },
        mechanism_properties => sub { !keys %$_ },
    },
    'GSSAPI'      => {
        username             => sub { length },
        source               => sub { $_ eq '$external' },
    },
    'PLAIN'       => {
        username             => sub { length },
        password             => sub { length },
        source               => sub { $_ eq '$external' },
        mechanism_properties => sub { !keys %$_ },
    },
    'SCRAM-SHA-1' => {
        username             => sub { length },
        password             => sub { length },
        source               => sub { length },
        mechanism_properties => sub { !keys %$_ },
    },
    'DEFAULT' => {
        username             => sub { length },
        password             => sub { length },
        source               => sub { length },
        mechanism_properties => sub { !keys %$_ },
    },
);
#>>>

sub BUILD {
    my ($self) = @_;

    my $mech = $self->mechanism;

    # validate attributes for given mechanism
    while ( my ( $key, $validator ) = each %{ $CONSTRAINTS{$mech} } ) {
        local $_ = $self->$key;
        unless ( $validator->() ) {
            MongoDB::UsageError->throw("invalid field $key in $mech credential");
        }
    }

    # fix up GSSAPI property defaults if not given
    if ( $mech eq 'GSSAPI' ) {
        my $mp = $self->mechanism_properties;
        $mp->{SERVICE_NAME} ||= 'mongodb';
    }

    return;
}

sub authenticate {
    my ( $self, $link, $bson_codec ) = @_;

    my $mech = $self->mechanism;
    if ( $mech eq 'DEFAULT' ) {
        $mech = $link->accepts_wire_version(3) ? 'SCRAM-SHA-1' : 'MONGODB-CR';
    }
    my $method = "_authenticate_$mech";
    $method =~ s/-/_/g;

    return $self->$method($link, $bson_codec);
}

#--------------------------------------------------------------------------#
# authentication mechanisms
#--------------------------------------------------------------------------#

sub _authenticate_NONE () { 1 }

sub _authenticate_MONGODB_CR {
    my ( $self, $link, $bson_codec ) = @_;

    my $nonce = $self->_send_command( $link, $bson_codec, 'admin', { getnonce => 1 } )->output->{nonce};
    my $key =
      md5_hex( encode( "UTF-8", $nonce . $self->username . $self->_digested_password ) );

    my $command = Tie::IxHash->new(
        authenticate => 1,
        user         => $self->username,
        nonce        => $nonce,
        key          => $key
    );
    $self->_send_command( $link, $bson_codec, $self->source, $command );

    return 1;
}

sub _authenticate_MONGODB_X509 {
    my ( $self, $link, $bson_codec ) = @_;

    my $username = $self->username;

    if ( !$username && !$link->accepts_wire_version(5) ) {
        $username = $link->client_certificate_subject
          or MongoDB::UsageError->throw("Could not extract subject from client SSL certificate");
    }

    my $command = Tie::IxHash->new(
        authenticate => 1,
        mechanism    => "MONGODB-X509",
        ( $username ? ( user => $username ) : () ),
    );
    $self->_send_command( $link, $bson_codec, $self->source, $command );

    return 1;
}

sub _authenticate_PLAIN {
    my ( $self, $link, $bson_codec ) = @_;

    my $auth_bytes =
      encode( "UTF-8", "\x00" . $self->username . "\x00" . $self->password );
    $self->_sasl_start( $link, $bson_codec, $auth_bytes, "PLAIN" );

    return 1;
}

sub _authenticate_GSSAPI {
    my ( $self, $link, $bson_codec ) = @_;

    eval { require Authen::SASL; 1 }
      or MongoDB::AuthError->throw(
        "GSSAPI requires Authen::SASL and GSSAPI or Authen::SASL::XS from CPAN");

    my ( $sasl, $client );
    try {
        $sasl = Authen::SASL->new(
            mechanism => 'GSSAPI',
            callback  => {
                user     => $self->username,
                authname => $self->username,
            },
        );
        $client =
          $sasl->client_new( $self->mechanism_properties->{SERVICE_NAME}, $link->host );
    }
    catch {
        MongoDB::AuthError->throw(
            "Failed to initialize a GSSAPI backend (did you install GSSAPI or Authen::SASL::XS?) Error was: $_"
        );
    };

    try {
        # start conversation
        my $step = $client->client_start;
        $self->_assert_gssapi( $client,
            "Could not start GSSAPI. Did you run kinit?  Error was: " );
        my ( $sasl_resp, $conv_id, $done ) = $self->_sasl_start( $link, $bson_codec, $step, 'GSSAPI' );

        # iterate, but with maximum number of exchanges to prevent endless loop
        for my $i ( 1 .. 10 ) {
            last if $done;
            $step = $client->client_step($sasl_resp);
            $self->_assert_gssapi( $client, "GSSAPI step error: " );
            ( $sasl_resp, $conv_id, $done ) = $self->_sasl_continue( $link, $bson_codec, $step, $conv_id );
        }
    }
    catch {
        MongoDB::AuthError->throw("GSSAPI error: $_");
    };

    return 1;
}

sub _authenticate_SCRAM_SHA_1 {
    my ( $self, $link, $bson_codec ) = @_;

    my $client = $self->_scram_client;

    my ( $msg, $sasl_resp, $conv_id, $done );
    try {
        $msg = $client->first_msg;
        ( $sasl_resp, $conv_id, $done ) = $self->_sasl_start( $link, $bson_codec, $msg, 'SCRAM-SHA-1' );
        $msg = $client->final_msg($sasl_resp);
        ( $sasl_resp, $conv_id, $done ) = $self->_sasl_continue( $link, $bson_codec, $msg, $conv_id );
        $client->validate($sasl_resp);
        # might require an empty payload to complete SASL conversation
        $self->_sasl_continue( $link, $bson_codec, "", $conv_id ) if !$done;
    }
    catch {
        MongoDB::AuthError->throw("SCRAM-SHA-1 error: $_");
    };

    return 1;
}

#--------------------------------------------------------------------------#
# GSSAPI/SASL methods
#--------------------------------------------------------------------------#

# GSSAPI backends report status/errors differently
sub _assert_gssapi {
    my ( $self, $client, $prefix ) = @_;
    my $type = ref $client;

    if ( $type =~ m{^Authen::SASL::(?:XS|Cyrus)$} ) {
        my $code = $client->code;
        if ( $code != 0 && $code != 1 ) { # not OK or CONTINUE
            my $error = join( "; ", $client->error );
            MongoDB::AuthError->throw("$prefix$error");
        }
    }
    else {
        # Authen::SASL::Perl::GSSAPI or some unknown backend
        if ( my $error = $client->error ) {
            MongoDB::AuthError->throw("$prefix$error");
        }
    }

    return 1;
}

sub _sasl_start {
    my ( $self, $link, $bson_codec, $payload, $mechanism ) = @_;

    my $command = Tie::IxHash->new(
        saslStart     => 1,
        mechanism     => $mechanism,
        payload       => \$payload,
        autoAuthorize => 1,
    );

    return $self->_sasl_send( $link, $bson_codec, $command );
}

sub _sasl_continue {
    my ( $self, $link, $bson_codec, $payload, $conv_id ) = @_;

    my $command = Tie::IxHash->new(
        saslContinue   => 1,
        conversationId => $conv_id,
        payload        => \$payload,
    );

    return $self->_sasl_send( $link, $bson_codec, $command );
}

sub _sasl_send {
    my ( $self, $link, $bson_codec, $command ) = @_;
    my $output = $self->_send_command( $link, $bson_codec, $self->source, $command )->output;

    my $sasl_resp = $output->{payload} ? $output->{payload}->data : "";
    return ( $sasl_resp, $output->{conversationId}, $output->{done} );
}

sub _send_command {
    my ($self, $link, $bson_codec, $db_name, $command) = @_;

    my $op = MongoDB::Op::_Command->_new(
        db_name => $db_name,
        query => $command,
        query_flags => {},
        bson_codec => $bson_codec,
    );
    my $res = $op->execute( $link );
    return $res;
}


1;
