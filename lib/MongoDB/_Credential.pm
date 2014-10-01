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

package MongoDB::_Credential;

# ABSTRACT: Encapsulate authentication credentials

use version;
our $VERSION = 'v0.704.4.1';

use Moose;
use MongoDB::_Types;

use Digest::MD5 qw/md5_hex/;
use Encode qw/encode/;
use MIME::Base64 qw/encode_base64 decode_base64/;
use Syntax::Keyword::Junction 'any';
use Tie::IxHash;
use Try::Tiny;
use namespace::clean -except => 'meta';

with 'MongoDB::Role::_Client';

has mechanism => (
    is       => 'ro',
    isa      => 'AuthMechanism',
    required => 1,
);

has username => (
    is      => 'ro',
    isa     => 'Str',
    default => '',
);

has source => (
    is      => 'ro',
    isa     => 'NonEmptyStr',
    lazy    => 1,
    builder => '_build_source',
);

has password => (
    is      => 'ro',
    isa     => 'Str',
    default => '',
);

has pw_is_digest => (
    is  => 'ro',
    isa => 'Bool',
);

has mechanism_properties => (
    is      => 'ro',
    isa     => 'HashRef',
    default => sub { {} },
);

has _digested_password => (
    is => 'ro',
    isa => 'Str',
    lazy => 1,
    builder => '_build__digested_password',
);

sub _build__digested_password {
    my ($self) = @_;
    return $self->password if $self->pw_is_digest;
    return md5_hex( Encode("UTF-8", $self->username . ":mongo:" . $self->password ) );
}

sub _build_source {
    my ($self) = @_;
    return $self->mechanism eq any(qw/MONGODB-CR SCRAM-SHA-1/) ? 'admin' : '$external';
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
        username             => sub { length },
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
);
#>>>

sub BUILD {
    my ($self) = @_;

    my $mech = $self->mechanism;

    # validate attributes for given mechanism
    while ( my ( $key, $validator ) = each %{ $CONSTRAINTS{$mech} } ) {
        local $_ = $self->$key;
        unless ( $validator->() ) {
            MongoDB::Error->throw("invalid field $key ('$_') in $mech credential");
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
    my ( $self, $link ) = @_;

    my $method = '_authenticate_' . $self->mechanism;
    $method =~ s/-/_/g;

    return $self->$method($link);
}

#--------------------------------------------------------------------------#
# authentication mechanisms
#--------------------------------------------------------------------------#

sub _authenticate_NONE () { 1 }

sub _authenticate_MONGODB_CR {
    my ( $self, $link ) = @_;

    my $nonce = $self->_send_admin_command( $link, { getnonce => 1 } )->result->{nonce};
    my $key = md5_hex( Encode("UTF-8", $nonce . $self->username . $self->digested_password ) );

    my $command = Tie::IxHash->new(
        authenticate => 1,
        user         => $self->username,
        nonce        => $nonce,
        key          => $key
    );
    $self->_send_command( $link, $self->source, $command );

    return 1;
}

sub _authenticate_MONGODB_X509 {
    my ( $self, $link ) = @_;

    my $command = Tie::IxHash->new(
        authenticate => 1,
        user         => $self->username,
        mechanism    => $self->mechanism
    );
    $self->_send_command( $link, $self->source, $command );

    return 1;
}

sub _authenticate_PLAIN {
    my ( $self, $link ) = @_;

    my $auth_bytes =
      encode( "UTF-8", "\x00" . $self->username . "\x00" . $self->password );
    $self->_sasl_start( $link, $auth_bytes );

    return 1;
}

sub _authenticate_GSSAPI {
    my ( $self, $link ) = @_;

    eval { require Authen::SASL; 1 }
      or MongoDB::Error->throw(
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
          $sasl->client_new( $self->mechanism_properties->{SERVICE_NAME}, $link->{host} );
    }
    catch {
        MongoDB::Error->throw(
            "Failed to initialize a GSSAPI backend (did you install GSSAPI or Authen::SASL::XS?) Error was: $_"
        );
    };

    # start conversation
    my $step = $client->client_start;
    $self->_assert_gssapi( $client,
        "Could not start GSSAPI. Did you run kinit?  Error was: " );
    my ( $sasl_resp, $conv_id, $done ) = $self->_sasl_start( $link, $step );

    # iterate, but with maximum number of exchanges to prevent endless loop
    for my $i ( 1 .. 10 ) {
        last if $done;
        $step = $client->client_step($sasl_resp);
        $self->_assert_gssapi( $client, "GSSAPI step error: " );
        ( $sasl_resp, $conv_id, $done ) = $self->_sasl_continue( $link, $step, $conv_id );
    }

    return 1;
}

sub _authenticate_SCRAM_SHA_1 {
    my ($self) = @_;
    die "unimplemented";
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
            MongoDB::Error->throw("$prefix$error");
        }
    }
    else {
        # Authen::SASL::Perl::GSSAPI or some unknown backend
        if ( my $error = $client->error ) {
            MongoDB::Error->throw("$prefix$error");
        }
    }

    return 1;
}

sub _sasl_start {
    my ( $self, $link, $payload ) = @_;

    my $command = Tie::IxHash->new(
        saslStart => 1,
        mechanism => $self->mechanism,
        payload   => $payload ? encode_base64( $payload, "" ) : "",
        autoAuthorize => 1,
    );

    return $self->_sasl_send( $link, $command );
}

sub _sasl_continue {
    my ( $self, $link, $payload, $conv_id ) = @_;

    my $command = Tie::IxHash->new(
        saslContinue   => 1,
        conversationId => $conv_id,
        payload        => $payload ? encode_base64( $payload, "" ) : "",
    );

    return $self->_sasl_send( $link, $command );
}

sub _sasl_send {
    my ( $self, $link, $command ) = @_;
    my $result = $self->_send_command( $link, $self->source, $command )->result;

    my $sasl_resp = $result->{payload} ? decode_base64( $result->{payload} ) : "";
    return ( $sasl_resp, $result->{conversationId}, $result->{done} );
}

1;
