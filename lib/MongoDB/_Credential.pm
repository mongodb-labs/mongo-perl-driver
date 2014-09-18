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
use Syntax::Keyword::Junction 'any';
use Tie::IxHash;
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
        $mp->{SERVICE_NAME}           ||= 'mongodb';
        $mp->{CANONICALIZE_HOST_NAME} ||= 0;
    }

    return;
}

sub authenticate {
    my ( $self, $link ) = @_;

    my $method = '_authenticate_' . $self->mechanism;
    $method =~ s/-/_/g;

    return $self->$method($link);
}

sub _authenticate_NONE () { 1 }

sub _authenticate_MONGODB_CR {
    my ( $self, $link ) = @_;

    my $nonce = $self->_send_admin_command( $link, { getnonce => 1 } )->result->{nonce};

    my $password_digest =
        $self->pw_is_digest
      ? $self->password
      : md5_hex( $self->username . ":mongo:" . $self->password );

    my $key = Digest::MD5::md5_hex( $nonce . $self->username . $password_digest );

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
    my $payload = MongoDB::BSON::Binary->new( data => $auth_bytes );
    $self->_sasl_start( $link, $payload );

    return 1;
}

sub _authenticate_GSSAPI {
    my ($self) = @_;
    die "unimplemented";
}

sub _authenticate_SCRAM_SHA_1 {
    my ($self) = @_;
    die "unimplemented";
}

sub _sasl_start {
    my ( $self, $link, $payload ) = @_;

    my $command = Tie::IxHash->new(
        saslStart     => 1,
        mechanism     => $self->mechanism,
        payload       => $payload,
        autoAuthorize => 1,
    );

    return $self->_send_command( $link, $self->source, $command )->result;
}

1;
