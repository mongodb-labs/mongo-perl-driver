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

# Some portions of this code were copied and adapted from the Perl module
# HTTP::Tiny, which is copyright Christian Hansen, David Golden and other
# contributors and used with permission under the terms of the Artistic License

use v5.8.0;
use strict;
use warnings;

package MongoDB::_Link;

use version;
our $VERSION = 'v2.2.2';

use Moo;
use Errno qw[EINTR EPIPE];
use IO::Socket qw[SOCK_STREAM];
use Scalar::Util qw/refaddr/;
use Socket qw/SOL_SOCKET SO_KEEPALIVE SO_RCVBUF IPPROTO_TCP TCP_NODELAY AF_INET/;
use Time::HiRes qw/time/;
use MongoDB::Error;
use MongoDB::_Constants;
use MongoDB::_Protocol;
use MongoDB::_Types qw(
    Boolish
    HostAddress
    NonNegNum
    Numish
    ServerDesc
);
use Types::Standard qw(
    HashRef
    Maybe
    Str
    Undef
);
use namespace::clean;

my $SOCKET_CLASS =
  eval { require IO::Socket::IP; IO::Socket::IP->VERSION(0.32) }
  ? 'IO::Socket::IP'
  : 'IO::Socket::INET';

has address => (
    is => 'ro',
    required => 1,
    isa => HostAddress,
);

has connect_timeout => (
    is => 'ro',
    default => 20,
    isa => Numish,
);

has socket_timeout => (
    is => 'ro',
    default => 30,
    isa => Numish|Undef,
);

has with_ssl => (
    is => 'ro',
    isa => Boolish,
);

has SSL_options => (
    is => 'ro',
    default => sub { {} },
    isa => HashRef,
);

has server => (
    is => 'rwp',
    init_arg => undef,
    isa => Maybe[ServerDesc],
);

has host => (
    is => 'lazy',
    init_arg => undef,
    isa => Str,
);

sub _build_host {
    my ($self) = @_;
    my ($host, $port) = split /:/, $self->address;
    return $host;
}

my @is_master_fields= qw(
  min_wire_version max_wire_version
  max_message_size_bytes max_write_batch_size max_bson_object_size
);

for my $f ( @is_master_fields ) {
    has $f => (
        is => 'rwp',
        init_arg => undef,
        isa => Maybe[NonNegNum],
    );
}

# wire version >= 2
has supports_write_commands => (
    is => 'rwp',
    init_arg => undef,
    isa => Boolish,
);

# wire version >= 3
has supports_list_commands => (
    is => 'rwp',
    init_arg => undef,
    isa => Boolish,
);

has supports_scram_sha1 => (
    is => 'rwp',
    init_arg => undef,
    isa => Boolish,
);

# wire version >= 4
has supports_document_validation => (
    is => 'rwp',
    init_arg => undef,
    isa => Boolish,
);

has supports_explain_command => (
    is => 'rwp',
    init_arg => undef,
    isa => Boolish,
);

has supports_query_commands => (
    is => 'rwp',
    init_arg => undef,
    isa => Boolish,
);

has supports_find_modify_write_concern => (
    is => 'rwp',
    init_arg => undef,
    isa => Boolish,
);

has supports_fsync_command => (
    is => 'rwp',
    init_arg => undef,
    isa => Boolish,
);

has supports_read_concern => (
    is => 'rwp',
    init_arg => undef,
    isa => Boolish,
);

# wire version >= 5
has supports_collation => (
    is => 'rwp',
    init_arg => undef,
    isa => Boolish,
);

has supports_helper_write_concern => (
    is => 'rwp',
    init_arg => undef,
    isa => Boolish,
);

has supports_x509_user_from_cert => (
    is => 'rwp',
    init_arg => undef,
    isa => Boolish,
);

# for caching wire version >=6
has supports_arrayFilters => (
    is => 'rwp',
    init_arg => undef,
    isa => Boolish,
);

has supports_clusterTime => (
    is => 'rwp',
    init_arg => undef,
    isa => Boolish,
);

has supports_db_aggregation => (
    is => 'rwp',
    init_arg => undef,
    isa => Boolish,
);

has supports_retryWrites => (
    is => 'rwp',
    init_arg => undef,
    isa => Boolish,
);

has supports_op_msg => (
    is => 'rwp',
    init_arg => undef,
    isa => Boolish,
);

has supports_retryReads => (
    is => 'rwp',
    init_arg => undef,
    isa => Boolish,
);

# for wire version >= 7
has supports_4_0_changestreams => (
    is => 'rwp',
    init_arg => undef,
    isa => Boolish,
  );

# wire version >= 8
has supports_aggregate_out_read_concern => (
    is => 'rwp',
    init_arg => undef,
    isa => Boolish,
);

my @connection_state_fields = qw(
    fh connected rcvbuf last_used fdset is_ssl
);

for my $f ( @connection_state_fields ) {
    has $f => (
        is => 'rwp',
        clearer => "_clear_$f",
        init_arg => undef,
    );
}

around BUILDARGS => sub {
    my $orig = shift;
    my $class = shift;
    my $hr = $class->$orig(@_);

    # shortcut on missing required field
    return $hr unless exists $hr->{address};

    ($hr->{host}, $hr->{port}) = split /:/, $hr->{address};

    return $hr;
};

sub connect {
    @_ == 1 || MongoDB::UsageError->throw( q/Usage: $handle->connect()/ . "\n" );
    my ($self) = @_;

    if ( $self->with_ssl ) {
        $self->_assert_ssl;
        # XXX possibly make SOCKET_CLASS an instance variable and set it here to IO::Socket::SSL
    }

    my ($host, $port) = split /:/, $self->address;

    # PERL-715: For 'localhost' where MongoDB is only listening on IPv4 and
    # getaddrinfo returns an IPv6 address before an IPv4 address, some
    # operating systems tickle a bug in IO::Socket::IP that causes
    # connection attempts to fail before trying the IPv4 address.  As a
    # workaround, we always force 'localhost' to use IPv4.

    my $fh = $SOCKET_CLASS->new(
        PeerHost => $ENV{TEST_MONGO_SOCKET_HOST} || $host,
        PeerPort => $port,
        ( lc($host) eq 'localhost' ? ( Family => AF_INET ) : () ),
        Proto    => 'tcp',
        Type     => SOCK_STREAM,
        Timeout  => $self->connect_timeout >= 0 ? $self->connect_timeout : undef,
      )
      or
      MongoDB::NetworkError->throw(qq/Could not connect to '@{[$self->address]}': $@\n/);

    unless ( binmode($fh) ) {
        undef $fh;
        MongoDB::InternalError->throw(qq/Could not binmode() socket: '$!'\n/);
    }

    unless ( defined( $fh->setsockopt( IPPROTO_TCP, TCP_NODELAY, 1 ) ) ) {
        undef $fh;
        MongoDB::InternalError->throw(qq/Could not set TCP_NODELAY on socket: '$!'\n/);
    }

    unless ( defined( $fh->setsockopt( SOL_SOCKET, SO_KEEPALIVE, 1 ) ) ) {
        undef $fh;
        MongoDB::InternalError->throw(qq/Could not set SO_KEEPALIVE on socket: '$!'\n/);
    }

    $self->_set_fh($fh);
    $self->_set_connected(1);

    my $fd = fileno $fh;
    unless ( defined $fd && $fd >= 0 ) {
        $self->_close;
        MongoDB::InternalError->throw(qq/select(2): 'Bad file descriptor'\n/);
    }
    vec( my $fdset = '', $fd, 1 ) = 1;
    $self->_set_fdset( $fdset );

    $self->start_ssl($host) if $self->with_ssl;

    $self->_set_last_used( time );
    $self->_set_rcvbuf( $fh->sockopt(SO_RCVBUF) );

    # Default max msg size is 2 * max BSON object size (DRIVERS-1)
    $self->_set_max_message_size_bytes( 2 * MAX_BSON_OBJECT_SIZE );

    return $self;
}

sub set_metadata {
    my ( $self, $server ) = @_;
    $self->_set_server($server);
    $self->_set_min_wire_version( $server->is_master->{minWireVersion} || "0" );
    $self->_set_max_wire_version( $server->is_master->{maxWireVersion} || "0" );
    $self->_set_max_bson_object_size( $server->is_master->{maxBsonObjectSize}
          || MAX_BSON_OBJECT_SIZE );
    $self->_set_max_write_batch_size( $server->is_master->{maxWriteBatchSize}
          || MAX_WRITE_BATCH_SIZE );

    # Default is 2 * max BSON object size (DRIVERS-1)
    $self->_set_max_message_size_bytes( $server->is_master->{maxMessageSizeBytes}
          || 2 * $self->max_bson_object_size );

    if ( $self->accepts_wire_version(2) ) {
        $self->_set_supports_write_commands(1);
    }
    if ( $self->accepts_wire_version(3) ) {
        $self->_set_supports_list_commands(1);
        $self->_set_supports_scram_sha1(1);
    }
    if ( $self->accepts_wire_version(4) ) {
        $self->_set_supports_document_validation(1);
        $self->_set_supports_explain_command(1);
        $self->_set_supports_query_commands(1);
        $self->_set_supports_find_modify_write_concern(1);
        $self->_set_supports_fsync_command(1);
        $self->_set_supports_read_concern(1);
    }
    if ( $self->accepts_wire_version(5) ) {
        $self->_set_supports_collation(1);
        $self->_set_supports_helper_write_concern(1);
        $self->_set_supports_x509_user_from_cert(1);
    }
    if ( $self->accepts_wire_version(6) ) {
        $self->_set_supports_arrayFilters(1);
        $self->_set_supports_clusterTime(1);
        $self->_set_supports_db_aggregation(1);
        $self->_set_supports_retryWrites(
            defined( $server->logical_session_timeout_minutes )
              && ( $server->type ne 'Standalone' )
            ? 1
            : 0
        );
        $self->_set_supports_op_msg(1);
        $self->_set_supports_retryReads(1);
    }
    if ( $self->accepts_wire_version(7) ) {
        $self->_set_supports_4_0_changestreams(1);
    }
    if ( $self->accepts_wire_version(8) ) {
        $self->_set_supports_aggregate_out_read_concern(1);
    }

    return;
}

sub accepts_wire_version {
    my ( $self, $version ) = @_;
    my $min = $self->min_wire_version || 0;
    my $max = $self->max_wire_version || 0;
    return $version >= $min && $version <= $max;
}

sub start_ssl {
    my ( $self, $host ) = @_;

    my $ssl_args = $self->_ssl_args($host);
    IO::Socket::SSL->start_SSL(
        $self->fh,
        %$ssl_args,
        SSL_create_ctx_callback => sub {
            my $ctx = shift;
            Net::SSLeay::CTX_set_mode( $ctx, Net::SSLeay::MODE_AUTO_RETRY() );
        },
    );

    unless ( ref( $self->fh ) eq 'IO::Socket::SSL' ) {
        my $ssl_err = IO::Socket::SSL->errstr;
        $self->_close;
        MongoDB::HandshakeError->throw(qq/SSL connection failed for $host: $ssl_err\n/);
    }
}

sub client_certificate_subject {
    my ($self) = @_;
    return "" unless $self->fh && $self->fh->isa("IO::Socket::SSL");

    my $client_cert = $self->fh->sock_certificate()
      or return "";

    my $subject_raw = Net::SSLeay::X509_get_subject_name($client_cert)
      or return "";

    my $subject =
      Net::SSLeay::X509_NAME_print_ex( $subject_raw, Net::SSLeay::XN_FLAG_RFC2253() );

    return $subject;
}

sub close {
    my ($self) = @_;
    $self->_close
      or MongoDB::NetworkError->throw(qq/Error closing socket: '$!'\n/);
}

# this is a quiet close so preexisting network errors can be thrown
sub _close {
    my ($self) = @_;
    $self->_clear_connected;
    my $ok = 1;
    if ( $self->fh ) {
        $ok = CORE::close( $self->fh );
        $self->_clear_fh;
    }
    return $ok;
}

sub is_connected {
    my ($self) = @_;
    return $self->connected && $self->fh;
}

sub write {
    my ( $self, $buf, $write_opt ) = @_;
    $write_opt ||= {};

    if (
        !$write_opt->{disable_compression}
        && $self->server
        && $self->server->compressor
    ) {
        $buf = MongoDB::_Protocol::compress(
            $buf,
            $self->server->compressor,
        );
    }

    my ( $len, $off, $pending, $nfound, $r ) = ( length($buf), 0 );

    MongoDB::ProtocolError->throw(
        qq/Message of size $len exceeds maximum of / . $self->{max_message_size_bytes} )
      if $len > $self->max_message_size_bytes;

    local $SIG{PIPE} = 'IGNORE';

    while () {

        # do timeout
        ( $pending, $nfound ) = ( $self->socket_timeout, 0 );
        TIMEOUT: while () {
            if ( -1 == ( $nfound = select( undef, $self->fdset, undef, $pending ) ) ) {
                unless ( $! == EINTR ) {
                    $self->_close;
                    MongoDB::NetworkError->throw(qq/select(2): '$!'\n/);
                }
                # to avoid overhead tracking monotonic clock times; assume
                # interrupts occur on average halfway through the timeout period
                # and restart with half the original time
                $pending = int( $pending / 2 );
                redo TIMEOUT;
            }
            last TIMEOUT;
        }
        unless ($nfound) {
            $self->_close;
            MongoDB::NetworkTimeout->throw(
                qq/Timed out while waiting for socket to become ready for writing\n/);
        }

        # do write
        if ( defined( $r = syswrite( $self->fh, $buf, $len, $off ) ) ) {
            ( $len -= $r ), ( $off += $r );
            last unless $len > 0;
        }
        elsif ( $! == EPIPE ) {
            $self->_close;
            MongoDB::NetworkError->throw(qq/Socket closed by remote server: $!\n/);
        }
        elsif ( $! != EINTR ) {
            if ( $self->fh->can('errstr') ) {
                my $err = $self->fh->errstr();
                $self->_close;
                MongoDB::NetworkError->throw(qq/Could not write to SSL socket: '$err'\n /);
            }
            else {
                $self->_close;
                MongoDB::NetworkError->throw(qq/Could not write to socket: '$!'\n/);
            }

        }
    }

    $self->_set_last_used(time);

    return;
}

sub read {
    my ($self) = @_;

    # len of undef triggers first pass through loop
    my ( $msg, $len, $pending, $nfound, $r ) = ( '', undef );

    while () {

        # do timeout
        ( $pending, $nfound ) = ( $self->socket_timeout, 0 );
        TIMEOUT: while () {
            # no need to select if SSL and has pending data from a frame
            if ( $self->with_ssl ) {
                ( $nfound = 1 ), last TIMEOUT
                  if $self->fh->pending;
            }

            if ( -1 == ( $nfound = select( $self->fdset, undef, undef, $pending ) ) ) {
                unless ( $! == EINTR ) {
                    $self->_close;
                    MongoDB::NetworkError->throw(qq/select(2): '$!'\n/);
                }
                # to avoid overhead tracking monotonic clock times; assume
                # interrupts occur on average halfway through the timeout period
                # and restart with half the original time
                $pending = int( $pending / 2 );
                redo TIMEOUT;
            }
            last TIMEOUT;
        }
        unless ($nfound) {
            $self->_close;
            MongoDB::NetworkTimeout->throw(
                q/Timed out while waiting for socket to become ready for reading/ . "\n" );
        }

        # read up to SO_RCVBUF if we can
        if ( defined( $r = sysread( $self->fh, $msg, $self->rcvbuf, length $msg ) ) ) {
            # because select said we're ready to read, if we read 0 then
            # we got EOF before the full message
            if ( !$r ) {
                $self->_close;
                MongoDB::NetworkError->throw(qq/Unexpected end of stream\n/);
            }
        }
        elsif ( $! != EINTR ) {
            if ( $self->fh->can('errstr') ) {
                my $err = $self->fh->errstr();
                $self->_close;
                MongoDB::NetworkError->throw(qq/Could not read from SSL socket: '$err'\n /);
            }
            else {
                $self->_close;
                MongoDB::NetworkError->throw(qq/Could not read from socket: '$!'\n/);
            }
        }

        if ( !defined $len ) {
            next if length($msg) < 4;
            $len = unpack( P_INT32, $msg );
            MongoDB::ProtocolError->throw(
                qq/Server reply of size $len exceeds maximum of / . $self->{max_message_size_bytes} )
              if $len > $self->max_message_size_bytes;
        }
        last unless length($msg) < $len;
    }

    $self->_set_last_used(time);

    return $msg;
}

sub _assert_ssl {
    # Need IO::Socket::SSL 1.42 for SSL_create_ctx_callback
    MongoDB::UsageError->throw(qq/IO::Socket::SSL 1.42 must be installed for SSL support\n/)
      unless eval { require IO::Socket::SSL; IO::Socket::SSL->VERSION(1.42) };
    # Need Net::SSLeay 1.49 for MODE_AUTO_RETRY
    MongoDB::UsageError->throw(qq/Net::SSLeay 1.49 must be installed for SSL support\n/)
      unless eval { require Net::SSLeay; Net::SSLeay->VERSION(1.49) };
}

# Try to find a CA bundle to validate the SSL cert,
# prefer Mozilla::CA or fallback to a system file
sub _find_CA_file {
    my $self = shift();

    return $self->SSL_options->{SSL_ca_file}
      if $self->SSL_options->{SSL_ca_file} and -e $self->SSL_options->{SSL_ca_file};

    return Mozilla::CA::SSL_ca_file()
      if eval { require Mozilla::CA };

    # cert list copied from golang src/crypto/x509/root_unix.go
    foreach my $ca_bundle (
        "/etc/ssl/certs/ca-certificates.crt",     # Debian/Ubuntu/Gentoo etc.
        "/etc/pki/tls/certs/ca-bundle.crt",       # Fedora/RHEL
        "/etc/ssl/ca-bundle.pem",                 # OpenSUSE
        "/etc/openssl/certs/ca-certificates.crt", # NetBSD
        "/etc/ssl/cert.pem",                      # OpenBSD
        "/usr/local/share/certs/ca-root-nss.crt", # FreeBSD/DragonFly
        "/etc/pki/tls/cacert.pem",                # OpenELEC
        "/etc/certs/ca-certificates.crt",         # Solaris 11.2+
    ) {
        return $ca_bundle if -e $ca_bundle;
    }

    MongoDB::UsageError->throw(
      qq/Couldn't find a CA bundle with which to verify the SSL certificate.\n/
      . qq/Try installing Mozilla::CA from CPAN\n/);
}

sub _ssl_args {
    my ( $self, $host ) = @_;

    my %ssl_args;

    # This test reimplements IO::Socket::SSL::can_client_sni(), which wasn't
    # added until IO::Socket::SSL 1.84
    if ( Net::SSLeay::OPENSSL_VERSION_NUMBER() >= 0x10000000 ) {
        $ssl_args{SSL_hostname} = $host, # Sane SNI support
    }

    if ( Net::SSLeay::OPENSSL_VERSION_NUMBER() >= 0x10100000 ) {
        $ssl_args{SSL_OP_NO_RENEGOTIATION} = Net::SSLeay::OP_NO_RENEGOTIATION();
    }

    $ssl_args{SSL_verifycn_scheme} = 'http';              # enable CN validation
    $ssl_args{SSL_verifycn_name}   = $host;               # set validation hostname
    $ssl_args{SSL_verify_mode}     = 0x01;                # enable cert validation
    $ssl_args{SSL_ca_file}         = $self->_find_CA_file;

    # user options override default settings
    for my $k ( keys %{ $self->SSL_options } ) {
        $ssl_args{$k} = $self->SSL_options->{$k} if $k =~ m/^SSL_/;
    }

    return \%ssl_args;
}

1;

# vim: ts=4 sts=4 sw=4 et:
