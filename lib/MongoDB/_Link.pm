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

# Some portions of this code were copied and adapted from the Perl module
# HTTP::Tiny, which is copyright Christian Hansen, David Golden and other
# contributors and used with permission under the terms of the Artistic License

use v5.8.0;
use strict;
use warnings;

package MongoDB::_Link;

use version;
our $VERSION = 'v0.999.999.4'; # TRIAL

use Moo;
use Errno qw[EINTR EPIPE];
use IO::Socket qw[SOCK_STREAM];
use Scalar::Util qw/refaddr/;
use Socket qw/SOL_SOCKET SO_KEEPALIVE SO_RCVBUF IPPROTO_TCP TCP_NODELAY/;
use Time::HiRes qw/time gettimeofday tv_interval/;
use MongoDB::Error;
use MongoDB::_Constants;
use MongoDB::_Types -types;
use Types::Standard -types;
use namespace::clean;

my $SOCKET_CLASS =
  eval { require IO::Socket::IP; IO::Socket::IP->VERSION(0.25) }
  ? 'IO::Socket::IP'
  : 'IO::Socket::INET';

has address => (
    is => 'ro',
    required => 1,
    ( WITH_ASSERTS ? ( isa => HostAddress ) : () ),
);

has connect_timeout => (
    is => 'ro',
    default => 20,
    ( WITH_ASSERTS ? ( isa => Num ) : () ),
);

has socket_timeout => (
    is => 'ro',
    default => 30,
    ( WITH_ASSERTS ? ( isa => Num ) : () ),
);

has with_ssl => (
    is => 'ro',
    ( WITH_ASSERTS ? ( isa => Bool ) : () ),
);

has SSL_options => (
    is => 'ro',
    default => sub { {} },
    ( WITH_ASSERTS ? ( isa => HashRef ) : () ),
);

has server => (
    is => 'rwp',
    init_arg => undef,
    ( WITH_ASSERTS ? ( isa => Maybe[ServerDesc] ) : () ),
);

my @is_master_fields= qw(
  min_wire_version max_wire_version
  max_message_size_bytes max_write_batch_size max_bson_object_size
);

for my $f ( @is_master_fields ) {
    has $f => (
        is => 'rwp',
        init_arg => undef,
        ( WITH_ASSERTS ? ( isa => Maybe[NonNegNum] ) : () ),
    );
}

my @connection_state_fields = qw(
    fh connected rcvbuf last_used fdset
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

    my $fh = $SOCKET_CLASS->new(
        PeerHost => $host,
        PeerPort => $port,
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

    $self->_set_last_used( [gettimeofday] );
    $self->_set_rcvbuf( $fh->sockopt(SO_RCVBUF) );

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

sub idle_time_ms {
    my ($self) = @_;
    return 1000 * tv_interval( $self->last_used );
}

sub write {
    @_ == 2 || MongoDB::UsageError->throw( q/Usage: $handle->write(buf)/ . "\n" );
    my ( $self, $buf ) = @_;

    if ( $] ge '5.008' ) {
        utf8::downgrade( $buf, 1 )
          or MongoDB::InternalError->throw(qq/Wide character in write()\n/);
    }

    my $len = length $buf;
    my $off = 0;

    if ( $self->max_message_size_bytes && $len > $self->max_message_size_bytes ) {
        MongoDB::ProtocolError->throw(
            qq/Message of size $len exceeds maximum of / . $self->{max_message_size_bytes} );
    }

    local $SIG{PIPE} = 'IGNORE';

    while () {
        unless ( $self->can_write ) {
            $self->_close;
            MongoDB::NetworkTimeout->throw(
                qq/Timed out while waiting for socket to become ready for writing\n/);
        }
        my $r = syswrite( $self->fh, $buf, $len, $off );
        if ( defined $r ) {
            $len -= $r;
            $off += $r;
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

    $self->_set_last_used( [gettimeofday] );

    return $off;
}

sub read {
    @_ == 1 || MongoDB::UsageError->throw( q/Usage: $handle->read()/ . "\n" );
    my ($self) = @_;
    my $msg = '';

    # read up to SO_RCVBUF if we can
    $self->_read_bytes(\$msg, 4, $self->rcvbuf);
    my $bytes_read = length($msg);
    my $len = unpack( P_INT32, substr($msg,0,4) );

    # read rest of the message
    if ( $len > $bytes_read ) {
        $self->_read_bytes( \$msg, $len - $bytes_read );
    }

    $self->_set_last_used( [gettimeofday] );

    return $msg;
}

sub _read_bytes {
    my ( $self, $bufref, $min_len, $req_size ) = @_;
    $req_size ||= $min_len;

    while ( $min_len > 0 ) {
        unless ( $self->can_read ) {
            $self->_close;
            MongoDB::NetworkTimeout->throw(
                q/Timed out while waiting for socket to become ready for reading/ . "\n" );
        }
        my $r = sysread( $self->fh, $$bufref, $req_size, length $$bufref );
        if ( defined $r ) {
            last unless $r;
            $min_len -= $r;
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
    }
    if ($min_len > 0) {
        $self->_close;
        MongoDB::NetworkError->throw(qq/Unexpected end of stream\n/);
    }
    return;
}

sub _do_timeout {
    my ( $self, $type, $timeout ) = @_;
    $timeout = $self->socket_timeout
      unless defined $timeout;

    my $pending = $timeout >= 0 ? $timeout : undef;
    my $nfound;

    while () {
        $nfound =
          ( $type eq 'read' )
          ? select( $self->fdset, undef,  undef, $pending )
          : select( undef,  $self->fdset, undef, $pending );
        if ( $nfound == -1 ) {
            unless ( $! == EINTR ) {
                $self->_close;
                MongoDB::NetworkError->throw(qq/select(2): '$!'\n/);
            }
            # to avoid overhead tracking monotonic clock times; assume
            # interrupts occur on average halfway through the timeout period
            # and restart with half the original time
            $pending = int( $pending/2 );
            redo;
        }
        last;
    }
    $! = 0;
    return $nfound;
}

sub can_read {
    @_ == 1 || @_ == 2 || MongoDB::UsageError->throw( q/Usage: $handle->can_read([timeout])/ . "\n" );
    my $self = shift;
    if ( ref( $self->fh ) eq 'IO::Socket::SSL' ) {
        return 1 if $self->fh->pending;
    }
    return $self->_do_timeout( 'read', @_ );
}

sub can_write {
    @_ == 1
      || @_ == 2
      || MongoDB::UsageError->throw( q/Usage: $handle->can_write([timeout])/ . "\n" );
    my $self = shift;
    return $self->_do_timeout( 'write', @_ );
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
    if ( Net::SSLeay::OPENSSL_VERSION_NUMBER() >= 0x01000000 ) {
        $ssl_args{SSL_hostname} = $host, # Sane SNI support
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
