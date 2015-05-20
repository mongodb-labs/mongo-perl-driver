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
our $VERSION = 'v0.999.998.7'; # TRIAL

use Config;
use Errno qw[EINTR EPIPE];
use IO::Socket qw[SOCK_STREAM];
use Scalar::Util qw/refaddr/;
use Time::HiRes qw/gettimeofday tv_interval/;
use MongoDB::Error;

use constant {
    HAS_THREADS          => $Config{usethreads},
    P_INT32              => $] lt '5.010' ? 'l' : 'l<',
    MAX_BSON_OBJECT_SIZE => 4_194_304,
    MAX_WRITE_BATCH_SIZE => 1000,
};

# fake thread-id for non-threaded perls
use if HAS_THREADS, 'threads';
*_get_tid = HAS_THREADS() ? sub { threads->tid } : sub () { 0 };

my $SOCKET_CLASS =
  eval { require IO::Socket::IP; IO::Socket::IP->VERSION(0.25) }
  ? 'IO::Socket::IP'
  : 'IO::Socket::INET';

sub new {
    @_ == 2
      || @_ == 3
      || MongoDB::UsageError->throw( q/Usage: MongoDB::_Link->new(address, [arg hashref])/ . "\n" );
    my ( $class, $address, $args ) = @_;
    my ( $host, $port ) = split /:/, $address;
    MongoDB::UsageError->throw("new requires 'host:port' address argument")
      unless defined($host) && length($host) && defined($port) && length($port);
    my $self = bless {
        host        => $host,
        port        => $port,
        address     => "$host:$port",
        timeout     => 60,
        with_ssl    => 0,
        SSL_options => {},
        ( $args ? (%$args) : () ),
    }, $class;
    return $self;
}

sub connect {
    @_ == 1 || MongoDB::UsageError->throw( q/Usage: $handle->connect()/ . "\n" );
    my ($self) = @_;

    if ( $self->{with_ssl} ) {
        $self->_assert_ssl;
        # XXX possibly make SOCKET_CLASS an instance variable and set it here to IO::Socket::SSL
    }

    my ( $host, $port ) = @{$self}{qw/host port/};

    $self->{fh} = $SOCKET_CLASS->new(
        PeerHost => $host,
        PeerPort => $port,
        $self->{local_address} ? ( LocalAddr => $self->{local_address} ) : (),
        Proto   => 'tcp',
        Type    => SOCK_STREAM,
        Timeout => $self->{timeout},
    ) or MongoDB::NetworkError->throw(qq/Could not connect to '$host:$port': $@\n/);

    binmode( $self->{fh} )
      or MongoDB::InternalError->throw(qq/Could not binmode() socket: '$!'\n/);

    $self->start_ssl($host) if $self->{with_ssl};

    $self->{pid}       = $$;
    $self->{tid}       = _get_tid();
    $self->{last_used} = [gettimeofday];

    return $self;
}

my @accessors = qw(
  address server min_wire_version max_wire_version
  max_message_size_bytes max_write_batch_size max_bson_object_size
);

for my $attr (@accessors) {
    no strict 'refs';
    *{$attr} = eval "sub { \$_[0]->{$attr} }";
}

sub set_metadata {
    my ( $self, $server ) = @_;
    $self->{server}           = $server;
    $self->{min_wire_version} = $server->is_master->{minWireVersion} || "0";
    $self->{max_wire_version} = $server->is_master->{maxWireVersion} || "0";
    $self->{max_bson_object_size} =
      $server->is_master->{maxBsonObjectSize} || MAX_BSON_OBJECT_SIZE;
    $self->{max_write_batch_size} =
      $server->is_master->{maxWriteBatchSize} || MAX_WRITE_BATCH_SIZE;

    # Default is 2 * max BSON object size (DRIVERS-1)
    $self->{max_message_size_bytes} =
      $server->is_master->{maxMessageSizeBytes} || 2 * $self->{max_bson_object_size};

    return;
}

sub accepts_wire_version {
    my ( $self, $version ) = @_;
    my $min = $self->{min_wire_version} || 0;
    my $max = $self->{max_wire_version} || 0;
    return $version >= $min && $version <= $max;
}

sub start_ssl {
    my ( $self, $host ) = @_;

    my $ssl_args = $self->_ssl_args($host);
    IO::Socket::SSL->start_SSL(
        $self->{fh},
        %$ssl_args,
        SSL_create_ctx_callback => sub {
            my $ctx = shift;
            Net::SSLeay::CTX_set_mode( $ctx, Net::SSLeay::MODE_AUTO_RETRY() );
        },
    );

    unless ( ref( $self->{fh} ) eq 'IO::Socket::SSL' ) {
        my $ssl_err = IO::Socket::SSL->errstr;
        MongoDB::HandshakeError->throw(qq/SSL connection failed for $host: $ssl_err\n/);
    }
}

sub close {
    @_ == 1 || MongoDB::UsageError->throw( q/Usage: $handle->close()/ . "\n" );
    my ($self) = @_;
    if ( $self->connected ) {
        CORE::close( $self->{fh} )
          or MongoDB::NetworkError->throw(qq/Error closing socket: '$!'\n/);
        delete $self->{fh};
    }
}

sub connection_valid {
    my ($self) = @_;
    return unless $self->{fh};

    if (  !$self->{fh}->connected
        || $self->{pid} != $$
        || $self->{tid} != _get_tid() )
    {
        $self->{fh}->close;
        delete $self->{fh};
        return;
    }

    return 1;
}

sub idle_time_ms {
    my ($self) = @_;
    return 1000 * tv_interval( $self->{last_used} );
}

sub remote_connected {
    my ($self) = @_;
    return unless $self->connection_valid;
    return if $self->can_read(0) && $self->{fh}->eof;
    return 1;
}

sub assert_valid_connection {
    my ($self) = @_;
    MongoDB::NetworkError->throw( "connection lost to " . $self->address )
      unless $self->connection_valid;
    return 1;
}

sub write {
    @_ == 2 || MongoDB::UsageError->throw( q/Usage: $handle->write(buf)/ . "\n" );
    my ( $self, $buf ) = @_;

    $self->assert_valid_connection;

    if ( $] ge '5.008' ) {
        utf8::downgrade( $buf, 1 )
          or MongoDB::InternalError->throw(qq/Wide character in write()\n/);
    }

    my $len = length $buf;
    my $off = 0;

    if ( exists $self->{max_message_size_bytes}
        && $len > $self->{max_message_size_bytes} )
    {
        MongoDB::ProtocolError->throw(
            qq/Message of size $len exceeds maximum of / . $self->{max_message_size_bytes} );
    }

    local $SIG{PIPE} = 'IGNORE';

    while () {
        $self->can_write
          or MongoDB::NetworkTimeout->throw(
            qq/Timed out while waiting for socket to become ready for writing\n/);
        my $r = syswrite( $self->{fh}, $buf, $len, $off );
        if ( defined $r ) {
            $len -= $r;
            $off += $r;
            last unless $len > 0;
        }
        elsif ( $! == EPIPE ) {
            MongoDB::NetworkError->throw(qq/Socket closed by remote server: $!\n/);
        }
        elsif ( $! != EINTR ) {
            if ( $self->{fh}->can('errstr') ) {
                my $err = $self->{fh}->errstr();
                MongoDB::NetworkError->throw(qq/Could not write to SSL socket: '$err'\n /);
            }
            else {
                MongoDB::NetworkError->throw(qq/Could not write to socket: '$!'\n/);
            }

        }
    }

    $self->{last_used} = [gettimeofday];

    return $off;
}

sub read {
    @_ == 1 || MongoDB::UsageError->throw( q/Usage: $handle->read()/ . "\n" );
    my ($self) = @_;
    my $msg = '';

    $self->assert_valid_connection;

    # read length
    $self->_read_bytes( 4, \$msg );

    my $len = unpack( P_INT32, $msg );

    # read rest of the message
    $self->_read_bytes( $len - 4, \$msg );

    $self->{last_used} = [gettimeofday];

    return $msg;
}

sub _read_bytes {
    @_ == 3 || MongoDB::UsageError->throw( q/Usage: $handle->read(len, bufref)/ . "\n" );
    my ( $self, $len, $bufref ) = @_;

    while ( $len > 0 ) {
        $self->can_read
          or MongoDB::NetworkTimeout->throw(
            q/Timed out while waiting for socket to become ready for reading/ . "\n" );
        my $r = sysread( $self->{fh}, $$bufref, $len, length $$bufref );
        if ( defined $r ) {
            last unless $r;
            $len -= $r;
        }
        elsif ( $! != EINTR ) {
            if ( $self->{fh}->can('errstr') ) {
                my $err = $self->{fh}->errstr();
                MongoDB::NetworkError->throw(qq/Could not read from SSL socket: '$err'\n /);
            }
            else {
                MongoDB::NetworkError->throw(qq/Could not read from socket: '$!'\n/);
            }
        }
    }
    if ($len) {
        MongoDB::NetworkError->throw(qq/Unexpected end of stream\n/);
    }
    return;
}

sub _do_timeout {
    my ( $self, $type, $timeout ) = @_;
    $timeout = $self->{timeout}
      unless defined $timeout && $timeout >= 0;

    my $fd = fileno $self->{fh};
    defined $fd && $fd >= 0
      or MongoDB::InternalError->throw(qq/select(2): 'Bad file descriptor'\n/);

    my $initial = time;
    my $pending = $timeout;
    my $nfound;

    vec( my $fdset = '', $fd, 1 ) = 1;

    while () {
        $nfound =
          ( $type eq 'read' )
          ? select( $fdset, undef,  undef, $pending )
          : select( undef,  $fdset, undef, $pending );
        if ( $nfound == -1 ) {
            $! == EINTR
              or MongoDB::NetworkError->throw(qq/select(2): '$!'\n/);
            redo if !defined($timeout) || ( $pending = $timeout - ( time - $initial ) ) > 0;
            $nfound = 0;
        }
        last;
    }
    $! = 0;
    return $nfound;
}

sub can_read {
    @_ == 1 || @_ == 2 || MongoDB::UsageError->throw( q/Usage: $handle->can_read([timeout])/ . "\n" );
    my $self = shift;
    if ( ref( $self->{fh} ) eq 'IO::Socket::SSL' ) {
        return 1 if $self->{fh}->pending;
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

    return $self->{SSL_options}->{SSL_ca_file}
      if $self->{SSL_options}->{SSL_ca_file} and -e $self->{SSL_options}->{SSL_ca_file};

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
    for my $k ( keys %{ $self->{SSL_options} } ) {
        $ssl_args{$k} = $self->{SSL_options}{$k} if $k =~ m/^SSL_/;
    }

    return \%ssl_args;
}

1;

# vim: ts=4 sts=4 sw=4 et:
