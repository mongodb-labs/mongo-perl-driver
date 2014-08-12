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
our $VERSION = 'v0.704.4.1';

use Config;
use Errno qw[EINTR EPIPE];
use IO::Socket qw[SOCK_STREAM];
use Scalar::Util qw/refaddr/;

use constant {
    HAS_THREADS => $Config{usethreads},
    P_INT32 => $] lt '5.010' ? 'l' : 'l<',
};

use if HAS_THREADS, 'Scalar::Util';

my $SOCKET_CLASS =
  eval { require IO::Socket::IP; IO::Socket::IP->VERSION(0.25) }
  ? 'IO::Socket::IP'
  : 'IO::Socket::INET';

my %LINKS;

sub new {
    @_ == 2 || @_ == 3 || Carp::confess( q/Usage: MongoDB::_Link->new(address, [arg hashref])/ . "\n" );
    my ( $class, $address, $args ) = @_;
    my ( $host, $port ) = split /:/, $address;
    Carp::confess("new requires 'host:port' address argument")
        unless defined($host) && length($host) && defined($port) && length($port);
    my $self = bless {
        host        => $host,
        port        => $port,
        timeout     => 60,
        reconnect   => 0,
        with_ssl    => 0,
        verify_SSL  => 0,
        SSL_options => {},
        ( $args ? (%$args) : () ),
    }, $class;
    if ( HAS_THREADS ) {
        Scalar::Util::weaken( $LINKS{ refaddr $self } = $self );
    }
    return $self;
}

sub connect {
    @_ == 1 || Carp::confess( q/Usage: $handle->connect()/ . "\n" );
    my ( $self ) = @_;

    if ($self->{with_ssl}) {
        $self->_assert_ssl;
        # XXX possibly make SOCKET_CLASS an instance variable and set it here to IO::Socket::SSL
    }

    my ($host, $port) = @{$self}{qw/host port/};

    $self->{fh} = $SOCKET_CLASS->new(
        PeerHost => $host,
        PeerPort => $port,
        $self->{local_address} ? ( LocalAddr => $self->{local_address} ) : (),
        Proto   => 'tcp',
        Type    => SOCK_STREAM,
        Timeout => $self->{timeout},
    ) or Carp::confess(qq/Could not connect to '$host:$port': $@\n/);

    binmode( $self->{fh} )
      or Carp::confess(qq/Could not binmode() socket: '$!'\n/);

    $self->start_ssl($host) if $self->{with_ssl};

    return $self;
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
        Carp::confess(qq/SSL connection failed for $host: $ssl_err\n/);
    }
}

sub close {
    @_ == 1 || Carp::confess( q/Usage: $handle->close()/ . "\n" );
    my ($self) = @_;
    if ( $self->connected ) {
        CORE::close( $self->{fh} )
          or Carp::confess(qq/Could not close socket: '$!'\n/);
        delete $self->{fh};
    }
}

sub connected {
    my ($self) = @_;
    return $self->{fh} && $self->{fh}->connected;
}

sub assert_connected {
    my ($self) = @_;
    unless ( $self->{fh} && $self->{fh}->connected ) {
        my ( $host, $port, $ssl ) = @{$self}{qw/host port ssl/};
        if ( $self->{reconnect} ) {
            $self->connect();
        }
        else {
            Carp::confess("connection lost to $host");
        }
    }
    return;
}

sub write {
    @_ == 2 || Carp::confess( q/Usage: $handle->write(buf)/ . "\n" );
    my ( $self, $buf ) = @_;

    $self->assert_connected;

    if ( $] ge '5.008' ) {
        utf8::downgrade( $buf, 1 )
          or Carp::confess(qq/Wide character in write()\n/);
    }

    my $len = length $buf;
    my $off = 0;

    local $SIG{PIPE} = 'IGNORE';

    while () {
        $self->can_write
          or Carp::confess(qq/Timed out while waiting for socket to become ready for writing\n/);
        my $r = syswrite( $self->{fh}, $buf, $len, $off );
        if ( defined $r ) {
            $len -= $r;
            $off += $r;
            last unless $len > 0;
        }
        elsif ( $! == EPIPE ) {
            Carp::confess(qq/Socket closed by remote server: $!\n/);
        }
        elsif ( $! != EINTR ) {
            if ( $self->{fh}->can('errstr') ) {
                my $err = $self->{fh}->errstr();
                Carp::confess(qq/Could not write to SSL socket: '$err'\n /);
            }
            else {
                Carp::confess(qq/Could not write to socket: '$!'\n/);
            }

        }
    }
    return $off;
}

sub read {
    @_ == 1 || Carp::confess( q/Usage: $handle->read()/ . "\n" );
    my ($self) = @_;
    my $msg = '';

    $self->assert_connected;

    # read length
    $self->_read_bytes( 4, \$msg );

    my $len = unpack( P_INT32, $msg );

    # read rest of the message
    $self->_read_bytes( $len - 4, \$msg );

    return $msg;
}

sub _read_bytes {
    @_ == 3 || Carp::confess( q/Usage: $handle->read(len, bufref)/ . "\n" );
    my ( $self, $len, $bufref ) = @_;

    while ( $len > 0 ) {
        $self->can_read
          or Carp::confess( q/Timed out while waiting for socket to become ready for reading/ . "\n" );
        my $r = sysread( $self->{fh}, $$bufref, $len, length $$bufref );
        if ( defined $r ) {
            last unless $r;
            $len -= $r;
        }
        elsif ( $! != EINTR ) {
            if ( $self->{fh}->can('errstr') ) {
                my $err = $self->{fh}->errstr();
                Carp::confess(qq/Could not read from SSL socket: '$err'\n /);
            }
            else {
                Carp::confess(qq/Could not read from socket: '$!'\n/);
            }
        }
    }
    if ($len) {
        Carp::confess(qq/Unexpected end of stream\n/);
    }
    return;
}

sub _do_timeout {
    my ( $self, $type, $timeout ) = @_;
    $timeout = $self->{timeout}
      unless defined $timeout && $timeout >= 0;

    my $fd = fileno $self->{fh};
    defined $fd && $fd >= 0
      or Carp::confess(qq/select(2): 'Bad file descriptor'\n/);

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
              or Carp::confess(qq/select(2): '$!'\n/);
            redo if !$timeout || ( $pending = $timeout - ( time - $initial ) ) > 0;
            $nfound = 0;
        }
        last;
    }
    $! = 0;
    return $nfound;
}

sub can_read {
    @_ == 1 || @_ == 2 || Carp::confess( q/Usage: $handle->can_read([timeout])/ . "\n" );
    my $self = shift;
    if ( ref( $self->{fh} ) eq 'IO::Socket::SSL' ) {
        return 1 if $self->{fh}->pending;
    }
    return $self->_do_timeout( 'read', @_ );
}

sub can_write {
    @_ == 1 || @_ == 2 || Carp::confess( q/Usage: $handle->can_write([timeout])/ . "\n" );
    my $self = shift;
    return $self->_do_timeout( 'write', @_ );
}

sub _assert_ssl {
    # Need IO::Socket::SSL 1.42 for SSL_create_ctx_callback
    Carp::confess(qq/IO::Socket::SSL 1.42 must be installed for SSL support\n/)
      unless eval { require IO::Socket::SSL; IO::Socket::SSL->VERSION(1.42) };
    # Need Net::SSLeay 1.49 for MODE_AUTO_RETRY
    Carp::confess(qq/Net::SSLeay 1.49 must be installed for SSL support\n/)
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

    foreach my $ca_bundle (
        qw{
        /etc/ssl/certs/ca-certificates.crt
        /etc/pki/tls/certs/ca-bundle.crt
        /etc/ssl/ca-bundle.pem
        }
      )
    {
        return $ca_bundle if -e $ca_bundle;
    }

    Carp::confess qq/Couldn't find a CA bundle with which to verify the SSL certificate.\n/
      . qq/Try installing Mozilla::CA from CPAN\n/;
}

sub _ssl_args {
    my ( $self, $host ) = @_;

    my %ssl_args;

    # This test reimplements IO::Socket::SSL::can_client_sni(), which wasn't
    # added until IO::Socket::SSL 1.84
    if ( Net::SSLeay::OPENSSL_VERSION_NUMBER() >= 0x01000000 ) {
        $ssl_args{SSL_hostname} = $host, # Sane SNI support
    }

    if ( $self->{verify_SSL} ) {
        $ssl_args{SSL_verifycn_scheme} = 'default';           # enable CN validation
        $ssl_args{SSL_verifycn_name}   = $host;               # set validation hostname
        $ssl_args{SSL_verify_mode}     = 0x01;                # enable cert validation
        $ssl_args{SSL_ca_file}         = $self->_find_CA_file;
    }
    else {
        $ssl_args{SSL_verifycn_scheme} = 'none';              # disable CN validation
        $ssl_args{SSL_verify_mode}     = 0x00;                # disable cert validation
    }

    # user options override settings from verify_SSL
    for my $k ( keys %{ $self->{SSL_options} } ) {
        $ssl_args{$k} = $self->{SSL_options}{$k} if $k =~ m/^SSL_/;
    }

    return \%ssl_args;
}

if ( HAS_THREADS ) {
    *DEMOLISH = sub { delete $LINKS{ refaddr $_[0] } };
}

# Threads need to reconnect
sub CLONE {
    while ( my ($k, $v) = each %LINKS ) {
        $v->close if ref $v;
        delete $LINKS{ $k };
    }
}

1;

# vim: ts=4 sts=4 sw=4 et:
