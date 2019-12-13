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
package MongoDB::_URI;

use version;
our $VERSION = 'v2.2.2';

use Moo;
use MongoDB::Error;
use Encode ();
use Time::HiRes qw(time);
use MongoDB::_Constants qw( RESCAN_SRV_FREQUENCY_SEC );
use Types::Standard qw(
    Any
    ArrayRef
    HashRef
    Str
    Int
    Num
);
use namespace::clean -except => 'meta';
use Scalar::Util qw/looks_like_number/;

my $uri_re =
    qr{
            mongodb(?:\+srv|)://
            (?: ([^:]*) (?: : ([^@]*) )? @ )? # [username(:password)?@]
            ([^/?]*) # host1[:port1][,host2[:port2],...[,hostN[:portN]]]
            (?:
               / ([^?]*) # /[database]
                (?: [?] (.*) )? # [?options]
            )?
    }x;

my %options_with_list_type = map { lc($_) => 1 } qw(
  readPreferenceTags
);

has uri => (
    is => 'ro',
    isa => Str,
    required => 1,
);

has username => (
    is => 'ro',
    isa => Any,
    writer => '_set_username',
);

has password => (
    is => 'ro',
    isa => Any,
    writer => '_set_password',
);

has db_name => (
    is => 'ro',
    isa => Str,
    writer => '_set_db_name',
    default => '',
);

has options => (
    is => 'ro',
    isa => HashRef,
    writer => '_set_options',
    default => sub { {} },
);

has hostids => (
    is => 'ro',
    isa => ArrayRef,
    writer => '_set_hostids',
    default => sub { [] },
);

has valid_options => (
    is => 'lazy',
    isa => HashRef,
);

has expires => (
    is => 'ro',
    isa => Num,
    writer => '_set_expires',
);

sub _build_valid_options {
    my $self = shift;
    return {
        map { lc($_) => 1 } qw(
            appName
            authMechanism
            authMechanismProperties
            authSource
            compressors
            connect
            connectTimeoutMS
            heartbeatFrequencyMS
            journal
            localThresholdMS
            maxStalenessSeconds
            maxTimeMS
            readConcernLevel
            readPreference
            readPreferenceTags
            replicaSet
            serverSelectionTimeoutMS
            serverSelectionTryOnce
            socketCheckIntervalMS
            socketTimeoutMS
            tlsCAFile
            tlsCertificateKeyFile
            tlsCertificateKeyFilePassword
            w
            wTimeoutMS
            zlibCompressionLevel
        ), keys %{ $self->_valid_str_to_bool_options }
    };
}

has valid_srv_options => (
    is => 'lazy',
    isa => HashRef,
);

sub _build_valid_srv_options {
    return {
        map { lc($_) => 1 } qw(
            authSource
            replicaSet
        )
    };
}

has _valid_str_to_bool_options => (
    is => 'lazy',
    isa => HashRef,
    builder => '_build_valid_str_to_bool_options',
);

sub _build_valid_str_to_bool_options {
    return {
        map { lc($_) => 1 } qw(
            journal
            retryReads
            retryWrites
            serverselectiontryonce
            ssl
            tls
            tlsAllowInvalidCertificates
            tlsAllowInvalidHostnames
            tlsInsecure
        )
    };
}

has _extra_options_validation => (
    is => 'lazy',
    isa => HashRef,
    builder => '_build_extra_options_validation',
);

sub _build_extra_options_validation {
  return {
      _PositiveInt => sub {
          my $v = shift;
          Int->($v) && $v >= 0;
      },
      wtimeoutms => '_PositiveInt',
      connecttimeoutms => '_PositiveInt',
      localthresholdms => '_PositiveInt',
      serverselectiontimeoutms => '_PositiveInt',
      sockettimeoutms => '_PositiveInt',
      w => sub {
          my $v = shift;
          if (looks_like_number($v)) {
              return $v >= 0;
          }
          return 1; # or any string
      },
      zlibcompressionlevel => sub {
          my $v = shift;
          Int->($v) && $v >= -1 && $v <= 9;
      },
      heartbeatfrequencyms => sub {
          my $v = shift;
          Int->($v) && $v >= 500;
      },
      maxstalenessseconds => sub {
          my $v = shift;
          Int->($v) && ( $v == 1 || $v == -1 || $v >= 90 );
      },
  };
}

sub _unescape_all {
    my $str = shift;
    return '' unless defined $str;
    if ( $str =~ s/%([0-9a-f]{2})/chr(hex($1))/ieg ) {
        $str = Encode::decode('UTF-8', $str);
    }
    return $str;
}

sub _parse_doc {
    my ($name, $string) = @_;
    my $set = {};
    for my $tag ( split /,/, $string ) {
        if ( $tag =~ /\S/ ) {
            my @kv = map { my $s = $_; $s =~ s{^\s*}{}; $s =~ s{\s*$}{}; $s } split /:/, $tag, 2;
            if ( @kv != 2 ) {
                warn "in option '$name', '$tag' is not a key:value pair\n";
                return
            }
            $set->{$kv[0]} = $kv[1];
        }
    }
    return $set;
}

sub _parse_options {
    my ( $self, $valid, $result, $txt_record ) = @_;

    my %parsed;
    for my $opt ( split '&', $result->{options} ) {
        my @kv = split '=', $opt, -1;
        MongoDB::UsageError->throw("expected key value pair") unless @kv == 2;
        my ( $k, $v ) = map { _unescape_all($_) } @kv;
        # connection string spec calls for case normalization
        ( my $lc_k = $k ) =~ tr[A-Z][a-z];
        if ( !$valid->{$lc_k} ) {
            if ( $txt_record ) {
                MongoDB::Error->throw("Unsupported option '$k' in URI $self for TXT record $txt_record\n");
            } else {
                warn "Unsupported option '$k' in URI $self\n";
            }
            next;
        }
        if ( exists $parsed{$lc_k} && !exists $options_with_list_type{$lc_k} ) {
            warn "Multiple options were found for the same value '$lc_k'. The first occurrence will be used\n";
            next;
        }
        if ( $lc_k eq 'authmechanismproperties' ) {
            my $temp = _parse_doc( $k, $v );
            if ( defined $temp ) {
                $parsed{$lc_k} = $temp;
                if ( exists $parsed{$lc_k}{CANONICALIZE_HOST_NAME} ) {
                    my $temp = __str_to_bool( 'CANONICALIZE_HOST_NAME', $parsed{$lc_k}{CANONICALIZE_HOST_NAME} );
                    if ( defined $temp ) {
                        $parsed{$lc_k}{CANONICALIZE_HOST_NAME} = $temp;
                    }
                }
            }
        }
        elsif ( $lc_k eq 'compressors' ) {
            my @compressors = split /,/, $v, -1;
            my $valid_compressors = {
                snappy => 1,
                zlib => 1,
                zstd => 1
            };
            for my $compressor ( @compressors ) {
                warn("Unsupported compressor $compressor\n")
                    unless $valid_compressors->{$compressor};
            }
            $parsed{$lc_k} = [ @compressors ];
        }
        elsif ( $lc_k eq 'authsource' ) {
            $parsed{$lc_k} = $v;
        }
        elsif ( $lc_k eq 'readpreferencetags' ) {
            $parsed{$lc_k} ||= [];
            my $temp = _parse_doc( $k, $v );
            if ( defined $temp ) {
                push @{$parsed{$lc_k}}, $temp;
            }
        }
        elsif ( $self->_valid_str_to_bool_options->{ $lc_k } ) {
            my $temp =  __str_to_bool( $k, $v );
            if ( defined $temp ) {
                $parsed{$lc_k} = $temp
            }
        }
        elsif ( my $opt_validation = $self->_extra_options_validation->{ $lc_k } ) {
            unless (ref $opt_validation eq 'CODE') {
                $opt_validation = $self->_extra_options_validation->{ $opt_validation };
            }
            my $valid = eval { $opt_validation->($v) };
            my $err = "$@";
            if ( ! $valid ) {
                warn("Unsupported URI value '$k' = '$v': $err");
            }
            else {
                $parsed{$lc_k} = $v;
            }
        }
        else {
            $parsed{$lc_k} = $v;
        }
    }
    if (
        exists $parsed{tlsinsecure}
        && (   exists $parsed{tlsallowinvalidcertificates}
            || exists $parsed{tlsallowinvalidhostnames} )
      )
    {
        MongoDB::Error->throw('tlsInsecure conflicts with other options');
    }
    # If both exist, they must be identical.
    if (   exists( $parsed{tls} )
        && exists( $parsed{ssl} )
        && $parsed{tls} != $parsed{ssl} )
    {
        MongoDB::Error->throw('tls and ssl must have the same value');
    }
    # If either exists, set them both.
    if ( exists $parsed{tls} ) {
        $parsed{ssl} = $parsed{tls};
    }
    elsif ( exists $parsed{ssl} ) {
        $parsed{tls} = $parsed{ssl};
    }
    return \%parsed;
}

sub _fetch_dns_seedlist {
    my ( $self, $host_name, $phase ) = @_;

    my @split_name = split( '\.', $host_name );
    MongoDB::Error->throw("URI '$self' must contain domain name and hostname")
        unless scalar( @split_name ) > 2;

    require Net::DNS;

    my $res = Net::DNS::Resolver->new;
    my $srv_data = $res->query( sprintf( '_mongodb._tcp.%s', $host_name ), 'SRV' );

    my @hosts;
    my $options = {};
    my $domain_name = join( '.', @split_name[1..$#split_name] );
    my $minimum_ttl;
    if ( $srv_data ) {
        SRV_RECORD: foreach my $rr ( $srv_data->answer ) {
            next unless $rr->type eq 'SRV';
            my $target = $rr->target;
            # search for dot before domain name for a valid hostname - can have sub-subdomain
            unless ( $target =~ /\.\Q$domain_name\E$/ ) {
                my $err_msg = "URI '$self' SRV record returns FQDN '$target'"
                    . " which does not match domain name '${$domain_name}'";
                if ($phase && $phase eq 'init') {
                    MongoDB::Error->throw($err_msg);
                }
                else {
                    warn $err_msg;
                }
                next SRV_RECORD;
            }
            push @hosts, {
              target => $target,
              port   => $rr->port,
            };
            $minimum_ttl = $rr->ttl
                if not defined $minimum_ttl or $rr->ttl < $minimum_ttl;
        }
        my $txt_data = $res->query( $host_name, 'TXT' );
        if ( defined $txt_data ) {
            my @txt_answers;
            foreach my $rr ( $txt_data->answer ) {
                next unless $rr->type eq 'TXT';
                push @txt_answers, $rr;
            }
            if ( scalar( @txt_answers ) > 1 ) {
                MongoDB::Error->throw("URI '$self' returned more than one TXT result");
            } elsif ( scalar( @txt_answers ) == 1 ) {
                my $txt_opt_string = join ( '', $txt_answers[0]->txtdata );
                $options = $self->_parse_options( $self->valid_srv_options, { options => $txt_opt_string }, $txt_opt_string );
            }
        }
    } else {
        MongoDB::Error->throw("URI '$self' does not return any SRV results");
    }

    unless (@hosts) {
        my $err_msg = "URI '$self' does not return any valid SRV results";
        if ($phase && $phase eq 'init') {
            MongoDB::Error->throw($err_msg);
        }
        else {
            warn $err_msg;
        }
    }

    $minimum_ttl = RESCAN_SRV_FREQUENCY_SEC
        if $minimum_ttl < RESCAN_SRV_FREQUENCY_SEC
            && $phase && $phase ne 'init';

    return ( \@hosts, $options, time + $minimum_ttl );
}

sub _parse_srv_uri {
    my ( $self, $uri, $phase ) = @_;

    my %result;

    $uri =~ m{^$uri_re$};

    (
        $result{username}, $result{password}, $result{hostids},
        $result{db_name},  $result{options}
    ) = ( $1, $2, $3, $4, $5 );

    $result{hostids} = lc _unescape_all( $result{hostids} );

    if ( !defined $result{hostids} || !length $result{hostids} ) {
        MongoDB::Error->throw("URI '$self' cannot be empty if using an SRV connection string");
    }

    if ( $result{hostids} =~ /,/ ) {
        MongoDB::Error->throw("URI '$self' cannot contain a comma or multiple host names if using an SRV connection string");
    }

    if ( $result{hostids} =~ /:\d+$/ ) {
        MongoDB::Error->throw("URI '$self' cannot contain port number if using an SRV connection string");
    }

    if ( defined $result{options} ) {
        $result{options} = $self->_parse_options( $self->valid_options, \%result );
    }

    my ( $hosts, $options, $expires ) = $self->_fetch_dns_seedlist( $result{hostids}, $phase );

    # Default to SSL on unless specified in conn string options
    $options = {
      ssl => 'true',
      %$options,
      %{ $result{options} || {} },
    };

    # Reset str to bool options to string value, as _parse_options changes it to 0/1 if it exists during parsing
    # means we get the correct value when re-building the uri below.
    for my $stb_key ( keys %{ $self->_valid_str_to_bool_options } ) {
        # use exists just in case
        next unless exists $options->{ $stb_key };
        $options->{ $stb_key } = ($options->{ $stb_key } || $options->{ $stb_key } eq 'true') ? 'true' : 'false';
    }

    my $auth = "";
    if ( defined $result{username} || defined $result{password} )  {
        $auth = join(":", map { $_ // "" } $result{username}, $result{password});
        $auth .= "@";
    }

    my $new_uri = sprintf(
        'mongodb://%s%s/%s%s%s',
        $auth,
        join( ',', map { sprintf( '%s:%s', $_->{target}, $_->{port} ) } @$hosts ),
        ($result{db_name} // ""),
        scalar( keys %$options ) ? '?' : '',
        join( '&', map { sprintf( '%s=%s', $_, __uri_escape( $options->{$_} ) ) } keys %$options ),
    );

    return( $new_uri, $expires );
}

sub BUILD {
    my ($self) = @_;

    $self->_initialize_from_uri;
}

# Options:
# - fallback_ttl_sec: Fallback TTL in seconds in case of an error
sub check_for_changes {
    my ($self, $options) = @_;

    if (defined $self->{expires} && $self->{expires} <= time) {
        my @current = sort @{ $self->{hostids} };
        local $@;
        my $ok = eval {

            $self->_update_from_uri;
            1;
        };
        if (!$ok) {
            warn "Error while fetching SRV records: $@";
            $self->{expires} = $options->{fallback_ttl_sec};
        };
        return 0
            unless $ok;
        my @new = sort @{ $self->{hostids} };
        return 1
            unless @current == @new;
        for my $index (0 .. $#current) {
            return 1
                unless $new[$index] eq $current[$index];
        }
        return 0;
    }

    return 0;
}

sub _prepare_dns_hosts {
    my ($self, $hostids) = @_;

    if ( !defined $hostids || !length $hostids ) {
        MongoDB::Error->throw("URI '$self' could not be parsed (missing host list)");
    }
    $hostids = [ map { lc _unescape_all($_) } split ',', $hostids ];
    for my $hostid (@$hostids) {
        MongoDB::Error->throw(
            "URI '$self' could not be parsed (Unix domain sockets are not supported)")
          if $hostid =~ /\// && $hostid =~ /\.sock/;
        MongoDB::Error->throw(
            "URI '$self' could not be parsed (IP literals are not supported)")
          if substr( $hostid, 0, 1 ) eq '[';
        my ( $host, $port ) = split ":", $hostid, 2;
        MongoDB::Error->throw("host list '@{ $hostids }' contains empty host")
          unless length $host;
        if ( defined $port ) {
            MongoDB::Error->throw("URI '$self' could not be parsed (invalid port '$port')")
              unless $port =~ /^\d+$/;
            MongoDB::Error->throw(
                "URI '$self' could not be parsed (invalid port '$port' (must be in range [1,65535])")
              unless $port >= 1 && $port <= 65535;
        }
    }
    $hostids = [ map { /:/ ? $_ : $_.":27017" } @$hostids ];
    return $hostids;
}

sub _update_from_uri {
    my ($self) = @_;

    my $uri = $self->uri;
    my %result;

    ($uri, my $expires) = $self->_parse_srv_uri( $uri );
    $self->{expires} = $expires;

    if ( $uri !~ m{^$uri_re$} ) {
        MongoDB::Error->throw("URI '$self' could not be parsed");
    }

    my $hostids = $3;
    $hostids = $self->_prepare_dns_hosts($hostids);

    $self->{hostids} = $hostids;
}

sub _initialize_from_uri {
    my ($self) = @_;

    my $uri = $self->uri;
    my %result;

    if ( $uri =~ m{^mongodb\+srv://} ) {
        ($uri, my $expires) = $self->_parse_srv_uri( $uri, 'init' );
        $result{expires} = $expires;
    }

    # we throw Error instead of UsageError for errors, to avoid stacktrace revealing credentials
    if ( $uri !~ m{^$uri_re$} ) {
        MongoDB::Error->throw("URI '$self' could not be parsed");
    }

    (
        $result{username}, $result{password}, $result{hostids},
        $result{db_name},  $result{options}
    ) = ( $1, $2, $3, $4, $5 );

    if ( defined $result{username} ) {
        MongoDB::Error->throw(
            "URI '$self' could not be parsed (username must be URL encoded)"
        ) if __userinfo_invalid_chars($result{username});
        $result{username} = _unescape_all( $result{username} );
    }

    if ( defined $result{password} ) {
        MongoDB::Error->throw(
            "URI '$self' could not be parsed (password must be URL encoded)"
        ) if __userinfo_invalid_chars($result{password});
        $result{password} = _unescape_all( $result{password} );
    }

    $result{hostids} = $self->_prepare_dns_hosts($result{hostids});

    if ( defined $result{db_name} ) {
        MongoDB::Error->throw(
            "URI '$self' could not be parsed (database name must be URL encoded, found unescaped '/'"
        ) if $result{db_name} =~ /\//;
        $result{db_name} = _unescape_all( $result{db_name} );
    }

    if ( defined $result{options} ) {
        $result{options} = $self->_parse_options( $self->valid_options, \%result );
    }

    for my $attr (qw/username password db_name options hostids expires/) {
        my $setter = "_set_$attr";
        $self->$setter( $result{$attr} ) if defined $result{$attr};
    }

    return;
}

sub __str_to_bool {
    my ($k, $str) = @_;
    MongoDB::UsageError->throw("cannot convert undef to bool for key '$k'")
      unless defined $str;
    my $ret = $str eq "true" ? 1 : $str eq "false" ? 0 : undef;
    warn("expected boolean string 'true' or 'false' for key '$k' but instead received '$str'. Ignoring '$k'.\n")
        unless defined $ret;
    return $ret;
}

# uri_escape borrowed from HTTP::Tiny 0.070
my %escapes = map { chr($_) => sprintf("%%%02X", $_) } 0..255;
$escapes{' '}="+";
my $unsafe_char = qr/[^A-Za-z0-9\-\._~]/;

sub __uri_escape {
    my ($str) = @_;
    if ( $] ge '5.008' ) {
        utf8::encode($str);
    }
    else {
        $str = pack("U*", unpack("C*", $str)) # UTF-8 encode a byte string
            if ( length $str == do { use bytes; length $str } );
        $str = pack("C*", unpack("C*", $str)); # clear UTF-8 flag
    }
    $str =~ s/($unsafe_char)/$escapes{$1}/ge;
    return $str;
}

# Rules for valid userinfo from RFC 3986 Section 3.2.1.
my $unreserved = q[a-z0-9._~-]; # use this class last so regex ends in '-'
my $subdelimit = q[!$&'()*+,;=];
my $allowed = "%$subdelimit$unreserved";
my $not_allowed_re = qr/[^$allowed]/i;
my $not_pct_enc_re = qr/%(?![0-9a-f]{2})/i;

sub __userinfo_invalid_chars {
    my ($str) = @_;
    return $str =~ $not_pct_enc_re || $str =~ $not_allowed_re;
}

# redact user credentials when stringifying
use overload
    '""' => sub {
        (my $s = $_[0]->uri) =~ s{^([^:]+)://[^/]+\@}{$1://[**REDACTED**]\@};
        return $s
    },
    'fallback' => 1;


1;

# vim: ts=4 sts=4 sw=4 et:
