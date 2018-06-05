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
our $VERSION = 'v1.999.0';

use Moo;
use MongoDB::Error;
use Encode ();
use Types::Standard qw(
    Any
    ArrayRef
    HashRef
    Str
);
use namespace::clean -except => 'meta';

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
    is => 'ro',
    isa => HashRef,
    builder => '_build_valid_options',
);

sub _build_valid_options {
    return {
        map { lc($_) => 1 } qw(
            appName
            authMechanism
            authMechanismProperties
            authSource
            connectTimeoutMS
            connect
            heartbeatFrequencyMS
            journal
            localThresholdMS
            maxStalenessSeconds
            maxTimeMS
            readPreference
            readPreferenceTags
            replicaSet
            retryWrites
            serverSelectionTimeoutMS
            serverSelectionTryOnce
            socketCheckIntervalMS
            socketTimeoutMS
            ssl
            w
            wTimeoutMS
            readConcernLevel
        )
    };
}

has valid_srv_options => (
    is => 'ro',
    isa => HashRef,
    builder => '_build_valid_srv_options',
);

sub _build_valid_srv_options {
    return {
        map { lc($_) => 1 } qw(
            authSource
            replicaSet
        )
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
            MongoDB::UsageError->throw("in option '$name', '$tag' is not a key:value pair")
              unless @kv == 2;
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
            $parsed{$lc_k} = _parse_doc( $k, $v );
        }
        elsif ( $lc_k eq 'authsource' ) {
            $result->{db_name} = $v;
            $parsed{$lc_k} = $v;
        }
        elsif ( $lc_k eq 'readpreferencetags' ) {
            $parsed{$lc_k} ||= [];
            push @{ $parsed{$lc_k} }, _parse_doc( $k, $v );
        }
        elsif ( $lc_k eq 'ssl' || $lc_k eq 'journal' || $lc_k eq 'serverselectiontryonce' ) {
            $parsed{$lc_k} = __str_to_bool( $k, $v );
        }
        else {
            $parsed{$lc_k} = $v;
        }
    }
    return \%parsed;
}

sub _fetch_dns_seedlist {
    my ( $self, $host_name ) = @_;

    my @split_name = split( '\.', $host_name );
    MongoDB::Error->throw("URI '$self' must contain domain name and hostname")
        unless scalar( @split_name ) > 2;

    require Net::DNS;

    my $res = Net::DNS::Resolver->new;
    my $srv_data = $res->query( sprintf( '_mongodb._tcp.%s', $host_name ), 'SRV' );

    my @hosts;
    my $options = {};
    my $domain_name = join( '.', @split_name[1..$#split_name] );
    if ( $srv_data ) {
        foreach my $rr ( $srv_data->answer ) {
            next unless $rr->type eq 'SRV';
            my $target = $rr->target;
            # search for dot before domain name for a valid hostname - can have sub-subdomain
            unless ( $target =~ /\.\Q$domain_name\E$/ ) {
                MongoDB::Error->throw(
                    "URI '$self' SRV record returns FQDN '$target'"
                    . " which does not match domain name '${$domain_name}'"
                );
            }
            push @hosts, {
              target => $target,
              port   => $rr->port,
            };
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

    return ( \@hosts, $options );
}

sub _parse_srv_uri {
    my ( $self, $uri ) = @_;

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

    my ( $hosts, $options ) = $self->_fetch_dns_seedlist( $result{hostids} );

    # Default to SSL on unless specified in conn string options
    $options = {
      ssl => 'true',
      %$options,
      %{ $result{options} || {} },
    };

    # URI requires string based booleans for re-constructing the URI
    if ( ! $options->{ssl} && $options->{ssl} == 0 ) {
      $options->{ssl} = 'false';
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

    return $new_uri;
}

sub BUILD {
    my ($self) = @_;

    my $uri = $self->uri;
    my %result;

    if ( $uri =~ m{^mongodb\+srv://} ) {
        $uri = $self->_parse_srv_uri( $uri );
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

    if ( !defined $result{hostids} || !length $result{hostids} ) {
        MongoDB::Error->throw("URI '$self' could not be parsed (missing host list)");
    }
    $result{hostids} = [ map { lc _unescape_all($_) } split ',', $result{hostids} ];
    for my $hostid ( @{ $result{hostids} } ) {
        MongoDB::Error->throw(
            "URI '$self' could not be parsed (Unix domain sockets are not supported)")
          if $hostid =~ /\// && $hostid =~ /\.sock/;
        MongoDB::Error->throw(
            "URI '$self' could not be parsed (IP literals are not supported)")
          if substr( $hostid, 0, 1 ) eq '[';
        my ( $host, $port ) = split ":", $hostid, 2;
        MongoDB::Error->throw("host list '@{ $result{hostids} }' contains empty host")
          unless length $host;
        if ( defined $port ) {
            MongoDB::Error->throw("URI '$self' could not be parsed (invalid port '$port')")
              unless $port =~ /^\d+$/;
            MongoDB::Error->throw(
                "URI '$self' could not be parsed (invalid port '$port' (must be in range [1,65535])")
              unless $port >= 1 && $port <= 65535;
        }
    }
    $result{hostids} = [ map { /:/ ? $_ : $_.":27017" } @{ $result{hostids} } ];

    if ( defined $result{db_name} ) {
        MongoDB::Error->throw(
            "URI '$self' could not be parsed (database name must be URL encoded, found unescaped '/'"
        ) if $result{db_name} =~ /\//;
        $result{db_name} = _unescape_all( $result{db_name} );
    }

    if ( defined $result{options} ) {
        $result{options} = $self->_parse_options( $self->valid_options, \%result );
    }

    for my $attr (qw/username password db_name options hostids/) {
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
    return $ret if defined $ret;
    MongoDB::UsageError->throw("expected boolean string 'true' or 'false' for key '$k' but instead received '$str'");
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

# Check if should have been escaped; allow safe chars plus '+' and '%'
my $unreserved = q[a-z0-9._~-]; # use last so it ends in '-'
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
        (my $s = $_[0]->uri) =~ s{^(\w+)://[^/]+\@}{$1://[**REDACTED**]\@};
        return $s
    },
    'fallback' => 1;


1;

# vim: ts=4 sts=4 sw=4 et:
