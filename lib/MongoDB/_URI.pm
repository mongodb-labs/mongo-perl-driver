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
package MongoDB::_URI;

use version;
our $VERSION = 'v1.999.0';

use Moo;
use MongoDB::Error;
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
    $str =~ s/%([0-9a-f]{2})/chr(hex($1))/ieg;
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

use Devel::Dwarn;
sub _parse_options {
    my ( $self, $valid, $result, $err_unsupported ) = @_;

    my %parsed;
    for my $opt ( split '&', $result->{options} ) {
        my @kv = split '=', $opt, -1;
        MongoDB::UsageError->throw("expected key value pair") unless @kv == 2;
        my ( $k, $v ) = map { _unescape_all($_) } @kv;
        # connection string spec calls for case normalization
        ( my $lc_k = $k ) =~ tr[A-Z][a-z];
        if ( !$valid->{$lc_k} ) {
            if ( $err_unsupported ) {
                MongoDB::Error->throw("Unsupported option '$k' in URI $self\n");
            } else {
                warn "Unsupported option '$k' in URI $self\n";
            }
            next;
        }
        if ( exists $parsed{$lc_k} && !exists $options_with_list_type{$lc_k} ) {
            warn "Multiple options were found for the same value '$lc_k'\n";
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

sub _parse_srv_uri {
    my ( $self, $uri ) = @_;

    my %result;

    $uri =~ m{^$uri_re$};

    (
        $result{username}, $result{password}, $result{hostids},
        $result{db_name},  $result{options}
    ) = ( $1, $2, $3, $4, $5 );

    Dwarn \%result;
    if ( defined $result{username} ) {
        MongoDB::Error->throw("URI '$self' cannot have a username if using an SRV connection string");
    }

    if ( defined $result{password} ) {
        MongoDB::Error->throw("URI '$self' cannot have a password if using an SRV connection string");
    }

    if ( defined $result{db_name} && length $result{db_name} ) {
        MongoDB::Error->throw("URI '$self' cannot have a database name if using an SRV connection string");
    }

    if ( !defined $result{hostids} || !length $result{hostids} ) {
        MongoDB::Error->throw("URI '$self' cannot be empty if using an SRV connection string");
    }

    if ( $result{hostids} =~ /,/ ) {
        MongoDB::Error->throw("URI '$self' cannot contain a comma or multiple host names if using an SRV connection string");
    }

    if ( defined $result{options} ) {
        my $valid = $self->valid_options;
        $result{options} = $self->_parse_options( $valid, \%result );
    }

    # TODO error on invalid domains

    Dwarn \%result;

    require Net::DNS;

    my $res = Net::DNS::Resolver->new;
    my $srv_data = $res->query( sprintf( '_mongodb._tcp.%s', $result{hostids} ), 'SRV' );

    my @hosts;
    my $options = {};

    if ( $srv_data ) {
        foreach my $rr ( $srv_data->answer ) {
            next unless $rr->type eq 'SRV';
            # TODO check url is correct?
            push @hosts, {
              target => $rr->target,
              port   => $rr->port,
            };
        }
        my $txt_data = $res->query( $result{hostids}, 'TXT' );
        my @txt_answers;
        foreach my $rr ( $txt_data->answer ) {
            next unless $rr->type eq 'TXT';
            push @txt_answers, $rr;
        }
        if ( scalar( @txt_answers ) > 1 ) {
            MongoDB::Error->throw("URI '$self' returned more than one TXT result");
        } elsif ( scalar( @txt_answers ) == 1 ) {
            my @txt_data = $txt_answers[0]->txtdata;
            my $txt_opt_string = join ( '', @txt_data );
            Dwarn $txt_opt_string;
            $options = $self->_parse_options( $self->valid_srv_options, { options => $txt_opt_string }, 1 );
        }
    } else {
        MongoDB::Error->throw("URI '$self' does not return any SRV results");
    }

    # Default to SSL on unless specified in conn string options
    $options = {
      ssl => 'true',
      %$options,
      %{ $result{options} || {} },
    };

    my $new_uri = sprintf(
        'mongodb://%s/%s%s',
        join( ',', map { sprintf( '%s:%s', $_->{target}, $_->{port} ) } @hosts ),
        scalar( keys %$options ) ? '?' : '',
        join( '&', map { sprintf( '%s=%s', $_, $options->{$_} ) } keys %$options ),
    );

    Dwarn $new_uri;
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
            "URI '$self' could not be parsed (username must be URL encoded, found unescaped '\@'"
        ) if $result{username} =~ /@/;
        $result{username} = _unescape_all( $result{username} );
    }

    if ( defined $result{password} ) {
        MongoDB::Error->throw(
            "URI '$self' could not be parsed (password must be URL encoded, found unescaped ':'")
          if $result{password} =~ /:/;
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
        my $valid = $self->valid_options;
        $result{options} = $self->_parse_options( $valid, \%result );
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

# redact user credentials when stringifying
use overload
    '""' => sub {
        (my $s = $_[0]->uri) =~ s{^(\w+)://[^/]+\@}{$1://[**REDACTED**]\@};
        return $s
    },
    'fallback' => 1;


1;

# vim: ts=4 sts=4 sw=4 et:
