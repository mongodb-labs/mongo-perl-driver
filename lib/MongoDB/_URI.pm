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

package MongoDB::_URI;

use version;
our $VERSION = 'v1.2.2';

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
            mongodb://
            (?: ([^:]*) (?: : ([^@]*) )? @ )? # [username(:password)?@]
            ([^/]*) # host1[:port1][,host2[:port2],...[,hostN[:portN]]]
            (?:
               / ([^?]*) # /[database]
                (?: [?] (.*) )? # [?options]
            )?
    }x;

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

has hostpairs => (
    is => 'ro',
    isa => ArrayRef,
    writer => '_set_hostpairs',
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
            authMechanism
            authMechanismProperties
            connectTimeoutMS
            connect
            heartbeatFrequencyMS
            journal
            localThresholdMS
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
            my @kv = map { s{^\s*}{}; s{\s*$}{}; $_ } split /:/, $tag, 2;
            MongoDB::UsageError->throw("in option '$name', '$tag' is not a key:value pair")
              unless @kv == 2;
            $set->{$kv[0]} = $kv[1];
        }
    }
    return $set;
}

sub BUILD {
    my ($self) = @_;

    my $uri = $self->uri;
    my %result;

    if ($uri =~ m{^$uri_re$}) {

        ($result{username}, $result{password}, $result{hostpairs}, $result{db_name}, $result{options}) = ($1, $2, $3, $4, $5);

        # Decode components
        for my $subcomponent ( qw/username password db_name/ ) {
            $result{$subcomponent} = _unescape_all($result{$subcomponent}) unless !(defined $result{$subcomponent});
        }

        $result{hostpairs} = 'localhost' unless $result{hostpairs};
        $result{hostpairs} = [
            map { lc $_ }
            map { @_ = split ':', $_; _unescape_all($_[0]).":"._unescape_all($_[1]) }
            map { $_ .= ':27017' unless $_ =~ /:/ ; $_ } split ',', $result{hostpairs}
        ];

        if ( defined $result{options} ) {
            my $valid = $self->valid_options;
            my %parsed;
            for my $opt ( split '&', $result{options} ) {
                my @kv = split '=', $opt;
                push @kv, '' if @kv == 1;
                MongoDB::UsageError->throw("expected key value pair") unless @kv == 2;
                my ($k, $v) = map { _unescape_all($_) } @kv;
                # connection string spec calls for case normalization
                (my $lc_k = $k) =~ tr[A-Z][a-z];
                if ( !$valid->{$lc_k} ) {
                    warn "Unsupported option '$k' in URI $self\n";
                    next;
                }
                if ( $lc_k eq 'authmechanismproperties' ) {
                    $parsed{$lc_k} = _parse_doc($k,$v);
                }
                elsif ( $lc_k eq 'readpreferencetags' ) {
                    $parsed{$lc_k} ||= [];
                    push @{$parsed{$lc_k}}, _parse_doc($k,$v);
                }
                elsif ( $lc_k eq 'ssl' || $lc_k eq 'journal' || $lc_k eq 'serverselectiontryonce' ) {
                    $parsed{$lc_k} = __str_to_bool($k, $v);
                }
                else {
                    $parsed{$lc_k} = $v;
                }
            }
            $result{options} = \%parsed;
        }

        delete $result{username} unless defined $result{username};
        delete $result{password} unless defined $result{password}; # can be empty string
        delete $result{db_name} unless defined $result{db_name} && length $result{db_name};
    }
    else {
        # NOT a UsageError to avoid stacktrace revealing credentials
        MongoDB::Error->throw("URI '$self' could not be parsed");
    }

    for my $attr ( qw/username password db_name options hostpairs/ ) {
        my $setter = "_set_$attr";
        $self->$setter( $result{$attr} ) if defined $result{$attr};
    }

    return;
}

sub __str_to_bool {
    my ($k, $str) = @_;
    MongoDB::UsageError->throw("cannot convert undef to bool for key '$k'")
      unless defined $str;
    # check for "true" and "false" (case-insensitively)
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
