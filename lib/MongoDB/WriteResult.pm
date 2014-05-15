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

package MongoDB::WriteResult;

# ABSTRACT: MongoDB write result document

use version;
our $VERSION = 'v0.703.5'; # TRIAL

use Moose;
use MongoDB::_Types;
use Syntax::Keyword::Junction qw/any/;
use namespace::clean -except => 'meta';

with 'MongoDB::Role::_LastError';

has [qw/writeErrors writeConcernErrors upserted/] => (
    is      => 'ro',
    isa     => 'ArrayOfHashRef',
    coerce  => 1,
    default => sub { [] },
);

for my $attr (qw/nInserted nUpserted nMatched nRemoved/) {
    has $attr => (
        is      => 'ro',
        isa     => 'Num',
        writer  => "_set_$attr",
        default => 0,
    );
}

has nModified => (
    is      => 'ro',
    isa     => 'Maybe[Num]',
    writer  => '_set_nModified',
    default => undef,
);

has op_count => (
    is      => 'ro',
    isa     => 'Num',
    writer  => '_set_op_count',
    default => 0,
);

has batch_count => (
    is      => 'ro',
    isa     => 'Num',
    writer  => '_set_batch_count',
    default => 0,
);

# defines how an logical operation type gets mapped to a result
# field from the actual command result
my %op_map = (
    insert => [ nInserted => sub { $_[0]->{n} } ],
    delete => [ nRemoved  => sub { $_[0]->{n} } ],
    update => [ nMatched  => sub { $_[0]->{n} } ],
    upsert => [ nMatched  => sub { $_[0]->{n} - @{ $_[0]->{upserted} || [] } } ],
);

my @op_map_keys = sort keys %op_map;

sub parse {
    my $class = shift;
    my $args = ref $_[0] eq 'HASH' ? shift : {@_};

    unless ( 2 == grep { exists $args->{$_} } qw/op result/ ) {
        confess "parse requires 'op' and 'result' arguments";
    }

    my ( $op, $op_count, $batch_count, $result ) = @{$args}{qw/op op_count batch_count result/};

    confess "op argument to parse must be one of: @op_map_keys"
      unless $op eq any(@op_map_keys);
    confess "results argument to parse must be a hash reference"
      unless ref $result eq 'HASH';

    # if we have an op count, use it, otherwise, let it use the default
    my $attrs = {
        batch_count => $batch_count || 1,
        $op_count ? ( op_count => $op_count ) : ()
    };

    # XXX need to detect and parse GLE: err|errmsg, wnote, jnote, wtimeout|error=timeout
    # and set code to unknown (8?) if not set

    # get writeErrors
    $attrs->{writeErrors} = $result->{writeErrors} if $result->{writeErrors};

    # rename writeConcernError -> writeConcernErrors; coercion will make it into an array later $attrs->{writeConcernErrors} = $result->{writeConcernError}
    $attrs->{writeConcernErrors} = $result->{writeConcernError}
      if $result->{writeConcernError};

    # if we have upserts, change type to calculate differently
    if ( $result->{upserted} ) {
        $op                 = 'upsert';
        $attrs->{upserted}  = $result->{upserted};
        $attrs->{nUpserted} = @{ $result->{upserted} };
    }

    # change 'n' into an op-specific count
    if ( exists $result->{n} ) {
        my ( $key, $builder ) = @{ $op_map{$op} };
        $attrs->{$key} = $builder->($result);
    }

    # nModified should stay undef unless we actually see it in a result
    if ( $op eq 'update' || $op eq 'upsert' ) {
        $attrs->{nModified} = $result->{nModified} if exists $result->{nModified};
    }

    return $class->new($attrs);
}

sub count_writeErrors {
    my ($self) = @_;
    return scalar @{ $self->writeErrors };
}

sub count_writeConcernErrors {
    my ($self) = @_;
    return scalar @{ $self->writeConcernErrors };
}

sub last_errmsg {
    my ($self) = @_;
    if ( $self->count_writeErrors ) {
        return $self->writeErrors->[-1]{errmsg};
    }
    elsif ( $self->count_writeConcernErrors ) {
        return $self->writeConcernErrors->[-1]{errmsg};
    }
    else {
        return "";
    }
}

sub merge_result {
    my ( $self, $result ) = @_;

    # Add counters
    for my $attr (qw/nInserted nUpserted nMatched nRemoved/) {
        my $setter = "_set_$attr";
        $self->$setter( $self->$attr + $result->$attr );
    }

    # If nModified is defined in either result we're merging, then we're on a
    # 2.6+ server and have done at least one update/upsert so we can combine
    # them; otherwise we leave it undefined
    if ( defined $self->nModified || defined $result->nModified ) {
        $self->_set_nModified( ( $self->nModified || 0 ) + ( $result->nModified || 0 ) );
    }

    # Append error and upsert docs, but modify index based on op count
    my $op_count = $self->op_count;
    for my $attr (qw/writeErrors upserted/) {
        for my $doc ( @{ $result->$attr } ) {
            $doc->{index} += $op_count;
        }
        push @{ $self->$attr }, @{ $result->$attr };
    }

    # Merge op and batch counts; this is largely for testing
    $self->_set_op_count( $op_count + $result->op_count );
    $self->_set_batch_count( $self->batch_count + $result->batch_count );

    # Append write concern errors without modification (they have no index)
    push @{ $self->writeConcernErrors }, @{ $result->writeConcernErrors };

    return 1;
}

__PACKAGE__->meta->make_immutable;

1;
