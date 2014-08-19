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
our $VERSION = 'v0.704.5.1';

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

# This should always be initialized either as a number or as undef so that
# merges accumulate correctly.  It should be undef if talking to a server < 2.6
# or if talking to a mongos and not getting the field back from an update.
# The default is undef, which will be sticky and ensure this field stays undef.

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

sub _parse {
    my $class = shift;
    my $args = ref $_[0] eq 'HASH' ? shift : {@_};

    unless ( 2 == grep { exists $args->{$_} } qw/op result/ ) {
        confess "parse requires 'op' and 'result' arguments";
    }

    my ( $op, $op_count, $batch_count, $result ) =
      @{$args}{qw/op op_count batch_count result/};

    confess "op argument to parse must be one of: @op_map_keys"
      unless $op eq any(@op_map_keys);
    confess "results argument to parse must be a hash reference"
      unless ref $result eq 'HASH';

    my $attrs = {
        batch_count => $batch_count || 1,
        $op_count ? ( op_count => $op_count ) : ()
    };

    $attrs->{writeErrors} = $result->{writeErrors} if $result->{writeErrors};

    # rename writeConcernError -> writeConcernErrors; coercion will make it
    # into an array later $attrs->{writeConcernErrors} =
    # $result->{writeConcernError}

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

    # for an update/upsert we want the exact response whether numeric or undef so that
    # new undef responses become sticky; for all other updates, we consider it 0
    # and let it get sorted out in the merging
    $attrs->{nModified} =
      ( $op eq 'update' || $op eq 'upsert' ) ? $result->{nModified} : 0;

    return $class->new($attrs);
}

=method count_writeErrors

Returns the number of write errors

=cut

sub count_writeErrors {
    my ($self) = @_;
    return scalar @{ $self->writeErrors };
}

=method count_writeConcernErrors

Returns the number of write errors

=cut

sub count_writeConcernErrors {
    my ($self) = @_;
    return scalar @{ $self->writeConcernErrors };
}

=method last_errmsg

Returns the last C<errmsg> field from either the list of C<writeErrors> or
C<writeConcernErrors> or the empty string if there are no errors.

=cut

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

sub _merge_result {
    my ( $self, $result ) = @_;

    # Add simple counters
    for my $attr (qw/nInserted nUpserted nMatched nRemoved/) {
        my $setter = "_set_$attr";
        $self->$setter( $self->$attr + $result->$attr );
    }

    # If nModified is defined in both results we're merging, then we're talking
    # to a 2.6+ mongod or we're talking to a 2.6+ mongos and have only seen
    # responses with nModified.  In any other case, we set nModified to undef,
    # which then becomes "sticky"
    if ( defined $self->nModified && defined $result->nModified ) {
        $self->_set_nModified( $self->nModified + $result->nModified );
    }
    else {
        $self->_set_nModified(undef);
    }

    # Append error and upsert docs, but modify index based on op count
    my $op_count = $self->op_count;
    for my $attr (qw/writeErrors upserted/) {
        for my $doc ( @{ $result->$attr } ) {
            $doc->{index} += $op_count;
        }
        push @{ $self->$attr }, @{ $result->$attr };
    }

    # Append write concern errors without modification (they have no index)
    push @{ $self->writeConcernErrors }, @{ $result->writeConcernErrors };

    $self->_set_op_count( $op_count + $result->op_count );
    $self->_set_batch_count( $self->batch_count + $result->batch_count );

    return 1;
}

__PACKAGE__->meta->make_immutable;

1;

__END__

=head1 SYNOPSIS

    # returned directly
    my $result = $bulk->execute;

    # from a WriteError or WriteConcernError
    my $result = $error->result;

=head1 DESCRIPTION

This class encapsulates the results from a bulk write operation. It may be
returned directly from C<execute> or it may be in the C<result> attribute of a
C<MongoDB::DatabaseError> subclass like C<MongoDB::WriteError> or
C<MongoDB::WriteConcernError>.

=attr nInserted

Number of documents inserted

=attr nUpserted

Number of documents upserted

=attr nMatched

Number of documents matched for an update or replace operation.

=attr nRemoved

Number of documents removed

=attr nModified

Number of documents actually modified by an update operation. This
is not necessarily the same as L</nMatched> if the document was
not actually modified as a result of the update.

This field is not available from legacy servers before version 2.6.
If results are seen from a legacy server (or from a mongos proxying
for a legacy server) this attribute will be C<undef>.

=attr upserted

An array reference containing information about upserted documetns (if any).
Each document will have the following fields:

=for :list
* index — 0-based index indicating which operation failed
* _id — the object ID of the upserted document

=attr writeErrors

An array reference containing write errors (if any).  Each error document
will have the following fields:

=for :list
* index — 0-based index indicating which operation failed
* code — numeric error code
* errmsg — textual error string
* op — a representation of the actual operation sent to the server

=attr writeConcernErrors

An array reference containing write concern errors (if any).  Each error
document will have the following fields:

=for :list
* index — 0-based index indicating which operation failed
* code — numeric error code

=attr op_count

The number of operations sent to the database.

=attr batch_count

The number of database commands issued to the server.  This will be less than the
C<op_count> if multiple operations were grouped together.

=cut
