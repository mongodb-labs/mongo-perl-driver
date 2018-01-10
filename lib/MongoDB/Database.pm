#
#  Copyright 2009-2013 MongoDB, Inc.
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
package MongoDB::Database;


# ABSTRACT: A MongoDB Database

use version;
our $VERSION = 'v1.999.0';

use MongoDB::CommandResult;
use MongoDB::Error;
use MongoDB::GridFS;
use MongoDB::GridFSBucket;
use MongoDB::Op::_Command;
use MongoDB::Op::_DropDatabase;
use MongoDB::Op::_ListCollections;
use MongoDB::ReadPreference;
use MongoDB::_Types qw(
    BSONCodec
    NonNegNum
    ReadPreference
    ReadConcern
    WriteConcern
    is_OrderedDoc
);
use Types::Standard qw(
    InstanceOf
    Str
);
use Carp 'carp';
use boolean;
use Moo;
use Try::Tiny;
use namespace::clean -except => 'meta';

has _client => (
    is       => 'ro',
    isa      => InstanceOf['MongoDB::MongoClient'],
    required => 1,
);

=attr name

The name of the database.

=cut

has name => (
    is       => 'ro',
    isa      => Str,
    required => 1,
);

=attr read_preference

A L<MongoDB::ReadPreference> object.  It may be initialized with a string
corresponding to one of the valid read preference modes or a hash reference
that will be coerced into a new MongoDB::ReadPreference object.
By default it will be inherited from a L<MongoDB::MongoClient> object.

=cut

has read_preference => (
    is       => 'ro',
    isa      => ReadPreference,
    required => 1,
    coerce   => ReadPreference->coercion,
);

=attr write_concern

A L<MongoDB::WriteConcern> object.  It may be initialized with a hash
reference that will be coerced into a new MongoDB::WriteConcern object.
By default it will be inherited from a L<MongoDB::MongoClient> object.

=cut

has write_concern => (
    is       => 'ro',
    isa      => WriteConcern,
    required => 1,
    coerce   => WriteConcern->coercion,
);

=attr read_concern

A L<MongoDB::ReadConcern> object.  May be initialized with a hash
reference or a string that will be coerced into the level of read
concern.

By default it will be inherited from a L<MongoDB::MongoClient> object.

=cut

has read_concern => (
    is       => 'ro',
    isa      => ReadConcern,
    required => 1,
    coerce   => ReadConcern->coercion,
);

=attr max_time_ms

Specifies the maximum amount of time in milliseconds that the server should use
for working on a query.

B<Note>: this will only be used for server versions 2.6 or greater, as that
was when the C<$maxTimeMS> meta-operator was introduced.

=cut

has max_time_ms => (
    is      => 'ro',
    isa     => NonNegNum,
    required => 1,
);

=attr bson_codec

An object that provides the C<encode_one> and C<decode_one> methods, such as
from L<MongoDB::BSON>.  It may be initialized with a hash reference that will
be coerced into a new MongoDB::BSON object.  By default it will be inherited
from a L<MongoDB::MongoClient> object.

=cut

has bson_codec => (
    is       => 'ro',
    isa      => BSONCodec,
    coerce   => BSONCodec->coercion,
    required => 1,
);

with $_ for qw(
  MongoDB::Role::_DeprecationWarner
);

#--------------------------------------------------------------------------#
# methods
#--------------------------------------------------------------------------#

=method list_collections

    $result = $coll->list_collections( $filter );
    $result = $coll->list_collections( $filter, $options );

Returns a L<MongoDB::QueryResult> object to iterate over collection description
documents.  These will contain C<name> and C<options> keys like so:

    use boolean;

    {
        name => "my_capped_collection",
        options => {
            capped => true,
            size => 10485760,
        }
    },

An optional filter document may be provided, which cause only collection
description documents matching a filter expression to be returned.  See the
L<listCollections command
documentation|http://docs.mongodb.org/manual/reference/command/listCollections/>
for more details on filtering for specific collections.

A hash reference of options may be provided. Valid keys include:

=for :list
* C<batchSize> – the number of documents to return per batch.
* C<maxTimeMS> – the maximum amount of time in milliseconds to allow the
  command to run.  (Note, this will be ignored for servers before version 2.6.)
* C<session> - the session to use for these operations. If not supplied, will
  use an implicit session. For more information see L<MongoDB::ClientSession>

=cut

my $list_collections_args;

sub list_collections {
    my ( $self, $filter, $options ) = @_;
    $filter  ||= {};
    $options ||= {};

    # possibly fallback to default maxTimeMS
    if ( ! exists $options->{maxTimeMS} && $self->max_time_ms ) {
        $options->{maxTimeMS} = $self->max_time_ms;
    }

    my $session = $self->_get_session_from_hashref( $options );

    my $op = MongoDB::Op::_ListCollections->_new(
        db_name    => $self->name,
        client     => $self->_client,
        bson_codec => $self->bson_codec,
        filter     => $filter,
        options    => $options,
        session    => $session,
    );

    return $self->_client->send_primary_op($op);
}

=method collection_names

    my @collections = $database->collection_names;
    my @collections = $database->collection_names( $filter );

Returns the list of collections in this database.

An optional filter document may be provided, which cause only collection
description documents matching a filter expression to be returned.  See the
L<listCollections command
documentation|http://docs.mongodb.org/manual/reference/command/listCollections/>
for more details on filtering for specific collections.

A hashref of options may also be provided.

Valid options include:

=for :list
* C<session> - the session to use for these operations. If not supplied, will
  use an implicit session. For more information see L<MongoDB::ClientSession>

B<Warning:> if the number of collections is very large, this may return
a very large result.  Either pass an appropriate filter, or use
L</list_collections> to iterate over collections instead.

=cut

sub collection_names {
    my $self = shift;

    my $res = $self->list_collections( @_ );

    return map { $_->{name} } $res->all;
}

=method get_collection, coll

    my $collection = $database->get_collection('foo');
    my $collection = $database->get_collection('foo', $options);
    my $collection = $database->coll('foo', $options);

Returns a L<MongoDB::Collection> for the given collection name within this
database.

It takes an optional hash reference of options that are passed to the
L<MongoDB::Collection> constructor.

The C<coll> method is an alias for C<get_collection>.

=cut

sub get_collection {
    my ( $self, $collection_name, $options ) = @_;
    return MongoDB::Collection->new(
        read_preference => $self->read_preference,
        write_concern   => $self->write_concern,
        read_concern    => $self->read_concern,
        bson_codec      => $self->bson_codec,
        max_time_ms     => $self->max_time_ms,
        ( $options ? %$options : () ),
        # not allowed to be overridden by options
        database => $self,
        name     => $collection_name,
    );
}

{ no warnings 'once'; *coll = \&get_collection }

=method get_gridfsbucket, gfs

    my $grid = $database->get_gridfsbucket;
    my $grid = $database->get_gridfsbucket($options);
    my $grid = $database->gfs($options);

This method returns a L<MongoDB::GridFSBucket> object for storing and
retrieving files from the database.

It takes an optional hash reference of options that are passed to the
L<MongoDB::GridFSBucket> constructor.

See L<MongoDB::GridFSBucket> for more information.

The C<gfs> method is an alias for C<get_gridfsbucket>.

=cut

sub get_gridfsbucket {
    my ($self, $options) = @_;

    return MongoDB::GridFSBucket->new(
        read_preference => $self->read_preference,
        write_concern   => $self->write_concern,
        read_concern    => $self->read_concern,
        bson_codec      => $self->bson_codec,
        max_time_ms     => $self->max_time_ms,
        ( $options ? %$options : () ),
        # not allowed to be overridden by options
        database => $self,
    )
}

{ no warnings 'once'; *gfs = \&get_gridfsbucket }

=method get_gridfs (DEPRECATED)

    my $grid = $database->get_gridfs;
    my $grid = $database->get_gridfs("fs");
    my $grid = $database->get_gridfs("fs", $options);

The L<MongoDB::GridFS> class has been deprecated in favor of the new MongoDB
driver-wide standard GridFS API, available via L<MongoDB::GridFSBucket> and
the C<get_gridfsbucket>/C<gfs> methods.

This method returns a L<MongoDB::GridFS> for storing and retrieving files
from the database.  Default prefix is "fs", making C<$grid-E<gt>files>
"fs.files" and C<$grid-E<gt>chunks> "fs.chunks".

It takes an optional hash reference of options that are passed to the
L<MongoDB::GridFS> constructor.

See L<MongoDB::GridFS> for more information.

=cut

sub get_gridfs {
    my ($self, $prefix, $options) = @_;
    $prefix = "fs" unless $prefix;

    $self->_warn_deprecated( 'get_gridfs' => [qw/get_gridfsbucket gfs/] );

    return MongoDB::GridFS->new(
        read_preference => $self->read_preference,
        write_concern   => $self->write_concern,
        max_time_ms     => $self->max_time_ms,
        bson_codec      => $self->bson_codec,
        ( $options ? %$options : () ),
        # not allowed to be overridden by options
        _database => $self,
        prefix => $prefix
    );
}

=method drop

    $database->drop;

Deletes the database.

A hashref of options may also be provided.

Valid options include:

=for :list
* C<session> - the session to use for these operations. If not supplied, will
  use an implicit session. For more information see L<MongoDB::ClientSession>

=cut

sub drop {
    my ( $self, $options ) = @_;

    my $session = $self->_get_session_from_hashref( $options );

    return $self->_client->send_write_op(
        MongoDB::Op::_DropDatabase->_new(
            client        => $self->_client,
            db_name       => $self->name,
            bson_codec    => $self->bson_codec,
            write_concern => $self->write_concern,
            session       => $session,
        )
    )->output;
}

=method run_command

    my $output = $database->run_command([ some_command => 1 ]);

    my $output = $database->run_command(
        [ some_command => 1 ],
        { mode => 'secondaryPreferred' }
    );

    my $output = $database->run_command(
        [ some_command => 1 ],
        $read_preference,
        $options
    );

This method runs a database command.  The first argument must be a document
with the command and its arguments.  It should be given as an array reference
of key-value pairs or a L<Tie::IxHash> object with the command name as the
first key.  An error will be thrown if the command is not an
L<ordered document|MongoDB::Collection/Ordered document>.

By default, commands are run with a read preference of 'primary'.  An optional
second argument may specify an alternative read preference.  If given, it must
be a L<MongoDB::ReadPreference> object or a hash reference that can be used to
construct one.

A hashref of options may also be provided.

Valid options include:

=for :list
* C<session> - the session to use for these operations. If not supplied, will
  use an implicit session. For more information see L<MongoDB::ClientSession>

It returns the output of the command (a hash reference) on success or throws a
L<MongoDB::DatabaseError|MongoDB::Error/MongoDB::DatabaseError> exception if
the command fails.

For a list of possible database commands, run:

    my $commands = $db->run_command([listCommands => 1]);

There are a few examples of database commands in the
L<MongoDB::Examples/"DATABASE COMMANDS"> section.  See also core documentation
on database commands: L<http://dochub.mongodb.org/core/commands>.

=cut

sub run_command {
    my ( $self, $command, $read_pref, $options ) = @_;
    MongoDB::UsageError->throw("command was not an ordered document")
       if ! is_OrderedDoc($command);

    $read_pref = MongoDB::ReadPreference->new(
        ref($read_pref) ? $read_pref : ( mode => $read_pref ) )
      if $read_pref && ref($read_pref) ne 'MongoDB::ReadPreference';

    my $session = $self->_get_session_from_hashref( $options );

    my $op = MongoDB::Op::_Command->_new(
        client      => $self->_client,
        db_name     => $self->name,
        query       => $command,
        query_flags => {},
        bson_codec  => $self->bson_codec,
        read_preference => $read_pref,
        session     => $session,
    );

    my $obj = $self->_client->send_read_op($op);

    return $obj->output;
}

sub _aggregate {
    MongoDB::UsageError->throw("pipeline argument must be an array reference")
      unless ref( $_[1] ) eq 'ARRAY';

    my ( $self, $pipeline, $options ) = @_;
    $options ||= {};

    my $session = $self->_get_session_from_hashref( $options );

    # boolify some options
    for my $k (qw/allowDiskUse explain/) {
        $options->{$k} = ( $options->{$k} ? true : false ) if exists $options->{$k};
    }

    # possibly fallback to default maxTimeMS
    if ( ! exists $options->{maxTimeMS} && $self->max_time_ms ) {
        $options->{maxTimeMS} = $self->max_time_ms;
    }

    # read preferences are ignored if the last stage is $out
    my ($last_op) = keys %{ $pipeline->[-1] };

    my $op = MongoDB::Op::_Aggregate->_new(
        pipeline        => $pipeline,
        options         => $options,
        read_concern    => $self->read_concern,
        has_out         => $last_op eq '$out',
        client          => $self->_client,
        bson_codec      => $self->bson_codec,
        db_name         => $self->name,
        coll_name       => 1,                     # Magic not-an-actual-collection number
        full_name       => $self->name . ".1",
        read_preference => $self->read_preference,
        write_concern   => $self->write_concern,
        session         => $session,
    );

    return $self->_client->send_read_op($op);
}

# Extracts a session from a provided hashref, or returns an implicit session
# Almost identical to same subroutine in Collection, however in Database the
# client attribute is private. 
sub _get_session_from_hashref {
    my ( $self, $hashref ) = @_;

    my $session = delete $hashref->{session};

    if ( defined $session ) {
        MongoDB::UsageError->throw( "Cannot use session from another client" )
            if ( $session->client->_id ne $self->_client->_id );
        MongoDB::UsageError->throw( "Cannot use session which has ended" )
            if $session->_has_ended;
    } else {
        $session = $self->_client->_maybe_get_implicit_session;
    }

    return $session;
}

#--------------------------------------------------------------------------#
# deprecated methods
#--------------------------------------------------------------------------#

sub eval {
    my ($self, $code, $args, $nolock) = @_;

    $self->_warn_deprecated( 'eval', "Run manually via run_command instead." );

    $nolock = boolean::false unless defined $nolock;

    my $cmd = tie(my %hash, 'Tie::IxHash');
    %hash = ('$eval' => $code,
             'args' => $args,
             'nolock' => $nolock);

    my $output = $self->run_command($cmd);
    if (ref $output eq 'HASH' && exists $output->{'retval'}) {
        return $output->{'retval'};
    }
    else {
        return $output;
    }
}

sub last_error {
    my ( $self, $opt ) = @_;

    $self->_warn_deprecated(
        'last_error' => "Use a write concern or manually run getlasterror with run_command." );

    return $self->run_command( [ getlasterror => 1, ( $opt ? %$opt : () ) ] );
}


1;

__END__

=for Pod::Coverage
last_error

=head1 SYNOPSIS

    # get a Database object via MongoDB::MongoClient
    my $db   = $client->get_database("foo");

    # get a Collection via the Database object
    my $coll = $db->get_collection("people");

    # run a command on a database
    my $res = $db->run_command([ismaster => 1]);

=head1 DESCRIPTION

This class models a MongoDB database.  Use it to construct
L<MongoDB::Collection> objects. It also provides the L</run_command> method and
some convenience methods that use it.

Generally, you never construct one of these directly with C<new>.  Instead, you
call C<get_database> on a L<MongoDB::MongoClient> object.

=head1 USAGE

=head2 Error handling

Unless otherwise explicitly documented, all methods throw exceptions if
an error occurs.  The error types are documented in L<MongoDB::Error>.

To catch and handle errors, the L<Try::Tiny> and L<Safe::Isa> modules
are recommended:

    use Try::Tiny;
    use Safe::Isa; # provides $_isa

    try {
        $db->run_command( @command )
    }
    catch {
        if ( $_->$_isa("MongoDB::DuplicateKeyError" ) {
            ...
        }
        else {
            ...
        }
    };

To retry failures automatically, consider using L<Try::Tiny::Retry>.

=head1 DEPRECATIONS

The methods still exist, but are no longer documented.  In a future version
they will warn when used, then will eventually be removed.

=for :list
* last_error

=cut
