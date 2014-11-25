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

package MongoDB::Database;


# ABSTRACT: A MongoDB Database

use version;
our $VERSION = 'v0.999.998.2'; # TRIAL

use MongoDB::CommandResult;
use MongoDB::Error;
use MongoDB::GridFS;
use MongoDB::_Query;
use Carp 'carp';
use boolean;
use Moose;
use Try::Tiny;
use namespace::clean -except => 'meta';

has _client => ( 
    is       => 'ro',
    isa      => 'MongoDB::MongoClient',
    required => 1,
);

=attr name

The name of the database.

=cut

has name => (
    is       => 'ro',
    isa      => 'Str',
    required => 1,
);

=attr write_concern

A L<MongoDB::WriteConcern> object.  It may be initialized with a hash
reference that will be coerced into a new MongoDB::WriteConcern object.

=cut

has write_concern => (
    is       => 'ro',
    isa      => 'WriteConcern',
    required => 1,
    coerce   => 1,
);

#--------------------------------------------------------------------------#
# methods
#--------------------------------------------------------------------------#

=method collection_names

    my @collections = $database->collection_names;

Returns the list of collections in this database.

=cut

sub collection_names {
    my ($self)   = @_;
    my $db_name  = $self->name;
    my $variants = {
        0 => sub {
            my ( $client, $link ) = @_;
            my $ns = "$db_name.system.namespaces";
            my $query = MongoDB::_Query->new( spec => {} );
            my $result =
              $client->_try_operation('_send_query', $link, $ns, $query->spec, undef, 0, 0, 0, undef, $client );
            return grep { not( index( $_, '$' ) >= 0 && index( $_, '.oplog.$' ) < 0 ) }
              map { substr $_->{name}, length($db_name) + 1 } $result->all;
        },
        3 => sub {
            my ( $client, $link ) = @_;
            my $cmd = Tie::IxHash->new( listCollections => 1 );
            my $result = $client->_try_operation(
                '_send_command', $link, { db => $db_name, command => MongoDB::_Query->new(spec => $cmd)}
            );
            return map { $_->{name} } @{ $result->result->{collections} };
        },
    };
    return $self->_client->send_versioned_read($variants);
}

=method get_collection

    my $collection = $database->get_collection('foo');
    my $collection = $database->get_collection('foo', $options);

Returns a L<MongoDB::Collection> for the collection called C<$name> within this
database.

It takes an optional hash reference of options that are passed to the
L<MongoDB::Collection> constructor.

=cut

sub get_collection {
    my ($self, $collection_name, $options) = @_;
    return MongoDB::Collection->new(
        write_concern => $self->write_concern,
        ( $options ? %$options : () ),
        # not allowed to be overridden by options
        _database => $self,
        name      => $collection_name,
    );
}

=method get_gridfs ($prefix?)

    my $grid = $database->get_gridfs;

Returns a L<MongoDB::GridFS> for storing and retrieving files from the database.
Default prefix is "fs", making C<$grid-E<gt>files> "fs.files" and C<$grid-E<gt>chunks>
"fs.chunks".

See L<MongoDB::GridFS> for more information.

=cut

sub get_gridfs {
    my ($self, $prefix) = @_;
    $prefix = "fs" unless $prefix;

    return MongoDB::GridFS->new(
        _database => $self,
        prefix => $prefix
    );
}

=method drop

    $database->drop;

Deletes the database.

=cut

sub drop {
    my ($self) = @_;
    return $self->run_command({ 'dropDatabase' => 1 });
}

=method run_command

    my $result = $database->run_command([ some_command => 1 ]);

    my $result = $database->run_command(
        [ some_command => 1 ],
        { mode => 'secondaryPreferred' }
    );

This method runs a database command.  The first argument must be a document
with the command and its arguments.  It should be given as an array reference
of key-value pairs or a L<Tie::IxHash> object with the command name as the
first key.  The use of a hash reference will only reliably work for commands
without additional parameters.

By default, commands are run with a read preference of 'primary'.  An optional
second argument may specify an alternative read preference.  If given, it must
be a L<MongoDB::ReadPreference> object or a hash reference that can be used to
construct one.

It returns the result of the command (a hash reference) on success or throws a
L<MongoDB::DatabaseError|MongoDB::Error/MongoDB::DatabaseError> exception if
the command fails.

For a list of possible database commands, run:

    my $commands = $db->run_command([listCommands => 1]);

There are a few examples of database commands in the
L<MongoDB::Examples/"DATABASE COMMANDS"> section.  See also core documentation
on database commands: L<http://dochub.mongodb.org/core/commands>.

=cut

sub run_command {
    my ($self, $command, $read_pref) = @_;

    if ( $read_pref && ref($read_pref) eq 'HASH' ) {
        $read_pref = MongoDB::ReadPreference->new($read_pref);
    }

    my $obj = $self->_client->send_command( $self->name, $command, $read_pref );

    return $obj->result;
}

=method eval ($code, $args?, $nolock?)

    my $result = $database->eval('function(x) { return "hello, "+x; }', ["world"]);

Evaluate a JavaScript expression on the Mongo server. The C<$code> argument can
be a string or an instance of L<MongoDB::Code>.  The C<$args> are an optional
array of arguments to be passed to the C<$code> function.  C<$nolock> (default
C<false>) prevents the eval command from taking the global write lock before
evaluating the JavaScript.

C<eval> is useful if you need to touch a lot of data lightly; in such a scenario
the network transfer of the data could be a bottleneck. The C<$code> argument
must be a JavaScript function. C<$args> is an array of parameters that will be
passed to the function.  C<$nolock> is a L<boolean> value.  For more examples of
using eval see
L<http://www.mongodb.org/display/DOCS/Server-side+Code+Execution#Server-sideCodeExecution-Using{{db.eval%28%29}}>.

=cut

sub eval {
    my ($self, $code, $args, $nolock) = @_;

    $nolock = boolean::false unless defined $nolock;

    my $cmd = tie(my %hash, 'Tie::IxHash');
    %hash = ('$eval' => $code,
             'args' => $args,
             'nolock' => $nolock);

    my $result = $self->run_command($cmd);
    if (ref $result eq 'HASH' && exists $result->{'retval'}) {
        return $result->{'retval'};
    }
    else {
        return $result;
    }
}

=method last_error (DEPRECATED)

    my $err = $db->last_error({w => 2});

Because write operations now return result information, this function is
deprecated.

Finds out if the last database operation completed successfully. If a hash
reference of options is provided, they are included with the database command.
Throws an exception if C<getLastError> itself fails.

See the
L<getLastError|http://docs.mongodb.org/manual/reference/command/getLastError/>
documentation for more on valid options and results.

=cut

sub last_error {
    my ( $self, $opt ) = @_;
    return $self->run_command( [ getlasterror => 1, ( $opt ? %$opt : () ) ] );
}

__PACKAGE__->meta->make_immutable;

1;

__END__

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

=cut
