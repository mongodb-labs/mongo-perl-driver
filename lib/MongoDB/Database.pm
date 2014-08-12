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
our $VERSION = 'v0.704.4.1';

use MongoDB::CommandResult;
use MongoDB::Error;
use MongoDB::GridFS;
use Carp 'carp';
use boolean;
use Moose;
use namespace::clean -except => 'meta';

has _client => ( 
    is       => 'ro',
    isa      => 'MongoDB::MongoClient',
    required => 1,
);

has name => (
    is       => 'ro',
    isa      => 'Str',
    required => 1,
);


sub collection_names {
    my ($self) = @_;
    my $it = $self->get_collection('system.namespaces')->query({});
    return grep { 
        not ( index( $_, '$' ) >= 0 && index( $_, '.oplog.$' ) < 0 ) 
    } map { 
        substr $_->{name}, length( $self->name ) + 1 
    } $it->all;
}


sub get_collection {
    my ($self, $collection_name) = @_;
    return MongoDB::Collection->new(
        _database => $self,
        name      => $collection_name,
    );
}


sub get_gridfs {
    my ($self, $prefix) = @_;
    $prefix = "fs" unless $prefix;

    return MongoDB::GridFS->new(
        _database => $self,
        prefix => $prefix
    );
}


sub drop {
    my ($self) = @_;
    return $self->run_command({ 'dropDatabase' => 1 });
}


sub last_error {
    my ($self, $options) = @_;

    my $cmd = Tie::IxHash->new("getlasterror" => 1);
    if ($options) {
        $cmd->Push("w", $options->{w})                  if $options->{w};
        $cmd->Push("wtimeout", $options->{wtimeout})    if $options->{wtimeout};
        $cmd->Push("fsync", $options->{fsync})          if $options->{fsync};
        $cmd->Push("j", 1)                              if $options->{j};
    }
                                                        
    return $self->run_command($cmd);
}


sub run_command {
    my ($self, $command) = @_;
    my $obj = $self->get_collection('$cmd')->find_one($command);
    return $obj if $obj->{ok};
    return exists $obj->{errmsg} ? $obj->{errmsg} : $obj->{'$err'};
}

# same as run_command but throws an exception on error; private
# for now until exception handling is overhauled
sub _try_run_command {
    my ($self, $command) = @_;
    my $obj = $self->get_collection('$cmd')->find_one($command);
    return $obj if $obj->{ok};
    MongoDB::DatabaseError->throw(
        message => $obj->{errmsg} || $obj->{'$err'},
        result => MongoDB::CommandResult->new(result => $obj),
    );
}

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

__PACKAGE__->meta->make_immutable;

1;

__END__


=head1 NAME

MongoDB::Database - A Mongo database

=head1 SYNOPSIS

The MongoDB::Database class accesses to a database.

    # accesses the foo database
    my $db = $connection->foo;

You can also access databases with the L<MongoDB::MongoClient/"get_database($name)">
method.

=head1 SEE ALSO

Core documentation on databases: L<http://dochub.mongodb.org/core/databases>.

=head1 ATTRIBUTES

=head2 name

The name of the database.

=head1 METHODS

=head2 collection_names

    my @collections = $database->collection_names;

Returns the list of collections in this database.

=head2 get_collection ($name)

    my $collection = $database->get_collection('foo');

Returns a L<MongoDB::Collection> for the collection called C<$name> within this
database.

=head2 get_gridfs ($prefix?)

    my $grid = $database->get_gridfs;

Returns a L<MongoDB::GridFS> for storing and retrieving files from the database.
Default prefix is "fs", making C<$grid-E<gt>files> "fs.files" and C<$grid-E<gt>chunks>
"fs.chunks".

See L<MongoDB::GridFS> for more information.

=head2 drop

    $database->drop;

Deletes the database.


=head2 last_error($options?)

    my $err = $db->last_error({w => 2});

Finds out if the last database operation completed successfully.  If the last
operation did not complete successfully, returns a hash reference of information
about the error that occurred.

The optional C<$options> parameter is a hash reference that can contain any of
the following:

=over 4

=item w

Guarantees that the previous operation will be replicated to C<w> servers before
this command will return success. See C<MongoDB::MongoClient> for more
information.

=item wtimeout

Milliseconds to wait for C<w> copies of the data to be made.  This parameter
should generally be specified, as the database will otherwise wait forever if
C<w> copies cannot be made.

=item fsync

If true, behaves identically to C<j> if journaling has been turned on for C<mongod>. 

If C<mongod> is not running with journaling, then this option requests that writes be 
immediately C<sync>ed to disk if true.

This option can not be used simultaneously with the C<j> flag.

=item j

If true, the client will block until write operations have been committed to the
server's journal. Prior to MongoDB 2.6, this option was ignored if the server was 
running without journaling. Starting with MongoDB 2.6, write operations will fail 
if this option is used when the server is running without journaling.

=back

C<last_error> returns a hash with fields that vary, depending on what the
previous operation was and if it succeeded or failed.  If the last operation
(before the C<last_error> call) failed, either:

=over 4

=item C<err> will be set or

=item C<errmsg> will be set and C<ok> will be 0.

=back

If C<err> is C<null> and C<ok> is 1, the previous operation succeeded.

The fields in the hash returned can include (but are not limited to):

=over 4

=item C<ok>

This should almost be 1 (unless C<last_error> itself failed).

=item C<err>

If this field is non-null, an error occurred on the previous operation. If this
field is set, it will be a string describing the error that occurred.

=item C<code>

If a database error occurred, the relevant error code will be passed back to the
client.

=item C<errmsg>

This field is set if something goes wrong with a database command.  It is
coupled with C<ok> being 0.  For example, if C<w> is set and times out,
C<errmsg> will be set to "timed out waiting for slaves" and C<ok> will be 0. If
this field is set, it will be a string describing the error that occurred.

=item C<n>

If the last operation was an update, upsert, or a remove, the number of
objects affected will be returned.

=item C<wtimeout>

If the previous option timed out waiting for replication.

=item C<waited>

How long the operation waited before timing out.

=item C<wtime>

If C<w> was set and the operation succeeded, how long it took to replicate to
C<w> servers.

=item C<upserted>

If an upsert occurred, this field will contain the new record's C<_id> field. For
upserts, either this field or C<updatedExisting> will be present (unless an
error occurred).

=item C<updatedExisting>

If an upsert updated an existing element, this field will be C<true>.  For
upserts, either this field or C<upserted> will be present (unless an error
occurred).

=back

See L<MongoDB::MongoClient/w> for more information.

=head2 run_command ($command)

    my $result = $database->run_command({ some_command => 1 });

Runs a database command. Returns a string with the error message if the
command fails. Returns the result of the command (a hash reference) on success.
For a list of possible database commands, run:

    my $commands = $db->run_command({listCommands => 1});

There are a few examples of database commands in the
L<MongoDB::Examples/"DATABASE COMMANDS"> section.

See also core documentation on database commands:
L<http://dochub.mongodb.org/core/commands>.

=head2 eval ($code, $args?, $nolock?)

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



