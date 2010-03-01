#
#  Copyright 2009 10gen, Inc.
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
our $VERSION = '0.29';

# ABSTRACT: A Mongo Database

use Any::Moose;
use MongoDB::GridFS;

has _connection => (
    is       => 'ro',
    isa      => 'MongoDB::Connection',
    required => 1,
    handles  => [qw/query find_one insert update remove ensure_index batch_insert/],
);

=head1 NAME

MongoDB::Database - A Mongo Database

=head1 SEE ALSO

Core documentation on databases: L<http://dochub.mongodb.org/core/databases>.

=head1 ATTRIBUTES

=head2 name

The name of the database.

=cut

has name => (
    is       => 'ro',
    isa      => 'Str',
    required => 1,
);


sub BUILD {
    my ($self) = @_;
    Any::Moose::load_class("MongoDB::Collection");
}

around qw/query find_one insert update remove ensure_index batch_insert/ => sub {
    my ($next, $self, $ns, @args) = @_;
    $self->_connection->_last_error(undef);
    return $self->$next($self->_query_ns($ns), @args);
};

sub _query_ns {
    my ($self, $ns) = @_;
    my $name = $self->name;
    return qq{${name}.${ns}};
}

=head1 METHODS

=head2 collection_names

    my @collections = $database->collection_names;

Returns the list of collections in this database.

=cut

sub collection_names {
    my ($self) = @_;
    my $it = $self->query('system.namespaces', {});
    return map {
        substr($_, length($self->name) + 1)
    } map { $_->{name} } $it->all;
}

=head2 get_collection ($name)

    my $collection = $database->get_collection('foo');

Returns a C<MongoDB::Collection> for the collection called C<$name> within this
database.

=cut

sub get_collection {
    my ($self, $collection_name) = @_;
    return MongoDB::Collection->new(
        _database => $self,
        name      => $collection_name,
    );
}

=head2 get_gridfs ($prefix?)

    my $grid = $database->get_gridfs;

Returns a C<MongoDB::GridFS> for storing and retrieving files from the database.
Default prefix is "fs", making C<$grid->files> "fs.files" and C<$grid->chunks>
"fs.chunks".

=cut

sub get_gridfs {
    my ($self, $prefix) = @_;
    $prefix = "fs" unless $prefix;

    my $files = $self->get_collection("${prefix}.files");
    my $chunks = $self->get_collection("${prefix}.chunks");

    return MongoDB::GridFS->new(
        _database => $self,
        files => $files,
        chunks => $chunks,
    );
}

=head2 drop

    $database->drop;

Deletes the database.

=cut

sub drop {
    my ($self) = @_;
    return $self->run_command({ 'dropDatabase' => 1 });
}


=head2 last_error

    my $err = $db->last_error;

Finds out if the last database operation completed successfully.  If the last
operation did not complete successfully, returns a hash reference of information
about the error that occured.

=cut

sub last_error {
    my ($self) = @_;

    if ($self->_connection->_last_error) {
        return $self->_connection->_last_error;
    }

    return $self->run_command({"getlasterror" => 1});
}


=head2 run_command ($command)

    my $result = $database->run_command({ some_command => 1 });

Runs a command for this database on the mongo server. Throws an exception with
an error message if the command fails. Returns the result of the command on
success.  For a list of possible database commands, see 
L<http://www.mongodb.org/display/DOCS/Table+of+Database+Commands>.

See also core documentation on database commands: 
L<http://dochub.mongodb.org/core/commands>.

=cut

sub run_command {
    my ($self, $command) = @_;
    my $obj = $self->find_one('$cmd', $command);
    return $obj if $obj->{ok};
    $obj->{'errmsg'};
}


=head2 eval ($code, $args?)

    my $result = $database->eval('function(x) { return "hello, "+x; }', ["world"]);

Evaluate a JavaScript expression on the Mongo server. 

Useful if you need to touch a lot of data lightly; in such a scenario 
the network transfer of the data could be a bottleneck. The $code 
argument must be a JavaScript function. $args is an array of 
parameters that will be passed to the function.  For more examples of using eval
see L<http://www.mongodb.org/display/DOCS/Server-side+Code+Execution#Server-sideCodeExecution-Using{{db.eval%28%29}}>.

=cut

sub eval {
    my ($self, $code, $args) = @_;

    my $cmd = tie(my %hash, 'Tie::IxHash');
    %hash = ('$eval' => $code,
             'args' => $args);

    my $result = $self->run_command($cmd);
    if (ref $result eq 'HASH' && exists $result->{'retval'}) {
        return $result->{'retval'};
    }
    else {
        return $result;
    }
}

no Any::Moose;
__PACKAGE__->meta->make_immutable;

1;

=head1 AUTHOR

  Kristina Chodorow <kristina@mongodb.org>
