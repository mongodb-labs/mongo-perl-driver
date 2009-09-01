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
# ABSTRACT: A Mongo Database

use Any::Moose;

has _connection => (
    is       => 'ro',
    isa      => 'MongoDB::Connection',
    required => 1,
    handles  => [qw/query find_one insert update remove ensure_index/],
);

=attr name

The name of the database.

=cut

has name => (
    is       => 'ro',
    isa      => 'Str',
    required => 1,
);

has _collection_class => (
    is       => 'ro',
    isa      => 'Str',
    required => 1,
    default  => 'MongoDB::Collection',
);

sub BUILD {
    my ($self) = @_;
    Any::Moose::load_class($self->_collection_class);
}

around qw/query find_one insert update remove ensure_index/ => sub {
    my ($next, $self, $ns, @args) = @_;
    return $self->$next($self->_query_ns($ns), @args);
};

sub _query_ns {
    my ($self, $ns) = @_;
    my $name = $self->name;
    return qq{${name}.${ns}};
}

=method collection_names

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

=method get_collection ($name)

    my $collection = $database->get_collection('foo');

Returns a C<MongoDB::Collection> for the collection called C<$name> within this
database.

=cut

sub get_collection {
    my ($self, $collection_name) = @_;
    return $self->_collection_class->new(
        _database => $self,
        name      => $collection_name,
    );
}

=method drop

    $database->drop;

Deletes the database.

=cut

sub drop {
    my ($self) = @_;
    return $self->run_command({ dropDatabase => 1 });
}


=method last_error

    my $err = $db->last_error;

Queries the database to check if the last operation caused an error.

=cut

sub last_error {
    my ($self) = @_;
    return $self->run_command({
        "getlasterror" => 1
    });
}


=method run_command ($command)

    my $result = $database->run_command({ some_command => 1 });

Runs a command for this database on the mongo server. Throws an exception with
an error message if the command fails. Returns the result of the command on
success.

=cut

sub run_command {
    my ($self, $command) = @_;
    my $obj = $self->find_one('$cmd', $command);
    return $obj if $obj->{ok};
    $obj->{'errmsg'};
}

no Any::Moose;
__PACKAGE__->meta->make_immutable;

1;
