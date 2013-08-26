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

package MongoDB::DBRef;

# ABSTRACT: Native DBRef support

use Moose;
use Moose::Util::TypeConstraints;
use Carp 'croak';
use Tie::IxHash;


subtype DBRefColl => as 'Str';
subtype DBRefDB   => as 'Str';

coerce 'DBRefColl'
  => from 'MongoDB::Collection'
  => via  { $_->name };

coerce 'DBRefDB' 
  => from 'MongoDB::Database'
  => via  { $_->name };


# no type constraint since an _id can be anything
has id => (
    is        => 'rw',
    required  => 1 
);

has ref => (
    is        => 'rw',
    isa       => 'DBRefColl',
    required  => 1,
    coerce    => 1,
);

has db => ( 
    is        => 'rw',
    isa       => 'DBRefDB',
    required  => 1,
    coerce    => 1,
);

has client => (
    is        => 'rw',
    isa       => 'MongoDB::MongoClient',
    required  => 0
);

has verify_db => (
    is        => 'rw',
    isa       => 'Bool',
    required  => 0,
    default   => 1
);

has verify_coll => ( 
    is        => 'rw',
    isa       => 'Bool',
    required  => 0,
    default   => 1
);


sub fetch { 
    my $self = shift;

    my $client = $self->client;

    croak "Can't fetch DBRef without a MongoClient. Specify a 'client' attribute."
      unless $client;

    my $db     = $self->db;

    if ( $self->verify_db ) { 
        croak sprintf "No such database %s", $db
          unless grep { $_ eq $db } $client->database_names;
    }

    my $ref    = $self->ref;

    if ( $self->verify_coll ) { 
        croak sprintf "No such collection %s", $ref
          unless grep { $_ eq $ref } $client->get_database( $db )->collection_names;
    }

    my $id     = $self->id;

    return $client->get_database( $db )->get_collection( $ref )->find_one( { _id => $id } );
}

sub _ordered {
    my $self = shift;
    
    return Tie::IxHash->new( '$ref' => $self->ref, '$id' => $self->id, '$db' => $self->db );
}

1;


__END__

=head1 NAME

MongoDB::DBRef - A MongoDB database reference

=head1 SYNOPSIS

    my $dbref = MongoDB::DBRef->new( db => 'my_db', ref => 'my_collection', id => 123 );
    $coll->insert( { foo => 'bar', other_doc => $dbref } );

    my $other_doc = $coll->find_one( { foo => 'bar' } )->{other_doc}->fetch;

=head1 DESCRIPTION

This module provides support for database references (DBRefs) in the Perl 
MongoDB driver. A DBRef is a special embedded document which points to 
another document in the database. DBRefs are not the same as foreign keys
and do not provide any referential integrity or constraint checking. For example,
a DBRef may point to a document that no longer exists (or never existed.)

=head1 ATTRIBUTES

=head2 db

Required. The database in which the referenced document lives. Either a L<MongoDB::Database>
object or a string containing the database name. The object will be coerced to string form.

=head2 ref

Required. The collection in which the referenced document lives. Either a L<MongoDB::Collection>
object or a string containing the collection name. The object will be coerced to string form.

=head2 id

Required. The C<_id> value of the referenced document. If the 
C<_id> is an ObjectID, then you must use a L<MongoDB::OID> object.

=head2 client

Optional. A L<MongoDB::MongoClient> object to be used to fetch the referenced document
from the database. You must supply this attribute if you want to use the C<fetch> method.

When you retrieve a document from MongoDB, any DBRefs will automatically be inflated
into C<MongoDB::DBRef> objects with the C<client> attribute automatically populated.

It is not necessary to specify a C<client> if you are just making DBRefs to insert
in the database as part of a larger document. 

=head2 verify_db

Optional. Check that the referenced database exists before trying to fetch a document
from it. The default is C<1>. Set to C<0> to disable checking.

=head2 verify_coll

Optional. Check that the referenced collection exists before trying to fetch a document
from it. The default is C<1>. Set to C<0> to disable checking.

=head1 METHODS

=head2 fetch

Retrieve the referenced document from the database. If the document does not exist, you'll
get back C<undef>. You must populate the C<client> attribute of the DBRef object in order
to use this method. If there's no C<client>, C<fetch> will throw a fatal error.

If C<verify_db> or <verify_coll> are on, C<fetch> will throw a fatal error if the 
database or collection concerned does not exist.

