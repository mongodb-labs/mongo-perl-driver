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

package MongoDB::Bulk;

# ABSTRACT: MongoDB bulk write interface

use boolean;
use Moose;
use MongoDB;
use Scalar::Util 'reftype';


has 'collection' => ( 
    is       => 'ro',
    isa      => 'MongoDB::Collection',
    required => 1
);

has 'ordered'  => ( 
    is       => 'ro',
    isa      => 'Bool',
    required => 1,
    default  => 0
);

has '_current_selector' => ( 
   is        => 'rw',
   isa       => 'HashRef',
   default   => sub { { } },
);

has '_executed' => ( 
   is        => 'rw',
   isa       => 'Bool',
   init_arg  => undef,
   default   => 0,
);

has '_inserts' => ( 
    is       => 'rw',
    isa      => 'ArrayRef[HashRef]',
    default  => sub { [ ] },
    traits   => [ 'Array' ]
);

has '_updates' => ( 
    is       => 'rw',
    isa      => 'ArrayRef[HashRef]',
    default  => sub { [ ] },
    traits   => [ 'Array' ]
);

has '_removes' => ( 
    is       => 'rw',
    isa      => 'ArrayRef[HashRef]',
    default  => sub { [ ] },
    traits   => [ 'Array' ]
);


sub find { 
    my ( $self, $selector ) = @_;

    die "find requires a criteria document. Use an empty hashref for no criteria."
      unless ref $selector && reftype $selector eq reftype { };

    $self->_current_selector( { query => $selector, upsert => false } );
}


sub insert { 
    my ( $self, $doc ) = @_;
    $self->_inserts->push( $doc );
    return $self;
}

sub update { 
    my ( $self, $update_doc, $multi ) = @_;

    die "update requires a replacement document."
      unless ref $update_doc && reftype $update_doc eq reftype { };

    $multi ||= false;

    $self->_updates->push( { q      => $self->_current_selector->{query}, 
                             u      => $update_doc,
                             upsert => $self->_current_selector->{upsert},
                             multi  => $multi } );

    return $self;
}

sub update_one { 
    my ( $self, $update_doc ) = @_;

    return $self->update( $update_doc, false );
}

sub upsert { 
    my ( $self ) = @_;

    die "upsert does not take any arguments" if @_ > 1;

    $self->_current_selector->{upsert} = true;
    return $self;
}

sub remove { 
    my ( $self, $limit ) = @_;

    # limit of zero means unlimited
    $limit = defined $limit ? $limit : 0;

    $self->_removes->push( { q     => $self->_current_selector->{query},
                             limit => $limit } );

    return $self;
}

sub remove_one { 
    my ( $self ) = @_;

    return $self->remove( 1 );
}

1;
