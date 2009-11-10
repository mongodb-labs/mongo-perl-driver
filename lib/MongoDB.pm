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

use strict;
use warnings;

package MongoDB;
# ABSTRACT: A Mongo Driver for Perl

our $VERSION = '0.26';


use XSLoader;
use MongoDB::Connection;

XSLoader::load(__PACKAGE__, $VERSION);

1;

=head1 NAME

MongoDB - A Mongo Driver for Perl

=head1 VERSION

version 0.26

=head1 SYNOPSIS

    use MongoDB;

    my $connection = MongoDB::Connection->new(host => 'localhost', port => 27017);
    my $database   = $connection->get_database('foo');
    my $collection = $database->get_collection('bar');
    my $id         = $collection->insert({ some => 'data' });
    my $data       = $collection->find_one({ _id => $id });

=head1 AUTHOR

  Florian Ragwitz <rafl@debian.org>
  Kristina Chodorow <kristina@mongodb.org>

=head1 COPYRIGHT AND LICENSE

This software is Copyright (c) 2009 by 10Gen.

This is free software, licensed under:

  The Apache License, Version 2.0, January 2004

