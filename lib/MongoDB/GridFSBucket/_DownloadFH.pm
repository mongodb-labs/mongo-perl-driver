#
#  Copyright 2009-2015 MongoDB, Inc.
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

package MongoDB::GridFSBucket::_DownloadFH;

use Test::More;

# Magic tie methods

sub TIEHANDLE {
    my ($class, $parent) = @_;
    bless \$parent, $class;
}

sub READ {
	my $parent = shift;
    $parent = $$parent;
    my $buffref = \$_[0];
	my(undef,$len,$offset) = @_;
    return $parent->read($$buffref, $len, $offset);
}

sub GETC {
	my $parent = shift;
    $parent = $$parent;
    return $parent->readbytes(1);
}

sub READLINE {
	my $parent = shift;
    $parent = $$parent;
    return $parent->readline;
}

sub CLOSE {
	...
}

sub UNTIE {
	...
}

sub DESTROY {
	...
}

sub PRINT {
    ...
}

sub PRINTF {
	...
}

sub WRITE {
	...
}

# possibly optional magic?
sub BINMODE {
	...
}

sub OPEN {
	...
}

sub EOF {
	...
}

sub FILENO {
	...
}

sub SEEK {
	...
}

sub TELL {
	...
}

1;
