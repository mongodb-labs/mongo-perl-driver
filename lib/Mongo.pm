use strict;
use warnings;

package Mongo;

our $VERSION = '0.01';

use XSLoader;
use Mongo::Connection;

XSLoader::load(__PACKAGE__, $VERSION);

1;
