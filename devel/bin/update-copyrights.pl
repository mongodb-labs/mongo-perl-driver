#!/usr/bin/env perl
#
#  Copyright 2018 - present MongoDB, Inc.
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

use v5.10;
use strict;
use warnings;
use utf8;
use open qw/:std :utf8/;
use Path::Tiny;
use PIR;

my $shebang = "#!/usr/bin/env perl\n#\n";

my $get_date_template = "git log --pretty='%%aI' --reverse -- '%s' | head -1";

my $next =
  PIR->new->skip_dirs( "blib", "inc" )->skip_vcs->perl_file->not_name("Makefile.PL")
  ->iter_fast(".");

my $copyright_template = <<'HERE';
#  Copyright %s - present MongoDB, Inc.
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

HERE

while ( my $file = $next->() ) {
    next if $file eq 'Makefile.PL';

    my $path = path($file);
    my $guts = $path->slurp_utf8;

    # remove leading comments
    $guts =~ s/(?:^#.*\n)*//m;

    # remove leading blank lines
    $guts =~ s/(?:^\s*\n)*//m;

    # find year file was first added to git
    my $cmd = sprintf( $get_date_template, $path );
    my $first_date = qx/$cmd/;
    chomp $first_date;
    $first_date ||= "Unknown";
    $first_date =~ s/\A([^-]+).*/$1/;

    $guts =
        ( substr( $file, -3, 3 ) eq '.pl' ? $shebang : "" )
      . sprintf( $copyright_template, $first_date )
      . $guts;
    $path->append_utf8( { truncate => 1 }, $guts );
    say $file;
}
