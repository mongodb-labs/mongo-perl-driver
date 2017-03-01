#!/bin/bash
set -o errexit

for d in $(ls perl); do
  echo "*** CHECKING $d ***"
  thisperl="$(pwd)/perl/$d"
  PERL5LIB="$thisperl/lib" $thisperl/bin/perl -v
  PERL5LIB="$thisperl/lib" $thisperl/bin/perl -V
  echo ""
done
