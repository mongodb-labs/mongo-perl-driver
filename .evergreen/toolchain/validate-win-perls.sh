#!/bin/bash
set -o errexit

for d in $(ls perl); do
  echo "*** CHECKING $d ***"
  thisperl="$(pwd)/perl/$d/perl/bin/perl"
  "$thisperl" -v
  "$thisperl" -V
  echo ""
done
