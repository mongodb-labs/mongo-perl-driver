#
#  Copyright 2015 MongoDB, Inc.
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
use Test::More;
use JSON::MaybeXS;
use Test::Fatal;
use Path::Tiny;

use MongoDB;

use lib "t/lib";
use MongoDBTest qw/skip_unless_mongod build_client get_test_db/;

skip_unless_mongod();

my $conn            = build_client();
my $testdb          = get_test_db($conn);
my $bucket          = $testdb->get_gridfsbucket;
my $e_files  = $testdb->get_collection('expected.files');
my $e_chunks = $testdb->get_collection('expected.chunks');

my $ampresult;
my $actualidcount = 0;

sub hex_to_str {
    my ($hex) = @_;
    my $result = '';
    while ( length $hex ) {
        $result .= chr( hex( substr $hex, 0, 2, '' ) );
    }
    return $result;
}

# Copied from http://cpansearch.perl.org/src/HIO/String-CamelCase-0.02/lib/String/CamelCase.pm
sub decamelize {
	my $s = shift;
	$s =~ s{([^a-zA-Z]?)([A-Z]*)([A-Z])([a-z]?)}{
		my $fc = pos($s)==0;
		my ($p0,$p1,$p2,$p3) = ($1,lc$2,lc$3,$4);
		my $t = $p0 || $fc ? $p0 : '_';
		$t .= $p3 ? $p1 ? "${p1}_$p2$p3" : "$p2$p3" : "$p1$p2";
		$t;
	}ge;
	$s;
}

sub fix_options {
    my $obj = shift;
	for my $key ( keys %{ $obj } ) {
        $obj->{decamelize( $key )} = delete $obj->{$key};
    }
    return $obj;
}

sub map_fix_types {
    return fix_types($_);
}

sub fix_types {
    my $obj = shift;
    if ( ( ref $obj ) eq 'HASH' ) {
        if ( exists $obj->{'$oid'} ) {
            $obj = MongoDB::OID->new(
                value => $obj->{'$oid'},
            );
        } elsif ( exists $obj->{'$hex'} ) {
            $obj = MongoDB::BSON::Binary->new({ data => hex_to_str( $obj->{'$hex'} ) });
        } else {
            for my $key ( keys %{ $obj } ) {
                $obj->{$key} = fix_types( $obj->{$key} );

                if ( $key eq 'chunkSizeBytes' ) {
                    $obj->{chunk_size_bytes} = delete $obj->{$key};
                } elsif ( $key eq '_id' && $obj->{$key} =~ /^\*actual$/ ) {
                        $obj->{$key} = $obj->{$key} . $actualidcount++;
                }
            }
        }
    } elsif ( ( ref $obj ) eq 'ARRAY' ) {
        my @arr = map( map_fix_types, @{ $obj } );
        $obj = \@arr;
    }
    return $obj;
}

sub run_commands {
    my ($commands) = @_;

    for my $cmd ( @{ $commands } ) {
        my $exec;
        if ( exists $cmd->{delete} ) {
            my @arr = map( map_fix_types, @{ $cmd->{deletes} } );
            $cmd->{deletes} = \@arr;
            $exec = [ delete => $cmd->{delete}, deletes => $cmd->{deletes} ];
        } elsif ( exists $cmd->{update} ) {
            my @arr = map( map_fix_types, @{ $cmd->{updates} } );
            $cmd->{updates} = \@arr;
            $exec = [ update => $cmd->{update}, updates => $cmd->{updates} ];
        } elsif ( exists $cmd->{insert} ) {
            my @arr = map( map_fix_types, @{ $cmd->{documents} } );
            $cmd->{documents} = \@arr;
            $exec = [ insert => $cmd->{insert}, documents => $cmd->{documents} ];
        } else {
            diag( explain $cmd );
            die "don't know how to handle some command";
        }
        $testdb->run_command( $exec );
    }
}

sub compare_collections {
    my ($label) = @_;
    my $actual_chunks = $bucket->chunks->find({},  { sort => { _id => 1 } } )->result;
    my $expected_chunks = $e_chunks->find({},  { sort => { _id => 1 } } )->result;
    my $actual_files = $bucket->files->find({}, { sort => { _id => 1 } } )->result;
    my $expected_files = $e_files->find({}, { sort => { _id => 1 } } )->result;

    while ( $actual_chunks->has_next && $expected_chunks->has_next ) {
        cmp_special($actual_chunks->next, $expected_chunks->next, $label);
    }
    ok( !$actual_chunks->has_next, 'Extra chunks in fs.chunks' );
    ok( !$expected_chunks->has_next, 'Extra chunks in expected.chunks' );

    while ( $actual_files->has_next && $expected_files->has_next ) {
        cmp_special( $actual_files->next, $expected_files->next, $label );
    }

    ok( !$actual_files->has_next, 'Extra files in fs.files' );
    ok( !$expected_files->has_next, 'Extra files in fs.files' );
}

sub cmp_special {
    my ($got, $expected, $label) = @_;

    if ( ( ref $expected ) eq 'HASH' ) {
        if ( ( ref $got ) eq 'HASH' ) {
            for my $key ( keys %{ $got } ) {
                cmp_special($got->{$key}, $expected->{$key}, $label);
            }
        } else {
            fail("Got $got but expected hashref");
        }
    } elsif ( ( ref $expected ) eq 'ARRAY' ) {
        if ( ( ref $expected ) eq 'ARRAY' && scalar( @{ $got } ) == scalar( @{ $expected } ) ) {
            for my $i (0 .. $#{ $got } ) {
                cmp_special( $$got[$i], $$expected[$i], $label );
            }
        } else {
            fail( "Got $got but expected arrayref, possibly of different size" );
        }
    } elsif ( !defined $expected ) {
        is($got, $expected, $label);
    } elsif ( $expected =~ /^\*actual[0-9]*$/ ) {
        # Any value with '*actual' as the expected result can't be known beforehand,
        # so is assumed to be correct. To work around using *actual for _id fields,
        # some may be in the form of the above regex.
        pass('Passing with special *actual value');
    } elsif ( $expected eq '&result' ) {
        # This value is not being tested for anything, but future tests may need to
        # refer to it. Store it in the global $ampresult.
        $ampresult = $got;
    } elsif ( $expected eq '*result' ) {
        # Should match a value that could not be known when the test was written, but
        # was saved earlier using &result.
        is($got, $ampresult);
    } elsif ( $expected eq 'void' ) {
        is($got, undef, $label);
    } else {
        is($got, $expected, $label);
    }
}

sub test_download {
    my ( undef, $args ) = @_;
    my $id = MongoDB::OID->new( value => $args->{id}->{'$oid'} );
    my $options = fix_options( $args->{options} );

    my $stream = $bucket->open_download_stream( $id, $args );
    my $str;
    $stream->read( $str, 999 );
    return $str;
}

sub test_delete {
    my ( undef, $args ) = @_;
    my $id = MongoDB::OID->new( value => $args->{id}->{'$oid'} );

    return $bucket->delete( $id );
}

sub test_upload {
    my ( undef, $args ) = @_;
    my $source = hex_to_str( $args->{source}->{'$hex'} );
    my $filename = $args->{filename};
    my $options = fix_options( $args->{options} );

    my $stream = $bucket->open_upload_stream( $filename, $options );
    $stream->print( $source );
    $stream->close;
    return $stream->id;
}

sub run_test {
    my $test = shift;
    my $assert = $test->{assert};
    my $label = $test->{description};
    my $method = $test->{act}->{operation};
    my $args = $test->{act}->{arguments};
    my $test_method = "test_$method";

    if ( exists $test->{arrange} ) {
        run_commands( $test->{arrange}->{data} );
    }

    my $except = exception {
        my $result = main->$test_method( $args );
            cmp_special( $result, fix_types( $assert->{result} ), $label ) if exists $assert->{result};
    };

    if ( exists $assert->{error} ) {
        my $expstr = $assert->{error};
        like(
            $except,
            qr/$expstr.*/,
            $test->{label},
        );
    }

    if ( $assert->{data} ) {
        run_commands( $assert->{data} );
        subtest "Compare collections for $label" => sub {
            compare_collections( $label );
        };
    }
}

my $dir = path("t/data/gridfs/tests");
my $iterator = $dir->iterator;
while ( my $path = $iterator->() ) {
    next unless -f $path && $path =~ /\.json$/;
    my $plan = eval { decode_json( $path->slurp_utf8 ) };
    if ( $@ ) {
        die "Error decoding $path: $@";
    }
    for my $collection ( qw(files chunks) ) {
        my @arr = map( map_fix_types, @{ $plan->{data}->{$collection} } );
        $plan->{data}->{$collection} = \@arr;
    }

    my $name = $path->relative($dir)->basename('.json');

    for my $test ( @{ $plan->{tests} } ) {
        $bucket->drop;
        $e_chunks->drop;
        $e_files->drop;
        $ampresult = undef;
        $bucket->chunks->insert_many( $plan->{data}->{chunks} );
        $e_chunks->insert_many( $plan->{data}->{chunks} );
        $bucket->files->insert_many( $plan->{data}->{files} );
        $e_files->insert_many( $plan->{data}->{files} );
        run_test($test);
    }
}

$testdb->drop;

done_testing;
