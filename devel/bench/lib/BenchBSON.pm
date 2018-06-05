#  Copyright 2016 - present MongoDB, Inc.
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

use 5.008001;
use strict;
use warnings;

package BenchBSON;

use JSON::MaybeXS;
use MongoDB::BSON::Binary;
use MongoDB::MongoClient;
use MongoDB::DBRef;
use MongoDB::OID;
use MongoDB::Timestamp;
use Path::Tiny;
use Time::Moment;

sub _decode_hashref {
    my ($doc) = @_;

    if ( exists $doc->{'$binary'} ) {
        return MongoDB::BSON::Binary->new( data => $doc->{'$binary'} );
    }
    elsif ( exists $doc->{'$date'} ) {
        my $date;

        # could be { '$numberLong' : ... } and need more decoding
        my $input = _decode_element($doc->{'$date'});

        # if number, it's milliseconds from epoch
        if ( $input =~ /\A\d+\z/ ) {
            $date = eval { Time::Moment->from_epoch( $input / 1000 ) };
        }
        # otherwise, assume it's ISO-8601
        else {
            $date = eval { Time::Moment->from_string($input) };
        }
        die "Error parsing '$input': $@" if $@;

        return $date;
    }
    elsif ( exists $doc->{'$timestamp'} ) {
        return MongoDB::Timestamp->new(
            sec => $doc->{'$timestamp'}{t},
            inc => $doc->{'$timestamp'}{i},
        );
    }
    elsif ( exists $doc->{'$regex'} ) {
        my ( $re, $opt ) = @{$doc}{qw/$regex $options/};
        my $qr = eval qq[qr/$re/$opt];
        die "Error parsing C< qr/$re/$opt >: $@\n" if $@;
        return $qr;
    }
    elsif ( exists $doc->{'$oid'} ) {
        return MongoDB::OID->new( value => $doc->{'$oid'} );
    }
    elsif ( exists $doc->{'$ref'} ) {
        return MongoDB::DBRef->new( ref => $doc->{'$ref'}, id => $doc->{'$id'} );
    }
    elsif ( exists $doc->{'$undefined'} ) {
        return undef;
    }
    elsif ( exists $doc->{'$minkey'} ) {
        return bless {}, 'MongoDB::MinKey';
    }
    elsif ( exists $doc->{'$maxkey'} ) {
        return bless {}, 'MongoDB::MaxKey';
    }
    elsif ( exists $doc->{'$numberLong'} ) {
        return 0+ $doc->{'$numberLong'};
    }
    else {
        while ( my ( $k, $v ) = each %$doc ) {
            $doc->{$k} = _decode_element($v);
        }
    }
    return $doc;
}

sub _decode_arrayref {
    my ($doc) = @_;

    for my $v (@$doc) {
        $v = _decode_element($v);
    }

    return $doc;
}

sub _decode_element {
    my ($elem) = @_;
    if ( ref($elem) eq 'HASH' ) {
        return _decode_hashref($elem);
    }
    elsif ( ref($elem) eq 'ARRAY' ) {
        return _decode_arrayref($elem);
    }
    else {
        return $elem;
    }
}

sub _load_json {
    my ($path) = @_;
    my $doc = decode_json( path($path)->slurp_utf8 );
    return _decode_hashref($doc);
}

sub _set_context {
    my ( $context, $file ) = @_;
    $context->{doc}   = _load_json("$context->{data_dir}/EXTENDED_BSON/$file");
    $context->{codec} = MongoDB::MongoClient->new(dt_type => "Time::Moment")->bson_codec;
    $context->{bson}  = $context->{codec}->encode_one( $context->{doc} );
}

#--------------------------------------------------------------------------#

package BenchBSONEncoder;

sub do_task {
    my ($context) = @_;
    my ( $codec, $doc ) = @{$context}{qw/codec doc/};
    $codec->encode_one($doc) for 1 .. 10_000;
}

package BenchBSONDecoder;

sub do_task {
    my ($context) = @_;
    my ( $codec, $bson ) = @{$context}{qw/codec bson/};
    $codec->decode_one($bson) for 1 .. 10_000;
}

#--------------------------------------------------------------------------#

package FlatBSON;

sub setup {
    push @_, "flat_bson.json";
    goto \&BenchBSON::_set_context;
}

package DeepBSON;

sub setup {
    push @_, "deep_bson.json";
    goto \&BenchBSON::_set_context;
}

package FullBSON;

sub setup {
    push @_, "full_bson.json";
    goto \&BenchBSON::_set_context;
}

#--------------------------------------------------------------------------#

package FlatBSONEncode;

our @ISA = qw/FlatBSON BenchBSONEncoder/;

#--------------------------------------------------------------------------#

package FlatBSONDecode;

our @ISA = qw/FlatBSON BenchBSONDecoder/;

#--------------------------------------------------------------------------#

package DeepBSONEncode;

our @ISA = qw/DeepBSON BenchBSONEncoder/;

#--------------------------------------------------------------------------#

package DeepBSONDecode;

our @ISA = qw/DeepBSON BenchBSONDecoder/;

#--------------------------------------------------------------------------#

package FullBSONEncode;

our @ISA = qw/FullBSON BenchBSONEncoder/;

#--------------------------------------------------------------------------#

package FullBSONDecode;

our @ISA = qw/FullBSON BenchBSONDecoder/;

#--------------------------------------------------------------------------#

1;
