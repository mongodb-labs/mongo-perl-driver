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

use strict;
use warnings;
use utf8;
use Test::More;
use Test::Fatal;

use MongoDB::_Protocol;
use BSON;

my $codec = BSON->new();

sub decode_with_codec {
  my $section = MongoDB::_Protocol::decode_section( @_ );
  my @docs = map { $codec->decode_one( $_ ) } @{ $section->{documents} };
  $section->{documents} = \@docs;
  return $section;
}

sub split_with_codec {
  my @sections = MongoDB::_Protocol::split_sections( @_ );
  @sections = map {
    my $cur = $_;
    my @docs = map {
      $codec->decode_one( $_ )
    } @{ $cur->{documents} };
    $cur->{documents} = \@docs;
    $cur
  } @sections;
  return @sections;
}

subtest 'insert_doc' => sub {
  my $insert_doc = [
    insert => 'collectionName',
    documents => [
      [ id => 'Document#1', example => 1 ],
      [ id => 'Document#2', example => 2 ],
      [ id => 'Document#3', example => 3 ]
    ],
    writeConcern => [ w => 'majority' ]
  ];

  push @{$insert_doc}, ( '$db', 'someDatabase' );
  my @packed_payloads = MongoDB::_Protocol::prepare_sections( $codec, $insert_doc );
  my @expected_payloads = (
    {
      type => 0,
      documents => [
        [
          insert => 'collectionName',
          writeConcern => [ w => 'majority' ],
          '$db' => 'someDatabase',
        ]
      ]
    },
    {
      type => 1,
      identifier => "documents",
      documents => [
        [ id => 'Document#1', example => 1 ],
        [ id => 'Document#2', example => 2 ],
        [ id => 'Document#3', example => 3 ]
      ],
    },
  );

  for my $i ( 0 .. $#expected_payloads ) {
    is_deeply $packed_payloads[$i], $expected_payloads[$i], "section $i prepared correctly";
  }
};
# struct Section {
#     uint8 payloadType;
#     union payload {
#         document  document; // payloadType == 0
#         struct sequence { // payloadType == 1
#             int32      size;
#             cstring    identifier;
#             document*  documents;
#         };
#     };
# };

my $raw_doc     = [ test => 'document' ];
my $doc         = $codec->encode_one( $raw_doc );
my $decoded_doc = $codec->decode_one( $doc );

subtest 'encode section' => sub {
  subtest 'payload 0' => sub {
    my $raw_section = {
      type => 0,
      documents => [ $raw_doc ],
    };
    my $got_section = MongoDB::_Protocol::encode_section( $codec, $raw_section );
    my $expected_section = "\0" . $doc;

    is $got_section, $expected_section, 'encode payload 0 correctly';
  };

  subtest 'payload 1 single doc' => sub {
    my $raw_section = {
      type => 1,
      identifier => 'documents',
      documents => [ $raw_doc ],
    };
    my $got_section = MongoDB::_Protocol::encode_section( $codec, $raw_section );
    my $expected_section = "\1&\0\0\0" . "documents\0" . $doc;

    is $got_section, $expected_section, 'encode payload 1 correctly';
  };

  subtest 'payload 1 multiple doc' => sub {
    my $raw_section = {
      type => 1,
      identifier => 'documents',
      documents => [ $raw_doc, $raw_doc ],
    };
    my $got_section = MongoDB::_Protocol::encode_section( $codec, $raw_section );
    my $expected_section = "\1>\0\0\0" . "documents\0" . $doc . $doc;

    is $got_section, $expected_section, 'encode payload 1 correctly';
  };
};

subtest 'decode section' => sub {
  subtest 'payload 0' => sub {
    my $encoded = "\0" . $doc;
    my $got_section = decode_with_codec( $encoded );

    is $got_section->{type}, 0, 'section type correct';
    is $got_section->{identifier}, undef, 'section identifier correct';
    is_deeply $got_section->{documents}, [ { test => 'document' } ], 'decoded document correctly';
  };

  subtest 'payload 1' => sub {
    my $encoded = "\1&\0\0\0" ."documents\0" . $doc;
    my $got_section = decode_with_codec( $encoded );

    is $got_section->{type}, 1, 'section type correct';
    is $got_section->{identifier}, 'documents', 'section identifier correct';
    is_deeply $got_section->{documents}, [ { test => 'document' } ], 'decoded document correctly';
  };

  subtest 'payload 1 multiple docs' => sub {
    my $encoded = "\1>\0\0\0" ."documents\0" . $doc . $doc;
    my $got_section = decode_with_codec( $encoded );

    is $got_section->{type}, 1, 'section type correct';
    is $got_section->{identifier}, 'documents', 'section identifier correct';
    is_deeply $got_section->{documents}, [ { test => 'document' }, { test => 'document' } ], 'decoded document correctly';
  };
};

subtest 'split sections' => sub {
  subtest 'payload 0' => sub {
    my $encoded = "\0" . $doc;
    my @got_sections = split_with_codec( $encoded );
    my @expected_sections = (
      [ 0, undef, [ $decoded_doc ] ],
    );

    for my $i ( 0 .. $#expected_sections ) {
      is $got_sections[$i]->{type}, $expected_sections[$i][0], 'type correct';
      is $got_sections[$i]->{identifier}, $expected_sections[$i][1], 'identifier correct';
      is_deeply $got_sections[$i]->{documents}, $expected_sections[$i][2], 'documents correct';
    }
  };

  subtest 'payload 0 + 1' => sub {
    subtest 'single document' => sub {
      my $encoded = "\0" . $doc . "\1&\0\0\0" ."documents\0" . $doc;
      my @got_sections = split_with_codec( $encoded );
      my @expected_sections = (
        [ 0, undef, [ $decoded_doc ] ],
        [ 1, 'documents', [ $decoded_doc ] ],
      );

      for my $i ( 0 .. $#expected_sections ) {
        is $got_sections[$i]->{type}, $expected_sections[$i][0], 'type correct';
        is $got_sections[$i]->{identifier}, $expected_sections[$i][1], 'identifier correct';
        is_deeply $got_sections[$i]->{documents}, $expected_sections[$i][2], 'documents correct';
      }
    };

    subtest 'multiple documents' => sub {
      my $encoded = "\0" . $doc . "\1>\0\0\0" ."documents\0" . $doc . $doc;
      my @got_sections = split_with_codec( $encoded );
      my @expected_sections = (
        [ 0, undef, [ $decoded_doc ] ],
        [ 1, 'documents', [ $decoded_doc, $decoded_doc ] ],
      );

      for my $i ( 0 .. $#expected_sections ) {
        is $got_sections[$i]->{type}, $expected_sections[$i][0], 'type correct';
        is $got_sections[$i]->{identifier}, $expected_sections[$i][1], 'identifier correct';
        is_deeply $got_sections[$i]->{documents}, $expected_sections[$i][2], 'documents correct';
      }
    };
  };

  subtest 'payload 0 + multiple 1' => sub {
    subtest 'single document' => sub {
      my $encoded = "\0" . $doc . "\1&\0\0\0" ."documents\0" . $doc . "\1&\0\0\0" ."documents\0" . $doc;
      my @got_sections = split_with_codec( $encoded );
      my @expected_sections = (
        [ 0, undef, [ $decoded_doc ] ],
        [ 1, 'documents', [ $decoded_doc ] ],
        [ 1, 'documents', [ $decoded_doc ] ],
      );

      for my $i ( 0 .. $#expected_sections ) {
        is $got_sections[$i]->{type}, $expected_sections[$i][0], 'type correct';
        is $got_sections[$i]->{identifier}, $expected_sections[$i][1], 'identifier correct';
        is_deeply $got_sections[$i]->{documents}, $expected_sections[$i][2], 'documents correct';
      }
    };

    subtest 'multiple documents' => sub {
      my $encoded = "\0" . $doc . "\1>\0\0\0" ."documents\0" . $doc . $doc . "\1>\0\0\0" ."documents\0" . $doc . $doc;
      my @got_sections = split_with_codec( $encoded );
      my @expected_sections = (
        [ 0, undef, [ $decoded_doc ] ],
        [ 1, 'documents', [ $decoded_doc, $decoded_doc ] ],
        [ 1, 'documents', [ $decoded_doc, $decoded_doc ] ],
      );

      for my $i ( 0 .. $#expected_sections ) {
        is $got_sections[$i]->{type}, $expected_sections[$i][0], 'type correct';
        is $got_sections[$i]->{identifier}, $expected_sections[$i][1], 'identifier correct';
        is_deeply $got_sections[$i]->{documents}, $expected_sections[$i][2], 'documents correct';
      }
    };
  };
};

done_testing;
