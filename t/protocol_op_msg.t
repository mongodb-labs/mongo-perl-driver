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

subtest 'encode section' => sub {
  my $doc = $codec->encode_one([ test => 'document' ]);
  subtest 'payload 0' => sub {
    my $got_section = MongoDB::_Protocol::encode_section( 0, undef, $doc );
    my $expected_section = "\0" . $doc;

    is $got_section, $expected_section, 'encode payload 0 correctly';

    ok exception{ MongoDB::_Protocol::encode_section( 0, undef, $doc, $doc ) }, 'multiple docs in payload 0 causes error';
  };

  subtest 'payload 1 single doc' => sub {
    my $got_section = MongoDB::_Protocol::encode_section( 1, 'documents', $doc );
    my $expected_section = "\1&\0\0\0" . "documents\0" . $doc;

    is $got_section, $expected_section, 'encode payload 1 correctly';
  };

  subtest 'payload 1 multiple doc' => sub {
    my $got_section = MongoDB::_Protocol::encode_section( 1, 'documents', $doc, $doc );
    my $expected_section = "\1>\0\0\0" . "documents\0" . $doc . $doc;

    is $got_section, $expected_section, 'encode payload 1 correctly';
  };
};

subtest 'decode section' => sub {
  my $doc = $codec->encode_one([ test => 'document' ]);
  subtest 'payload 0' => sub {
    my $encoded = "\0" . $doc;
    my @got_decoded = MongoDB::_Protocol::decode_section( $encoded );
    my @expected_decoded = ( 0, undef, $doc );

    is_deeply \@got_decoded, \@expected_decoded, 'decoded payload 0 correctly';
  };

  subtest 'payload 1' => sub {
    my $encoded = "\1&\0\0\0" ."documents\0" . $doc;
    my @got_decoded = MongoDB::_Protocol::decode_section( $encoded );
    my @expected_decoded = ( 1, 'documents', $doc );


    is_deeply \@got_decoded, \@expected_decoded, 'decoded payload 1 correctly';
  };

  subtest 'payload 1 multiple docs' => sub {
    my $encoded = "\1>\0\0\0" ."documents\0" . $doc . $doc;
    my @got_decoded = MongoDB::_Protocol::decode_section( $encoded );
    my @expected_decoded = ( 1, 'documents', $doc, $doc );

    is_deeply \@got_decoded, \@expected_decoded, 'decoded payload 1 correctly';
  };
};

subtest 'join sections' => sub {
  my $doc = $codec->encode_one([ test => 'document' ]);
  subtest 'payload 0' => sub {
    my @sections = (
      [ 0, undef, $doc ],
    );
    my $got_sections = MongoDB::_Protocol::join_sections( @sections );
    my $expected_sections = "\0" . $doc;

    is_deeply $got_sections, $expected_sections, 'joined correctly';
  };

  subtest 'payload 0 + 1' => sub {
    subtest 'single document' => sub {
      my @sections = (
        [ 0, undef, $doc ],
        [ 1, 'documents', $doc ],
      );
      my $got_sections = MongoDB::_Protocol::join_sections( @sections );
      my $expected_sections = "\0" . $doc . "\1&\0\0\0" ."documents\0" . $doc;

      is_deeply $got_sections, $expected_sections, 'joined correctly';
    };

    subtest 'multiple documents' => sub {
      my @sections = (
        [ 0, undef, $doc ],
        [ 1, 'documents', $doc, $doc ],
      );
      my $got_sections = MongoDB::_Protocol::join_sections( @sections );
      my $expected_sections = "\0" . $doc . "\1>\0\0\0" ."documents\0" . $doc . $doc;

      is_deeply $got_sections, $expected_sections, 'joined correctly';
    };
  };

  subtest 'payload 0 + multiple 1' => sub {
    subtest 'single document' => sub {
      my @sections = (
        [ 0, undef, $doc ],
        [ 1, 'documents', $doc ],
        [ 1, 'documents', $doc ],
      );
      my $got_sections = MongoDB::_Protocol::join_sections( @sections );
      my $expected_sections = "\0" . $doc . "\1&\0\0\0" ."documents\0" . $doc . "\1&\0\0\0" ."documents\0" . $doc;

      is_deeply $got_sections, $expected_sections, 'joined correctly';
    };

    subtest 'multiple documents' => sub {
      my @sections = (
        [ 0, undef, $doc ],
        [ 1, 'documents', $doc, $doc ],
        [ 1, 'documents', $doc, $doc ],
      );
      my $got_sections = MongoDB::_Protocol::join_sections( @sections );
      my $expected_sections = "\0" . $doc . "\1>\0\0\0" ."documents\0" . $doc . $doc . "\1>\0\0\0" ."documents\0" . $doc . $doc;

      is_deeply $got_sections, $expected_sections, 'joined correctly';
    };
  };
};

subtest 'split sections' => sub {
  my $doc = $codec->encode_one([ test => 'document' ]);
  subtest 'payload 0' => sub {
    my $encoded = "\0" . $doc;
    my @got_sections = MongoDB::_Protocol::split_sections( $encoded );
    my @expected_sections = (
      [ 0, undef, $doc ],
    );

    is_deeply \@got_sections, \@expected_sections, 'split correctly';
  };

  subtest 'payload 0 + 1' => sub {
    subtest 'single document' => sub {
      my $encoded = "\0" . $doc . "\1&\0\0\0" ."documents\0" . $doc;
      my @got_sections = MongoDB::_Protocol::split_sections( $encoded );
      my @expected_sections = (
        [ 0, undef, $doc ],
        [ 1, 'documents', $doc ],
      );

      is_deeply \@got_sections, \@expected_sections, 'split correctly';
    };

    subtest 'multiple documents' => sub {
      my $encoded = "\0" . $doc . "\1>\0\0\0" ."documents\0" . $doc . $doc;
      my @got_sections = MongoDB::_Protocol::split_sections( $encoded );
      my @expected_sections = (
        [ 0, undef, $doc ],
        [ 1, 'documents', $doc, $doc ],
      );

      is_deeply \@got_sections, \@expected_sections, 'split correctly';
    };
  };

  subtest 'payload 0 + multiple 1' => sub {
    subtest 'single document' => sub {
      my $encoded = "\0" . $doc . "\1&\0\0\0" ."documents\0" . $doc . "\1&\0\0\0" ."documents\0" . $doc;
      my @got_sections = MongoDB::_Protocol::split_sections( $encoded );
      my @expected_sections = (
        [ 0, undef, $doc ],
        [ 1, 'documents', $doc ],
        [ 1, 'documents', $doc ],
      );

      is_deeply \@got_sections, \@expected_sections, 'split correctly';
    };

    subtest 'multiple documents' => sub {
      my $encoded = "\0" . $doc . "\1>\0\0\0" ."documents\0" . $doc . $doc . "\1>\0\0\0" ."documents\0" . $doc . $doc;
      my @got_sections = MongoDB::_Protocol::split_sections( $encoded );
      my @expected_sections = (
        [ 0, undef, $doc ],
        [ 1, 'documents', $doc, $doc ],
        [ 1, 'documents', $doc, $doc ],
      );

      is_deeply \@got_sections, \@expected_sections, 'split correctly';
    };
  };
};

done_testing;
