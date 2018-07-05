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
use MongoDB::Protocol::_Section;
use BSON;

my $codec = BSON->new();

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
    MongoDB::Protocol::_Section->new(
      bson_codec => $codec,
      type => 0,
      identifier => undef,
      encoded_documents => [ "b\0\0\0\2insert\0\17\0\0\0collectionName\0\4writeConcern\0\36\0\0\0\0020\0\2\0\0\0w\0\0021\0\t\0\0\0majority\0\0\2\$db\0\r\0\0\0someDatabase\0\0" ]
    ),
    MongoDB::Protocol::_Section->new(
      bson_codec => $codec,
      type => 1,
      identifier => "documents",
      encoded_documents => [
        "%\0\0\0\2id\0\13\0\0\0Document#1\0\20example\0\1\0\0\0\0",
        "%\0\0\0\2id\0\13\0\0\0Document#2\0\20example\0\2\0\0\0\0",
        "%\0\0\0\2id\0\13\0\0\0Document#3\0\20example\0\3\0\0\0\0"
      ],
    ),
  );

  for my $i ( 0 .. $#expected_payloads ) {
    is $packed_payloads[$i]->binary, $expected_payloads[$i]->binary, "section $i prepared correctly";
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

my $raw_doc = [ test => 'document' ];
my $doc = $codec->encode_one( $raw_doc );
my $decoded_doc = $codec->decode_one( $doc );

subtest 'encode section' => sub {
  subtest 'payload 0' => sub {
    my $got_section = MongoDB::Protocol::_Section->new(
      bson_codec => $codec,
      type => 0,
      identifier => undef,
      documents => [$raw_doc],
    );
    my $expected_section = "\0" . $doc;

    is $got_section->binary, $expected_section, 'encode payload 0 correctly';

    ok exception { MongoDB::Protocol::_Section->new(
      bson_codec => $codec,
      type => 0,
      identifier => undef,
      documents => [$raw_doc, $raw_doc],
    )->binary; }, 'multiple docs in payload 0 causes error';
  };

  subtest 'payload 1 single doc' => sub {
    my $got_section = MongoDB::Protocol::_Section->new(
      bson_codec => $codec,
      type => 1,
      identifier => 'documents',
      documents => [$raw_doc],
    );
    my $expected_section = "\1&\0\0\0" . "documents\0" . $doc;

    is $got_section->binary, $expected_section, 'encode payload 1 correctly';
  };

  subtest 'payload 1 multiple doc' => sub {
    my $got_section = MongoDB::Protocol::_Section->new(
      bson_codec => $codec,
      type => 1,
      identifier => 'documents',
      documents => [$raw_doc, $raw_doc],
    );
    my $expected_section = "\1>\0\0\0" . "documents\0" . $doc . $doc;

    is $got_section->binary, $expected_section, 'encode payload 1 correctly';
  };
};

subtest 'decode section' => sub {
  subtest 'payload 0' => sub {
    my $encoded = "\0" . $doc;
    my $got_section = MongoDB::Protocol::_Section->new(
      bson_codec => $codec,
      binary => $encoded,
    );

    is $got_section->type, 0, 'section type correct';
    is $got_section->identifier, undef, 'section identifier correct';
    is_deeply $got_section->documents, [ { test => 'document' } ], 'decoded document correctly';
  };

  subtest 'payload 1' => sub {
    my $encoded = "\1&\0\0\0" ."documents\0" . $doc;
    my $got_section = MongoDB::Protocol::_Section->new(
      bson_codec => $codec,
      binary => $encoded,
    );

    is $got_section->type, 1, 'section type correct';
    is $got_section->identifier, 'documents', 'section identifier correct';
    is_deeply $got_section->documents, [ { test => 'document' } ], 'decoded document correctly';
  };

  subtest 'payload 1 multiple docs' => sub {
    my $encoded = "\1>\0\0\0" ."documents\0" . $doc . $doc;
    my $got_section = MongoDB::Protocol::_Section->new(
      bson_codec => $codec,
      binary => $encoded,
    );

    is $got_section->type, 1, 'section type correct';
    is $got_section->identifier, 'documents', 'section identifier correct';
    is_deeply $got_section->documents, [ { test => 'document' }, { test => 'document' } ], 'decoded document correctly';
  };
};

subtest 'join sections' => sub {
  subtest 'payload 0' => sub {
    my @sections = (
      MongoDB::Protocol::_Section->new(
        bson_codec => $codec,
        type => 0,
        identifier => undef,
        documents => [$raw_doc],
      )
    );
    my $got_sections = MongoDB::_Protocol::join_sections( @sections );
    my $expected_sections = "\0" . $doc;

    is_deeply $got_sections, $expected_sections, 'joined correctly';
  };

  subtest 'payload 0 + 1' => sub {
    subtest 'single document' => sub {
      my @sections = (
        MongoDB::Protocol::_Section->new(
          bson_codec => $codec,
          type => 0,
          identifier => undef,
          documents => [$raw_doc],
        ),
        MongoDB::Protocol::_Section->new(
          bson_codec => $codec,
          type => 1,
          identifier => 'documents',
          documents => [$raw_doc],
        ),
      );
      my $got_sections = MongoDB::_Protocol::join_sections( @sections );
      my $expected_sections = "\0" . $doc . "\1&\0\0\0" ."documents\0" . $doc;

      is_deeply $got_sections, $expected_sections, 'joined correctly';
    };

    subtest 'multiple documents' => sub {
      my @sections = (
        MongoDB::Protocol::_Section->new(
          bson_codec => $codec,
          type => 0,
          identifier => undef,
          documents => [$raw_doc],
        ),
        MongoDB::Protocol::_Section->new(
          bson_codec => $codec,
          type => 1,
          identifier => 'documents',
          documents => [$raw_doc, $raw_doc],
        ),
      );
      my $got_sections = MongoDB::_Protocol::join_sections( @sections );
      my $expected_sections = "\0" . $doc . "\1>\0\0\0" ."documents\0" . $doc . $doc;

      is_deeply $got_sections, $expected_sections, 'joined correctly';
    };
  };

  subtest 'payload 0 + multiple 1' => sub {
    subtest 'single document' => sub {
      my @sections = (
        MongoDB::Protocol::_Section->new(
          bson_codec => $codec,
          type => 0,
          identifier => undef,
          documents => [$raw_doc],
        ),
        MongoDB::Protocol::_Section->new(
          bson_codec => $codec,
          type => 1,
          identifier => 'documents',
          documents => [$raw_doc],
        ),
        MongoDB::Protocol::_Section->new(
          bson_codec => $codec,
          type => 1,
          identifier => 'documents',
          documents => [$raw_doc],
        ),
      );
      my $got_sections = MongoDB::_Protocol::join_sections( @sections );
      my $expected_sections = "\0" . $doc . "\1&\0\0\0" ."documents\0" . $doc . "\1&\0\0\0" ."documents\0" . $doc;

      is_deeply $got_sections, $expected_sections, 'joined correctly';
    };

    subtest 'multiple documents' => sub {
      my @sections = (
        MongoDB::Protocol::_Section->new(
          bson_codec => $codec,
          type => 0,
          identifier => undef,
          documents => [$raw_doc],
        ),
        MongoDB::Protocol::_Section->new(
          bson_codec => $codec,
          type => 1,
          identifier => 'documents',
          documents => [$raw_doc, $raw_doc],
        ),
        MongoDB::Protocol::_Section->new(
          bson_codec => $codec,
          type => 1,
          identifier => 'documents',
          documents => [$raw_doc, $raw_doc],
        ),
      );
      my $got_sections = MongoDB::_Protocol::join_sections( @sections );
      my $expected_sections = "\0" . $doc . "\1>\0\0\0" ."documents\0" . $doc . $doc . "\1>\0\0\0" ."documents\0" . $doc . $doc;

      is_deeply $got_sections, $expected_sections, 'joined correctly';
    };
  };
};

subtest 'split sections' => sub {
  subtest 'payload 0' => sub {
    my $encoded = "\0" . $doc;
    my @got_sections = MongoDB::_Protocol::split_sections( $codec, $encoded );
    my @expected_sections = (
      [ 0, undef, [ $decoded_doc ] ],
    );

    for my $i ( 0 .. $#expected_sections ) {
      is $got_sections[$i]->type, $expected_sections[$i][0], 'type correct';
      is $got_sections[$i]->identifier, $expected_sections[$i][1], 'identifier correct';
      is_deeply $got_sections[$i]->documents, $expected_sections[$i][2], 'documents correct';
    }
  };

  subtest 'payload 0 + 1' => sub {
    subtest 'single document' => sub {
      my $encoded = "\0" . $doc . "\1&\0\0\0" ."documents\0" . $doc;
      my @got_sections = MongoDB::_Protocol::split_sections( $codec, $encoded );
      my @expected_sections = (
        [ 0, undef, [ $decoded_doc ] ],
        [ 1, 'documents', [ $decoded_doc ] ],
      );

      for my $i ( 0 .. $#expected_sections ) {
        is $got_sections[$i]->type, $expected_sections[$i][0], 'type correct';
        is $got_sections[$i]->identifier, $expected_sections[$i][1], 'identifier correct';
        is_deeply $got_sections[$i]->documents, $expected_sections[$i][2], 'documents correct';
      }
    };

    subtest 'multiple documents' => sub {
      my $encoded = "\0" . $doc . "\1>\0\0\0" ."documents\0" . $doc . $doc;
      my @got_sections = MongoDB::_Protocol::split_sections( $codec, $encoded );
      my @expected_sections = (
        [ 0, undef, [ $decoded_doc ] ],
        [ 1, 'documents', [ $decoded_doc, $decoded_doc ] ],
      );

      for my $i ( 0 .. $#expected_sections ) {
        is $got_sections[$i]->type, $expected_sections[$i][0], 'type correct';
        is $got_sections[$i]->identifier, $expected_sections[$i][1], 'identifier correct';
        is_deeply $got_sections[$i]->documents, $expected_sections[$i][2], 'documents correct';
      }
    };
  };

  subtest 'payload 0 + multiple 1' => sub {
    subtest 'single document' => sub {
      my $encoded = "\0" . $doc . "\1&\0\0\0" ."documents\0" . $doc . "\1&\0\0\0" ."documents\0" . $doc;
      my @got_sections = MongoDB::_Protocol::split_sections( $codec, $encoded );
      my @expected_sections = (
        [ 0, undef, [ $decoded_doc ] ],
        [ 1, 'documents', [ $decoded_doc ] ],
        [ 1, 'documents', [ $decoded_doc ] ],
      );

      for my $i ( 0 .. $#expected_sections ) {
        is $got_sections[$i]->type, $expected_sections[$i][0], 'type correct';
        is $got_sections[$i]->identifier, $expected_sections[$i][1], 'identifier correct';
        is_deeply $got_sections[$i]->documents, $expected_sections[$i][2], 'documents correct';
      }
    };

    subtest 'multiple documents' => sub {
      my $encoded = "\0" . $doc . "\1>\0\0\0" ."documents\0" . $doc . $doc . "\1>\0\0\0" ."documents\0" . $doc . $doc;
      my @got_sections = MongoDB::_Protocol::split_sections( $codec, $encoded );
      my @expected_sections = (
        [ 0, undef, [ $decoded_doc ] ],
        [ 1, 'documents', [ $decoded_doc, $decoded_doc ] ],
        [ 1, 'documents', [ $decoded_doc, $decoded_doc ] ],
      );

      for my $i ( 0 .. $#expected_sections ) {
        is $got_sections[$i]->type, $expected_sections[$i][0], 'type correct';
        is $got_sections[$i]->identifier, $expected_sections[$i][1], 'identifier correct';
        is_deeply $got_sections[$i]->documents, $expected_sections[$i][2], 'documents correct';
      }
    };
  };
};

done_testing;
