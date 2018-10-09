#  Copyright 2009 - present MongoDB, Inc.
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

{
  package Test::Local::MongoDB;

  use Moo;

  has bson_codec => (
      is => 'ro',
      required => 1
  );
  with qw/
    MongoDB::Role::_InsertPreEncoder
    MongoDB::Role::_UpdatePreEncoder
  /;
}

use Test::More 0.96;
use Test::Fatal;
use Tie::IxHash;

use MongoDB::_Constants;
use BSON::Types ':all';

use lib "t/lib";

my $test_lib = Test::Local::MongoDB->new( bson_codec => BSON->new );

# create encoded ordered hash then make it BSON::Raw with _id
my $orig_doc = $test_lib->bson_codec->encode_one( Tie::IxHash->new(
    _id => 12,
    1234 => 314159,
    1235 => 300
) );

my $orig_doc_bson = BSON::Raw->new(bson=>$orig_doc);

subtest "pre-encode insert" => sub {
  my $insert_doc = $test_lib->_pre_encode_insert(
      MAX_BSON_WIRE_SIZE,
      $orig_doc_bson,
      '.'
  );

  # check it didn't explode and matches
  is($insert_doc->bson,$orig_doc_bson->bson);
  is($insert_doc->metadata->{_id},12);


  # create encoded ordered hash then make it BSON::Raw without _id
  my $orig_doc_no_key = $test_lib->bson_codec->encode_one( Tie::IxHash->new(
      "notakey" => 12,
      1234 => 314159,
      1235 => 300
  ) );

  my $orig_doc_bson_no_key = BSON::Raw->new(bson=>$orig_doc_no_key);

  my $insert_doc_no_key = $test_lib->_pre_encode_insert(
      MAX_BSON_WIRE_SIZE,
      $orig_doc_bson_no_key,
      '.'
  );

  # check the id field exists
  ok(exists $insert_doc_no_key->metadata->{_id});

  # check it's a OID field
  isa_ok($insert_doc_no_key->metadata->{_id},"BSON::OID");

};

subtest "pre-encode update" => sub {
  # update orig_doc with this
  my $orig_doc_update = {
      '$set' => {
          1235 => 999
      }
  };

  my $update_doc = $test_lib->_pre_encode_update(
      MAX_BSON_WIRE_SIZE,
      $orig_doc_update,
      0
  );

  is_deeply($orig_doc_update,$test_lib->bson_codec->decode_one($update_doc->bson));

};

done_testing;
