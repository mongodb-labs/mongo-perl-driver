# Dist::Zilla::Plugin::MakeMaker::Awesome 0.41.
# Don't edit it but the dist.ini and plugins used to construct it.

use strict;
use warnings;

use 5.010001;
use ExtUtils::MakeMaker 6.17;

my %WriteMakefileArgs = (
  "ABSTRACT" => "Official MongoDB Driver for Perl",
  "AUTHOR" => "David Golden <david\@mongodb.com>, Rassi <rassi\@mongodb.com>, Mike Friedman <friedo\@friedo.com>, Kristina Chodorow <k.chodorow\@gmail.com>, Florian Ragwitz <rafl\@debian.org>",
  "CONFIGURE_REQUIRES" => {
    "Config::AutoConf" => "0.22",
    "ExtUtils::MakeMaker" => "6.17",
    "Path::Tiny" => "0.052"
  },
  "DISTNAME" => "MongoDB",
  "LICENSE" => "apache",
  "MIN_PERL_VERSION" => "5.010001",
  "NAME" => "MongoDB",
  "PREREQ_PM" => {
    "Authen::SASL::SASLprep" => 0,
    "Authen::SCRAM::Client" => "0.009",
    "BSON" => "1.006003",
    "BSON::Bytes" => 0,
    "BSON::Code" => 0,
    "BSON::DBRef" => 0,
    "BSON::OID" => 0,
    "BSON::Raw" => 0,
    "BSON::Regex" => 0,
    "BSON::Time" => 0,
    "BSON::Timestamp" => 0,
    "BSON::Types" => 0,
    "Carp" => 0,
    "Class::XSAccessor" => 0,
    "Digest::MD5" => 0,
    "Encode" => 0,
    "Exporter" => "5.57",
    "IO::File" => 0,
    "IO::Socket" => 0,
    "List::Util" => 0,
    "MIME::Base64" => 0,
    "Math::BigInt" => 0,
    "Moo" => 2,
    "Moo::Role" => 0,
    "Net::DNS" => 0,
    "Safe::Isa" => 0,
    "Scalar::Util" => 0,
    "Socket" => 0,
    "Sub::Quote" => 0,
    "Tie::IxHash" => 0,
    "Time::HiRes" => 0,
    "Try::Tiny" => 0,
    "Type::Library" => 0,
    "Type::Tiny::XS" => 0,
    "Type::Utils" => 0,
    "Types::Standard" => 0,
    "UUID::URandom" => 0,
    "boolean" => "0.25",
    "bytes" => 0,
    "constant" => 0,
    "if" => 0,
    "namespace::clean" => 0,
    "overload" => 0,
    "re" => 0,
    "strict" => 0,
    "version" => 0,
    "warnings" => 0
  },
  "TEST_REQUIRES" => {
    "BSON::Decimal128" => 0,
    "Data::Dumper" => 0,
    "ExtUtils::MakeMaker" => 0,
    "File::Compare" => 0,
    "File::Spec" => 0,
    "File::Temp" => 0,
    "FileHandle" => 0,
    "JSON::MaybeXS" => 0,
    "Path::Tiny" => "0.054",
    "Test::Deep" => "0.111",
    "Test::Fatal" => 0,
    "Test::More" => "0.96",
    "lib" => 0,
    "threads::shared" => 0,
    "utf8" => 0
  },
  "VERSION" => "v1.999.0",
  "test" => {
    "TESTS" => "t/*.t t/deprecated/*.t t/examples/*.t t/threads/*.t t/unit/*.t"
  }
);

my %FallbackPrereqs = (
  "Authen::SASL::SASLprep" => 0,
  "Authen::SCRAM::Client" => "0.009",
  "BSON" => "1.006003",
  "BSON::Bytes" => 0,
  "BSON::Code" => 0,
  "BSON::DBRef" => 0,
  "BSON::Decimal128" => 0,
  "BSON::OID" => 0,
  "BSON::Raw" => 0,
  "BSON::Regex" => 0,
  "BSON::Time" => 0,
  "BSON::Timestamp" => 0,
  "BSON::Types" => 0,
  "Carp" => 0,
  "Class::XSAccessor" => 0,
  "Data::Dumper" => 0,
  "Digest::MD5" => 0,
  "Encode" => 0,
  "Exporter" => "5.57",
  "ExtUtils::MakeMaker" => 0,
  "File::Compare" => 0,
  "File::Spec" => 0,
  "File::Temp" => 0,
  "FileHandle" => 0,
  "IO::File" => 0,
  "IO::Socket" => 0,
  "JSON::MaybeXS" => 0,
  "List::Util" => 0,
  "MIME::Base64" => 0,
  "Math::BigInt" => 0,
  "Moo" => 2,
  "Moo::Role" => 0,
  "Net::DNS" => 0,
  "Path::Tiny" => "0.054",
  "Safe::Isa" => 0,
  "Scalar::Util" => 0,
  "Socket" => 0,
  "Sub::Quote" => 0,
  "Test::Deep" => "0.111",
  "Test::Fatal" => 0,
  "Test::More" => "0.96",
  "Tie::IxHash" => 0,
  "Time::HiRes" => 0,
  "Try::Tiny" => 0,
  "Type::Library" => 0,
  "Type::Tiny::XS" => 0,
  "Type::Utils" => 0,
  "Types::Standard" => 0,
  "UUID::URandom" => 0,
  "boolean" => "0.25",
  "bytes" => 0,
  "constant" => 0,
  "if" => 0,
  "lib" => 0,
  "namespace::clean" => 0,
  "overload" => 0,
  "re" => 0,
  "strict" => 0,
  "threads::shared" => 0,
  "utf8" => 0,
  "version" => 0,
  "warnings" => 0
);

unless ( eval { ExtUtils::MakeMaker->VERSION(6.63_03) } ) {
  delete $WriteMakefileArgs{TEST_REQUIRES};
  delete $WriteMakefileArgs{BUILD_REQUIRES};
  $WriteMakefileArgs{PREREQ_PM} = \%FallbackPrereqs;
}

delete $WriteMakefileArgs{CONFIGURE_REQUIRES}
  unless eval { ExtUtils::MakeMaker->VERSION(6.52) };

WriteMakefile(%WriteMakefileArgs);

package MY;

sub postamble {
    my $self = shift;
    return $self->SUPER::postamble . <<'HERE';

cover : pure_all
	HARNESS_PERL_SWITCHES=-MDevel::Cover make test

ptest : pure_all
	HARNESS_OPTIONS=j9 make test

HERE
}
