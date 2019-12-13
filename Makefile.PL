# Dist::Zilla::Plugin::MakeMaker::Awesome 0.48.
# Don't edit it but the dist.ini and plugins used to construct it.

use strict;
use warnings;

use 5.010001;
use ExtUtils::MakeMaker 6.17;

my %WriteMakefileArgs = (
  "ABSTRACT" => "Official MongoDB Driver for Perl",
  "AUTHOR" => "David Golden <david\@mongodb.com>, Rassi <rassi\@mongodb.com>, Mike Friedman <friedo\@friedo.com>, Kristina Chodorow <k.chodorow\@gmail.com>, Florian Ragwitz <rafl\@debian.org>",
  "CONFIGURE_REQUIRES" => {
    "ExtUtils::MakeMaker" => "6.17",
    "Text::ParseWords" => 0
  },
  "DISTNAME" => "MongoDB",
  "LICENSE" => "apache",
  "MIN_PERL_VERSION" => "5.010001",
  "NAME" => "MongoDB",
  "PREREQ_PM" => {
    "Authen::SASL::SASLprep" => 0,
    "Authen::SCRAM::Client" => "0.011",
    "BSON" => "1.012000",
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
    "Compress::Zlib" => 0,
    "Digest::MD5" => 0,
    "Encode" => 0,
    "Exporter" => "5.57",
    "IO::Socket" => 0,
    "List::Util" => 0,
    "MIME::Base64" => 0,
    "Math::BigInt" => 0,
    "Moo" => 2,
    "Moo::Role" => 0,
    "Net::DNS" => 0,
    "Safe::Isa" => "1.000007",
    "Scalar::Util" => 0,
    "Socket" => 0,
    "Sub::Defer" => 0,
    "Sub::Quote" => 0,
    "Tie::IxHash" => 0,
    "Time::HiRes" => 0,
    "Type::Library" => 0,
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
    "IO::File" => 0,
    "JSON::MaybeXS" => 0,
    "Path::Tiny" => "0.058",
    "Storable" => 0,
    "Test::Deep" => "0.111",
    "Test::Fatal" => 0,
    "Test::More" => "0.96",
    "lib" => 0,
    "utf8" => 0
  },
  "VERSION" => "v2.2.2",
  "test" => {
    "TESTS" => "t/*.t t/deprecated/*.t t/examples/*.t t/unit/*.t"
  }
);

my %FallbackPrereqs = (
  "Authen::SASL::SASLprep" => 0,
  "Authen::SCRAM::Client" => "0.011",
  "BSON" => "1.012000",
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
  "Compress::Zlib" => 0,
  "Data::Dumper" => 0,
  "Digest::MD5" => 0,
  "Encode" => 0,
  "Exporter" => "5.57",
  "ExtUtils::MakeMaker" => 0,
  "File::Compare" => 0,
  "File::Spec" => 0,
  "IO::File" => 0,
  "IO::Socket" => 0,
  "JSON::MaybeXS" => 0,
  "List::Util" => 0,
  "MIME::Base64" => 0,
  "Math::BigInt" => 0,
  "Moo" => 2,
  "Moo::Role" => 0,
  "Net::DNS" => 0,
  "Path::Tiny" => "0.058",
  "Safe::Isa" => "1.000007",
  "Scalar::Util" => 0,
  "Socket" => 0,
  "Storable" => 0,
  "Sub::Defer" => 0,
  "Sub::Quote" => 0,
  "Test::Deep" => "0.111",
  "Test::Fatal" => 0,
  "Test::More" => "0.96",
  "Tie::IxHash" => 0,
  "Time::HiRes" => 0,
  "Type::Library" => 0,
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
  "utf8" => 0,
  "version" => 0,
  "warnings" => 0
);

# inserted by Dist::Zilla::Plugin::DynamicPrereqs 0.035
if (!want_pp() && can_xs()) {
runtime_requires('Class::XSAccessor');
runtime_requires('Type::Tiny::XS');
}

# inserted by Dist::Zilla::Plugin::DynamicPrereqs 0.035
if (!want_pp() && can_xs() && ($^O ne "MSWin32" || do {my (undef,$major,undef,undef,$id)=Win32::GetOSVersion(); $id>2 || ($id==2 && $major>=6)})) {
runtime_requires('BSON::XS', '0.008000');
}

unless ( eval { ExtUtils::MakeMaker->VERSION('6.63_03') } ) {
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

package main;

# inserted by Dist::Zilla::Plugin::DynamicPrereqs 0.035
sub _add_prereq {
  my ($mm_key, $module, $version_or_range) = @_;
  $version_or_range ||= 0;
  warn "$module already exists in $mm_key (at version $WriteMakefileArgs{$mm_key}{$module}) -- need to do a sane metamerge!"
    if exists $WriteMakefileArgs{$mm_key}{$module}
      and $WriteMakefileArgs{$mm_key}{$module} ne '0'
      and $WriteMakefileArgs{$mm_key}{$module} ne $version_or_range;
  warn "$module already exists in FallbackPrereqs (at version $WriteMakefileArgs{$mm_key}{$module}) -- need to do a sane metamerge!"
    if exists $FallbackPrereqs{$module} and $FallbackPrereqs{$module} ne '0'
        and $FallbackPrereqs{$module} ne $version_or_range;
  $WriteMakefileArgs{$mm_key}{$module} = $FallbackPrereqs{$module} = $version_or_range;
  return;
}

use lib 'inc';
use ExtUtils::HasCompiler 0.014 'can_compile_loadable_object';
{
  my $can_xs;
  sub can_xs {
    return $can_xs if defined $can_xs;
    $can_xs = can_compile_loadable_object(quiet => 1) ? 1 : 0;
  }
}

{
  my $parsed_args;
  sub parse_args {
    return $parsed_args if defined $parsed_args;
    require ExtUtils::MakeMaker;
    require Text::ParseWords;
    ExtUtils::MakeMaker::parse_args(
      my $tmp = {},
      Text::ParseWords::shellwords($ENV{PERL_MM_OPT} || ''),
      @ARGV,
    );
    $parsed_args = $tmp->{ARGS} || {};
  }
}

sub runtime_requires {
  my ($module, $version_or_range) = @_;
  _add_prereq(PREREQ_PM => $module, $version_or_range);
}

{
  my $want_pp;
  sub want_pp {
    return $want_pp if defined $want_pp;
    $want_pp = parse_args()->{PUREPERL_ONLY} ? 1 : 0
  }
}
