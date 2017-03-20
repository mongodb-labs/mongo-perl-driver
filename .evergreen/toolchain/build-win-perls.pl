#!/usr/bin/env perl
use v5.10;
use strict;
use warnings;
use version;
use Cwd 'getcwd';
use File::Path qw/mkpath rmtree/;
use File::Find qw/find/;
use HTTP::Tiny;
use JSON::PP;
use CPAN::Meta::YAML;

# helper subroutine

sub try_system {
    my @command = @_;
    say "\nRunning: @command\n";
    system(@command) and die "Aborting: '@command' failed";
}

sub fix_permissions {
    return unless -f;
    chmod 0777, $File::Find::name;
}

# constants

my $orig_dir     = getcwd();
my $unzip_dir    = "$orig_dir/perl";
my $manifest_url = "http://strawberryperl.com/releases.json";

my @perl_versions = qw(
  14.4
  16.3
  18.4
  20.3
  22.2
  24.0
);

my $target_arch = "MSWin32-x64-multi-thread";
my $ht          = HTTP::Tiny->new;

# Get manifest

my $response = $ht->get($manifest_url);

die
  "Failed to get Strawberry Perl manifest: $response->{status} $response->{reason}\n"
  unless $response->{success};
die "Strawberry Perl manifest was empty! Aborting.\n"
  unless length $response->{content};

my $manifest = eval { decode_json( $response->{content} ) };
die "Manifest failed to decode: $@\n" if $@;

my %url_index;

# Loop the manifest to index latest releases for a perl version

for my $h (@$manifest) {
    next unless $h->{archname} eq $target_arch;
    next unless exists $h->{edition}{portable};

    my ($version) = $h->{version} =~ m{^(\d+ \. \d+ \. \d+)}x;
    my $full_version = version->new( $h->{version} );

    # only take latest release of a given version
    if ( $url_index{$version} ) {
        next if $full_version < $url_index{$version}{full_version};
    }

    $url_index{$version} = {
        url          => $h->{edition}{portable}{url},
        full_version => $full_version
    };
}

# Retrieve perls
mkdir $unzip_dir or die $!;
chdir $unzip_dir or die $!;

for my $ver (@perl_versions) {
    # Download
    my $url  = $url_index{"5.$ver"}{url};
    my $file = "$ver.zip";
    say "Downloading: $url";
    my $response = $ht->mirror( $url, $file );
    if ( !$response->{success} ) {
        die "Failed to mirror 5.$ver: $response->{status} $response->{reason}\n";
    }

    # Unzip
    try_system( "unzip", "-q", "-d", "$unzip_dir/$ver", $file );

    # Remove zip
    unlink $file or die $!;

    # Fix portable.perl on old Strawberries
    my $portable = "$unzip_dir/$ver/portable.perl";
    chmod 0644, $portable;
    my $yaml = CPAN::Meta::YAML->read($portable);
    $yaml->write($portable);
    chmod 0444, $portable;

    # Fix executable bit permissions
    find( \&fix_permissions, map { "$unzip_dir/$ver/$_/bin" } qw/perl c/ );
}

chdir $orig_dir;

# tar up the perls
try_system("tar -czf perl.tar.gz perl");
