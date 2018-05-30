#!/usr/bin/env perl
use v5.10;
use strict;
use warnings;
use utf8;
use version;
use open qw/:std :utf8/;

# Get helpers
use FindBin qw($Bin);
use lib "$Bin/../lib";
use EvergreenConfig;

#--------------------------------------------------------------------------#
# Constants
#--------------------------------------------------------------------------#

# $OS_FILTER is a filter definition to allow all operating systems
my $OS_FILTER =
  { os =>
      [ 'rhel62', 'windows64', 'suse12_z', 'ubuntu1604_power8' ] };
##      [ 'rhel62', 'windows64', 'suse12_z', 'ubuntu1604_arm64', 'ubuntu1604_power8' ] };

# This allows only the non-ZAP subset
my $NON_ZAP_OS_FILTER = { os => [ 'rhel62', 'windows64' ] };

#--------------------------------------------------------------------------#
# Functions
#--------------------------------------------------------------------------#

# calc_depends: Given an orchestration configuration, this calculates the
# tasks that must have run successfully before this one.

sub calc_depends {
    my $args = shift;
    state $plain = { ssl => 'nossl', auth => 'noauth' };
    my @depends;

    # if not topology=server, depend on server with same auth/ssl settings
    if ( $args->{topology} ne 'server' ) {
        push @depends, test_name( { %$args, topology => 'server' } );
    }

    # if auth or ssl, depend on same-topology/noauth/nossl
    if ( $args->{auth} eq 'auth' || $args->{ssl} eq 'ssl' ) {
        push @depends, test_name( { %$args, %$plain } );
    }

    return @depends ? \@depends : ['check'];
}

# calc_filter: Given a set of variables defining a orchestrated task's
# configuration, this generates a filter expression that limits the
# task to certain variants.

sub calc_filter {
    my $opts = shift;

    # ZAP should only run on MongoDB 3.4 or latest
    my $filter =
        $opts->{version} eq 'latest'                             ? {%$OS_FILTER}
      : version->new( $opts->{version} ) >= version->new("v3.4") ? {%$OS_FILTER}
      :                                                            {%$NON_ZAP_OS_FILTER};

    # Server without auth/ssl should run on all perls, so in that case,
    # we return existing filter with only an 'os' key.
    return $filter
      if $opts->{topology} eq 'server'
      && $opts->{auth} eq 'noauth'
      && $opts->{ssl} eq 'nossl';

    # Everything else should run on whatever 'os' subset is defined, but
    # only on 5.14 and 5.24 (default config).
    $filter->{perl} = [ qr/^24$/, qr/^14$/ ];

    return $filter;
}

# generate_test_variations: produce a list of orchestration configuration
# hashrefs

sub generate_test_variations {

    # We test every topology without auth/ssl and with auth, but without ssl.
    # For standalone, we also test with ssl but with no auth.
    my @topo_tests = (
        with_topology( server      => [ "noauth nossl", "auth nossl", "noauth ssl" ] ),
        with_topology( replica_set => [ "noauth nossl", "auth nossl" ] ),
        with_topology( sharded_cluster => [ "noauth nossl", "auth nossl" ] ),
    );

    # For the topology specific configs, we repeat the list for each server
    # version we're testing.
    my @matrix =
      map { with_version( $_ => \@topo_tests ) } qw/v2.6 v3.0 v3.2 v3.4 v3.6 v4.0 latest/;

    return @matrix;
}

# orch_test: given an orchestration config (e.g. from
# generate_test_variations), this produces a task hashref to run a test
# with that configuration.

sub orch_test {
    my $args = shift;
    die 'orch_tests needs a hashref' unless ref $args eq 'HASH';

    # Overwrite defaults with config
    my %opts = (
        version  => 'v4.0',
        topology => 'server',
        ssl      => 'nossl',
        auth     => 'noauth',
        %$args,
    );

    return test(
        name   => test_name( \%opts ),
        deps   => calc_depends( \%opts ),
        filter => calc_filter( \%opts ),
        extra  => [ [ 'setupOrchestration' => \%opts ] ],
    );
}

# test: creates a test task; this extends the 'task' config helper to
# interpose extra steps before actually testing the driver

sub test {
    my %opts  = @_;
    my $name  = $opts{name} // 'unit_test';
    my $deps  = $opts{deps} // ['build'];
    my @extra = $opts{extra} ? @{ $opts{extra} } : ();
    return task(
        $name      => [ qw/whichPerl downloadBuildArtifacts/, @extra, 'testDriver' ],
        depends_on => $deps,
        filter     => $opts{filter},
    );
}

# test_name: given an orchestration config, generates a task name from the
# config components

sub test_name {
    my $args = shift;
    ( my $version = $args->{version} ) =~ s/^v//;
    my @parts = ( "test", $version );
    push @parts, "DB"   if $args->{topology} eq 'server';
    push @parts, "RS"   if $args->{topology} eq 'replica_set';
    push @parts, "SC"   if $args->{topology} eq 'sharded_cluster';
    push @parts, "ssl"  if $args->{ssl} eq 'ssl';
    push @parts, "auth" if $args->{auth} eq 'auth';
    return join( "_", @parts );
}

# with_key: given an array ref of hashrefs, returns a list of copies of the
# input hashrefs but with a key/value prepended.

sub with_key {
    my ( $key, $val, $templates ) = @_;
    return map {
        { $key => $val, %$_ }
    } @$templates;
}

# with_version: given a 'version' value and an array ref of hashrefs, add
# the key 'version' and the value to the inputs

sub with_version { return with_key( version => @_ ) }

# with_topology: given a topology and a string with "X Y" where X
# represents "auth" or "noauth" and Y represents "ssl" or "nossl",
# constructs hashrefs with those three factors (topo, auth, ssl) as
# individual keys

sub with_topology {
    my ( $topo, $templates ) = @_;
    my @hashes;
    for my $t (@$templates) {
        my @parts = split " ", $t;
        push @hashes, { auth => $parts[0], ssl => $parts[1] };
    }
    return with_key( topology => $topo, \@hashes );
}

sub main {
    # Common tasks for all variants use this filter
    my $filter = {%$OS_FILTER};

    # repo_directory is replaced later from an Evergreen project variable.
    # It must go into the config.yml as '${repo_directory}'.  (I.e. this is
    # not a perl typo that fails to interpolate a variable.)
    my $download = [ 'downloadPerl5Lib' => { target => '${repo_directory}' } ];

    my @tasks = (
        pre( qw/dynamicVars cleanUp fetchSource/, $download ),
        post(qw/teardownOrchestration cleanUp/),
        task( build => [qw/whichPerl buildModule uploadBuildArtifacts/], filter => $filter ),
        test( name => "check", filter => $filter ),
    );

    # Add orchestrated tests. These will provide their own filter.
    my @test_variations = generate_test_variations();
    push @tasks, map { orch_test($_) } @test_variations;

    # Add Atlas proxy test (plus build/check deps on Ubuntu)
    my $atlas_filter = { os => ['ubuntu1604'], perl => [qr/^24$/] };
    push @tasks,
      task(
        build_for_atlas => [qw/whichPerl buildModule uploadBuildArtifacts/],
        filter          => $atlas_filter
      ),
      test(
        name   => "check_for_atlas",
        deps   => ['build_for_atlas'],
        filter => $atlas_filter
      ),
      test(
        name   => 'test_atlas',
        filter => $atlas_filter,
        deps   => ['build_for_atlas'],
        extra  => [qw/setupAtlasProxy testAtlasProxy/],
      );

    # Build filter to avoid "ld" Perls on Z-series
    my $variant_filter = sub {
        my ($os, $ver) = @_;
        return 0 if $os eq 'suse12_z' && $ver =~ m/ld$/;
        return 1;
    };

    # Generate config
    print assemble_yaml(
        ignore( "/.evergreen/dependencies", "/.evergreen/toolchain" ),
        timeout(1800), buildvariants( \@tasks, $variant_filter ),
    );

    return 0;
}

# execution
exit main();
