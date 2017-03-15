#!/usr/bin/env perl
use v5.10;
use strict;
use warnings;
use utf8;
use open qw/:std :utf8/;

# Get helpers
use FindBin qw($Bin);
use lib "$Bin/../lib";
use EvergreenConfig;

#--------------------------------------------------------------------------#
# Constants
#--------------------------------------------------------------------------#

# Limit tasks to certain operating systems
my $OS_FILTER = { os => [ 'rhel62', 'windows64' ] };

#--------------------------------------------------------------------------#
# Functions
#--------------------------------------------------------------------------#

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

sub calc_filter {
    my $opts = shift;

    my $filter = {%$OS_FILTER};

    # Server without auth/ssl should run on all perls
    # on rhel62 and windows
    return $filter
      if $opts->{topology} eq 'server'
      && $opts->{auth} eq 'noauth'
      && $opts->{ssl} eq 'nossl';

    # Everything else should run everywhere, but only on 14 and 24
    $filter->{perl} = [ qr/24\.\d+$/, qr/14\.\d+$/ ];

    return $filter;
}

sub generate_test_variations {

    my @topo_tests = (
        with_topology( server      => [ "noauth nossl", "auth nossl", "noauth ssl" ] ),
        with_topology( replica_set => [ "noauth nossl", "auth nossl" ] ),
        with_topology( sharded_cluster => [ "noauth nossl", "auth nossl" ] ),
    );

    my @matrix =
      map { with_version( $_ => \@topo_tests ) } qw/v2.4 v2.6 v3.0 v3.2 v3.4 latest/;

    return @matrix;
}

sub orch_test {
    my $args = shift;
    die 'orch_tests needs a hashref' unless ref $args eq 'HASH';
    my %opts = (
        version  => '3.4',
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

sub test {
    my %opts  = @_;
    my $name  = $opts{name} // 'unit_test';
    my $deps  = $opts{deps} // ['build'];
    my @extra = $opts{extra} ? @{ $opts{extra} } : ();
    return task(
        $name      => [ qw/whichPerl downloadBuildArtifacts/, @extra, 'testModule' ],
        depends_on => $deps,
        filter     => $opts{filter},
    );
}

sub test_name {
    my $args = shift;
    ( my $version = $args->{version} ) =~ s/^v//;
    return join( "_", "test", $version, @{$args}{qw/topology ssl auth/} );
}

sub with_key {
    my ( $key, $val, $templates ) = @_;
    return map {
        { $key => $val, %$_ }
    } @$templates;
}

sub with_version { return with_key( version => @_ ) }

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
    # Common tasks for all variants
    my $filter = {%$OS_FILTER};

    my $download = [ 'downloadPerl5Lib' => { target => '${repo_directory}' } ];

    my @tasks = (
        pre( qw/dynamicVars cleanUp fetchSource/, $download ),
        post(qw/teardownOrchestration cleanUp/),
        task( build => [qw/whichPerl buildModule uploadBuildArtifacts/], filter => $filter ),
        test( name => "check", filter => $filter ),
    );

    # Add orchestrated tests
    my @test_variations = generate_test_variations();
    push @tasks, map { orch_test($_) } @test_variations;

    # Generate config
    print assemble_yaml(
        ignore( "/.evergreen/dependencies", "/.evergreen/toolchain" ),
        timeout(1800), buildvariants( \@tasks ),
    );

    return 0;
}

# execution
exit main();
