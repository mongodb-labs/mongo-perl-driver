#
#  Copyright 2009-2013 MongoDB, Inc.
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
#

package MongoDB::MongoClient;

# ABSTRACT: A connection to a MongoDB server

use version;
our $VERSION = 'v0.704.4.1';

use Moose;
use MongoDB;
use MongoDB::Cursor;
use MongoDB::BSON::Binary;
use MongoDB::BSON::Regexp;
use MongoDB::Error;
use MongoDB::ReadPreference;
use MongoDB::WriteConcern;
use MongoDB::_Topology;
use MongoDB::_Credential;
use MongoDB::_URI;
use Digest::MD5;
use Tie::IxHash;
use Time::HiRes qw/usleep/;
use Carp 'carp', 'croak';
use Safe::Isa;
use Scalar::Util 'reftype';
use boolean;
use Encode;
use Try::Tiny;
use MongoDB::_Types;
use namespace::clean -except => 'meta';

use constant {
    PRIMARY             => 'primary',
    SECONDARY           => 'secondary',
    PRIMARY_PREFERRED   => 'primaryPreferred',
    SECONDARY_PREFERRED => 'secondaryPreferred',
    NEAREST             => 'nearest',
};

with 'MongoDB::Role::_Client', 'MongoDB::Role::_HasReadPreference';

#--------------------------------------------------------------------------#
# public attributes
#
# XXX too many of these are mutable
#--------------------------------------------------------------------------#

# connection attributes

has host => (
    is      => 'ro',
    isa     => 'Str',
    default => 'mongodb://localhost:27017', # XXX eventually, make this localhost
);

has port => (
    is      => 'ro',
    isa     => 'Int',
    default => 27017,
);

has connect_type => (
    is      => 'ro',
    isa     => 'ConnectType',
    builder => '_build_connect_type',
    lazy    => 1
);

has timeout => (
    is      => 'rw',
    isa     => 'Int',
    default => 20000,
);

has query_timeout => (
    is      => 'rw',
    isa     => 'Int',
    default => sub { return $MongoDB::Cursor::timeout; },
);

has ssl => (
    is      => 'rw',
    isa     => 'Bool|HashRef',
    default => 0,
);

# write concern attributes

has w => (
    is      => 'rw',
    isa     => 'Int|Str',
    default => 1,
    trigger => \&_update_write_concern,
);

has wtimeout => (
    is      => 'rw',
    isa     => 'Int',
    default => 1000,
    trigger => \&_update_write_concern,
);

has j => (
    is      => 'rw',
    isa     => 'Bool',
    default => 0,
    trigger => \&_update_write_concern,
);

# server selection attributes

has server_selection_timeout_ms => (
    is      => 'ro',
    isa     => 'Num',
    default => 30_000,
);

# authentication attributes

has username => (
    is      => 'rw',
    isa     => 'Str',
    lazy    => 1,
    builder => '_build_username',
);

has password => (
    is      => 'rw',
    isa     => 'Str',
    lazy    => 1,
    builder => '_build_password',
);

has db_name => (
    is      => 'rw',
    isa     => 'Str',
    lazy    => 1,
    builder => '_build_db_name',
);

has auth_mechanism => (
    is      => 'ro',
    isa     => 'AuthMechanism',
    lazy    => 1,
    builder => '_build_auth_mechanism',
    writer  => '_set_auth_mechanism',
);

has auth_mechanism_properties => (
    is      => 'ro',
    isa     => 'HashRef',
    lazy    => 1,
    builder => '_build_auth_mechanism_properties',
    writer  => '_set_auth_mechanism_properties',
);

# XXX deprecate this
has sasl => (
    is      => 'ro',
    isa     => 'Bool',
    default => 0
);

# XXX deprecate this
has sasl_mechanism => (
    is      => 'ro',
    isa     => 'AuthMechanism',
    default => 'GSSAPI',
);

# BSON conversion attributes

has dt_type => (
    is      => 'rw',
    default => 'DateTime'
);

has inflate_dbrefs => (
    is      => 'rw',
    isa     => 'Bool',
    default => 1
);

has inflate_regexps => (
    is      => 'rw',
    isa     => 'Bool',
    default => 0,
);

#--------------------------------------------------------------------------#
# deprecated public attributes
#--------------------------------------------------------------------------#

has auto_connect => (
    is      => 'ro',
    isa     => 'Bool',
    default => 1,
);

has auto_reconnect => (
    is      => 'ro',
    isa     => 'Bool',
    default => 1,
);

has find_master => (
    is      => 'ro',
    isa     => 'Bool',
    default => 0,
);

#--------------------------------------------------------------------------#
# private attributes
#--------------------------------------------------------------------------#

has _topology => (
    is         => 'ro',
    isa        => 'MongoDB::_Topology',
    lazy_build => 1,
    handles    => { topology_type => 'type' },
    clearer    => '_clear__topology',
);

has _credential => (
    is         => 'ro',
    isa        => 'MongoDB::_Credential',
    builder    => '_build__credential',
    lazy       => 1,
    writer     => '_set__credential',
);

has _min_wire_version => (
    is      => 'ro',
    isa     => 'Int',
    default => 0
);

has _max_wire_version => (
    is      => 'ro',
    isa     => 'Int',
    default => 3
);

has _uri => (
    is      => 'ro',
    isa     => 'MongoDB::_URI',
    lazy    => 1,
    builder => '_build__uri',
);

has _write_concern => (
    is     => 'ro',
    isa    => 'MongoDB::WriteConcern',
    writer => '_set_write_concern',
);

#--------------------------------------------------------------------------#
# builders
#--------------------------------------------------------------------------#

sub _build_auth_mechanism {
    my ($self) = @_;

    if ( $self->sasl ) {
        # XXX support deprecated legacy experimental API
        return $self->sasl_mechanism;
    }
    elsif ( my $mech = $self->_uri->options->{authMechanism} ) {
        return $mech;
    }
    elsif ( $self->username ) {
        return 'DEFAULT';
    }
    else {
        return 'NONE';
    }
}

sub _build_auth_mechanism_properties {
    my ($self) = @_;
    my $service_name = $self->_uri->options->{'authMechanism.SERVICE_NAME'};
    return {
        ( defined $service_name ? ( SERVICE_NAME => $service_name ) : () ),
    };
}

sub _build__topology {
    my ($self) = @_;

    my $type =
        $self->connect_type eq 'replicaSet' ? 'ReplicaSetNoPrimary'
      : $self->connect_type eq 'direct'     ? 'Single'
      :                                       'Unknown';

    MongoDB::_Topology->new(
        uri                         => $self->_uri,
        type                        => $type,
        server_selection_timeout_ms => $self->server_selection_timeout_ms,
        max_wire_version            => $self->_max_wire_version,
        min_wire_version            => $self->_min_wire_version,
        credential                  => $self->_credential,
        link_options                => {
            with_ssl   => !!$self->ssl,
            ( ref( $self->ssl ) eq 'HASH' ? ( SSL_options => $self->ssl ) : () ),
        },
    );
}

sub _build_connect_type {
    my ($self) = @_;
    return
      exists $self->_uri->options->{connect} ? $self->_uri->options->{connect} : 'none';
}

sub _build_db_name {
    my ($self) = @_;
    return $self->_uri->options->{authSource} || $self->_uri->db_name;
}

sub _build_password {
    my ($self) = @_;
    return $self->_uri->password;
}

sub _build_username {
    my ($self) = @_;
    return $self->_uri->username;
}

sub _build__credential {
    my ($self) = @_;
    my $mechanism = $self->auth_mechanism;
    my $cred = MongoDB::_Credential->new(
        mechanism            => $mechanism,
        mechanism_properties => $self->auth_mechanism_properties,
        ( $self->username ? ( username => $self->username ) : () ),
        ( $self->password ? ( password => $self->password ) : () ),
        ( $self->db_name  ? ( source   => $self->db_name )  : () ),
    );
    return $cred;
}

sub _build__uri {
    my ($self) = @_;
    if ( $self->host =~ m{^mongodb://} ) {
        return MongoDB::_URI->new( uri => $self->host );
    }
    else {
        my $uri = $self->host =~ /:\d+$/
                ? $self->host
                : sprintf("%s:%s", map { $self->$_ } qw/host port/ );
        return MongoDB::_URI->new( uri => ("mongodb://$uri") );
    }
}

sub BUILD {
    my ($self, $opts) = @_;

    my $uri = $self->_uri;

    my @addresses = @{ $uri->hostpairs };
    if ( $self->connect_type eq 'direct' && @addresses > 1 ) {
        confess "Connect type 'direct' cannot be used with multiple addresses: @addresses";
    }

    my $options = $uri->options;

    # Add options from URI
    $self->ssl(_str_to_bool($options->{ssl}))       if exists $options->{ssl};
    $self->timeout($options->{connectTimeoutMS})    if exists $options->{connectTimeoutMS};
    $self->w($options->{w})                         if exists $options->{w};
    $self->wtimeout($options->{wtimeoutMS})         if exists $options->{wtimeoutMS};
    $self->j(_str_to_bool($options->{journal}))     if exists $options->{journal};

    $self->_update_write_concern;

    $self->read_preference( $options->{readPreference}, $options->{readPreferenceTags} )
        if exists $options->{readPreference} || exists $options->{readPreferenceTags};

    # XXX this should be deprecated or removed
    if ($self->auto_connect) {
        $self->connect;
    }
}

#--------------------------------------------------------------------------#
# helper functions
#--------------------------------------------------------------------------#

sub _str_to_bool {
    my $str = shift;
    confess "cannot convert undef to bool" unless defined $str;
    my $ret = $str eq "true" ? 1 : $str eq "false" ? 0 : undef;
    return $ret unless !defined $ret;
    confess "expected boolean string 'true' or 'false' but instead received '$str'";
}

sub _use_write_cmd {
    my ($link) = @_;
    return $link->min_wire_version <= 2 && 2 <= $link->max_wire_version;
}

#--------------------------------------------------------------------------#
# public methods - network communication and wire protocol
#--------------------------------------------------------------------------#

sub connect {
    my ($self) = @_;
    $self->_topology->scan_all_servers;
    return 1;
}

sub disconnect {
    my ($self) = @_;
    $self->_topology->close_all_links;
    return 1;
}

sub send_admin_command {
    my ($self, $command, $flags, $read_preference) = @_;

    $read_preference ||= MongoDB::ReadPreference->new;
    my $link = $self->_topology->get_readable_link( $read_preference );
    my $query = MongoDB::_Query->new( spec => $command );
    $self->_apply_read_prefs( $link, $query, $flags, $read_preference );

    return $self->_try_operation('_send_admin_command', $link, $query->spec, $flags );
}

sub send_command {
    my ($self, $db, $command, $flags, $read_preference) = @_;

    $read_preference ||= MongoDB::ReadPreference->new;
    my $link = $self->_topology->get_readable_link( $read_preference );
    my $query = MongoDB::_Query->new( spec => $command );
    $self->_apply_read_prefs( $link, $query, $flags, $read_preference );

    return $self->_try_operation('_send_command', $link, $db, $query->spec, $flags );
}

sub send_delete {
    my ( $self, $ns, $op_doc, $write_concern ) = @_;
    # $op_doc is { q: $query, limit: $limit }

    $write_concern ||= $self->_write_concern;
    my $link = $self->_topology->get_writable_link;

    return $self->_try_operation('_send_delete', $link, $ns, $op_doc, $write_concern );
}

sub send_get_more {
    my ( $self, $address, $ns, $cursor_id, $size ) = @_;

    my $link = $self->_topology->get_specific_link( $address );

    return $self->_try_operation('_send_get_more', $link, $ns, $cursor_id, $size, $self );
}

sub send_insert {
    my ( $self, $ns, $docs, $write_concern, $flags, $check_keys ) = @_;

    $docs = [ $docs ] unless ref $docs eq 'ARRAY'; # XXX from BulkWrite

    $write_concern ||= $self->_write_concern;
    my $link = $self->_topology->get_writable_link;

    return $self->_try_operation('_send_insert', $link, $ns, $docs, $flags, $check_keys, $write_concern );
}

sub send_kill_cursors {
    my ( $self, $address, @cursors ) = @_;

    my $link = $self->_topology->get_specific_link( $address );

    return $self->_try_operation('_send_kill_cursors', $link, @cursors );
}

sub send_update {
    my ( $self, $ns, $op_doc, $write_concern ) = @_;
    # $op_doc is { q: $query, u: $update, multi: $multi, upsert: $upsert }

    $write_concern ||= $self->_write_concern;
    my $link = $self->_topology->get_writable_link;

    return $self->_try_operation('_send_update', $link, $ns, $op_doc, $write_concern );
}

# XXX eventually, passing $self to _send_query should go away and we should
# pass in a BSON codec object
sub send_query {
    my ($self, $ns, $query, $fields, $skip, $limit, $batch_size, $flags, $read_preference) = @_;

    $read_preference ||= $self->_read_preference || MongoDB::ReadPreference->new;
    my $link = $self->_topology->get_readable_link( $read_preference );

    $self->_apply_read_prefs( $link, $query, $flags, $read_preference );

    return $self->_try_operation('_send_query', $link, $ns, $query->spec, $fields, $skip, $limit, $batch_size, $flags, $self );
}

# variants is a hash of wire protocol version to coderef
sub send_versioned_read {
    my ( $self, $variants, $read_preference ) = @_;

    $read_preference ||= MongoDB::ReadPreference->new;
    my $link = $self->_topology->get_readable_link($read_preference);

    # try highest protocol versions first
    for my $version ( sort { $b <=> $a } keys %$variants ) {
        if ( $link->accepts_wire_version($version) ) {
            return $variants->{$version}->($self, $link);
        }
    }

    MongoDB::Error->throw(
        sprintf(
            "Wire protocol error: server %s selected but doesn't accept protocol(s) %",
            join( ", ", keys %$variants ),
            $link->address
        )
    );
}

sub _apply_read_prefs {
    my ( $self, $link, $query, $flags, $read_preference ) = @_;

    if ( $link->server->type eq 'Mongos' ) {
        if ( $read_preference->has_empty_tag_sets ) {
            if ( $read_preference->mode eq 'primary' ) {
                $flags->{slave_ok} = 0;
            }
            elsif ( $read_preference->mode eq 'secondaryPreferred' ) {
                $flags->{slave_ok} = 1;
            }
            else {
                $query->set_modifier( '$readPreference' => $read_preference->for_mongos );
            }
        }
        else {
            $query->set_modifier( '$readPreference' => $read_preference->for_mongos );
        }
    }
    else {
        $flags->{slave_ok} = 1 if $read_preference->mode ne 'primary';
    }
}

sub _try_operation {
    my ($self, $method, $link, @args) = @_;

    my $result = try {
        $self->$method($link, @args);
    }
    catch {
        if ( $_->$_isa("MongoDB::ConnectionError") ) {
            $self->_topology->mark_server_unknown( $link->server, $_ );
        }
        elsif ( $_->$_isa("MongoDB::NotMasterError") ) {
            $self->_topology->mark_server_unknown( $link->server, $_ );
            $self->_topology->mark_stale;
        }
        # regardless of cleanup, rethrow the error
        die $_;
    };

    return $result;
}

#--------------------------------------------------------------------------#
# bulk operations
#--------------------------------------------------------------------------#

# XXX this can be a wrapper to grab link, write concern, and sizes off the
# link and then move the bulk of this code to _Client

sub send_bulk_queue {
    my ($self, %args) = @_;

    my $ns = $args{ns};
    my $queue = $args{queue} || [];
    my $ordered = $args{ordered};
    my $write_concern = $args{write_concern} || $self->_write_concern;

    my $link = $self->_topology->get_writable_link;

    my $use_write_cmd = _use_write_cmd($link);

    # If using legacy write ops, then there will never be a valid nModified
    # result so we set that to undef in the constructor; otherwise, we set it
    # to 0 so that results accumulate normally. If a mongos on a mixed topology
    # later fails to set it, results merging will handle it that case.
    my $result = MongoDB::WriteResult->new( nModified => $use_write_cmd ? 0 : undef, );

    for my $batch ( $ordered ? $self->_batch_ordered($link, $queue) : $self->_batch_unordered($link, $queue) ) {
        if ($use_write_cmd) {
            $self->_execute_write_command_batch( $link, $ns, $batch, $result, $ordered, $write_concern );
        }
        else {
            $self->_execute_legacy_batch( $link, $ns, $batch, $result, $ordered, $write_concern );
        }
    }

    # only reach here with an error for unordered bulk ops
    $result->assert_no_write_error;

    # write concern errors are thrown only for the entire batch
    $result->assert_no_write_concern_error;

    return $result;
}

my %OP_MAP = (
    insert => [ insert => 'documents' ],
    update => [ update => 'updates' ],
    delete => [ delete => 'deletes' ],
);

# _execute_write_command_batch may split batches if they are too large and
# execute them separately

sub _execute_write_command_batch {
    my ( $self, $link, $ns, $batch, $result, $ordered, $write_concern ) = @_;

    my ( $type, $docs )   = @$batch;
    my ( $cmd,  $op_key ) = @{ $OP_MAP{$type} };

    my $boolean_ordered = $ordered ? boolean::true : boolean::false;
    my ($db_name, $coll_name) = $ns =~ m{^([^.]+)\.(.*)$};

    my @left_to_send = ($docs);

    while (@left_to_send) {
        my $chunk = shift @left_to_send;

        my $cmd_doc = [
            $cmd    => $coll_name,
            $op_key => $chunk,
            ordered => $boolean_ordered,
            ( $write_concern ? ( writeConcern => $write_concern->as_struct ) : () )
        ];

        my $cmd_result = try {
            $self->send_command($db_name, $cmd_doc);
        }
        catch {
            if ( $_->$_isa("MongoDB::_CommandSizeError") ) {
                if ( @$chunk == 1 ) {
                    MongoDB::DocumentSizeError->throw(
                        message  => "document too large",
                        document => $chunk->[0],
                    );
                }
                else {
                    unshift @left_to_send, $self->_split_chunk( $link, $chunk, $_->size );
                }
            }
            else {
                die $_;
            }
            return;
        };

        redo unless $cmd_result; # restart after a chunk split

        my $r = MongoDB::WriteResult->_parse(
            op       => $type,
            op_count => scalar @$chunk,
            result   => $cmd_result,
        );

        # append corresponding ops to errors
        if ( $r->count_writeErrors ) {
            for my $error ( @{ $r->writeErrors } ) {
                $error->{op} = $chunk->[ $error->{index} ];
                # convert boolean::true|false back to 1 or 0
                for my $k (qw/upsert multi/) {
                    $error->{op}{$k} = 0+ $error->{op}{$k} if exists $error->{op}{$k};
                }
            }
        }

        $result->_merge_result($r);
        $result->assert_no_write_error if $ordered;
    }

    return;
}

sub _split_chunk {
    my ( $self, $link, $chunk, $size ) = @_;

    my $max_wire_size = $self->MAX_BSON_WIRE_SIZE; # XXX blech

    my $avg_cmd_size       = $size / @$chunk;
    my $new_cmds_per_chunk = int( $max_wire_size / $avg_cmd_size );

    my @split_chunks;
    while (@$chunk) {
        push @split_chunks, [ splice( @$chunk, 0, $new_cmds_per_chunk ) ];
    }

    return @split_chunks;
}

sub _batch_ordered {
    my ($self, $link, $queue) = @_;
    my @batches;
    my $last_type = '';
    my $count     = 0;

    my $max_batch_count = $link->max_write_batch_size;

    for my $op ( @$queue ) {
        my ( $type, $doc ) = @$op;
        if ( $type ne $last_type || $count == $max_batch_count ) {
            push @batches, [ $type => [$doc] ];
            $last_type = $type;
            $count     = 1;
        }
        else {
            push @{ $batches[-1][-1] }, $doc;
            $count++;
        }
    }

    return @batches;
}

sub _batch_unordered {
    my ($self, $link, $queue) = @_;
    my %batches = map { ; $_ => [ [] ] } keys %OP_MAP;

    my $max_batch_count = $link->max_write_batch_size;

    for my $op ( @$queue ) {
        my ( $type, $doc ) = @$op;
        if ( @{ $batches{$type}[-1] } == $max_batch_count ) {
            push @{ $batches{$type} }, [$doc];
        }
        else {
            push @{ $batches{$type}[-1] }, $doc;
        }
    }

    # insert/update/delete are guaranteed to be in random order on Perl 5.18+
    my @batches;
    for my $type ( grep { scalar @{ $batches{$_}[-1] } } keys %batches ) {
        push @batches, map { [ $type => $_ ] } @{ $batches{$type} };
    }
    return @batches;
}

sub _execute_legacy_batch {
    my ( $self, $link, $ns, $batch, $result, $ordered, $write_concern ) = @_;
    my ( $type, $docs ) = @$batch;

    my $method = "send_$type";

    # if write concern is not safe, we have to proxy with a safe one so that
    # we can interrupt ordered bulks, even while ignoring the actual error
    my $w_0;
    if ( ! $write_concern->is_safe ) {
        my $wc = $write_concern->as_struct;
        $wc->{w} = 1;
        $w_0 = MongoDB::WriteConcern->new( $wc );
    }

    # XXX successive inserts ought to get batched up, up to the max size for batch,
    # but we have no feedback on max size to know how many to put together. I wonder
    # if send_insert should return a list of write results, or if it should just
    # strip out however many docs it can from an arrayref and leave the rest, and
    # then this code can iterate.

    for my $doc (@$docs) {

        # legacy server doesn't check keys on insert; we fake an error if it happens
        if ( $type eq 'insert' && ( my $r = $self->_check_no_dollar_keys($doc) ) ) {
            if ($w_0) {
                last if $ordered;
            }
            else {
                $result->_merge_result($r);
                $result->assert_no_write_error if $ordered;
            }
            next;
        }

        my $gle_result = try {
            $self->$method( $ns, $doc, $w_0 ? $w_0 : $write_concern );
        }
        catch {
            die $_ unless $w_0 && /exceeds maximum size/;
            undef;
        };

        # Even for {w:0}, if the batch is ordered we have to break on the first
        # error, but we don't throw the error to the user.
        if ( $w_0 ) {
            last if $ordered && (!$gle_result || $gle_result->count_writeErrors);
        }
        else {
            $result->_merge_result($gle_result);
            $result->assert_no_write_error if $ordered;
        }
    }

    return;
}

sub _check_no_dollar_keys {
    my ( $self, $doc ) = @_;

    my @keys = ref $doc eq 'Tie::IxHash' ? $doc->Keys : keys %$doc;
    if ( my @bad = grep { substr( $_, 0, 1 ) eq '$' } @keys ) {
        my $errdoc = {
            index  => 0,
            errmsg => "Document can't have '\$' prefixed field names: @bad",
            code   => UNKNOWN_ERROR
        };

        return MongoDB::WriteResult->new(
            op_count    => 1,
            nModified   => undef,
            writeErrors => [$errdoc]
        );
    }

    return;
}


#--------------------------------------------------------------------------#
# authentication methods
#--------------------------------------------------------------------------#

=method authenticate (DEPRECATED)

    $client->authenticate($dbname, $username, $password, $is_digest);

B<This legacy method is deprecated but kept for backwards compatibility.>

Instead, authentication credentials should be provided as constructor arguments
or as part of the connection URI.

When C<authenticate> is called, it disconnects the client (if any connections
had been made), sets client attributes as if the username and password had been
used initially in the client constructor, and reconnects to the configured
servers.  The authentication mechanism will be MONGO-CR for servers before
version 2.8 and SCRAM-SHA-1 for 2.8 or later.

Passwords are expected to be cleartext and will be automatically hashed before
sending over the wire, unless C<$is_digest> is true, which will assume you
already did the proper hashing yourself.

See also the L</AUTHENTICATION> section.

=cut

sub authenticate {
    my ( $self, $db_name, $username, $password, $is_digest ) = @_;

    # set client properties
    $self->_set_auth_mechanism('DEFAULT');
    $self->_set_auth_mechanism_properties( {} );
    $self->db_name($db_name);
    $self->username($username);
    $self->password($password);

    my $cred = MongoDB::_Credential->new(
        mechanism            => $self->auth_mechanism,
        mechanism_properties => $self->auth_mechanism_properties,
        username             => $self->username,
        password             => $self->password,
        source               => $self->db_name,
        pw_is_digest         => $is_digest,
    );
    $self->_set__credential($cred);

    # ensure that we've authenticated by clearing the topology and trying a
    # command that opens a socket
    $self->_clear__topology;
    $self->send_admin_command( { ismaster => 1 } );

    return 1;
}

#--------------------------------------------------------------------------#
# write concern methods
#--------------------------------------------------------------------------#

sub _update_write_concern {
    my ($self) = @_;
    my $wc = MongoDB::WriteConcern->new(
        w        => $self->w,
        wtimeout => $self->wtimeout,
        ( $self->j ? ( j => $self->j ) : () ),
    );
    $self->_set_write_concern($wc);
}

#--------------------------------------------------------------------------#
# database helper methods
#--------------------------------------------------------------------------#

sub database_names {
    my ($self) = @_;

    my @databases;
    my $max_tries = 3;
    for my $try ( 1 .. $max_tries ) {
        last if try {
            my $result = $self->get_database('admin')->_try_run_command({ listDatabases => 1 });
            if (ref($result) eq 'HASH' && exists $result->{databases}) {
                @databases = map { $_->{name} } @{ $result->{databases} };
            }
            return 1;
        } catch {
            # can't open db in a read lock
            return if $_->{result}->{result}{code} == CANT_OPEN_DB_IN_READ_LOCK() || $try < $max_tries;
            die $_;
        };
    }

    return @databases;
}

sub get_database {
    my ($self, $database_name) = @_;
    return MongoDB::Database->new(
        _client     => $self,
        name        => $database_name,
    );
}

sub fsync {
    my ($self, $args) = @_;

    $args ||= {};

    # Pass this in as array-ref to ensure that 'fsync => 1' is the first argument.
    return $self->get_database('admin')->run_command([fsync => 1, %$args]);
}

sub fsync_unlock {
    my ($self) = @_;

    # Have to fetch from a special collection to unlock.
    return $self->get_database('admin')->get_collection('$cmd.sys.unlock')->find_one();
}

__PACKAGE__->meta->make_immutable( inline_destructor => 0 );

1;


__END__

=pod

=head1 SYNOPSIS

    use strict;
    use warnings;
    use MongoDB;

    # connects to localhost:27017
    my $client = MongoDB::MongoClient->new;

    my $db = $client->get_database("test");

=head1 DESCRIPTION

The C<MongoDB::MongoClient> class creates a client connection to one or
more MongoDB servers.

By default, it connects to a single server running on the local machine
listening on the default port 27017:

    # connects to localhost:27017
    my $client = MongoDB::MongoClient->new;

It can connect to a database server running anywhere, though:

    my $client = MongoDB::MongoClient->new(host => 'example.com:12345');

See the L</"host"> section for more options for connecting to MongoDB.

MongoDB can be started in I<authentication mode>, which requires clients to log in
before manipulating data.  By default, MongoDB does not start in this mode, so no
username or password is required to make a fully functional connection.  If you
would like to learn more about authentication, see the L</AUTHENTICATE> section.

Connecting is relatively expensive, so try not to open superfluous connections.

There is no way to explicitly disconnect from the database.  However, the
connection will automatically be closed and cleaned up when no references to
the C<MongoDB::MongoClient> object exist, which occurs when C<$client> goes out of
scope (or earlier if you undefine it with C<undef>).

=head1 AUTHENTICATION

The MongoDB server provides several authentication mechanisms, though some
are only available in the Enterprise edition.

MongoDB client authentication is controlled via the L</auth_mechanism>
attribute, which takes one of the following values:

=for :list
* MONGODB-CR -- legacy username-password challenge-response
* SCRAM-SHA-1 -- secure username-password challenge-response (2.8+)
* MONGODB-X509 -- SSL client certificate authentication (2.6+)
* PLAIN -- LDAP authentication via SASL PLAIN (Enterprise only)
* GSSAPI -- Kerberos authentication (Enterprise only)

The mechanism to use depends on the authentication configuration of the
server.  See the core documentation on authentication:
L<http://docs.mongodb.org/manual/core/access-control/>.

Usage information for each mechanism is given below.

=head2 MONGODB-CR and SCRAM-SHA-1 (for username/password)

These mechnisms require a username and password, given either as
constructor attributes or in the C<host> connection string.

If a username is provided and an authentication mechanism is not specified,
the client will use SCRAM-SHA-1 for version 2.8 or later servers and will
fall back to MONGODB-CR for older servers.

    my $mc = MongoDB::MongoClient->new(
        host => "mongodb://mongo.example.com/",
        username => "johndoe",
        password => "trustno1",
    );

    my $mc = MongoDB::MongoClient->new(
        host => "mongodb://johndoe:trustno1@mongo.example.com/",
    );

Usernames and passwords will be UTF-8 encoded before use.  The password is
never sent over the wire -- only a secure digest is used.  The SCRAM-SHA-1
mechanism is the Salted Challenge Response Authentication Mechanism
definedin L<RFC 5802|http://tools.ietf.org/html/rfc5802>.

The default database for authentication is 'admin'.  If another database
name should be used, specify it with the C<db_name> attribute or via the
connection string.

    db_name => auth_db

    mongodb://johndoe:trustno1@mongo.example.com/auth_db

=head2 MONGODB-X509 (for SSL client certificate)

X509 authentication requires SSL support (L<IO::Socket::SSL>) and requires
that a client certificate be configured and that the username attribute be
set to the "Subject" field, formatted according to RFC 2253.  To find the
correct username, run the C<openssl> program as follows:

  $ openssl x509 -in certs/client.pem -inform PEM -subject -nameopt RFC2253
  subject= CN=XXXXXXXXXXX,OU=XXXXXXXX,O=XXXXXXX,ST=XXXXXXXXXX,C=XX

In this case the C<username> attribute would be
C<CN=XXXXXXXXXXX,OU=XXXXXXXX,O=XXXXXXX,ST=XXXXXXXXXX,C=XX>.

Configure your client with the correct username and ssl parameters, and
specify the "MONGODB-X509" authentication mechanism.

    my $mc = MongoDB::MongoClient->new(
        host => "mongodb://sslmongo.example.com/",
        ssl => {
            SSL_ca_file   => "certs/ca.pem",
            SSL_cert_file => "certs/client.pem",
        },
        auth_mechanism => "MONGODB-X509",
        username       => "CN=XXXXXXXXXXX,OU=XXXXXXXX,O=XXXXXXX,ST=XXXXXXXXXX,C=XX"
    );

=head2 PLAIN (for LDAP)

This mechanism requires a username and password, which will be UTF-8
encoded before use.  The C<auth_mechanism> parameter must be given as a
constructor attribute or in the C<host> connection string:

    my $mc = MongoDB::MongoClient->new(
        host => "mongodb://mongo.example.com/",
        username => "johndoe",
        password => "trustno1",
        auth_mechanism => "PLAIN",
    );

    my $mc = MongoDB::MongoClient->new(
        host => "mongodb://johndoe:trustno1@mongo.example.com/authMechanism=PLAIN",
    );

=head2 GSSAPI (for Kerberos)

Kerberos authentication requires the CPAN module L<Authen::SASL> and a
GSSAPI-capable backend.

On Debian systems, L<Authen::SASL> may be available as
C<libauthen-sasl-perl>; on RHEL systems, it may be available as
C<perl-Authen-SASL>.

The L<Authen::SASL::Perl> backend comes with L<Authen::SASL> and requires
the L<GSSAPI> CPAN module for GSSAPI support.  On Debian systems, this may
be available as C<libgssapi-perl>; on RHEL systems, it may be available as
C<perl-GSSAPI>.

Installing the L<GSSAPI> module from CPAN rather than an OS package
requires C<libkrb5> and the C<krb5-config> utility (available for
Debian/RHEL systems in the C<libkrb5-dev> package).

Alternatively, the L<Authen::SASL::XS> or L<Authen::SASL::Cyrus> modules
may be used.  Both rely on Cyrus C<libsasl>.  L<Authen::SASL::XS> is
preferred, but not yet available as an OS package.  L<Authen::SASL::Cyrus>
is available on Debian as C<libauthen-sasl-cyrus-perl> and on RHEL as
C<perl-Authen-SASL-Cyrus>.

Installing L<Authen::SASL::XS> or L<Authen::SASL::Cyrus> from CPAM requires
C<libsasl>.  On Debian systems, it is available from C<libsasl2-dev>; on
RHEL, it is available in C<cyrus-sasl-devel>.

To use the GSSAPI mechanism, first run C<kinit> to authenticate with the ticket
granting service:

    $ kinit johndoe@EXAMPLE.COM

Configure MongoDB::MongoClient with the principal name as the C<username>
parameter and specify 'GSSAPI' as the C<auth_mechanism>:

    my $mc = MongoDB::MongoClient->new(
        host => 'mongodb://mongo.example.com',
        username => 'johndoe@EXAMPLE.COM',
        auth_mechanism => 'GSSAPI',
    );

Both can be specified in the C<host> connection string, keeping in mind
that the '@' in the principal name must be encoded as "%40":

    my $mc = MongoDB::MongoClient->new(
        host =>
          'mongodb://johndoe%40EXAMPLE.COM@mongo.examplecom/?authMechanism=GSSAPI',
    );

The default service name is 'mongodb'.  It can be changed with the
C<auth_mechanism_properties> attribute or in the connection string.

    auth_mechanism_properties => { SERVICE_NAME => 'other_service' }

    mongodb://.../?authMechanism=GSSAPI&authMechanism.SERVICE_NAME=other_service

=head1 MULTITHREADING

Existing connections are closed when a thread is created.  If C<auto_reconnect>
is true, then connections will be re-established as needed.

=head1 CONNECTION STRING URI

Core documentation on connections: L<http://docs.mongodb.org/manual/reference/connection-string/>.

The currently supported connection string options are:

=for :list
*authMechanism
*authMechanism.SERVICE_NAME
*connect
*connectTimeoutMS
*journal
*readPreference
*readPreferenceTags
*ssl
*w
*wtimeoutMS


=attr host

Server or servers to connect to. Defaults to C<mongodb://localhost:27017>.

To connect to more than one database server, use the format:

    mongodb://host1[:port1][,host2[:port2],...[,hostN[:portN]]]

An arbitrary number of hosts can be specified.

The connect method will return success if it can connect to at least one of the
hosts listed.  If it cannot connect to any hosts, it will die.

If a port is not specified for a given host, it will default to 27017. For
example, to connecting to C<localhost:27017> and C<localhost:27018>:

    my $client = MongoDB::MongoClient->new("host" => "mongodb://localhost,localhost:27018");

This will succeed if either C<localhost:27017> or C<localhost:27018> are available.

The connect method will also try to determine who is the primary if more than one
server is given.  It will try the hosts in order from left to right.  As soon as
one of the hosts reports that it is the primary, the connect will return success.  If
no hosts report themselves as a primary, the connect will die.

If username and password are given, success is conditional on being able to log
into the database as well as connect.  By default, the driver will attempt to
authenticate with the admin database.  If a different database is specified
using the C<db_name> property, it will be used instead.

=attr w

The client I<write concern>.

=over 4

=item * C<-1> Errors ignored. Do not use this.

=item * C<0> Unacknowledged. MongoClient will B<NOT> wait for an acknowledgment that
the server has received and processed the request. Older documentation may refer
to this as "fire-and-forget" mode. You must call C<getLastError> manually to check
if a request succeeds. This option is not recommended.

=item * C<1> Acknowledged. This is the default. MongoClient will wait until the
primary MongoDB acknowledges the write.

=item * C<2> Replica acknowledged. MongoClient will wait until at least two
replicas (primary and one secondary) acknowledge the write. You can set a higher
number for more replicas.

=item * C<all> All replicas acknowledged.

=item * C<majority> A majority of replicas acknowledged.

=back

In MongoDB v2.0+, you can "tag" replica members. With "tagging" you can specify a
new "getLastErrorMode" where you can create new
rules on how your data is replicated. To used you getLastErrorMode, you pass in the
name of the mode to the C<w> parameter. For more information see:
http://www.mongodb.org/display/DOCS/Data+Center+Awareness

=attr wtimeout

The number of milliseconds an operation should wait for C<w> slaves to replicate
it.

Defaults to 1000 (1 second).

See C<w> above for more information.

=attr j

If true, the client will block until write operations have been committed to the
server's journal. Prior to MongoDB 2.6, this option was ignored if the server was
running without journaling. Starting with MongoDB 2.6, write operations will fail
if this option is used when the server is running without journaling.

=attr auto_reconnect

Boolean indicating whether or not to reconnect if the connection is
interrupted. Defaults to C<1>.

=attr auto_connect

Boolean indication whether or not to connect automatically on object
construction. Defaults to C<1>.

=attr timeout

Connection timeout in milliseconds. Defaults to C<20000>.

=attr username

Username for this client connection.  Optional.  If this and the password field are
set, the client will attempt to authenticate on connection/reconnection.

=attr password

Password for this connection.  Optional.  If this and the username field are
set, the client will attempt to authenticate on connection/reconnection.

=attr db_name

Database to authenticate on for this connection.  Optional.  If this, the
username, and the password fields are set, the client will attempt to
authenticate against this database on connection/reconnection.  Defaults to
"admin".

=attr query_timeout

    # set query timeout to 1 second
    my $client = MongoDB::MongoClient->new(query_timeout => 1000);

    # set query timeout to 6 seconds
    $client->query_timeout(6000);

This will cause all queries (including C<find_one>s and C<run_command>s) to die
after this period if the database has not responded.

This value is in milliseconds and defaults to the value of
L<MongoDB::Cursor/timeout>.

    $MongoDB::Cursor::timeout = 5000;
    # query timeout for $conn will be 5 seconds
    my $client = MongoDB::MongoClient->new;

A value of -1 will cause the driver to wait forever for responses and 0 will
cause it to die immediately.

This value overrides L<MongoDB::Cursor/timeout>.

    $MongoDB::Cursor::timeout = 1000;
    my $client = MongoDB::MongoClient->new(query_timeout => 10);
    # timeout for $conn is 10 milliseconds

=attr max_bson_size

This is the largest document, in bytes, storable by MongoDB. The driver queries
MongoDB on connection to determine this value.  It defaults to 4MB.

=attr find_master

If this is true, the driver will attempt to find a primary given the list of
hosts.  The primary-finding algorithm looks like:

    for host in hosts

        if host is the primary
             return host

        else if host is a replica set member
            primary := replica set's primary
            return primary

If no primary is found, the connection will fail.

If this is not set (or set to the default, 0), the driver will simply use the
first host in the host list for all connections.  This can be useful for
directly connecting to secondaries for reads.

If you are connecting to a secondary, you should read
L<MongoDB::Cursor/slave_okay>.

You can use the C<ismaster> command to find the members of a replica set:

    my $result = $db->run_command({ismaster => 1});

The primary and secondary hosts are listed in the C<hosts> field, the slaves are
in the C<passives> field, and arbiters are in the C<arbiters> field.

=attr ssl

    ssl => 1
    ssl => \%ssl_options

This tells the driver that you are connecting to an SSL mongodb instance.

You must have L<IO::Socket::SSL> 1.42+ and L<Net::SSLeay> 1.49+ installed for
SSL support.

The C<ssl> attribute takes either a boolean value or a hash reference of
options to pass to IO::Socket::SSL.  For example, to set a CA file to validate
the server certificate and set a client certificate for the server to validate,
you could set the attribute like this:

    ssl => {
        SSL_ca_file   => "/path/to/ca.pem",
        SSL_cert_file => "/path/to/client.pem",
    }

If C<SSL_ca_file> is not provided, server certificates are verified against a
default list of CAs, either L<Mozilla::CA> or an operating-system-specific
default CA file.  To disable verification, you can use
C<< SSL_verify_mode => 0x00 >>.

B<You are strongly encouraged to use your own CA file for increased security>.

Server hostnames are also validated against the CN name in the server
certificate using C<< SSL_verifycn_scheme => 'default' >>.  You can use the
scheme 'none' to disable this check.

B<Disabling certificate or hostname verification is a security risk and is not
recommended>.

=attr sasl

This attribute is experimental.

If set to C<1>, the driver will attempt to negotiate SASL authentication upon
connection. See L</sasl_mechanism> for a list of the currently supported mechanisms. The
driver must be built as follows for SASL support:

    perl Makefile.PL --sasl
    make
    make install

Alternatively, you can set the C<PERL_MONGODB_WITH_SASL> environment variable before
installing:

    PERL_MONGODB_WITH_SASL=1 cpan MongoDB

The C<libgsasl> library is required for SASL support. RedHat/CentOS users can find it
in the EPEL repositories.

Future versions of this driver may switch to L<Cyrus SASL|http://www.cyrusimap.org/docs/cyrus-sasl/2.1.25/>
in order to be consistent with the MongoDB server, which now uses Cyrus.

=attr sasl_mechanism

This attribute is experimental.

This specifies the SASL mechanism to use for authentication with a MongoDB server. (See L</sasl>.)
The default is GSSAPI. The supported SASL mechanisms are:

=over 4

=item * C<GSSAPI>. This is the default. GSSAPI will attempt to authenticate against Kerberos
for MongoDB Enterprise 2.4+. You must run your program from within a C<kinit> session and set
the C<username> attribute to the Kerberos principal name, e.g. C<user@EXAMPLE.COM>.

=item * C<PLAIN>. The SASL PLAIN mechanism will attempt to authenticate against LDAP for
MongoDB Enterprise 2.6+. Because the password is not encrypted, you should only use this
mechanism over a secure connection. You must set the C<username> and C<password> attributes
to your LDAP credentials.

=back

=attr dt_type

Sets the type of object which is returned for DateTime fields. The default is L<DateTime>. Other
acceptable values are L<DateTime::Tiny> and C<undef>. The latter will give you the raw epoch value
rather than an object.

=attr inflate_dbrefs

Controls whether L<DBRef|http://docs.mongodb.org/manual/applications/database-references/#dbref>s
are automatically inflated into L<MongoDB::DBRef> objects. Defaults to true.
Set this to C<0> if you don't want to auto-inflate them.

=attr inflate_regexps

Controls whether regular expressions stored in MongoDB are inflated into L<MongoDB::BSON::Regexp> objects instead of native Perl Regexps. The default is false. This can be dangerous, since the JavaScript regexps used internally by MongoDB are of a different dialect than Perl's. The default for this attribute may become true in future versions of the driver.

=method connect

    $client->connect;

Connects to the MongoDB server. Called automatically on object construction if
L</auto_connect> is true.

=method database_names

    my @dbs = $client->database_names;

Lists all databases on the MongoDB server.

=method get_database($name)

    my $database = $client->get_database('foo');

Returns a L<MongoDB::Database> instance for the database with the given C<$name>.


=method send($str)

    my ($insert, $ids) = MongoDB::write_insert('foo.bar', $bson_document );
    $client->send($insert);

Low-level function to send a string directly to the database.  Use
L<MongoDB::write_insert>, L<MongoDB::write_update>, L<MongoDB::write_remove>, or
L<MongoDB::write_query> to create a valid string.

=method recv($cursor)

    my $ok = $client->recv($cursor);

Low-level function to receive a response from the database into a cursor.
Dies on error.  Returns true if any results were received and false otherwise.

=method fsync(\%args)

    $client->fsync();

A function that will forces the server to flush all pending writes to the storage layer.

The fsync operation is synchronous by default, to run fsync asynchronously, use the following form:

    $client->fsync({async => 1});

The primary use of fsync is to lock the database during backup operations. This will flush all data to the data storage layer and block all write operations until you unlock the database. Note: you can still read while the database is locked.

    $conn->fsync({lock => 1});

=method fsync_unlock

    $conn->fsync_unlock();

Unlocks a database server to allow writes and reverses the operation of a $conn->fsync({lock => 1}); operation.

=method read_preference

    $conn->read_preference(MongoDB::MongoClient->PRIMARY_PREFERRED, [{'disk' => 'ssd'}, {'rack' => 'k'}]);

Sets the read preference for this connection. The first argument is the read
preference mode and should be one of four constants: PRIMARY, SECONDARY,
PRIMARY_PREFERRED, or SECONDARY_PREFERRED (NEAREST is not yet supported).  In
order to use read preference, L<MongoDB::MongoClient/find_master> must be set.
The second argument (optional) is an array reference containing one or more tag
sets. The tag set list can be used to match the tag sets of replica set secondaries.
See also L<MongoDB::Cursor/read_preference>. For core documentation on read
preference see L<http://docs.mongodb.org/manual/core/read-preference/>.

=method repin

    $conn->repin()

Chooses a replica set member to which this connection should route read operations,
according to the read preference that has been set via L<MongoDB::MongoClient/read_preference>
or L<MongoDB::Cursor/read_preference>. This method is called automatically
when the read preference or replica set state changes, and generally does not
need to be called by application code.

=method rs_refresh

    $conn->rs_refresh()

If it has been at least 5 seconds since last checking replica set state,
then ping all replica set members. Calls L<MongoDB::MongoClient/repin> if
a previously reachable node is now unreachable, or a previously unreachable
node is now reachable. This method is called automatically before communicating
with the server, and therefore should not generally be called by client code.

