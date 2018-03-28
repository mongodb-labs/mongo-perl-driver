package Test::Role::BSONDebug;

use Moo::Role;

our @ENCODE_ONE_QUEUE;
our @DECODE_ONE_QUEUE;

around encode_one => sub {
    my $orig = shift;

    my $cmd = $_[1];
    my $ret = $orig->(@_);

    push @ENCODE_ONE_QUEUE, $cmd;
    return $ret;
};

around decode_one => sub {
    my $orig = shift;

    my $ret = $orig->(@_);

    push @DECODE_ONE_QUEUE, $ret;
    return $ret;
};

sub GET_LAST_ENCODE_ONE {
    return pop @ENCODE_ONE_QUEUE;
}

sub GET_LAST_DECODE_ONE {
    return pop @DECODE_ONE_QUEUE;
}

sub CLEAR_ENCODE_ONE_QUEUE {
    @ENCODE_ONE_QUEUE = ();
}

sub CLEAR_DECODE_ONE_QUEUE {
    @DECODE_ONE_QUEUE = ();
}

1;
