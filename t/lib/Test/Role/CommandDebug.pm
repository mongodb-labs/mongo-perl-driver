package Test::Role::CommandDebug;

use Moo::Role;

our @COMMAND_QUEUE;

around _new => sub {
    my $orig = shift;
    my $ret = $orig->(@_);

    push @COMMAND_QUEUE, $ret;
    return $ret;
};

sub CLEAR_COMMAND_QUEUE {
    @COMMAND_QUEUE = ();
}

sub GET_LAST_COMMAND {
    return pop @COMMAND_QUEUE;
}

1;
