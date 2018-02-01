package Test::Role::CommandDebug;

use Moo::Role;

our @COMMAND_QUEUE;
our @EXECUTE_QUEUE;

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

around 'execute' => sub {
    my $orig = shift;
    my $ret = $orig->(@_);
    push @EXECUTE_QUEUE, $ret;
    return $ret;
};

sub CLEAR_EXECUTE_QUEUE {
    @EXECUTE_QUEUE = ();
}

sub GET_LAST_EXECUTE {
    return pop @EXECUTE_QUEUE;
}

1;
