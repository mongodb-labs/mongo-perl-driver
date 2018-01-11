package Test::Role::CommandDebug;

use Moo::Role;

our @COMMAND_QUEUE;

around _new => sub {
    my $orig = shift;
    my $ret = $orig->(@_);

    push @COMMAND_QUEUE, $ret;
    return $ret;
};

1;
