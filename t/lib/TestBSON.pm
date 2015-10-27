use 5.008001;
use strict;
use warnings;

package TestBSON;

use Config;
use Exporter 'import';
use Test::More;

our @EXPORT = qw(
    BSON_DATETIME
    BSON_DOC
    BSON_DOUBLE
    BSON_INT32
    BSON_INT64
    BSON_NULL
    BSON_OID
    BSON_BOOL
    BSON_REGEXP
    BSON_STRING
    HAS_INT64
    MAX_LONG
    MIN_LONG
    _cstring
    _datetime
    _dbref
    _doc
    _double
    _ename
    _hexdump
    _int32
    _int64
    _pack_bigint
    _regexp
    _string
    is_bin
);

use constant {
    PERL58    => $] lt '5.010',
    HAS_INT64 => $Config{use64bitint}
};

use constant {
    P_INT32 => PERL58 ? "l" : "l<",
    P_INT64 => PERL58 ? "q" : "q<",
    MAX_LONG      => 2147483647,
    MIN_LONG      => -2147483647,
    BSON_DOUBLE   => "\x01",
    BSON_STRING   => "\x02",
    BSON_DOC      => "\x03",
    BSON_OID      => "\x07",
    BSON_BOOL     => "\x08",
    BSON_DATETIME => "\x09",
    BSON_NULL     => "\x0A",
    BSON_REGEXP   => "\x0B",
    BSON_INT32    => "\x10",
    BSON_INT64    => "\x12",
};

sub _hexdump {
    my ($str) = @_;
    $str =~ s{([^[:graph:]])}{sprintf("\\x{%02x}",ord($1))}ge;
    return $str;
}

sub is_bin {
    my ( $got, $exp, $label ) = @_;
    $label ||= '';
    $got = _hexdump($got);
    $exp = _hexdump($exp);
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    is( $got, $exp, $label );
}

sub _doc {
    my ($string) = shift;
    return pack( P_INT32, 5 + length($string) ) . $string . "\x00";
}

sub _cstring { return $_[0] . "\x00" }
BEGIN { *_ename = \&_cstring }

sub _double { return pack( PERL58 ? "d" : "d<", shift ) }

sub _int32 { return pack( P_INT32, shift ) }

sub _int64 {
    my $val = shift;
    if ( ref($val) && eval { $val->isa("Math::BigInt") } ) {
        return _pack_bigint($val);
    }
    elsif (HAS_INT64) {
         return pack( P_INT64, $val );
    }
    else {
        my $big = Math::BigInt->new( $val );
        return _pack_bigint($big);
    }
}

sub _string {
    my ($string) = shift;
    return pack( P_INT32, 1 + length($string) ) . $string . "\x00";
}

sub _datetime {
    my $dt = shift;
    if (HAS_INT64) {
        return pack( P_INT64, 1000 * $dt->epoch + $dt->millisecond );
    }
    else {
        my $big = Math::BigInt->new( $dt->epoch );
        $big->bmul(1000);
        $big->badd( $dt->millisecond );
        return _pack_bigint($big);
    }
}

sub _regexp {
    my ( $pattern, $flags ) = @_;
    return _cstring($pattern) . _cstring($flags);
}

sub _dbref {
    my $dbref = shift;
    #<<< No perltidy
    return _doc(
          BSON_STRING . _ename('$ref') . _string($dbref->ref)
        . BSON_STRING . _ename('$id' ) . _string($dbref->id)
        . BSON_STRING . _ename('$db' ) . _string($dbref->db)
    );
    #>>>
}

# pack to int64_t
sub _pack_bigint {
    my $bi = shift;
    my $binary = $bi->as_bin;
    $binary =~ s{^-?0b}{};
    $binary = "0"x(64-length($binary)) . $binary if length($binary) < 64;

    if ( $bi->sign eq '+' ) {
        return pack("b*", scalar reverse $binary);
    }
    else {
        my @lendian = split //, reverse $binary;
        my $saw_first_one = 0;
        for (@lendian) {
            if ( ! $saw_first_one ) {
                $saw_first_one = $_ == '1';
                next;
            }
            else {
                tr[01][10];
            }
        }
        return pack("b*", join("", @lendian));
    }
}

1;

# vim: set ts=4 sts=4 sw=4 et tw=75:
