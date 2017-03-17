# How to generate test certs

1. Install openssl and find the "CA.pl" file that ships with it.  On
   linux that might be "/usr/lib/ssl/misc/CA.pl".  On OS X with MacPorts,
   that is "/opt/local/etc/openssl/misc/CA.pl".

    find / -name CA.pl

2. Copy the CA.pl file and patch it with CA.pl.patch to eliminate passwords
   and set expiration time to 10 years for everything.

    cp /opt/local/etc/openssl/misc/CA.pl .
    patch < CA.pl.patch

2. Use the local, custom CA.pl file to generate a demo CA:

    ./CA.pl -newCA

3. Generate a certificate:

    ./CA.pl -newreq
    ./CA.pl -sign
    cat newkey.pem newcert.pem > somename.pem

somename.pem can be edited to remove plaintext parts if needed.
