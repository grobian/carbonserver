carbonserver
============

Simple whisper file server over HTTP.

This project aims to be a replacement of the graphite web API used on carbon stores to retrieve the whisper data for viewing.

The main reason to build a replacement is performance.  This server only
supports the find and render calls that return raw data (e.g.  no
rendered images).

carbonserver understands the /metrics/find and /render URLs sent by the
carbon web-frontend to the stores (their web-frontend), and responds in
a compatible way.  As such, it can be used as a drop-in replacement.
When used in combination with carbonzipper, carbonserver uses a more
optimal communication protocol that puts less strains on Go's memory
usage and garbage collector.


Authors
-------
Fabian Groffen
Damian Gryski


Acknowledgement
---------------
This program was originally developed for Booking.com.  With approval
from Booking.com, the code was generalised and published as Open Source
on github, for which the authors would like to express their gratitude.
