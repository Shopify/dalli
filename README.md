Dalli [![Tests](https://github.com/petergoldstein/dalli/actions/workflows/tests.yml/badge.svg)](https://github.com/petergoldstein/dalli/actions/workflows/tests.yml)
=====

Dalli is a high performance pure Ruby client for accessing memcached servers.

Dalli supports:

* Simple and complex memcached configurations
* Failover between memcached instances
* Fine-grained control of data serialization and compression
* Thread-safe operation (either through use of a connection pool, or by using the Dalli client in threadsafe mode)
* SSL/TLS connections to memcached
* SASL authentication

The name is a variant of Salvador Dali for his famous painting [The Persistence of Memory](http://en.wikipedia.org/wiki/The_Persistence_of_Memory).

![Persistence of Memory](https://upload.wikimedia.org/wikipedia/en/d/dd/The_Persistence_of_Memory.jpg)


## Documentation and Information

* [User Documentation](https://github.com/petergoldstein/dalli/wiki) - The documentation is maintained in the repository's wiki.  
* [Announcements](https://github.com/petergoldstein/dalli/discussions/categories/announcements) - Announcements of interest to the Dalli community will be posted here.
* [Bug Reports](https://github.com/petergoldstein/dalli/issues) - If you discover a problem with Dalli, please submit a bug report in the tracker.
* [Forum](https://github.com/petergoldstein/dalli/discussions/categories/q-a) - If you have questions about Dalli, please post them here.
* [Client API](https://www.rubydoc.info/gems/dalli) - Ruby documentation for the `Dalli::Client` API

## Single-get response correlation

Single-key meta gets (`get`, `gat`, CAS retrieval, `get_with_status`, and `touch`)
include an internally generated `O` opaque token. Memcached and any intermediary
must echo the token on value-bearing (`VA`) responses. A wrong token on any get
response, or a missing token on a `VA` response, is a stream-correlation failure:
Dalli returns the operation's normal cache-miss result and closes the connection
without reading or deserializing the body. The rejected operation is **not
retried**, and its existing metrics record a miss rather than an error.

Correlation failures log a warning and share failure accounting with network
errors. With the default `socket_max_failures: 2`, two consecutive failed
operations mark the server down for `down_retry_delay`. Even the operation that
reaches this limit returns its miss; subsequent requests use the normal
server-availability/failover behavior. A successful response resets the failure
budget, but a reconnect/version handshake alone does not.

For compatibility with peers that omit the token on bodyless responses, an
`EN` or `HD` response **without** an `O` flag is treated as a normal cache miss
without closing the connection. This includes `touch`, which returns `nil`
rather than accepting an uncorrelated hit. An explicitly wrong (including empty)
opaque is still an error. This compatibility exception does not establish
correlation for bodyless responses.

The `O` flag is reserved for internal correlation on single gets. Passing an
`O...` flag in `meta_flags` to `get` or `gat` raises `ArgumentError` rather than
silently replacing the caller's token. Routing tokens (`p_token` and `l_token`)
remain supported. Multi-get/pipeline request formatting and response matching
are unchanged; opaque correlation applies only to single gets.

Correlation detects response mix-ups, not incorrect data already stored under a
key or a wrong body attached to an otherwise correctly correlated header.

## Development

After checking out the repo, run `bin/setup` to install dependencies. You can run `bin/console` for an interactive prompt that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`.

## Contributing

If you have a fix you wish to provide, please fork the code, fix in your local project and then send a pull request on github.  Please ensure that you include a test which verifies your fix and update the [changelog](CHANGELOG.md) with a one sentence description of your fix so you get credit as a contributor.

## Appreciation

Dalli would not exist in its current form without the contributions of many people.  But special thanks go to several individuals and organizations:

* Mike Perham - for originally authoring the Dalli project and serving as maintainer and primary contributor for many years
* Eric Wong - for help using his [kgio](http://bogomips.org/kgio/) library.
* Brian Mitchell - for his remix-stash project which was helpful when implementing and testing the binary protocol support.
* [CouchBase](http://couchbase.com) - for their sponsorship of the original development


## Authors

* [Peter M. Goldstein](https://github.com/petergoldstein) - current maintainer
* [Mike Perham](https://github.com/mperham) and contributors


## Copyright

Copyright (c) Mike Perham, Peter M. Goldstein. See LICENSE for details.
