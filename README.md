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

## Optional response correlation

Enable single-key response validation with `Dalli::Client.new(servers, correlate_with_opaques: true)`.
Reads use the first caller-supplied `O` token in `meta_flags`, or generate a four-character (24-bit) token if none is supplied;
writes always generate one. Dalli sends that token exactly once and checks the echoed value against it.

Mismatched or missing tokens on accepted replies close the connection, without retrying or down-marking.
Reads return a miss. `set`, `add`, `replace`, `append`, and `prepend` return `false`, and `incr`/`decr` return `nil`;
the server may still have applied a rejected write.
Other protocol errors are unchanged. Omit the option or set it to `false` to leave caller opaques and default requests unchanged.
Caller tokens must be protocol-safe and suitably unique per request; reused tokens cannot detect swaps between those requests.
Deletes, `quiet`/`multi` blocks, and multi-key operations such as `get_multi`, `set_multi`, and `delete_multi` are unchanged.

For opt-in, per-request opaque correlation, also set `opaque_correlation_request_only: true` on the client and pass `correlate_with_opaques: true` in request options.  The PRNG is still initialized at connection setup, but unflagged requests retain their baseline behavior:

```ruby
client = Dalli::Client.new(servers, correlate_with_opaques: true, opaque_correlation_request_only: true)
client.get('key')                             # no correlation
client.get('key', correlate_with_opaques: true) # correlate this read
client.set('key', 'value', nil, correlate_with_opaques: true) # correlate this write
```

Request-only mode defaults to false. Requests can explicitly opt out with `correlate_with_opaques: false`
in either mode, but cannot enable correlation when the client option is false or omitted.
An omitted or `nil` request option inherits the client mode.
Request options do not mutate client configuration. They also apply to `gat`, CAS reads, `get_with_status`, `touch`,
`add`, `replace`, `append`, `prepend`, `incr`, `decr`, and the reads and writes made by `fetch`/`cas`/`cas!`.

## Timeout recovery under fiber schedulers

Under a fiber scheduler, `socket_timeout` is enforced by a scheduler timer that starts when a read begins to wait.
If another fiber keeps the thread busy past that deadline, a scheduler can run the expired timer before it delivers
the readiness of a reply that has already arrived. The read would then fail, the connection would be closed, and
the reply would be lost.

Dalli reads responses through a connection-local buffer with an explicit deadline on each wait. When a wait
expires, Dalli polls the socket once more, outside the scheduler, before it reports a timeout. A reply that does not
arrive still fails after `socket_timeout`. Writes are unchanged, and so is the pipelined `get_multi` path used with
multiple servers or a block. The buffer is 8 KiB per connection and grows to 64 KiB once a reply on that connection
outgrows it. It is allocated on a connection's first read and released when the connection closes.

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
