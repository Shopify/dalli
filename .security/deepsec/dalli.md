# dalli

## What this codebase does

Dalli is a pure-Ruby memcached client gem (Shopify's fork of the open-source project). It
speaks only the memcached meta text protocol (`Dalli::Protocol::Meta`) over TCP, optional TLS
(`ssl_context` wraps the socket in `Dalli::Socket::SSLSocket`) or UNIX sockets. `Dalli::Client`
consistent-hashes keys across servers (`Dalli::Ring`), pipelines multi-key reads, writes and
deletes, and ships `Rack::Session::Dalli`, a Rack session store. It has no listener, no deploy
config and no secrets of its own: it runs inside the host application's process, and every
input (keys, values, options, server strings) comes from that application.

## Auth shape

- No memcached authentication: `Meta#authenticate_connection` raises. `username`/`password`
  parsed from a `memcached://user:pass@host` URI by `ServerConfigParser.parse_uri` are carried
  in options only; `Socket::InstanceMethods#logged_options` strips them before error messages.
- Transport security is opt-in TLS via a caller-supplied `OpenSSL::SSL::SSLContext`;
  `TCP.wrapping_ssl_socket` sets `hostname` for SNI, and verification is whatever the context says.
- Server list comes from the constructor or `ENV['MEMCACHE_SERVERS']` (`ServersArgNormalizer`).
- Session ids: `Rack::Session::Dalli` stores sessions under `sid.private_id` (hashed id from
  `Rack::Session::Abstract::PersistedSecure`), never the public cookie value.

## Threat model

Memcached is trusted only as far as its network path is. The worst outcome is remote code
execution in the host app: values flagged `FLAG_SERIALIZED` (0x1) are passed to `Marshal.load`
by default (`ValueSerializer#retrieve`), so anyone who can write to the cache or sit on the wire
can get arbitrary object deserialization. Next is protocol injection: a key, flag or token that
smuggles CR/LF into a command line lets a caller issue extra commands (`flush_all`, writes to
other keys) or desynchronize responses so one request reads another's value — for
`Rack::Session::Dalli`, that is session confusion between users. Then resource exhaustion from
server-declared lengths and decompression bombs.

## Project-specific patterns to flag

- Any string interpolated into a command in `Meta::RequestFormatter` or `Protocol::Meta` (e.g.
  the `mg #{encoded_key} v` fast path, `meta_flags.join(' ')`, `stats #{arg}`) that has not gone
  through `KeyRegularizer.encode` (base64 when non-ASCII or whitespace) or
  `validate_routing_token!` (rejects `\r`, `\n`, `\0`). Keys must always be regularized.
- Changes to `ValueSerializer`, `ValueMarshaller` or `ValueCompressor` that deserialize or
  decompress without the bitflag check, or that widen what `serializer.load` receives.
- Reading a byte count from a response (`read_data(tokens[1].to_i)`, `BufferedReader#read`)
  without bounds, or any path where a short read, timeout or opaque mismatch leaves the socket
  reusable — `handle_correlation_failure` must call `discard_after_request!`.
- Credentials, `ssl_context` or full option hashes reaching `Dalli.logger`, exception messages
  or OpenTelemetry span attributes (`OpentelemetryMiddleware`); `logged_options` is the filter.
- `Rack::Session::Dalli` paths that key memcached by the public session id instead of
  `private_id`, or that swallow errors so a failed `add` reuses an existing session.

## Known false-positives

- `Marshal` as the default serializer is documented, intentional behavior; flag new
  unguarded load paths, not the default itself.
- `KeyManager#truncated_key` uses MD5 for key shortening only, not for security.
- `'keys' => key` span attributes in `Protocol::Meta` are intended tracing metadata.
- `bin/`, `scripts/install_memcached.sh` and `test/` (including `test/sasl`) are dev and CI
  tooling, not shipped in the gem.
