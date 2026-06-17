# StellPulsar Service

`stellpulsar-service` is a Go service for weakly consistent distributed rate limiting in the Stell ecosystem.

StellPulsar is designed for traffic governance scenarios where low latency, local decisions, and graceful degradation are more important than globally serializable quota accounting. It is suitable for API gateways, service meshes, middleware control planes, and multi-tenant platform traffic protection.

## Positioning

StellPulsar is a distributed rate limiting server, not an application-side library. Clients submit limit check requests to the service and receive allow/deny decisions with remaining quota metadata.

The first implementation intentionally starts with an in-memory token bucket engine. Future versions can add Redis-backed counters, shard coordination, gossip replication, warm standby, and stronger policy distribution through StellOrbit or StellCloud.

## Consistency Model

StellPulsar targets weak consistency:

- Each node can make local decisions without waiting for a global consensus path.
- Short-term over-admission is acceptable within bounded operational tolerance.
- The system prioritizes availability, low latency, and failure isolation.
- Global convergence can be added through async replication, shard ownership, or external counters.

## Capabilities

- HTTP API for rate limit check decisions.
- Token bucket based local limiter.
- Per-key limit, window, and request cost support.
- Health endpoint for runtime readiness.
- Standard library only runtime implementation.
- Designed for future distributed coordination and policy management.

## Current Status

| Item | Value |
| --- | --- |
| Stability | Early development |
| Language | Go |
| Project type | Distributed rate limiting server |
| Consistency | Weak consistency |
| Runtime engine | In-memory token bucket |
| Maintainer | StellHub |

## API

### Health

```text
GET /health
```

### Check Rate Limit

```text
POST /api/stellpulsar/v1/limits/check
```

Request:

```json
{
  "key": "tenant-a:/api/orders",
  "limit": 100,
  "windowSeconds": 60,
  "cost": 1
}
```

Response:

```json
{
  "allowed": true,
  "key": "tenant-a:/api/orders",
  "remaining": 99,
  "resetAt": "2026-06-17T00:00:00Z",
  "decision": "allowed"
}
```

## Development

Run tests:

```bash
go test ./...
```

Run locally:

```bash
go run ./cmd/stellpulsar-service
```

The default HTTP address is `:8080`. Override it with `STELLPULSAR_HTTP_ADDR`.

## Roadmap

- Policy model for tenant, route, service, and method scopes.
- Redis-backed counters for shared quota windows.
- Async replication for weakly consistent multi-node deployments.
- Local fallback mode for fail-open and fail-closed behavior.
- OpenAPI contract and generated SDKs.
- StellOrbit integration for traffic governance policy distribution.
- StellCloud console integration for policy authoring and audit.

## License

The license will be defined before the first stable release.
