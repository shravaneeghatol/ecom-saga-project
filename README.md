# E-commerce Saga: Choreography + Outbox + Kafka (KRaft)

Four independently deployable Spring Boot microservices that fulfill an order
purely by reacting to each other's events on Kafka — **no orchestrator, no
central saga manager**. Each service owns its own embedded H2 in-memory
database and uses the **Outbox Pattern** so a DB write and the corresponding
Kafka publish are never inconsistent.

```
order-service        → owns orderdb        → produces topic "order.events"
inventory-service     → owns inventorydb    → produces topic "inventory.events"
payment-service       → owns paymentdb      → produces topic "payment.events"
notification-service  → owns notificationdb → produces topic "notification.events"
```

## Architecture at a glance

**Happy path (choreography):**

```
client -> POST /api/orders (order-service)
   order-service:        saves Order(CREATED) + outbox row  --publishes-->  order.events
   inventory-service:    consumes order.events, reserves stock,
                          saves outbox row                  --publishes-->  inventory.events
   payment-service:       consumes inventory.events, charges,
                          saves outbox row                  --publishes-->  payment.events
   notification-service:  consumes payment.events, notifies,
                          saves outbox row                  --publishes-->  notification.events
   order-service:        consumes notification.events -> Order(COMPLETED)
```

When every step succeeds, **all four databases end up with a row for that
order** (orders, inventory_reservation, payment, notification) — that's the
explicit "success" signal the saga is working end-to-end.

**Compensation (choreography, not orchestration):** every service that ran
*before* a failing step listens directly for that step's failure event and
reverts its own local state. There's no central coordinator deciding this —
each service independently knows what to undo:

```
InventoryReservationFailed  -> order-service cancels the order
PaymentFailed                -> inventory-service releases the reservation
                                  order-service cancels the order
NotificationFailed (or DLT) -> payment-service refunds the payment
                                  inventory-service releases the reservation
                                  order-service cancels the order
```

## The Outbox Pattern

Every service has one `outbox_event` table. Business writes (e.g. "create
the Order") and the outbox row are written in the **same local transaction**.
A `@Scheduled` poller (`OutboxPublisher`, every 1.5s) reads `PENDING` rows and
publishes them to Kafka, then marks them `PUBLISHED`. This means a service
can never "commit the DB change but lose the event" — even if Kafka is down
when the transaction commits, the event ships as soon as Kafka is back.

## Retry topics + Dead Letter Topic

Each of the 4 main topics has **exactly one primary consumer** (the service
doing the actual business processing for that step). That listener is
annotated with Spring Kafka's `@RetryableTopic`, which automatically creates:

| Main topic            | Primary consumer        | Auto-created topics                                                          |
|------------------------|--------------------------|-------------------------------------------------------------------------------|
| `order.events`         | inventory-service        | `order.events-retry-0`, `-retry-1`, `-retry-2`, `order.events-dlt`            |
| `inventory.events`     | payment-service          | `inventory.events-retry-0`, `-retry-1`, `-retry-2`, `inventory.events-dlt`    |
| `payment.events`       | notification-service     | `payment.events-retry-0`, `-retry-1`, `-retry-2`, `payment.events-dlt`        |
| `notification.events`  | order-service             | `notification.events-retry-0`, `-retry-1`, `-retry-2`, `notification.events-dlt` |

So **every service owns one event + 3 retry topics + 1 DLT topic** (5 topics
each, 20 total), exactly as requested. Backoff is exponential (2s, 4s, 8s),
3 retries, then the message lands on the `-dlt` topic and a `@DltHandler`
method publishes a compensating "Failed" event through that service's own
outbox — so a DLT-exhausted message triggers the **same compensation chain**
as a normal business-rule failure.

All topics are created automatically the first time each service starts
(via `NewTopic` beans for the 4 main topics, and automatically by
`@RetryableTopic` for the retry/DLT topics) — you don't pre-create anything.

## Circuit Breaker (Resilience4j)

This is a **choreographed** saga: services never call each other synchronously
(no REST/RPC between order/inventory/payment/notification), so there's no
service-to-service call to "break". The one genuinely external, blocking call
in every service is inside `OutboxPublisher`:

```java
kafkaTemplate.send(event.getTopic(), event.getPartitionKey(), event.getPayload())
        .get(10, TimeUnit.SECONDS);
```

If the Kafka broker is down or slow, that `.get()` blocks for up to 10
seconds **per pending row**. With up to 50 rows fetched per tick (the
`@Scheduled(fixedDelay = 1500)` poller), a broker outage can turn into a
thread blocked for minutes, repeated every 1.5s, across all four services
simultaneously — exactly the cascading-failure scenario circuit breakers
exist to prevent.

Each service now wraps that call with a Resilience4j `@CircuitBreaker`
named `kafkaPublisher`:

- The call lives in its own bean, `KafkaEventSender.sendToKafka(...)`,
  separate from `OutboxPublisher`. Spring implements `@CircuitBreaker` via
  an AOP proxy, and proxies don't intercept calls a bean makes to its own
  methods ("self-invocation") — so the Kafka call had to move into a
  collaborator bean for the breaker to actually take effect.
- **CLOSED** (normal): every PENDING outbox row is sent to Kafka as before.
- **OPEN**: tripped once ≥50% of the last 10 calls failed or were slow
  (>8s). While open, `sendToKafka(...)` isn't even attempted —
  Resilience4j throws `CallNotPermittedException` immediately. The current
  outbox poll tick stops processing the rest of its batch right away
  instead of queuing up several more 10-second blocks.
- **HALF_OPEN**: after 10s, 3 trial calls are allowed through to check if
  Kafka has recovered, then the breaker returns to CLOSED or back to OPEN.
- Either way (timeout, broker error, or breaker-open), the outbox row is
  simply **left `PENDING`** — never marked `FAILED` — so the next
  `@Scheduled` poll (1.5s later) retries it automatically once Kafka (or
  the breaker) allows calls again. (`FAILED` exists in `OutboxStatus` but
  the repository only ever re-queries `PENDING` rows, so marking a row
  `FAILED` here would silently strand it forever — this was true before
  this change too.)

Config lives in each service's `application.yml` under
`resilience4j.circuitbreaker.instances.kafkaPublisher`, and breaker state
is observable two ways:

```bash
curl http://localhost:8081/actuator/health              # shows kafkaPublisher CB status
curl http://localhost:8081/actuator/circuitbreakers       # current state per breaker
curl http://localhost:8081/actuator/circuitbreakerevents  # recent state-transition log
```

(swap port `8081` for `8082`/`8083`/`8084` for inventory/payment/notification)

State transitions are also logged at WARN level by each service's
`CircuitBreakerConfig`, e.g.:

```
[CIRCUIT-BREAKER:kafkaPublisher] state transition: CLOSED -> OPEN
[CIRCUIT-BREAKER:kafkaPublisher] call rejected - breaker is OPEN, failing fast
[CIRCUIT-BREAKER:kafkaPublisher] state transition: HALF_OPEN -> CLOSED
```

**To see it trip:** stop the Kafka container (`docker compose stop kafka`)
while orders are flowing, watch any service's logs for the `CLOSED -> OPEN`
transition, then `docker compose start kafka` and watch it recover through
`HALF_OPEN -> CLOSED` — outbox rows that piled up as `PENDING` during the
outage all get published once it does.

## Kafka in KRaft mode + Kafka UI

`docker-compose.yml` runs the official `apache/kafka` image in **native
KRaft mode** (combined broker+controller, single node, no Zookeeper at all).
[`provectuslabs/kafka-ui`](http://localhost:8090) is wired up to the broker so
you can browse every topic, its partitions/consumer groups, and inspect
individual messages (including ones sitting in the DLT topics) from your
browser — this is the "Docker Desktop Kafka UI" piece.

## Running it

```bash
docker compose up --build
```

First boot takes a few minutes (4 Maven builds + Kafka). Once
healthy:

- Kafka UI: http://localhost:8090
- order-service: http://localhost:8081
- inventory-service: http://localhost:8082
- payment-service: http://localhost:8083
- notification-service: http://localhost:8084

> **Note:** I wrote and reviewed this code carefully, but I wasn't able to
> actually `docker compose up` or `mvn package` it inside this sandbox (no
> network access to Docker Hub / Maven Central here). Standard Spring
> Boot/Kafka patterns are used throughout, but please run it and paste back
> any error you hit — happy to debug.

## Trying every path

```bash
chmod +x scripts/test-saga.sh
./scripts/test-saga.sh
```

Or manually with curl, against `POST http://localhost:8081/api/orders`:

**1. Happy path** — every service gets a row, order ends `COMPLETED`:
```json
{ "customerId": "cust-1", "productId": "PROD-1", "quantity": 2, "amount": 49.99 }
```

**2. Business failure — out of stock** (immediate compensation, no retries):
```json
{ "customerId": "cust-2", "productId": "PROD-OUT-OF-STOCK", "quantity": 1, "amount": 19.99 }
```

**3. Business failure — payment declined** (inventory gets released):
```json
{ "customerId": "cust-3", "productId": "PROD-1", "quantity": 1, "amount": 9.99, "simulatePaymentFailure": true }
```

**4. Transient failure → retries → DLT** (watch `order.events-retry-0/1/2`
and `order.events-dlt` fill up live in Kafka UI):
```json
{ "customerId": "cust-4", "productId": "PROD-1", "quantity": 1, "amount": 29.99, "simulateTransientErrorAt": "inventory" }
```
`simulateTransientErrorAt` also accepts `"payment"` or `"notification"` to
exercise the other two retry/DLT chains.

Seeded products in inventory-service: `PROD-1` (qty 100), `PROD-2` (qty 5),
`PROD-OUT-OF-STOCK` (qty 0).

## Inspecting state

```bash
curl http://localhost:8081/api/orders
curl http://localhost:8082/api/inventory/reservations
curl http://localhost:8083/api/payments
curl http://localhost:8084/api/notifications
```

Each service also exposes an H2 web console for browsing tables directly
(handy for watching `outbox_event` rows flip from `PENDING` to `PUBLISHED`,
or for the circuit-breaker drill in the section above):

```
http://localhost:8081/h2-console   # order-service
http://localhost:8082/h2-console   # inventory-service
http://localhost:8083/h2-console   # payment-service
http://localhost:8084/h2-console   # notification-service
```

Connection settings on the login screen (JDBC URL must match exactly,
including the service's DB name — `orderdb`, `inventorydb`, `paymentdb`,
or `notificationdb`):

```
JDBC URL: jdbc:h2:mem:orderdb;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE
User:     (check your .env file, defaults to root)
Password: (check your .env file, defaults to root)
```

Note: H2 here is **in-memory** — all data is lost whenever a service
container/process restarts. There's no separate DB container or volume
to manage; the database lives entirely inside each service's own JVM.

## Project & Infrastructure Layout

```
.github/workflows/
  ci.yml                       # CI Pipeline: Builds Java jars, Docker images & pushes to Docker Hub
  deploy.yml                   # CD Pipeline: Deploys App Stack & Monitoring Stack via AWS SSM
config/
  prometheus.yml               # Scrape configuration for Prometheus metrics (Instance #2)
  promtail-config.yml          # Docker log harvesting config for Promtail (Instance #1)
  loki-config.yml              # Log aggregation storage & retention engine config for Loki
grafana/
  provisioning/datasources/    # Auto-registers Prometheus and Loki datasources in Grafana
  provisioning/dashboards/     # Auto-provisions Grafana dashboards from JSON files
  dashboards/
    containers-overview.json   # Docker container CPU/RAM/Network dashboard (cAdvisor)
    system-node.json           # Host system CPU/RAM/Disk dashboard (Node Exporter)
    logs-explorer.json         # Real-time log streaming explorer (Loki)
Infrastructure/
  main.tf                      # Terraform IaC: VPC, Security Groups, IAM Roles, 2 EC2 instances, Alarms
  variables.tf                 # Configurable deployment parameters (region, instance type, IP whitelist)
  outputs.tf                   # Terraform output links (Public IPs, Swagger UI, H2 Consoles, SSH commands)
  user_data.sh                 # EC2 Instance #1 bootstrap (Docker, SSM Agent, CloudWatch Agent, Swap space)
  monitoring_user_data.sh      # EC2 Instance #2 bootstrap (Docker, SSM Agent, CloudWatch Agent, Swap space)
  docker-compose.prod.yml      # Production compose overlay for Instance #1 app containers
terraform.tfstate              # Terraform state file mapping local IaC code to live AWS resources
scripts/
  configure-monitoring-ips.sh  # Dynamic IP discovery helper for 2-instance networking
  print-endpoints.sh           # Utility script to query live AWS IPs and print service endpoints
  test-saga.sh                 # End-to-end integration test runner for Saga & Outbox flows
docker-compose.yml             # Local/App Stack: Kafka (KRaft) + Kafka UI + 4 Spring Boot Microservices
docker-compose.monitoring.yml  # Observability Stack: Prometheus + Loki + Grafana
order-service/
inventory-service/
payment-service/
notification-service/
```

### Detailed Role of Deployment Files & Folders

| Folder / File | Primary Purpose | Key Details |
|---|---|---|
| **`.github/workflows/`** | CI/CD Automation | Contains `ci.yml` (builds and pushes Docker images) and `deploy.yml` (authenticates via AWS OIDC and deploys to EC2 using SSM). |
| **`config/`** | Observability Configurations | Holds `prometheus.yml` (metric scrape targets), `promtail-config.yml` (log collection), and `loki-config.yml` (log storage). |
| **`grafana/`** | Grafana Dashboards & Datasources | Auto-wires Prometheus and Loki data sources and provisions pre-built performance/log dashboards on startup. |
| **`Infrastructure/`** | Infrastructure as Code (IaC) | Declarative HCL code for provisioning 2 EC2 instances, IAM roles, security groups, swap space, and CloudWatch alarms. |
| **`terraform.tfstate`** | Terraform State Database | JSON file recording the exact mapping between your local Terraform code and real-world AWS infrastructure IDs. |

---

## Things deliberately simplified for a demo

- `spring.jpa.hibernate.ddl-auto=update` auto-creates tables; use Flyway/Liquibase in production.
- Each service uses an embedded H2 **in-memory** database instead of a real RDBMS — convenient for running this demo with zero external DB setup, but data doesn't survive a restart.
- The "payment gateway" and "notification provider" are simulated in-process.
- No idempotency dedupe table on consumers (at-least-once delivery is handled via status checks).
- Single Kafka broker / single partition replica — fine for a demo, not for HA.
