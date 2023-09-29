# pagopa-ecommerce-event-dispatcher-service

## What is this?

This is a PagoPA microservice that handles scheduled retry mechanism for the eCommerce product.

### Environment variables

These are all environment variables needed by the application:

| Variable name                                                | Description                                                                                                                                                                                                                                                        | type                              | default       |
|--------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------|---------------|
| MONGO_HOST                                                   | Host where MongoDB instance used to persise events and view resides                                                                                                                                                                                                | string                            |               |
| MONGO_USERNAME                                               | Username used for connecting to MongoDB instance                                                                                                                                                                                                                   | string                            |               |
| MONGO_PASSWORD                                               | Password used for connecting to MongoDB instance                                                                                                                                                                                                                   | string                            |               |
| MONGO_PORT                                                   | Port used for connecting to MongoDB instance                                                                                                                                                                                                                       | number                            |               |
| MONGO_SSL_ENABLED                                            | Boolean value indicating if use SSL for connecting to MongoDB instance                                                                                                                                                                                             | boolean                           |               |
| REDIS_HOST                                                   | Host where the redis instance used to persist idempotency keys can be found                                                                                                                                                                                        | string                            |               |
| REDIS_PASSWORD                                               | Password used for connecting to Redis instance                                                                                                                                                                                                                     | string                            |               |
| PAYMENT_TRANSACTION_GATEWAY_URI                              | Payment transactions gateway service connection URI                                                                                                                                                                                                                | string                            |               |
| PAYMENT_TRANSACTION_GATEWAY_READ_TIMEOUT                     | Timeout for requests towards Payment transactions gateway service                                                                                                                                                                                                  | number                            |               |
| PAYMENT_TRANSACTION_GATEWAY_CONNECTION_TIMEOUT               | Timeout for establishing connections towards Payment transactions gateway service                                                                                                                                                                                  | number                            |               |
| NODO_URI                                                     | Nodo connection URI                                                                                                                                                                                                                                                | string                            |               |
| NODO_READ_TIMEOUT                                            | Timeout for requests towards Nodo                                                                                                                                                                                                                                  | number                            |               |
| NODO_CONNECTION_TIMEOUT                                      | Timeout for establishing connections towards Nodo                                                                                                                                                                                                                  | number                            |               |
| NODO_CLOSEPAYMENT_API_KEY                                    |                                                                                                                                                                                                                                                                    | API Key for Nodo closePayment API | string        |         |
| QUEUE_TRANSIENT_CONNECTION_STRING                            | eCommerce storage transient connection string                                                                                                                                                                                                                      | string                            |               |
| ECOMMERCE_STORAGE_TRANSIENT_QUEUE_KEY                        | eCommerce storage transient account access key                                                                                                                                                                                                                     | string                            |               |
| ECOMMERCE_STORAGE_TRANSIENT_QUEUE_ACCOUNT_NAME               | eCommerce storage transient account name                                                                                                                                                                                                                           | string                            |               |
| ECOMMERCE_STORAGE_TRANSIENT_QUEUE_ENDPOINT                   | eCommerce storage transient account queue endpoint                                                                                                                                                                                                                 | string                            |               |
| QUEUE_DEADLETTER_CONNECTION_STRING                           | eCommerce storage deadletter connection string                                                                                                                                                                                                                     | string                            |               |˙
| TRANSACTIONS_CLOSE_PAYMENT_RETRY_QUEUE_NAME                  | Queue name for closure events scheduled for retries                                                                                                                                                                                                                | string                            |               |
| TRANSACTIONS_CLOSE_PAYMENT_QUEUE_NAME                        | Queue name for closure events scheduled                                                                                                                                                                                                                            | string                            |               |
| TRANSACTIONS_NOTIFICATIONS_RETRY_QUEUE_NAME                  | Queue name for notification events scheduled for retries                                                                                                                                                                                                           | string                            |               |
| TRANSACTIONS_NOTIFICATIONS_QUEUE_NAME                        | Queue name for notifications events scheduler                                                                                                                                                                                                                      | string                            |               |
| TRANSACTIONS_EXPIRATION_QUEUE_NAME                           | Queue name for all events scheduled for expiration                                                                                                                                                                                                                 | string                            |               |
| TRANSACTIONS_REFUND_QUEUE_NAME                               | Queue name for refund scheduled                                                                                                                                                                                                                                    | string                            |               |
| TRANSACTIONS_REFUND_RETRY_QUEUE_NAME                         | Queue name for refund scheduler for retries                                                                                                                                                                                                                        | string                            |               |
| TRANSACTIONS_DEAD_LETTER_QUEUE_NAME                          | Queue name were event that cannot be processed successfully are forwarded                                                                                                                                                                                          | string                            |               |
| TRANSIENT_QUEUES_TTL_SECONDS                                 | TTL to be used when sending events on transient queues                                                                                                                                                                                                             | number                            | 7 days        |
| DEAD_LETTER_QUEUE_TTL_SECONDS                                | TTL to be used when sending events on dead letter queues                                                                                                                                                                                                           | number                            | -1 (infinite) |
| QUEUE_CONNECTION_STRING                                      | Queue connection string used by event producers                                                                                                                                                                                                                    | string                            |               |
| NOTIFICATIONS_SERVICE_URI                                    | Notification service URI                                                                                                                                                                                                                                           | string                            |               |
| NOTIFICATIONS_SERVICE_READ_TIMEOUT                           | Notification service HTTP read timeout                                                                                                                                                                                                                             | integer                           |               |
| NOTIFICATIONS_SERVICE_CONNECTION_TIMEOUT                     | Notification service HTTP connection timeout                                                                                                                                                                                                                       | integer                           |               |
| NOTIFICATIONS_SERVICE_API_KEY                                | Notification service API key                                                                                                                                                                                                                                       | string                            |               |
| PERSONAL_DATA_VAULT_API_KEY                                  | Personal data vault API key                                                                                                                                                                                                                                        | string                            |               |
| PERSONAL_DATA_VAULT_API_BASE_PATH                            | Persona data vault API base path                                                                                                                                                                                                                                   | string                            |               |
| PAYMENT_TRANSACTIONS_GATEWAY_URI                             | Payment transaction gateway URI                                                                                                                                                                                                                                    | string                            |               |
| PAYMENT_TRANSACTIONS_GATEWAY_READ_TIMEOUT                    | Payment transaction gateway HTTP read timeout                                                                                                                                                                                                                      | integer                           |               |
| PAYMENT_TRANSACTIONS_GATEWAY_CONNECTION_TIMEOUT              | Payment transaction gateway HTTP connection timeout                                                                                                                                                                                                                | integer                           |               |
| PAYMENT_TRANSACTIONS_GATEWAY_API_KEY                         | Payment transaction gateway API subscription-key                                                                                                                                                                                                                   | integer                           |               |
| REFUND_RETRY_EVENT_BASE_INTERVAL_SECONDS                     | Base interval used to calculate visibility for next retries refund event                                                                                                                                                                                           | integer                           |               |
| REFUND_RETRY_EVENT_MAX_ATTEMPTS                              | Max attempts to be performed for refund                                                                                                                                                                                                                            | integer                           |               |
| CLOSE_PAYMENT_RETRY_EVENT_BASE_INTERVAL_SECONDS              | Base interval used to calculate visibility for next retried closure event                                                                                                                                                                                          | integer                           |               |
| CLOSE_PAYMENT_RETRY_EVENT_MAX_ATTEMPTS                       | Max attempts to be performed for close payment                                                                                                                                                                                                                     | integer                           |               |
| CLOSE_PAYMENT_RETRY_EVENT_PAYMENT_TOKEN_VALIDITY_TIME_OFFSET | Configurable offset (in seconds) that will be taken in account for payment token validity time vs retry event visibility timeout check                                                                                                                             | integer                           | 10 sec        |
| NOTIFICATION_RETRY_EVENT_BASE_INTERVAL_SECONDS               | Base interval used to calculate visibility for next retried notification event                                                                                                                                                                                     | integer                           |               |
| NOTIFICATION_RETRY_EVENT_MAX_ATTEMPTS                        | Max attempts to be performed for notification                                                                                                                                                                                                                      | integer                           |               |
| SEND_PAYMENT_RESULT_TIMEOUT_SECONDS                          | Max time (in seconds) to be awaited for the `sendPaymentResult` callback (`POST /user-receipts` on `transactions-service`) to be received for a given transaction                                                                                                  | integer                           |               |
| SEND_PAYMENT_RESULT_EXPIRATION_OFFSET_SECONDS                | Offset to SEND_PAYMENT_RESULT_TIMEOUT_SECONDS. Transactions that are stuck in CLOSED status for which an OK response has been received by Nodo after (SEND_PAYMENT_RESULT_TIMEOUT_SECONDS - SEND_PAYMENT_RESULT_EXPIRATION_OFFSET_SECONDS) are considered expired. | integer                           |               |

An example configuration of these environment variables is in the `.env.example` file.

## Run the application with `Docker`

Create your environment typing :

```sh
cp .env.example .env
```

Then from current project directory run :

```sh
docker-compose up
```

## Run the application with `springboot-plugin`

Create your environment:

```sh
export $(grep -v '^#' .env.local | xargs)
```

Then from current project directory run :

```sh
mvn spring-boot:run
```

Note that with this method you would also need an active Redis instance on your local machine.
We suggest you to use the [ecommerce-local](https://github.com/pagopa/pagopa-ecommerce-local) instead.

## Code formatting

Code formatting checks are automatically performed during build phase.
If the code is not well formatted an error is raised blocking the maven build.

Helpful commands:

```sh
mvn spotless:check # --> used to perform format checks
mvn spotless:apply # --> used to format all misformatted files
```

## CI

Repo has Github workflow and actions that trigger Azure devops deploy pipeline once a PR is merged on main branch.

In order to properly set version bump parameters for call Azure devops deploy pipelines will be check for the following
tags presence during PR analysis:

| Tag                | Semantic versioning scope | Meaning                                                           |
|--------------------|---------------------------|-------------------------------------------------------------------|
| patch              | Application version       | Patch-bump application version into pom.xml and Chart app version |
| minor              | Application version       | Minor-bump application version into pom.xml and Chart app version |
| major              | Application version       | Major-bump application version into pom.xml and Chart app version |
| ignore-for-release | Application version       | Ignore application version bump                                   |
| chart-patch        | Chart version             | Patch-bump Chart version                                          |
| chart-minor        | Chart version             | Minor-bump Chart version                                          |
| chart-major        | Chart version             | Major-bump Chart version                                          |
| skip-release       | Any                       | The release will be skipped altogether                            |

For the check to be successfully passed only one of the `Application version` labels and only ones of
the `Chart version` labels must be contemporary present for a given PR or the `skip-release` for skipping release step
