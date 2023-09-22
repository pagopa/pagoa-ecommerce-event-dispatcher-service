package it.pagopa.ecommerce.eventdispatcher.repositories;

import it.pagopa.ecommerce.commons.documents.BaseTransactionView;
import it.pagopa.ecommerce.commons.documents.v1.Transaction;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Mono;

public interface TransactionsViewRepository extends ReactiveCrudRepository<BaseTransactionView, String> {
    Mono<Transaction> findByTransactionId(String transactionId);
}
