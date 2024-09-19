package com.cbs.streaming.constants;

public final class EventsConfig {

    public static final String DLQ_PREFIX = "dlq-";

    public interface Account {

        String ACCOUNT_POSTING_STORE = "ACCOUNT-POSTING-STORE";

        String POSTING_TOPIC = "ACCOUNT-POSTING";
        String CREATE_TOPIC = "ACCOUNT-CREATE";
        String UPDATE_TOPIC = "ACCOUNT-UPDATE";
        String BALANCE_TOPIC = "ACCOUNT-BALANCE";


        int RETENTION_PERIOD_DAYS = 7;
        int PARTITIONS = 10;

        String ACCOUNT_PARTITION_KEY = "ACCOUNT_ID";
        String TRANSACTION_PARTITION_KEY = "TRANSACTION_ID";

        String ACCOUNT_CREATED_EVENT = "account_created";
        String ACCOUNT_UPDATED_EVENT = "account_updated";
        String ACCOUNT_UPDATED_CREATED_EVENT = "account_update_created";
        String ACCOUNT_BALANCE_EVENT = "balances";

        String TERM_DEPOSIT_ACCOUNT_EVENT = "TermDepositAccountEvent";
        String SAVINGS_ACCOUNT_EVENT = "SavingsAccountEvent";
    }
}
