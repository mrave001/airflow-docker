CALL {{ params.jla_mart_schema }}.acct_jla_drop_if_exist('{{ params.jla_mart_schema }}','{{ params.chargebacks_staging_table }}');
CREATE MULTISET TABLE {{ params.jla_mart_schema }}.{{ params.chargebacks_staging_table }}, NO FALLBACK 
(
        record_type          CHAR(9) NOT NULL,
        entity_type          CHAR(2) NOT NULL,
        entity_number        INTEGER NOT NULL,
        chargeback_amount    DECIMAL(13,2),
        previous_partial     CHAR(1),
        presentment_currency CHAR(3),
        chargeback_category  VARCHAR(12),
        status_flag          CHAR(1),
        sequence_number      INTEGER,
        merchant_order_id    VARCHAR(22),
        account_number       VARCHAR(22),
        reason_code          VARCHAR(12),
        transaction_date     CHAR(10),
        chargeback_date      CHAR(10),
        activity_date        CHAR(10),
        current_action       DECIMAL(13,2),
        fee_amount           DECIMAL(13,2),
        usage_code           SMALLINT,
        acquirer_reference_number VARCHAR(23),
        mop_code             CHAR(2),
        authorization_date   CHAR(10),
        chargeback_due_date  CHAR(10),
        ticket_no            VARCHAR(15),
        potential_bundled_cb  CHAR(1),
        token_ind            CHAR(1),
        filename             VARCHAR(100)
        ) 
PRIMARY INDEX(sequence_number);
COLLECT STATS ON {{ params.chargebacks_staging_table }} INDEX (sequence_number);

GRANT ALL ON {{ params.chargebacks_staging_table }} TO UB_AcctPost WITH GRANT OPTION;
GRANT ALL ON {{ params.chargebacks_staging_table }} TO barobinson WITH GRANT OPTION;
GRANT ALL ON {{ params.chargebacks_staging_table }} TO jalbrecht WITH GRANT OPTION;