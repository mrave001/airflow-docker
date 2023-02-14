CALL {{ params.jla_mart_schema }}.acct_jla_drop_if_exist('{{ params.jla_mart_schema }}','{{ params.deposits_staging_table }}') ;
CREATE MULTISET TABLE {{ params.jla_mart_schema }}.{{ params.deposits_staging_table }}, NO FALLBACK 
(
         record_type            CHAR(8),        -- string, 
         submission_date        CHAR(10),       -- MM/DD/YYYY format, date in Eastern timezone when submission was received at Paymentech
         pid_number             INTEGER,        -- intger, merchant''s presenter ID number (constant 871889)
         pid_short_name         VARCHAR(6),     -- string, presenter abbreviated name (constant "cyber3")
         submission_number      VARCHAR(11),    -- a unique ID for set of records (example "20627.01F4i")
         record_number          INTEGER,        -- sequence ID within submission_number (example 2594)
         entity_type            CHAR(2),        -- string, constant value "TD"
         entity_number          INTEGER,        -- integer, transaction division entity ID (example 201622)
         presentment_currency   CHAR(3),        -- string, currency abbreviation (example "USD")
         merchant_order_id      VARCHAR(22),    -- string, reconciliation ID (example "92544831C3DUA68X")
         rdfi_number            INTEGER,        -- integer (empty in non-European markets)
         account_number         VARCHAR(22),    -- string, masked credit card number (example "XXXXXXXXXXXX1234")
         expiration_date        CHAR(5),        -- MM/YY format, credit card expiration date (example "07/26")
         amount                 DECIMAL(13,2),  -- decimal, transaction amount (may be negative or decimal-only; example 12.34)
         mop                    CHAR(2),        -- two-character payment method code (example "VI" for Visa)
         action_code            CHAR(2),        -- two-character transaction type code (example "DP")
         auth_date              CHAR(10),       -- MM/DD/YYYY format, date on which authorization was performed
         auth_code              CHAR(6),        -- six-character code related to positive response from endpoint (example "535918")
         auth_response_code     CHAR(3),        -- string, Chase Paymentech response code (example "100")
         trace_number           INTEGER,        -- integer, non-unique debit transaction identifier (empty)
         consumer_country_code  CHAR(12),       -- two character consumer country code, EU debit transactions only (empty)
         reserved               CHAR(1),        -- reserved field (always empty)
         mcc                    INTEGER,        -- merchant category code (example 7299)
         token_ind              CHAR(1),        -- valid values 1,2,3,0. Could be blank.
         interchange_qual_cd    CHAR(4),        -- Interchange Qualification Code (example VPDM)
         durbin_regulated       CHAR(1),        -- Indicates whether issuing bank''s card range is regulated or not as defined by durbin amendment (Y/N)
         interchange_unit_fee   DECIMAL(5,4),   -- Interchange unit fee (+ve amt is debit and -ve amt is credit to merchant). Could be 0 or blank
         interchange_face_prcnt DECIMAL(8,6),   -- Interchange % rate fee applied to the face value of this transaction. Could be 0 or blank
         total_interchange_amt  DECIMAL(10,2),  -- Total interchange fee assessed by the card association. Could be 0 or blank
         total_assess_amt       DECIMAL(10,2),  -- Total assessment fee assessed by the card association. Could be 0 or blank
         other_debit_passthru_fee DECIMAL(10,2),  -- Fee per item identified in merchange fee schedule for each line item. Could be 0 or blank
         filename               VARCHAR(100)
)
PRIMARY INDEX(record_number);
COLLECT STATS ON {{ params.deposits_staging_table }} INDEX (record_number);

GRANT ALL ON {{ params.deposits_staging_table }} TO UB_AcctPost WITH GRANT OPTION;
GRANT ALL ON {{ params.deposits_staging_table }} TO barobinson WITH GRANT OPTION;
GRANT ALL ON {{ params.deposits_staging_table }} TO jalbrecht WITH GRANT OPTION;