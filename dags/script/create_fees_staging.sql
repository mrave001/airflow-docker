CALL {{ params.jla_mart_schema }}.acct_jla_drop_if_exist('{{ params.jla_mart_schema }}','{{ params.fees_staging_table }}');
CREATE MULTISET TABLE {{ params.jla_mart_schema }}.{{ params.fees_staging_table }}, NO FALLBACK 
(
        record_type VARCHAR(8),
        category VARCHAR(6),
        sub_category VARCHAR(6),
        entity_type VARCHAR(2),
        entity_number VARCHAR(10),
        funds_transfer_instr_num VARCHAR(10),
        secure_bank_acct_num VARCHAR(10),
        currency VARCHAR(3),
        fee_schedule VARCHAR(10),
        mop VARCHAR(2),
        interchange_qualification VARCHAR(4),
        fee_type_description VARCHAR(30),
        action_type CHAR(1),
        unit_quantity INTEGER,
        unit_fee DECIMAL(11,4),
        amount DECIMAL(17,2),
        rate DECIMAL(8,6),
        total_charge DECIMAL(17,2),
        filename VARCHAR(100)
) 
PRIMARY INDEX(record_type);
COLLECT STATS ON {{ params.fees_staging_table }} INDEX (sequence_number);

GRANT ALL ON {{ params.fees_staging_table }} TO UB_AcctPost WITH GRANT OPTION;
GRANT ALL ON {{ params.fees_staging_table }} TO barobinson WITH GRANT OPTION;
GRANT ALL ON {{ params.fees_staging_table }} TO jalbrecht WITH GRANT OPTION;