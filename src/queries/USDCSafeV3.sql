WITH
idleV3_txs AS (
    SELECT idleV3.*
    FROM
        erc20."ERC20_evt_Transfer" idleV3
    WHERE
        idleV3."evt_tx_hash" IN (SELECT evt_Transfer."evt_tx_hash" FROM idle_v3."IdleUSDC_riskAdjusted_evt_Transfer" evt_Transfer)
    AND
        idleV3."contract_address" IN ('\xcDdB1Bceb7a1979C6caa0229820707429dd3Ec6C')
    ORDER BY
        idleV3."evt_block_number" ASC
),
idleV3_redeems AS (
    SELECT
        idleV3_txs.*,
        idleV3_txs.from as account,
        (-idleV3_txs.value/1000000000000000000) as amount,
        'REDEEM' as action
    FROM
        idleV3_txs
    INNER JOIN
        (
        SELECT
            idleV3_txs."evt_tx_hash",
            MIN(idleV3_txs."evt_index") evt_index
        FROM idlev3_txs
        GROUP BY idleV3_txs."evt_tx_hash"
        ) idleV3_txs_2
        ON idleV3_txs."evt_tx_hash"=idleV3_txs_2."evt_tx_hash" AND idleV3_txs."evt_index"=idleV3_txs_2."evt_index"
    INNER JOIN
        idle_v3."IdleUSDC_riskAdjusted_call_redeemIdleToken" redeemIdleToken
        ON redeemIdleToken."call_tx_hash"=idleV3_txs."evt_tx_hash"
    WHERE
        redeemIdleToken.call_success='true'
),
idleV3_mints AS (
    SELECT
        idleV3_txs.*,
        idleV3_txs.to as account,
        (idleV3_txs.value/1000000000000000000) as amount,
        'MINT' as action
    FROM
        idleV3_txs
    INNER JOIN
        (
        SELECT
            idleV3_txs."evt_tx_hash",
            MAX(idleV3_txs."evt_index") evt_index
        FROM idlev3_txs
        GROUP BY idleV3_txs."evt_tx_hash"
        ) idleV3_txs_2
        ON idleV3_txs."evt_tx_hash"=idleV3_txs_2."evt_tx_hash" AND idleV3_txs."evt_index"=idleV3_txs_2."evt_index"
    INNER JOIN
        idle_v3."IdleUSDC_riskAdjusted_call_mintIdleToken" mintIdleToken
        ON mintIdleToken."call_tx_hash"=idleV3_txs."evt_tx_hash"
    WHERE
        mintIdleToken.call_success='true'
),
idleV3_transfers_out AS (
    SELECT
        idleV3_txs.*,
        idleV3_txs."from" as account,
        -(idleV3_txs.value/1000000000000000000) as amount,
        'TRANSFER_OUT' as action
    FROM
        idleV3_txs
    WHERE
        idleV3_txs."evt_tx_hash" NOT IN (SELECT "evt_tx_hash" FROM idleV3_mints)
    AND
        idleV3_txs."evt_tx_hash" NOT IN (SELECT "evt_tx_hash" FROM idleV3_redeems)
    AND
        idleV3_txs.from <> '\x0000000000000000000000000000000000000000'
),
idleV3_transfers_in AS (
    SELECT
        idleV3_txs.*,
        idleV3_txs.to as account,
        (idleV3_txs.value/1000000000000000000) as amount,
        'TRANSFER_IN' as action
    FROM
        idleV3_txs
    WHERE
        idleV3_txs."evt_tx_hash" NOT IN (SELECT "evt_tx_hash" FROM idleV3_mints)
    AND
        idleV3_txs."evt_tx_hash" NOT IN (SELECT "evt_tx_hash" FROM idleV3_redeems)
    AND
        idleV3_txs.to <> '\x0000000000000000000000000000000000000000'
),
idleV3_allTxs AS (
    SELECT *
    FROM idleV3_redeems
    UNION SELECT * FROM idleV3_mints
    UNION SELECT * FROM idleV3_transfers_out
    UNION SELECT * FROM idleV3_transfers_in
),
idleV3_balances AS (
    SELECT
        idleV3_allTxs.account,
        (
          SELECT
            COALESCE(SUM(idleV3_mints."amount"),0)
           FROM
            idleV3_mints
           WHERE
            idleV3_mints."evt_block_number"<=idleV3_allTxs."evt_block_number"
            AND
            idleV3_mints."account"=idleV3_allTxs."account"
        ) as minted,
        (
            SELECT
            COALESCE(SUM(idleV3_redeems."amount"),0)
           FROM
            idleV3_redeems
           WHERE
            idleV3_redeems."evt_block_number"<=idleV3_allTxs."evt_block_number"
            AND
            idleV3_redeems."account"=idleV3_allTxs."account"
        ) as redeemed,
        (
            SELECT
            COALESCE(SUM(idleV3_transfers_out."amount"),0)
           FROM
            idleV3_transfers_out
           WHERE
            idleV3_transfers_out."evt_block_number"<=idleV3_allTxs."evt_block_number"
            AND
            idleV3_transfers_out."account"=idleV3_allTxs."account"
        ) as transferred_out,
        (
            SELECT
            COALESCE(SUM(idleV3_transfers_in."amount"),0)
           FROM
            idleV3_transfers_in
           WHERE
            idleV3_transfers_in."evt_block_number"<=idleV3_allTxs."evt_block_number"
            AND
            idleV3_transfers_in."account"=idleV3_allTxs."account"
        ) as transferred_in,
        GREATEST((
          (
          SELECT
            COALESCE(SUM(idleV3_mints."amount"),0)
           FROM
            idleV3_mints
           WHERE
            idleV3_mints."evt_block_number"<=idleV3_allTxs."evt_block_number"
            AND
            idleV3_mints."account"=idleV3_allTxs."account"
          )
          +
          (
          SELECT
            COALESCE(SUM(idleV3_redeems."amount"),0)
           FROM
            idleV3_redeems
           WHERE
            idleV3_redeems."evt_block_number"<=idleV3_allTxs."evt_block_number"
            AND
            idleV3_redeems."account"=idleV3_allTxs."account"
          )
          +
           (
            SELECT
            COALESCE(SUM(idleV3_transfers_out."amount"),0)
           FROM
            idleV3_transfers_out
           WHERE
            idleV3_transfers_out."evt_block_number"<=idleV3_allTxs."evt_block_number"
            AND
            idleV3_transfers_out."account"=idleV3_allTxs."account"
          )
          +
          (
            SELECT
            COALESCE(SUM(idleV3_transfers_in."amount"),0)
           FROM
            idleV3_transfers_in
           WHERE
            idleV3_transfers_in."evt_block_number"<=idleV3_allTxs."evt_block_number"
            AND
            idleV3_transfers_in."account"=idleV3_allTxs."account"
          )
        ),0) AS balance,
        idleV3_allTxs."evt_block_number"
    FROM
        idleV3_allTxs
    GROUP BY
        idleV3_allTxs.account,
        idleV3_allTxs.evt_block_number
),
idleV3_tlv AS (
    SELECT
        (
          SELECT
            COALESCE(SUM(idleV3_mints."amount"),0)
           FROM
            idleV3_mints
           WHERE
            idleV3_mints."evt_block_number"<=idleV3_txs."evt_block_number"
        ) as minted,
        (
            SELECT
            COALESCE(SUM(idleV3_redeems."amount"),0)
           FROM
            idleV3_redeems
           WHERE
            idleV3_redeems."evt_block_number"<=idleV3_txs."evt_block_number"
        ) as redeemed,
        (
            SELECT
            COALESCE(SUM(idleV3_transfers_in."amount"),0)
           FROM
            idleV3_transfers_in
           WHERE
            idleV3_transfers_in."evt_block_number"<=idleV3_txs."evt_block_number"
        ) as trasferred_in,
        (
            SELECT
            COALESCE(SUM(idleV3_transfers_out."amount"),0)
           FROM
            idleV3_transfers_out
           WHERE
            idleV3_transfers_out."evt_block_number"<=idleV3_txs."evt_block_number"
        ) as transferred_out,
        (
          (
          SELECT
            COALESCE(SUM(idleV3_mints."amount"),0)
           FROM
            idleV3_mints
           WHERE
            idleV3_mints."evt_block_number"<=idleV3_txs."evt_block_number"
          )
          +
          (
          SELECT
            COALESCE(SUM(idleV3_redeems."amount"),0)
           FROM
            idleV3_redeems
           WHERE
            idleV3_redeems."evt_block_number"<=idleV3_txs."evt_block_number"
          )
          +
           (
            SELECT
            COALESCE(SUM(idleV3_transfers_out."amount"),0)
           FROM
            idleV3_transfers_out
           WHERE
            idleV3_transfers_out."evt_block_number"<=idleV3_txs."evt_block_number"
          )
          +
          (
            SELECT
            COALESCE(SUM(idleV3_transfers_in."amount"),0)
           FROM
            idleV3_transfers_in
           WHERE
            idleV3_transfers_in."evt_block_number"<=idleV3_txs."evt_block_number"
          )
        ) AS tlv,
        idleV3_txs."evt_block_number"
    FROM
        idleV3_txs
    GROUP BY
        idleV3_txs."evt_block_number"
)

SELECT idleV3_balances."evt_block_number",idleV3_balances."account",idleV3_balances."balance" FROM idleV3_balances ORDER BY idleV3_balances."evt_block_number" ASC
-- SELECT idleV3_tlv."evt_block_number",idleV3_tlv."tlv" FROM idleV3_tlv ORDER BY idleV3_tlv."evt_block_number" ASC