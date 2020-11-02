WITH idleV4_txs AS (
    SELECT idleV4.*
    FROM
        erc20."ERC20_evt_Transfer" idleV4
    WHERE
        idleV4."evt_tx_hash" IN (SELECT evt_Transfer."evt_tx_hash" FROM idle_v4."IdleTUSD_v4_Yield_evt_Transfer" evt_Transfer)
    AND
        idleV4."contract_address" IN ('\xc278041fDD8249FE4c1Aad1193876857EEa3D68c')
    ORDER BY
        idleV4."evt_block_number" ASC
),
idleV4_redeems AS (
    SELECT
        idleV4_txs.*,
        idleV4_txs.from as account,
        (-idleV4_txs.value/1000000000000000000) as amount,
        'REDEEM' as action
    FROM
        idleV4_txs
    INNER JOIN
        (
        SELECT
            idleV4_txs."evt_tx_hash",
            MIN(idleV4_txs."evt_index") evt_index
        FROM idleV4_txs
        GROUP BY idleV4_txs."evt_tx_hash"
        ) idleV4_txs_2
        ON idleV4_txs."evt_tx_hash"=idleV4_txs_2."evt_tx_hash" AND idleV4_txs."evt_index"=idleV4_txs_2."evt_index"
    INNER JOIN
        idle_v4."IdleTUSD_v4_Yield_call_redeemIdleToken" redeemIdleToken
        ON redeemIdleToken."call_tx_hash"=idleV4_txs."evt_tx_hash"
    WHERE
        redeemIdleToken.call_success='true'
),
idleV4_mints AS (
    SELECT
        idleV4_txs.*,
        idleV4_txs.to as account,
        (idleV4_txs.value/1000000000000000000) as amount,
        'MINT' as action
    FROM
        idleV4_txs
    INNER JOIN
        (
        SELECT
            idleV4_txs."evt_tx_hash",
            MAX(idleV4_txs."evt_index") evt_index
        FROM idleV4_txs
        GROUP BY idleV4_txs."evt_tx_hash"
        ) idleV4_txs_2
        ON idleV4_txs."evt_tx_hash"=idleV4_txs_2."evt_tx_hash" AND idleV4_txs."evt_index"=idleV4_txs_2."evt_index"
    INNER JOIN
        idle_v4."IdleTUSD_v4_Yield_call_mintIdleToken" mintIdleToken
        ON mintIdleToken."call_tx_hash"=idleV4_txs."evt_tx_hash"
    WHERE
        mintIdleToken.call_success='true'
),
idleV4_transfers_out AS (
    SELECT
        idleV4_txs.*,
        idleV4_txs."from" as account,
        -(idleV4_txs.value/1000000000000000000) as amount,
        'TRANSFER_OUT' as action
    FROM
        idleV4_txs
    WHERE
        idleV4_txs."evt_tx_hash" NOT IN (SELECT "evt_tx_hash" FROM idleV4_mints)
    AND
        idleV4_txs."evt_tx_hash" NOT IN (SELECT "evt_tx_hash" FROM idleV4_redeems)
    AND
        idleV4_txs.from <> '\x0000000000000000000000000000000000000000'
),
idleV4_transfers_in AS (
    SELECT
        idleV4_txs.*,
        idleV4_txs.to as account,
        (idleV4_txs.value/1000000000000000000) as amount,
        'TRANSFER_IN' as action
    FROM
        idleV4_txs
    WHERE
        idleV4_txs."evt_tx_hash" NOT IN (SELECT "evt_tx_hash" FROM idleV4_mints)
    AND
        idleV4_txs."evt_tx_hash" NOT IN (SELECT "evt_tx_hash" FROM idleV4_redeems)
    AND
        idleV4_txs.to <> '\x0000000000000000000000000000000000000000'
),
idleV4_allTxs AS (
    SELECT *
    FROM idleV4_redeems
    UNION SELECT * FROM idleV4_mints
    UNION SELECT * FROM idleV4_transfers_out
    UNION SELECT * FROM idleV4_transfers_in
),
idleV4_balances AS (
    SELECT
        idleV4_allTxs.account,
        (
          SELECT
            COALESCE(SUM(idleV4_mints."amount"),0)
           FROM
            idleV4_mints
           WHERE
            idleV4_mints."evt_block_number"<=idleV4_allTxs."evt_block_number"
            AND
            idleV4_mints."account"=idleV4_allTxs."account"
        ) as minted,
        (
            SELECT
            COALESCE(SUM(idleV4_redeems."amount"),0)
           FROM
            idleV4_redeems
           WHERE
            idleV4_redeems."evt_block_number"<=idleV4_allTxs."evt_block_number"
            AND
            idleV4_redeems."account"=idleV4_allTxs."account"
        ) as redeemed,
        (
            SELECT
            COALESCE(SUM(idleV4_transfers_out."amount"),0)
           FROM
            idleV4_transfers_out
           WHERE
            idleV4_transfers_out."evt_block_number"<=idleV4_allTxs."evt_block_number"
            AND
            idleV4_transfers_out."account"=idleV4_allTxs."account"
        ) as transferred_out,
        (
            SELECT
            COALESCE(SUM(idleV4_transfers_in."amount"),0)
           FROM
            idleV4_transfers_in
           WHERE
            idleV4_transfers_in."evt_block_number"<=idleV4_allTxs."evt_block_number"
            AND
            idleV4_transfers_in."account"=idleV4_allTxs."account"
        ) as transferred_in,
        GREATEST((
          (
          SELECT
            COALESCE(SUM(idleV4_mints."amount"),0)
           FROM
            idleV4_mints
           WHERE
            idleV4_mints."evt_block_number"<=idleV4_allTxs."evt_block_number"
            AND
            idleV4_mints."account"=idleV4_allTxs."account"
          )
          +
          (
          SELECT
            COALESCE(SUM(idleV4_redeems."amount"),0)
           FROM
            idleV4_redeems
           WHERE
            idleV4_redeems."evt_block_number"<=idleV4_allTxs."evt_block_number"
            AND
            idleV4_redeems."account"=idleV4_allTxs."account"
          )
          +
           (
            SELECT
            COALESCE(SUM(idleV4_transfers_out."amount"),0)
           FROM
            idleV4_transfers_out
           WHERE
            idleV4_transfers_out."evt_block_number"<=idleV4_allTxs."evt_block_number"
            AND
            idleV4_transfers_out."account"=idleV4_allTxs."account"
          )
          +
          (
            SELECT
            COALESCE(SUM(idleV4_transfers_in."amount"),0)
           FROM
            idleV4_transfers_in
           WHERE
            idleV4_transfers_in."evt_block_number"<=idleV4_allTxs."evt_block_number"
            AND
            idleV4_transfers_in."account"=idleV4_allTxs."account"
          )
        ),0) AS balance,
        idleV4_allTxs."evt_block_number"
    FROM
        idleV4_allTxs
    GROUP BY
        idleV4_allTxs.account,
        idleV4_allTxs.evt_block_number
),
idleV4_tlv AS (
    SELECT
        (
          SELECT
            COALESCE(SUM(idleV4_mints."amount"),0)
           FROM
            idleV4_mints
           WHERE
            idleV4_mints."evt_block_number"<=idleV4_txs."evt_block_number"
        ) as minted,
        (
            SELECT
            COALESCE(SUM(idleV4_redeems."amount"),0)
           FROM
            idleV4_redeems
           WHERE
            idleV4_redeems."evt_block_number"<=idleV4_txs."evt_block_number"
        ) as redeemed,
        (
            SELECT
            COALESCE(SUM(idleV4_transfers_in."amount"),0)
           FROM
            idleV4_transfers_in
           WHERE
            idleV4_transfers_in."evt_block_number"<=idleV4_txs."evt_block_number"
        ) as trasferred_in,
        (
            SELECT
            COALESCE(SUM(idleV4_transfers_out."amount"),0)
           FROM
            idleV4_transfers_out
           WHERE
            idleV4_transfers_out."evt_block_number"<=idleV4_txs."evt_block_number"
        ) as transferred_out,
        (
          (
          SELECT
            COALESCE(SUM(idleV4_mints."amount"),0)
           FROM
            idleV4_mints
           WHERE
            idleV4_mints."evt_block_number"<=idleV4_txs."evt_block_number"
          )
          +
          (
          SELECT
            COALESCE(SUM(idleV4_redeems."amount"),0)
           FROM
            idleV4_redeems
           WHERE
            idleV4_redeems."evt_block_number"<=idleV4_txs."evt_block_number"
          )
          +
           (
            SELECT
            COALESCE(SUM(idleV4_transfers_out."amount"),0)
           FROM
            idleV4_transfers_out
           WHERE
            idleV4_transfers_out."evt_block_number"<=idleV4_txs."evt_block_number"
          )
          +
          (
            SELECT
            COALESCE(SUM(idleV4_transfers_in."amount"),0)
           FROM
            idleV4_transfers_in
           WHERE
            idleV4_transfers_in."evt_block_number"<=idleV4_txs."evt_block_number"
          )
        ) AS tlv,
        idleV4_txs."evt_block_number"
    FROM
        idleV4_txs
    GROUP BY
        idleV4_txs."evt_block_number"
)

SELECT idleV4_balances."evt_block_number",idleV4_balances."account",idleV4_balances."balance" FROM idleV4_balances ORDER BY idleV4_balances."evt_block_number" ASC
-- SELECT idleV4_tlv."evt_block_number",idleV4_tlv."tlv" FROM idleV4_tlv ORDER BY idleV4_tlv."evt_block_number" ASC