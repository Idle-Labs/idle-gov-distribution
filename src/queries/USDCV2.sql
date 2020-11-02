WITH idleV2_txs AS (
    SELECT idleV2.*
    FROM
        erc20."ERC20_evt_Transfer" idleV2
    WHERE
        idleV2."evt_tx_hash" IN (SELECT evt_Transfer."evt_tx_hash" FROM idle_v2."IdleToken_evt_Transfer" evt_Transfer)
    AND
        idleV2."contract_address" IN (
            '\xeB66ACc3d011056B00ea521F8203580C2E5d3991'
        )
    ORDER BY
        idleV2."evt_block_number" ASC
),
idleV2_redeems AS (
    SELECT
        idleV2_txs.*,
        idleV2_txs.from as account,
        (-idleV2_txs.value/1000000000000000000) as amount,
        'REDEEM' as action
    FROM
        idleV2_txs
    INNER JOIN
        (
        SELECT
            idleV2_txs."evt_tx_hash",
            MIN(idleV2_txs."evt_index") evt_index
        FROM idleV2_txs
        GROUP BY idleV2_txs."evt_tx_hash"
        ) idleV2_txs_2
        ON idleV2_txs."evt_tx_hash"=idleV2_txs_2."evt_tx_hash" AND idleV2_txs."evt_index"=idleV2_txs_2."evt_index"
    INNER JOIN
        idle_v2."IdleToken_call_redeemIdleToken" redeemIdleToken
        ON redeemIdleToken."call_tx_hash"=idleV2_txs."evt_tx_hash"
    WHERE
        redeemIdleToken.call_success='true'
        AND idleV2_txs.from<>'\x0000000000000000000000000000000000000000'
),
idleV2_mints AS (
    SELECT
        idleV2_txs.*,
        idleV2_txs.to as account,
        (idleV2_txs.value/1000000000000000000) as amount,
        'MINT' as action
    FROM
        idleV2_txs
    INNER JOIN
        (
        SELECT
            idleV2_txs."evt_tx_hash",
            MAX(idleV2_txs."evt_index") evt_index
        FROM idleV2_txs
        GROUP BY idleV2_txs."evt_tx_hash"
        ) idleV2_txs_2
        ON idleV2_txs."evt_tx_hash"=idleV2_txs_2."evt_tx_hash" AND idleV2_txs."evt_index"=idleV2_txs_2."evt_index"
    INNER JOIN
        idle_v2."IdleToken_call_mintIdleToken" mintIdleToken
        ON mintIdleToken."call_tx_hash"=idleV2_txs."evt_tx_hash"
    WHERE
        mintIdleToken.call_success='true'
        AND idleV2_txs.to<>'\x0000000000000000000000000000000000000000'
),
idleV2_transfers_out AS (
    SELECT
        idleV2_txs.*,
        idleV2_txs."from" as account,
        -(idleV2_txs.value/1000000000000000000) as amount,
        'TRANSFER_OUT' as action
    FROM
        idleV2_txs
    WHERE
        idleV2_txs."evt_tx_hash" NOT IN (SELECT "evt_tx_hash" FROM idleV2_mints)
    AND
        idleV2_txs."evt_tx_hash" NOT IN (SELECT "evt_tx_hash" FROM idleV2_redeems)
    AND
        idleV2_txs.from <> '\x0000000000000000000000000000000000000000'
),
idleV2_transfers_in AS (
    SELECT
        idleV2_txs.*,
        idleV2_txs.to as account,
        (idleV2_txs.value/1000000000000000000) as amount,
        'TRANSFER_IN' as action
    FROM
        idleV2_txs
    WHERE
        idleV2_txs."evt_tx_hash" NOT IN (SELECT "evt_tx_hash" FROM idleV2_mints)
    AND
        idleV2_txs."evt_tx_hash" NOT IN (SELECT "evt_tx_hash" FROM idleV2_redeems)
    AND
        idleV2_txs.to <> '\x0000000000000000000000000000000000000000'
),
idleV2_allTxs AS (
    SELECT *
    FROM idleV2_redeems
    UNION SELECT * FROM idleV2_mints
    UNION SELECT * FROM idleV2_transfers_out
    UNION SELECT * FROM idleV2_transfers_in
),
idleV2_balances AS (
    SELECT
        idleV2_allTxs.account,
        (
          SELECT
            COALESCE(SUM(idleV2_mints."amount"),0)
           FROM
            idleV2_mints
           WHERE
            idleV2_mints."evt_block_number"<=idleV2_allTxs."evt_block_number"
            AND
            idleV2_mints."account"=idleV2_allTxs."account"
        ) as minted,
        (
            SELECT
            COALESCE(SUM(idleV2_redeems."amount"),0)
           FROM
            idleV2_redeems
           WHERE
            idleV2_redeems."evt_block_number"<=idleV2_allTxs."evt_block_number"
            AND
            idleV2_redeems."account"=idleV2_allTxs."account"
        ) as redeemed,
        (
            SELECT
            COALESCE(SUM(idleV2_transfers_out."amount"),0)
           FROM
            idleV2_transfers_out
           WHERE
            idleV2_transfers_out."evt_block_number"<=idleV2_allTxs."evt_block_number"
            AND
            idleV2_transfers_out."account"=idleV2_allTxs."account"
        ) as transferred_out,
        (
            SELECT
            COALESCE(SUM(idleV2_transfers_in."amount"),0)
           FROM
            idleV2_transfers_in
           WHERE
            idleV2_transfers_in."evt_block_number"<=idleV2_allTxs."evt_block_number"
            AND
            idleV2_transfers_in."account"=idleV2_allTxs."account"
        ) as transferred_in,
        GREATEST((
          (
          SELECT
            COALESCE(SUM(idleV2_mints."amount"),0)
           FROM
            idleV2_mints
           WHERE
            idleV2_mints."evt_block_number"<=idleV2_allTxs."evt_block_number"
            AND
            idleV2_mints."account"=idleV2_allTxs."account"
          )
          +
          (
          SELECT
            COALESCE(SUM(idleV2_redeems."amount"),0)
           FROM
            idleV2_redeems
           WHERE
            idleV2_redeems."evt_block_number"<=idleV2_allTxs."evt_block_number"
            AND
            idleV2_redeems."account"=idleV2_allTxs."account"
          )
          +
           (
            SELECT
            COALESCE(SUM(idleV2_transfers_out."amount"),0)
           FROM
            idleV2_transfers_out
           WHERE
            idleV2_transfers_out."evt_block_number"<=idleV2_allTxs."evt_block_number"
            AND
            idleV2_transfers_out."account"=idleV2_allTxs."account"
          )
          +
          (
            SELECT
            COALESCE(SUM(idleV2_transfers_in."amount"),0)
           FROM
            idleV2_transfers_in
           WHERE
            idleV2_transfers_in."evt_block_number"<=idleV2_allTxs."evt_block_number"
            AND
            idleV2_transfers_in."account"=idleV2_allTxs."account"
          )
        ),0) AS balance,
        idleV2_allTxs."evt_block_number"
    FROM
        idleV2_allTxs
    GROUP BY
        idleV2_allTxs.account,
        idleV2_allTxs.evt_block_number
),
idleV2_tlv AS (
    SELECT
        (
          SELECT
            COALESCE(SUM(idleV2_mints."amount"),0)
           FROM
            idleV2_mints
           WHERE
            idleV2_mints."evt_block_number"<=idleV2_txs."evt_block_number"
        ) as minted,
        (
            SELECT
            COALESCE(SUM(idleV2_redeems."amount"),0)
           FROM
            idleV2_redeems
           WHERE
            idleV2_redeems."evt_block_number"<=idleV2_txs."evt_block_number"
        ) as redeemed,
        (
            SELECT
            COALESCE(SUM(idleV2_transfers_in."amount"),0)
           FROM
            idleV2_transfers_in
           WHERE
            idleV2_transfers_in."evt_block_number"<=idleV2_txs."evt_block_number"
        ) as trasferred_in,
        (
            SELECT
            COALESCE(SUM(idleV2_transfers_out."amount"),0)
           FROM
            idleV2_transfers_out
           WHERE
            idleV2_transfers_out."evt_block_number"<=idleV2_txs."evt_block_number"
        ) as transferred_out,
        (
          (
          SELECT
            COALESCE(SUM(idleV2_mints."amount"),0)
           FROM
            idleV2_mints
           WHERE
            idleV2_mints."evt_block_number"<=idleV2_txs."evt_block_number"
          )
          +
          (
          SELECT
            COALESCE(SUM(idleV2_redeems."amount"),0)
           FROM
            idleV2_redeems
           WHERE
            idleV2_redeems."evt_block_number"<=idleV2_txs."evt_block_number"
          )
          +
           (
            SELECT
            COALESCE(SUM(idleV2_transfers_out."amount"),0)
           FROM
            idleV2_transfers_out
           WHERE
            idleV2_transfers_out."evt_block_number"<=idleV2_txs."evt_block_number"
          )
          +
          (
            SELECT
            COALESCE(SUM(idleV2_transfers_in."amount"),0)
           FROM
            idleV2_transfers_in
           WHERE
            idleV2_transfers_in."evt_block_number"<=idleV2_txs."evt_block_number"
          )
        ) AS tlv,
        idleV2_txs."evt_block_number"
    FROM
        idleV2_txs
    GROUP BY
        idleV2_txs."evt_block_number"
)

SELECT idleV2_balances."evt_block_number",idleV2_balances."account",idleV2_balances."balance" FROM idleV2_balances ORDER BY idleV2_balances."evt_block_number" ASC
-- SELECT idleV2_tlv."evt_block_number",idleV2_tlv."tlv" FROM idleV2_tlv ORDER BY idleV2_tlv."evt_block_number" ASC