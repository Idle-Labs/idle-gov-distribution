WITH idleV1_txs AS (
    SELECT idleV1.*
    FROM
        erc20."ERC20_evt_Transfer" idleV1
    WHERE
        idleV1."evt_tx_hash" IN (SELECT evt_Transfer."evt_tx_hash" FROM idlefinance_v1."IdleDAI_evt_Transfer" evt_Transfer)
    AND
        idleV1."contract_address" IN (
            '\xacf651aad1cbb0fd2c7973e2510d6f63b7e440c9'
        )
    ORDER BY
        idleV1."evt_block_number" ASC
),
idleV1_redeems AS (
    SELECT
        idleV1_txs.*,
        idleV1_txs.from as account,
        (-idleV1_txs.value/1000000000000000000) as amount,
        'REDEEM' as action
    FROM
        idleV1_txs
    INNER JOIN
        (
        SELECT
            idleV1_txs."evt_tx_hash",
            MIN(idleV1_txs."evt_index") evt_index
        FROM idleV1_txs
        GROUP BY idleV1_txs."evt_tx_hash"
        ) idleV1_txs_2
        ON idleV1_txs."evt_tx_hash"=idleV1_txs_2."evt_tx_hash" AND idleV1_txs."evt_index"=idleV1_txs_2."evt_index"
    INNER JOIN
        idlefinance_v1."IdleDAI_call_redeemIdleToken" redeemIdleToken
        ON redeemIdleToken."call_tx_hash"=idleV1_txs."evt_tx_hash"
    WHERE
        redeemIdleToken.call_success='true'
        AND idleV1_txs.from<>'\x0000000000000000000000000000000000000000'
),
idleV1_mints AS (
    SELECT
        idleV1_txs.*,
        idleV1_txs.to as account,
        (idleV1_txs.value/1000000000000000000) as amount,
        'MINT' as action
    FROM
        idleV1_txs
    INNER JOIN
        (
        SELECT
            idleV1_txs."evt_tx_hash",
            MAX(idleV1_txs."evt_index") evt_index
        FROM idleV1_txs
        GROUP BY idleV1_txs."evt_tx_hash"
        ) idleV1_txs_2
        ON idleV1_txs."evt_tx_hash"=idleV1_txs_2."evt_tx_hash" AND idleV1_txs."evt_index"=idleV1_txs_2."evt_index"
    INNER JOIN
        idlefinance_v1."IdleDAI_call_mintIdleToken" mintIdleToken
        ON mintIdleToken."call_tx_hash"=idleV1_txs."evt_tx_hash"
    WHERE
        mintIdleToken.call_success='true'
        AND idleV1_txs.to<>'\x0000000000000000000000000000000000000000'
),
idleV1_transfers_out AS (
    SELECT
        idleV1_txs.*,
        idleV1_txs."from" as account,
        -(idleV1_txs.value/1000000000000000000) as amount,
        'TRANSFER_OUT' as action
    FROM
        idleV1_txs
    WHERE
        idleV1_txs."evt_tx_hash" NOT IN (SELECT "evt_tx_hash" FROM idleV1_mints)
    AND
        idleV1_txs."evt_tx_hash" NOT IN (SELECT "evt_tx_hash" FROM idleV1_redeems)
    AND
        idleV1_txs.from <> '\x0000000000000000000000000000000000000000'
),
idleV1_transfers_in AS (
    SELECT
        idleV1_txs.*,
        idleV1_txs.to as account,
        (idleV1_txs.value/1000000000000000000) as amount,
        'TRANSFER_IN' as action
    FROM
        idleV1_txs
    WHERE
        idleV1_txs."evt_tx_hash" NOT IN (SELECT "evt_tx_hash" FROM idleV1_mints)
    AND
        idleV1_txs."evt_tx_hash" NOT IN (SELECT "evt_tx_hash" FROM idleV1_redeems)
    AND
        idleV1_txs.to <> '\x0000000000000000000000000000000000000000'
),
idleV1_allTxs AS (
    SELECT *
    FROM idleV1_redeems
    UNION SELECT * FROM idleV1_mints
    UNION SELECT * FROM idleV1_transfers_out
    UNION SELECT * FROM idleV1_transfers_in
),
idleV1_balances AS (
    SELECT
        idleV1_allTxs.account,
        (
          SELECT
            COALESCE(SUM(idleV1_mints."amount"),0)
           FROM
            idleV1_mints
           WHERE
            idleV1_mints."evt_block_number"<=idleV1_allTxs."evt_block_number"
            AND
            idleV1_mints."account"=idleV1_allTxs."account"
        ) as minted,
        (
            SELECT
            COALESCE(SUM(idleV1_redeems."amount"),0)
           FROM
            idleV1_redeems
           WHERE
            idleV1_redeems."evt_block_number"<=idleV1_allTxs."evt_block_number"
            AND
            idleV1_redeems."account"=idleV1_allTxs."account"
        ) as redeemed,
        (
            SELECT
            COALESCE(SUM(idleV1_transfers_out."amount"),0)
           FROM
            idleV1_transfers_out
           WHERE
            idleV1_transfers_out."evt_block_number"<=idleV1_allTxs."evt_block_number"
            AND
            idleV1_transfers_out."account"=idleV1_allTxs."account"
        ) as transferred_out,
        (
            SELECT
            COALESCE(SUM(idleV1_transfers_in."amount"),0)
           FROM
            idleV1_transfers_in
           WHERE
            idleV1_transfers_in."evt_block_number"<=idleV1_allTxs."evt_block_number"
            AND
            idleV1_transfers_in."account"=idleV1_allTxs."account"
        ) as transferred_in,
        GREATEST((
          (
          SELECT
            COALESCE(SUM(idleV1_mints."amount"),0)
           FROM
            idleV1_mints
           WHERE
            idleV1_mints."evt_block_number"<=idleV1_allTxs."evt_block_number"
            AND
            idleV1_mints."account"=idleV1_allTxs."account"
          )
          +
          (
          SELECT
            COALESCE(SUM(idleV1_redeems."amount"),0)
           FROM
            idleV1_redeems
           WHERE
            idleV1_redeems."evt_block_number"<=idleV1_allTxs."evt_block_number"
            AND
            idleV1_redeems."account"=idleV1_allTxs."account"
          )
          +
           (
            SELECT
            COALESCE(SUM(idleV1_transfers_out."amount"),0)
           FROM
            idleV1_transfers_out
           WHERE
            idleV1_transfers_out."evt_block_number"<=idleV1_allTxs."evt_block_number"
            AND
            idleV1_transfers_out."account"=idleV1_allTxs."account"
          )
          +
          (
            SELECT
            COALESCE(SUM(idleV1_transfers_in."amount"),0)
           FROM
            idleV1_transfers_in
           WHERE
            idleV1_transfers_in."evt_block_number"<=idleV1_allTxs."evt_block_number"
            AND
            idleV1_transfers_in."account"=idleV1_allTxs."account"
          )
        ),0) AS balance,
        idleV1_allTxs."evt_block_number"
    FROM
        idleV1_allTxs
    GROUP BY
        idleV1_allTxs.account,
        idleV1_allTxs.evt_block_number
),
idleV1_tlv AS (
    SELECT
        (
          SELECT
            COALESCE(SUM(idleV1_mints."amount"),0)
           FROM
            idleV1_mints
           WHERE
            idleV1_mints."evt_block_number"<=idleV1_txs."evt_block_number"
        ) as minted,
        (
            SELECT
            COALESCE(SUM(idleV1_redeems."amount"),0)
           FROM
            idleV1_redeems
           WHERE
            idleV1_redeems."evt_block_number"<=idleV1_txs."evt_block_number"
        ) as redeemed,
        (
            SELECT
            COALESCE(SUM(idleV1_transfers_in."amount"),0)
           FROM
            idleV1_transfers_in
           WHERE
            idleV1_transfers_in."evt_block_number"<=idleV1_txs."evt_block_number"
        ) as trasferred_in,
        (
            SELECT
            COALESCE(SUM(idleV1_transfers_out."amount"),0)
           FROM
            idleV1_transfers_out
           WHERE
            idleV1_transfers_out."evt_block_number"<=idleV1_txs."evt_block_number"
        ) as transferred_out,
        (
          (
          SELECT
            COALESCE(SUM(idleV1_mints."amount"),0)
           FROM
            idleV1_mints
           WHERE
            idleV1_mints."evt_block_number"<=idleV1_txs."evt_block_number"
          )
          +
          (
          SELECT
            COALESCE(SUM(idleV1_redeems."amount"),0)
           FROM
            idleV1_redeems
           WHERE
            idleV1_redeems."evt_block_number"<=idleV1_txs."evt_block_number"
          )
          +
           (
            SELECT
            COALESCE(SUM(idleV1_transfers_out."amount"),0)
           FROM
            idleV1_transfers_out
           WHERE
            idleV1_transfers_out."evt_block_number"<=idleV1_txs."evt_block_number"
          )
          +
          (
            SELECT
            COALESCE(SUM(idleV1_transfers_in."amount"),0)
           FROM
            idleV1_transfers_in
           WHERE
            idleV1_transfers_in."evt_block_number"<=idleV1_txs."evt_block_number"
          )
        ) AS tlv,
        idleV1_txs."evt_block_number"
    FROM
        idleV1_txs
    GROUP BY
        idleV1_txs."evt_block_number"
)
SELECT idleV1_balances."evt_block_number",idleV1_balances."account",idleV1_balances."balance" FROM idleV1_balances ORDER BY idleV1_balances."evt_block_number" ASC
 --SELECT idleV1_tlv."evt_block_number",idleV1_tlv."tlv" FROM idleV1_tlv ORDER BY idleV1_tlv."evt_block_number" ASC