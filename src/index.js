BigNumber = require('bignumber.js');

// V1
DAIV1 = require('./v1/DAIV1.js');

// V2
DAIYieldV2 = require('./v2/DAIYieldV2');
USDCYieldV2 = require('./v2/USDCYieldV2');

// V3
DAIYieldV3 = require('./v3/DAIYieldV3');
USDCYieldV3 = require('./v3/USDCYieldV3');
USDTYieldV3 = require('./v3/USDTYieldV3');
SUSDYieldV3 = require('./v3/SUSDYieldV3');
TUSDYieldV3 = require('./v3/TUSDYieldV3');
WBTCYieldV3 = require('./v3/WBTCYieldV3');

DAISafeV3 = require('./v3/DAISafeV3');
USDCSafeV3 = require('./v3/USDCSafeV3');
USDTSafeV3 = require('./v3/USDTSafeV3');


// V4 Yield
DAIYieldV4 = require('./v4/DAIYieldV4');
USDCYieldV4 = require('./v4/USDCYieldV4');
USDTYieldV4 = require('./v4/USDTYieldV4');
SUSDYieldV4 = require('./v4/SUSDYieldV4');
TUSDYieldV4 = require('./v4/TUSDYieldV4');
WBTCYieldV4 = require('./v4/WBTCYieldV4');

// V4 Safe
DAISafeV4 = require('./v4/DAISafeV4');
USDCSafeV4 = require('./v4/USDCSafeV4');
USDTSafeV4 = require('./v4/USDTSafeV4');

const balances_blocks = {
  // V2
  DAIYieldV2:DAIYieldV2.balances,
  USDCYieldV2:USDCYieldV2.balances,

  // V3
  DAIYieldV3:DAIYieldV3.balances,
  USDCYieldV3:USDCYieldV3.balances,
  USDTYieldV3:USDTYieldV3.balances,
  SUSDYieldV3:SUSDYieldV3.balances,
  TUSDYieldV3:TUSDYieldV3.balances,
  WBTCYieldV3:WBTCYieldV3.balances,

  DAISafeV3:DAISafeV3.balances,
  USDCSafeV3:USDCSafeV3.balances,
  USDTSafeV3:USDTSafeV3.balances,

  // V4 Yield
  DAIYieldV4:DAIYieldV4.balances,
  USDCYieldV4:USDCYieldV4.balances,
  USDTYieldV4:USDTYieldV4.balances,
  SUSDYieldV4:SUSDYieldV4.balances,
  TUSDYieldV4:TUSDYieldV4.balances,
  WBTCYieldV4:WBTCYieldV4.balances,

  // V4 Safe
  DAISafeV4:DAISafeV4.balances,
  USDCSafeV4:USDCSafeV4.balances,
  USDTSafeV4:USDTSafeV4.balances,
};

// Customizable variables
const startBlock = null;;
const endBlock = 11174327;
// 4% of total supply
const govTokensV1 = 20000; // Tokens to be distributed for IdleV1
const govTokensV2V4 = 500000-govTokensV1; // Tokens to be distributed from IdleV2 to IdleV4

let all_blocks = [];
Object.keys(balances_blocks).forEach( token => {
  const blocks = balances_blocks[token].map( t => (t[0]) );
  all_blocks = all_blocks.concat(blocks);
});

all_blocks = all_blocks
  .filter( (value,index,self) => (self.indexOf(value) === index) )
  .filter( blockNumber => ((!startBlock || parseInt(blockNumber)>=startBlock ) && (!endBlock || parseInt(blockNumber)<=endBlock )) )
  .sort( (a,b) => (a-b) );

const BNify = s => new BigNumber( typeof s === 'object' ? s : String(s) );

const firstBlock = BNify(all_blocks[0]);
const lastBlock = BNify(all_blocks[all_blocks.length-1]);
const totalBlocks = lastBlock.minus(firstBlock);
const tokensPerBlock = BNify(govTokensV2V4).div(totalBlocks);

const accounts_tokens = {};
const accounts_shares = {};
const all_tokens = Object.keys(balances_blocks);

// Distribute $IDLE for V1 addresses
if (govTokensV1){
  const idleV1UniqueAddresses = DAIV1.balances
                        .map( b => (b[1]) )
                        .filter( (value,index,self) => (self.indexOf(value) === index) );

  const govTokensPerAddress = BNify(govTokensV1).div(BNify(idleV1UniqueAddresses.length));
  idleV1UniqueAddresses.forEach( address => {
    accounts_tokens[address] = govTokensPerAddress;
  });
}

// Distribute remaining tokens for V2-V3-V4
const getUsersBalances = (token,blockNumber,convertBalance=false) => {
  const filtered_balances = balances_blocks[token].filter( b => (parseInt(b[0])<=blockNumber) );

  if (!filtered_balances || !filtered_balances.length){
    return {};
  }

  const lastBalance = filtered_balances[filtered_balances.length-1];
  const lastPrice = lastBalance.length>3 ? BNify(lastBalance[3]) : null;

  const latest_balances = filtered_balances.reduce( (latest_balances,b) => {
    const account = b[1];
    let balance = BNify(b[2]);
    if (convertBalance && lastPrice){
      balance = balance.times(lastPrice);
    }
    latest_balances[account] = balance;
    return latest_balances;
  },{});

  return latest_balances;
}

const getTokenAUM = (token,blockNumber,convertBalance=true) => {
  const latest_balances = getUsersBalances(token,blockNumber,convertBalance);
  const tokenAUM = Object.values(latest_balances).reduce( (tokenAUM,balance) => {
    return tokenAUM.plus(balance);
  },BNify(0));

  return tokenAUM;
}

let prevBlockNumber = null;
all_blocks.forEach( blockNumber => {
  // Init accounts_shares
  accounts_shares[blockNumber] = {};

  const totalAUM = all_tokens.reduce( (totalAUM,token) => {
    const tokenAUM = getTokenAUM(token,blockNumber);
    return totalAUM.plus(tokenAUM);
  },BNify(0));

  all_tokens.forEach( token => {
    // Take balances in underlying token
    const latest_balances = getUsersBalances(token,blockNumber);

    // Take AUM in underlying (for WBTC)
    const tokenAUM = getTokenAUM(token,blockNumber,false);

    // Take AUM in USD (for WBTC)
    const convertedAUM = getTokenAUM(token,blockNumber);

    Object.keys(latest_balances).forEach( account => {
      const balance = latest_balances[account];
      const shares = balance.div(tokenAUM);

      if (!accounts_shares[blockNumber][account]){
        accounts_shares[blockNumber][account] = {
          total:{
            totalAUM,
            shares:BNify(0),
            balance:BNify(0),
            govTokens:BNify(0),
          }
        };
      }

      accounts_shares[blockNumber][account][token] = {
        shares,
        balance,
        tokenAUM
      };

      accounts_shares[blockNumber][account].total.balance = accounts_shares[blockNumber][account].total.balance.plus(balance);
      accounts_shares[blockNumber][account].total.shares = accounts_shares[blockNumber][account].total.shares.plus(shares.times(convertedAUM));
    });
  });

  // Calculate total shares
  if (prevBlockNumber){
    const blocks = prevBlockNumber ? blockNumber-prevBlockNumber : 0;
    Object.keys(accounts_shares[blockNumber]).forEach( account => {
      const totalBalance = accounts_shares[blockNumber][account].total.balance;
      const totalShares = accounts_shares[blockNumber][account].total.shares.div(totalAUM);
      const govTokens = BNify(totalShares).times(BNify(blocks)).times(tokensPerBlock);
      accounts_shares[blockNumber][account].total.shares = totalShares;
      accounts_shares[blockNumber][account].total.govTokens = govTokens;

      if (!accounts_tokens[account]){
        accounts_tokens[account] = BNify(0);
      }
      accounts_tokens[account] = accounts_tokens[account].plus(govTokens);
    });
  }

  prevBlockNumber = blockNumber;
});

// Count total gov tokens distributed
const totalGovTokensTest = Object.keys(accounts_tokens).reduce( (totalGovTokensTest,account) => {
  return totalGovTokensTest.plus(accounts_tokens[account]);
},BNify(0));

const sorted_accounts_tokens = Object.keys(accounts_tokens)
                                .filter( account => (accounts_tokens[account].gt(0)) )
                                .map( account => ([account,accounts_tokens[account]]) )
                                .sort( (a,b) => (b[1].minus(a[1])) );


// console.log('TOTAL $IDLE DISTRIBUTED:',totalGovTokensTest.times(1e18).integerValue(BigNumber.ROUND_FLOOR).toFixed());

// Create sorted CSV
sorted_accounts_tokens.forEach( t => {
  const normalizedTokenAmount = t[1].times(1e18).integerValue(BigNumber.ROUND_FLOOR).toFixed();
  console.log('0x'+t[0]+','+normalizedTokenAmount);
});

// console.log(firstBlock,lastBlock,tokensPerBlock,totalGovTokensTest.toString(),accounts_tokens);
