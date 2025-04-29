// config.js
require('dotenv').config();

// Helper function to parse integer environment variables with a default value
const parseIntEnv = (envVar, defaultValue) => {
    const value = parseInt(envVar, 10);
    return isNaN(value) ? defaultValue : value;
};

// Ethereum specific constants
const WEI_IN_ETH = 10n ** 18n; // Using BigInt for precision

// --- Define Known CEX Label Keywords (Case-Insensitive) ---
// Automatically generated from the unique values in eth_wallets.txt
const KNOWN_CEX_KEYWORDS = new Set([
    'bitfinex',
    'indodax',
    'htx', // Covers former Huobi as well
    'gemini',
    'korbit',
    'binance',
    'okx',
    'gate.io',
    'kucoin',
    'lbank',
    'coinex',
    'bitmart',
    'bitrue',
    'bitkub',
    'bitget',
    'coinw',
    'bybit',
    'deribit',
    'mexc',
    'pionex',
    'hashkey exchange',
    'biconomy.com',
    'hotcoin',
    'coindcx',
    'phemex',
    'bingx',
    'crypto.com exchange', // More specific than just 'crypto.com' if labels match
    'deepcoin',
    'fameex',
    'toobit',
    'flipster',
    'blofin',
    'bitunix',
    'bvox',
    'orangex',
    'backpack exchange',
    'hashkey global',
    'ourbit',
    'arkham',
    // Add any other common variations or keywords if needed, even if not in the file
    'kraken',
    'coinbase',
    'robinhood',
    'bitstamp',
    'crypto.com' // Keep generic one too if labels might vary
]);
// --- End CEX Keywords ---

module.exports = {
    // Server config
    port: process.env.PORT || 3005, // Use the port from .env or default

    // MongoDB Config
    mongoUsername: process.env.MONGODB_USERNAME,
    mongoPassword: process.env.MONGODB_PASSWORD,
    mongoCluster: process.env.MONGODB_CLUSTER,
    dbName: process.env.DATABASE_NAME || 'quicknode', // Default DB name
    // Use the specific ETH label collection name from .env
    labelCollection: process.env.WALLET_LABEL_COLLECTION_NAME || 'ETH_Wallet_Labels',
    // Optional: Collection for storing processed transactions
    // ethTransactionCollectionName: process.env.ETH_TRANSACTION_COLLECTION_NAME || 'ETH_Transactions',
    mongoAuthSource: process.env.MONGODB_AUTH_SOURCE || 'admin',
    mongoAuthMechanism: process.env.MONGODB_AUTH_MECHANISM, // Can be undefined if not set

    // Discord Config
    discordWebhookUrl: process.env.DISCORD_WEBHOOK_URL,

    // Ethereum Config
    WEI_IN_ETH: WEI_IN_ETH,

    // Report Config
    // *** UPDATED VARIABLE NAME AND VALUE ***
    topNCexEntriesToShow: parseIntEnv(process.env.TOP_N_CEX_ENTRIES_TO_SHOW, 15), // Max CEX entries to show in the report list
    knownCexKeywords: KNOWN_CEX_KEYWORDS, // Export the updated set of CEX keywords

    // Price Fetcher Config
    priceApiId: process.env.COINGECKO_API_ID || 'ethereum', // CoinGecko ID for ETH

    // App specific settings - Etherscan URLs
    blockExplorerUrlTemplate: "https://etherscan.io/tx/{txHash}",
    addressExplorerUrlTemplate: "https://etherscan.io/address/{address}",
};
