// config.js
// Load environment variables from .env file into process.env
require('dotenv').config();

// --- Helper Functions for Parsing Environment Variables ---

/**
 * Parses an environment variable as an integer with a default fallback.
 * @param {string | undefined} envVar - The environment variable value (process.env.VAR_NAME).
 * @param {number} defaultValue - The value to return if parsing fails or the variable is not set.
 * @returns {number} The parsed integer or the default value.
 */
const parseIntEnv = (envVar, defaultValue) => {
    const value = parseInt(envVar, 10); // Base 10 parsing
    return isNaN(value) ? defaultValue : value;
};

/**
 * Parses an environment variable as a float (decimal number) with a default fallback.
 * @param {string | undefined} envVar - The environment variable value (process.env.VAR_NAME).
 * @param {number} defaultValue - The value to return if parsing fails or the variable is not set.
 * @returns {number} The parsed float or the default value.
 */
const parseFloatEnv = (envVar, defaultValue) => {
    const value = parseFloat(envVar);
    return isNaN(value) ? defaultValue : value;
};

// --- Constants ---

// Ethereum specific constants
const WEI_IN_ETH = 10n ** 18n; // 1 Ether in Wei, using BigInt for precision

// --- Define Known CEX Label Keywords (Case-Insensitive) ---
// Add or remove keywords based on the labels used in your MongoDB 'ETH_whales' collection
// This set is used to identify CEX addresses during processing.
const KNOWN_CEX_KEYWORDS = new Set([
    'bitfinex', 'indodax', 'htx', 'gemini', 'korbit', 'binance', 'okx',
    'gate.io', 'kucoin', 'lbank', 'coinex', 'bitmart', 'bitrue', 'bitkub',
    'bitget', 'coinw', 'bybit', 'deribit', 'mexc', 'pionex', 'hashkey exchange',
    'biconomy.com', 'hotcoin', 'coindcx', 'phemex', 'bingx', 'crypto.com exchange',
    'deepcoin', 'fameex', 'toobit', 'flipster', 'blofin', 'bitunix', 'bvox',
    'orangex', 'backpack exchange', 'hashkey global', 'ourbit', 'arkham',
    // Add other common exchanges if needed
    'kraken', 'coinbase', 'robinhood', 'bitstamp', 'crypto.com'
]);
// --- End CEX Keywords ---


// --- Module Exports ---
// Export all configuration values for use throughout the application
module.exports = {
    // --- Server Configuration ---
    port: process.env.PORT || 3005, // Port for the webhook server (default: 3005)

    // --- MongoDB Configuration ---
    // Loaded directly from .env file
    mongoUsername: process.env.MONGODB_USERNAME,
    mongoPassword: process.env.MONGODB_PASSWORD,
    mongoCluster: process.env.MONGODB_CLUSTER, // e.g., cluster0.abcde.mongodb.net
    // Database and Collection names (with defaults)
    dbName: process.env.DATABASE_NAME || 'quicknode',
    labelCollection: process.env.WALLET_LABEL_COLLECTION_NAME || 'ETH_whales', // IMPORTANT: Ensure this matches your actual collection name in .env
    // Authentication details (optional, depends on DB setup)
    mongoAuthSource: process.env.MONGODB_AUTH_SOURCE || 'admin',
    mongoAuthMechanism: process.env.MONGODB_AUTH_MECHANISM, // e.g., SCRAM-SHA-256

    // --- Discord Configuration ---
    // Webhook URL for general reports (e.g., CEX flow) - can be undefined if not used
    discordWebhookUrl: process.env.DISCORD_WEBHOOK_URL,
    // Specific Webhook URL for Whale Alerts - MUST be set in .env for whale alerts to work
    discordWhaleWebhookUrl: process.env.DISCORD_WHALE_WEBHOOK_URL,

    // --- Ethereum Configuration ---
    WEI_IN_ETH: WEI_IN_ETH, // Export the BigInt constant

    // --- Report Configuration ---
    // Settings for CEX Flow Reports (if used)
    topNCexEntriesToShow: parseIntEnv(process.env.TOP_N_CEX_ENTRIES_TO_SHOW, 15), // Max CEX entries in report
    knownCexKeywords: KNOWN_CEX_KEYWORDS, // Export the set of CEX keywords

    // Settings for Whale Alert Reports
    whaleThresholdEth: parseFloatEnv(process.env.WHALE_THRESHOLD_ETH, 10.0), // Minimum ETH value for a whale tx (default: 10 ETH)
    topNWhalesToShow: parseIntEnv(process.env.TOP_N_WHALES_TO_SHOW, 5),        // Max whale txs to list in report (default: 5)

    // --- Price Fetcher Configuration ---
    // ID used for fetching price data (e.g., from CoinGecko)
    priceApiId: process.env.COINGECKO_API_ID || 'ethereum', // Default to 'ethereum'

    // --- Block Explorer Configuration ---
    // URL templates for creating clickable links in Discord messages
    blockExplorerUrlTemplate: "https://etherscan.io/tx/{txHash}",      // Etherscan TX URL template
    addressExplorerUrlTemplate: "[https://etherscan.io/address/](https://etherscan.io/address/){address}", // Etherscan Address URL template
};

