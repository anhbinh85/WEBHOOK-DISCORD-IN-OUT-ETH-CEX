// utils/formatters.js
const config = require('../config');

// Re-usable BigInt constant for Wei conversion
const WEI_IN_ETH = config.WEI_IN_ETH; // 10n ** 18n

/**
 * Formats a number with commas.
 * @param {number|string} num - The number to format.
 * @returns {string} Formatted number string or 'N/A'.
 */
function formatNumber(num) {
    const number = typeof num === 'string' ? parseFloat(num) : num;
    if (number === undefined || number === null || isNaN(number)) {
        return 'N/A';
    }
    return number.toLocaleString('en-US');
}

/**
 * Formats Wei (as string or BigInt) to an ETH string with appropriate decimals.
 * @param {string|bigint} weiValue - The value in Wei.
 * @returns {string} Formatted ETH string (e.g., "10.5000 ETH").
 */
function formatWeiToETH(weiValue) {
    if (weiValue === undefined || weiValue === null) {
        return '0.0000 ETH';
    }
    try {
        const weiBigInt = BigInt(weiValue);

        if (weiBigInt === 0n) {
            return '0.0000 ETH';
        }

        const ethIntegerPart = weiBigInt / WEI_IN_ETH;
        const ethRemainderPart = weiBigInt % WEI_IN_ETH;

        // Format the fractional part with leading zeros if necessary
        const fractionalString = ethRemainderPart.toString().padStart(18, '0');

        // Determine precision: show more decimals for smaller amounts
        let precision = 4; // Default precision
        if (ethIntegerPart === 0n) {
            if (ethRemainderPart > 0n) { // If it's less than 1 ETH but not zero
                 precision = 8; // Show more decimals
            } else {
                 precision = 4; // Should be 0.0000 if remainder is also 0
            }
        } else if (ethIntegerPart >= 1000n) {
            precision = 2; // Less precision for large amounts
        }

        // Combine integer and fractional parts, then truncate/round
        const fullEthString = `${ethIntegerPart}.${fractionalString}`;
        const ethNumber = parseFloat(fullEthString); // Convert for formatting

        // Use toLocaleString for formatting and comma separation
        return `${ethNumber.toLocaleString('en-US', { minimumFractionDigits: precision, maximumFractionDigits: precision })} ETH`;

    } catch (e) {
        console.error(`[Formatter] Error formatting Wei to ETH (${weiValue}):`, e);
        return 'Error ETH';
    }
}


/**
 * Formats a USD value.
 * @param {number|string} usdValue - The USD value.
 * @returns {string} Formatted USD string (e.g., "$1,234.56").
 */
function formatUSD(usdValue) {
    const number = typeof usdValue === 'string' ? parseFloat(usdValue) : usdValue;
    if (number === undefined || number === null || isNaN(number)) {
        return 'N/A USD';
    }
    return number.toLocaleString('en-US', { style: 'currency', currency: 'USD' });
}

/**
 * Formats a timestamp (in milliseconds).
 * @param {number|string} timestampMs - Timestamp in milliseconds.
 * @returns {string} Formatted date/time string (UTC).
 */
function formatTimestamp(timestampMs) {
    const number = typeof timestampMs === 'string' ? parseInt(timestampMs, 10) : timestampMs;
     if (number === undefined || number === null || isNaN(number)) {
        return 'N/A';
    }
    try {
        const date = new Date(number);
        if (isNaN(date.getTime())) {
             return 'Invalid Date';
        }
        // Format as YYYY-MM-DD HH:MM:SS UTC
        return date.toISOString().replace('T', ' ').replace(/\.\d+Z$/, ' UTC');
    } catch (e) {
        console.error("[Formatter] Error formatting timestamp:", e);
        return 'Invalid Date';
    }
}

/**
 * Shortens a blockchain address (e.g., 0xabc...def).
 * @param {string} address - The address string.
 * @param {number} [startChars=6] - Number of starting characters to show.
 * @param {number} [endChars=4] - Number of ending characters to show.
 * @returns {string} Shortened address or original if too short/invalid.
 */
function shortenAddress(address, startChars = 6, endChars = 4) {
    if (!address || typeof address !== 'string' || !address.startsWith('0x')) {
        return address || 'N/A'; // Return original or N/A if invalid
    }
    const prefix = '0x';
    const body = address.substring(2); // Remove '0x'
    const minLength = startChars + endChars + 1; // Need at least one char between start/end

    if (body.length <= minLength) {
        return address; // Return original if too short to shorten meaningfully
    }
    return `${prefix}${body.substring(0, startChars)}...${body.substring(body.length - endChars)}`;
}


/**
 * Creates an Etherscan link for a transaction hash.
 * @param {string} txHash - The transaction hash.
 * @returns {string} Markdown formatted link or N/A.
 */
function formatTxLink(txHash) {
     if (!txHash) return 'N/A';
     if (!config.blockExplorerUrlTemplate) {
         return `\`${txHash}\``; // Fallback to code block
     }
     const explorerUrl = config.blockExplorerUrlTemplate.replace('{txHash}', txHash);
     // Shorten for display within the link
     return `[${shortenAddress(txHash, 10, 8)}](${explorerUrl})`;
}

/**
 * Creates an Etherscan link for an address.
 * @param {string} address - Wallet address.
 * @param {string|null} [displayText=null] - Optional text for the link. Defaults to shortened address.
 * @returns {string} Markdown formatted link or N/A.
 */
function formatAddressLink(address, displayText = null) {
    if (!address || typeof address !== 'string' || !address.startsWith('0x')) {
        return `\`${address || 'N/A'}\``; // Handle null/undefined/invalid
    }
    if (!config.addressExplorerUrlTemplate) {
        console.warn("[Formatter] addressExplorerUrlTemplate not found in config.");
        return `\`${address}\``; // Fallback to code block
    }
    const explorerUrl = config.addressExplorerUrlTemplate.replace('{address}', address);
    // Use provided displayText or default to shortenAddress
    const linkText = displayText ? displayText : shortenAddress(address);
    return `[${linkText}](${explorerUrl})`;
}

module.exports = {
    formatNumber,
    formatWeiToETH,
    formatUSD,
    formatTimestamp,
    shortenAddress,
    formatTxLink,
    formatAddressLink,
};
