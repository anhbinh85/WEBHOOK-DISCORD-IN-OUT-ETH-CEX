// utils/ethFormatters.js
const config = require('../config'); // Use centralized config

// Re-usable BigInt constant for Wei conversion
const WEI_IN_ETH = config.WEI_IN_ETH || (10n ** 18n); // 1 ETH = 10^18 Wei

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
        const fractionalString = ethRemainderPart.toString().padStart(18, '0');

        let precision = 4;
        if (ethIntegerPart === 0n) {
            precision = ethRemainderPart > 0n ? 8 : 4;
        } else if (ethIntegerPart >= 1000n) {
            precision = 2;
        }

        const displayFractional = fractionalString.substring(0, precision);
        // Format the integer part with commas
        const formattedInteger = ethIntegerPart.toLocaleString('en-US');
        // Combine, ensuring trailing zeros are handled correctly for the desired precision
        const fullNumberString = `${formattedInteger}.${displayFractional}`;

        // Trim unnecessary trailing zeros *after* the decimal, but keep required precision
        // Example: 1.2300 -> 1.23, 1.0000 -> 1.00 (if precision is 2)
        // This part might need refinement based on exact desired output for whole numbers
        // For simplicity, let's stick to the fixed precision for now.
        // const formattedNumber = parseFloat(fullNumberString).toLocaleString('en-US', {minimumFractionDigits: precision, maximumFractionDigits: precision});

        return `${fullNumberString} ETH`; // Use the combined string directly

    } catch (e) {
        console.error(`[ETH Formatter] Error formatting Wei to ETH (${weiValue}):`, e);
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
    return number.toLocaleString('en-US', { style: 'currency', currency: 'USD', minimumFractionDigits: 2, maximumFractionDigits: 2 });
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
        return address || 'N/A';
    }
    const prefix = '0x';
    const body = address.substring(2);
    if (body.length <= startChars + endChars) {
        return address;
    }
    return `${prefix}${body.substring(0, startChars)}...${body.substring(body.length - endChars)}`;
}


/**
 * Creates an Etherscan link for a transaction hash.
 * Returns markdown: [0xabc...def](url)
 * @param {string} txHash - The transaction hash.
 * @returns {string} Markdown formatted link or `N/A`.
 */
function formatTxLink(txHash) {
     if (!txHash) return '`N/A`';
     if (!config.blockExplorerUrlTemplate) {
         console.warn("[ETH Formatter] blockExplorerUrlTemplate not found in config.");
         return `\`${shortenAddress(txHash, 10, 8)}\``; // Fallback to code block
     }
     const explorerUrl = config.blockExplorerUrlTemplate.replace('{txHash}', txHash);
     // Use shortenAddress for the link text
     return `[${shortenAddress(txHash, 10, 8)}](${explorerUrl})`; // ONLY the markdown link
}

/**
 * Creates an Etherscan link for an address.
 * Returns markdown: [0xabc...def](url)
 * @param {string} address - Wallet address.
 * @param {string|null} [displayText=null] - Optional text. If null, uses shortened address.
 * @returns {string} Markdown formatted link or `N/A` or the address in backticks.
 */
function formatAddressLink(address, displayText = null) {
    if (!address || typeof address !== 'string' || !address.startsWith('0x')) {
        return `\`${address || 'N/A'}\``;
    }
    if (!config.addressExplorerUrlTemplate) {
        console.warn("[ETH Formatter] addressExplorerUrlTemplate not found in config.");
        return `\`${shortenAddress(address)}\``; // Fallback
    }
    const explorerUrl = config.addressExplorerUrlTemplate.replace('{address}', address);
    // Use provided displayText or default to shortenAddress
    const linkText = displayText ? displayText : shortenAddress(address);
    // Return ONLY the markdown link
    return `[${linkText}](${explorerUrl})`;
}

module.exports = {
    formatNumber,
    formatWeiToETH,
    formatUSD,
    shortenAddress,
    formatTxLink,
    formatAddressLink,
};
