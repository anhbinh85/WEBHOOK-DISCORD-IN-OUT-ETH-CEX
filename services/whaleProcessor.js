// services/whaleProcessor.js
const config = require('../config'); // To get the threshold

// Define WEI_IN_ETH constant locally for reliable BigInt math
const WEI_IN_ETH = 10n ** 18n; // 1 Ether = 10^18 Wei

// Note: Removed safeHexToBigInt as it's not needed for value_wei parsing here.
// Keep it if other parts of your code use it for actual hex values.

/**
 * Processes a list of transactions to identify whales based on a threshold.
 * Calculates total whale value and returns the top N whales sorted by value.
 *
 * @param {Array<object>} allTransactions - Array of transaction objects received from the stream.
 * Expected tx object structure: { value_wei: string (decimal), from: string, to: string, txHash: string, ... }
 * @returns {{
 * whaleTransactionsWithValue: Array<object>,
 * totalWhaleTxCount: number,
 * totalWhaleValueWei: bigint,
 * topWhales: Array<object>
 * }} - Object containing processed whale data.
 */
function processWhaleTransactions(allTransactions) {
    console.log(`[WhaleProcessor] Processing ${allTransactions.length} transactions for whales...`);

    // Calculate threshold in Wei using BigInt math directly for accuracy.
    let whaleThresholdWei;
    try {
         whaleThresholdWei = BigInt(config.whaleThresholdEth) * WEI_IN_ETH;
    } catch (e) {
         console.warn(`[WhaleProcessor] Could not directly convert threshold ${config.whaleThresholdEth} ETH to BigInt. Using float multiplication fallback (less precise). Error: ${e.message}`);
         whaleThresholdWei = BigInt(Math.round(config.whaleThresholdEth * Number(WEI_IN_ETH)));
    }

    console.log(`[WhaleProcessor] Whale threshold: ${config.whaleThresholdEth} ETH (${whaleThresholdWei.toString()} Wei)`);

    let whaleTransactionsWithValue = []; // Store whales with their BigInt value
    let totalWhaleValueWei = 0n;

    // Filter transactions based on the threshold
    allTransactions.forEach(tx => {
        let valueWei = 0n; // Default to 0n

        // --- CORRECTED VALUE PARSING ---
        // Directly convert the decimal string `tx.value_wei` to BigInt
        if (typeof tx.value_wei === 'string' && /^\d+$/.test(tx.value_wei)) { // Check if it's a string of digits
            try {
                valueWei = BigInt(tx.value_wei);
            } catch (e) {
                console.warn(`[WhaleProcessor] Could not convert value_wei string "${tx.value_wei}" to BigInt for tx ${tx.txHash}:`, e.message);
                // valueWei remains 0n
            }
        } else {
             console.warn(`[WhaleProcessor] Invalid or missing value_wei format for tx ${tx.txHash}: Expected decimal string, got ${typeof tx.value_wei}`);
             // valueWei remains 0n
        }
        // --- END CORRECTION ---

        // Compare the transaction value (BigInt) with the threshold (BigInt)
        if (valueWei >= whaleThresholdWei) {
            console.log(`[WhaleProcessor] Found whale tx: ${tx.txHash} - Value: ${valueWei.toString()} Wei`); // Log found whales
            // Add the parsed BigInt value to the transaction object for sorting later
            whaleTransactionsWithValue.push({ ...tx, valueWeiParsed: valueWei });
            totalWhaleValueWei += valueWei; // Accumulate total value using BigInt math
        }
    });

    const totalWhaleTxCount = whaleTransactionsWithValue.length;
    console.log(`[WhaleProcessor] Found ${totalWhaleTxCount} whale transactions meeting threshold.`);

    // Sort the identified whales by their value (which is BigInt) in descending order
    whaleTransactionsWithValue.sort((a, b) => {
        // Direct BigInt comparison
        if (b.valueWeiParsed > a.valueWeiParsed) return 1;
        if (b.valueWeiParsed < a.valueWeiParsed) return -1;
        return 0;
    });

    // Get the top N whales based on config
    const topWhales = whaleTransactionsWithValue.slice(0, config.topNWhalesToShow);
    console.log(`[WhaleProcessor] Selected top ${topWhales.length} whales to report.`);

    return {
        whaleTransactionsWithValue, // Full list of whales found (with parsed value)
        totalWhaleTxCount,
        totalWhaleValueWei, // Return as BigInt
        topWhales // Only the top N whales
    };
}

// Export the processing function
module.exports = {
    processWhaleTransactions
};
