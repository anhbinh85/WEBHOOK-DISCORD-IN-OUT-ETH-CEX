// services/discordSender.js
const axios = require('axios');
const config = require('../config');
const {
    formatWeiToETH,
    formatUSD,
    formatTimestamp,
    // shortenAddress, // Not directly needed for CEX labels
    // formatTxLink, // Not needed for this report format
    // formatAddressLink, // Not needed for this report format
    formatNumber
} = require('../utils/formatters');

const MAX_FIELD_VALUE_LENGTH = 1020; // Discord embed field value limit
const WEI_IN_ETH = config.WEI_IN_ETH; // BigInt 10^18

/**
 * Helper function to sort CEX flow data by total flow descending (using BigInt strings).
 * @param {Array<object>} cexFlowList - Array of { label, inflow (str), outflow (str), totalFlow (str) }
 * @returns {Array<object>} - Sorted array.
 */
function sortCexFlows(cexFlowList) {
    if (!cexFlowList) return [];
    // Sort by totalFlow (as BigInt) descending, then alphabetically by label
    return cexFlowList.sort((a, b) => {
        const totalFlowA = BigInt(a.totalFlow || '0');
        const totalFlowB = BigInt(b.totalFlow || '0');

        if (totalFlowB > totalFlowA) return 1;
        if (totalFlowB < totalFlowA) return -1;

        // If total flows are equal, sort alphabetically by label
        return (a.label || '').localeCompare(b.label || '');
    });
}

/**
 * Formats the CEX flow list for the Discord embed field, mimicking the target image.
 * @param {Array<object>} sortedCexFlows - Sorted list of CEX flow entries.
 * @param {number|null} ethPrice - Current ETH price in USD.
 * @returns {string} Formatted markdown string.
 */
function formatCexFlowList(sortedCexFlows, ethPrice) {
    if (!sortedCexFlows || sortedCexFlows.length === 0) {
        return "`None detected in this batch`";
    }

    // Apply Top N limit from config
    const limitedList = sortedCexFlows.slice(0, config.topNCexEntriesToShow);
    let listString = "";
    let truncated = sortedCexFlows.length > config.topNCexEntriesToShow;

    for (const item of limitedList) {
        const { label, inflow, outflow } = item; // inflow/outflow are Wei strings

        // Format ETH amounts
        const inflowEthStr = formatWeiToETH(inflow);
        const outflowEthStr = formatWeiToETH(outflow);

        // Format USD amounts
        let inflowUsdStr = 'N/A USD';
        let outflowUsdStr = 'N/A USD';
        if (ethPrice !== null) {
            try {
                const inflowEthNum = Number(BigInt(inflow)) / Number(WEI_IN_ETH);
                const outflowEthNum = Number(BigInt(outflow)) / Number(WEI_IN_ETH);
                inflowUsdStr = formatUSD(inflowEthNum * ethPrice);
                outflowUsdStr = formatUSD(outflowEthNum * ethPrice);
            } catch (e) {
                 console.error(`Error calculating USD for CEX ${label}:`, e);
                 inflowUsdStr = 'Error USD';
                 outflowUsdStr = 'Error USD';
            }
        }

        // Format the line: "* **Label**: IN: X ETH (~$Y) | OUT: A ETH (~$B)"
        // Using standard emojis for inflow/outflow indication
        const line = `* **${label}**: 📥 ${inflowEthStr} (*~${inflowUsdStr}*) | 📤 ${outflowEthStr} (*~${outflowUsdStr}*)\n`;

        // Check length before adding
        if (listString.length + line.length > MAX_FIELD_VALUE_LENGTH) {
            truncated = true; // Mark as truncated if this line doesn't fit
            break; // Stop adding lines
        }
        listString += line;
    }

    // Add truncation notice if necessary
    if (truncated) {
        const notice = `\n*... (showing top ${config.topNCexEntriesToShow} of ${sortedCexFlows.length})*`;
        if (listString.length + notice.length <= MAX_FIELD_VALUE_LENGTH) {
             listString += notice;
        } else {
            // Attempt to shorten the last added line slightly if possible, otherwise just cut
             listString = listString.substring(0, MAX_FIELD_VALUE_LENGTH - notice.length) + notice;
        }
    }

    return listString || "`Error formatting list`"; // Fallback
}


/**
 * Sends an aggregated CEX flow report for an ETH block batch to Discord.
 * @param {object} reportData - Aggregated CEX data for the batch.
 */
async function sendEthTransactionReportToDiscord(reportData) { // Renamed function for clarity
    if (!config.discordWebhookUrl) {
        console.error("[DiscordSender] DISCORD_WEBHOOK_URL is not configured.");
        return;
    }
    if (!reportData) {
        console.error("[DiscordSender] No CEX report data provided.");
        return;
    }

    try {
        const {
            startBlock,
            endBlock,
            batchTimestamp,
            totalCexInflowWei, // Aggregated CEX inflow (Wei string)
            totalCexOutflowWei, // Aggregated CEX outflow (Wei string)
            cexFlows, // Array of { label, inflow (str), outflow (str), totalFlow(str) }
            txCountAnalyzed, // Total TXs analyzed in the batch
            priceInfo
        } = reportData;

        const currentEthPrice = priceInfo ? priceInfo.usd : null;
        const change24h = priceInfo ? priceInfo.change24h : null;
        console.log(`[DiscordSender] Formatting ETH CEX report. Price: ${currentEthPrice}, Change: ${change24h}`);

        // --- Format Price Trend String ---
        let priceTrendStr = '';
        if (currentEthPrice !== null && change24h !== null) {
             const trendEmoji = change24h >= 0 ? '📈' : '📉';
             priceTrendStr = ` | ETH: ${formatUSD(currentEthPrice)} (${trendEmoji}${change24h >= 0 ? '+' : ''}${change24h.toFixed(2)}% 24h)`;
        } else if (currentEthPrice !== null) {
            priceTrendStr = ` | ETH: ${formatUSD(currentEthPrice)}`;
        } else {
            priceTrendStr = ` | ETH Price: N/A`;
        }

        // --- Format CEX Totals ---
        const totalInflowEthStr = formatWeiToETH(totalCexInflowWei);
        const totalOutflowEthStr = formatWeiToETH(totalCexOutflowWei);
        let totalInflowUsdStr = 'N/A';
        let totalOutflowUsdStr = 'N/A';
        if (currentEthPrice !== null) {
            try {
                const totalInflowEthNum = Number(BigInt(totalCexInflowWei)) / Number(WEI_IN_ETH);
                const totalOutflowEthNum = Number(BigInt(totalCexOutflowWei)) / Number(WEI_IN_ETH);
                totalInflowUsdStr = formatUSD(totalInflowEthNum * currentEthPrice);
                totalOutflowUsdStr = formatUSD(totalOutflowEthNum * currentEthPrice);
            } catch (e) {
                console.error("Error calculating total USD value:", e);
                totalInflowUsdStr = 'Error';
                totalOutflowUsdStr = 'Error';
            }
        }

        // --- Format CEX Flow List ---
        const sortedCexFlows = sortCexFlows(cexFlows); // Sort CEX flows by total volume
        const cexListStr = formatCexFlowList(sortedCexFlows, currentEthPrice); // Format the list

        // --- Build Embed (CEX Focused) ---
        const embed = {
            title: `📊 ETH CEX Flow Report: Blocks ${formatNumber(startBlock)} - ${formatNumber(endBlock)}${priceTrendStr}`, // Updated title
            color: 0x627eea, // Ethereum blue-ish color
            fields: [
                { // CEX Totals for the Block Batch
                    name: 'Batch CEX Totals', // Updated field name
                    value: `Inflow: **${totalInflowEthStr}** (*~${totalInflowUsdStr}*)\nOutflow: **${totalOutflowEthStr}** (*~${totalOutflowUsdStr}*)`,
                    inline: false,
                },
                { // CEX Flows Section
                    name: `🏦 CEX Flows (${(cexFlows || []).length} Involved)`, // Show count of involved CEXs
                    value: cexListStr.substring(0, MAX_FIELD_VALUE_LENGTH), // Use formatted CEX list
                    inline: false,
                },
            ],
            timestamp: new Date(batchTimestamp).toISOString(),
            footer: {
                text: `ETH CEX Flow Monitor | ${formatNumber(txCountAnalyzed)} Total TXs Analyzed in Batch` // Updated footer
            }
        };

        // --- Prepare and Send ---
        const discordPayload = { embeds: [embed] };
        console.log(`[DiscordSender] Sending ETH CEX flow report for blocks ${startBlock}-${endBlock}...`);
        await axios.post(config.discordWebhookUrl, discordPayload, {
            headers: { 'Content-Type': 'application/json' },
            timeout: 15000
        });
        console.log(`[DiscordSender] Successfully sent ETH CEX flow report for blocks ${startBlock}-${endBlock}.`);

    } catch (error) {
        console.error(`[DiscordSender] Failed to send ETH CEX flow report for blocks ${reportData?.startBlock}-${reportData?.endBlock}:`, error.message);
        if (error.response) {
            console.error(`[DiscordSender] Discord API Response Status: ${error.response.status}`);
            console.error(`[DiscordSender] Discord API Response Data:`, JSON.stringify(error.response.data, null, 2));
        } else if (error.request) {
            console.error("[DiscordSender] No response received from Discord.");
        } else {
             console.error("[DiscordSender] Error details:", error);
        }
    }
}

module.exports = {
    sendEthTransactionReportToDiscord // Export the renamed function
};
