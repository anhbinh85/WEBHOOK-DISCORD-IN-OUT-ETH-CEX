// services/discordWhaleSender.js

// Required modules
const axios = require('axios');
const config = require('../config'); // Access configuration
// Use the ETH-specific formatters
const {
    formatWeiToETH,
    formatUSD,
    shortenAddress,      // Now used directly for non-labeled addresses in the list
    formatTxLink,        // Returns "[short_hash](link)"
    // formatAddressLink is NOT used directly in the list formatting below
    formatNumber
} = require('../utils/ethFormatters'); // Use the ETH formatters

// --- Constants ---
const MAX_FIELD_VALUE_LENGTH = 1020;
const MAX_FIELD_LINES = 20; // Adjust as needed
const WEI_IN_ETH = config.WEI_IN_ETH || (10n ** 18n);

// --- Helper Functions ---

/**
 * Creates the formatted list of largest whale transactions for the Discord embed field.
 * **Strictly follows BTC example formatting.**
 * Format: • TxLink: Amount ETH (~Amount USD) From: Label/**`Addr`** -> To: Label/**`Addr`**
 *
 * @param {Array<object>} topWhales - Array of top N whale transaction objects.
 * @param {Map<string, string>} labelsMap - Map of lowercase addresses to labels.
 * @param {number|null} ethPrice - Current ETH price in USD, or null if unavailable.
 * @returns {string} Formatted markdown string for the Discord embed field.
 */
function formatWhaleList(topWhales, labelsMap, ethPrice) {
    if (!topWhales || topWhales.length === 0) {
        return "`None detected meeting threshold in this batch.`";
    }

    let listString = "";
    let lines = 0;
    for (const tx of topWhales) {
        if (lines >= MAX_FIELD_LINES) {
            listString += `\n*... (showing top ${lines} of ${topWhales.length})*`;
            break;
        }

        // Format ETH and USD values
        const valueEthFormatted = formatWeiToETH(tx.value_wei);
        let valueUsdFormatted = 'N/A USD';
        if (ethPrice !== null) {
            try {
                 const valueWeiBigInt = BigInt(tx.value_wei);
                 const valueEthNum = Number(valueWeiBigInt * 10000n / WEI_IN_ETH) / 10000;
                 valueUsdFormatted = formatUSD(valueEthNum * ethPrice);
            } catch (e) {
                console.error(`[WhaleFormat] Error calculating USD for tx ${tx.txHash}:`, e);
                valueUsdFormatted = 'Error USD';
            }
        }

        // *** Get the PRE-FORMATTED markdown link for the transaction hash ***
        const txLinkMarkdown = formatTxLink(tx.txHash); // Should be "[short_hash](link)"

        // --- Format From/To exactly like BTC example ---
        const fromAddrLower = tx.from?.toLowerCase();
        const toAddrLower = tx.to?.toLowerCase();
        const fromLabel = fromAddrLower ? labelsMap.get(fromAddrLower) : null;
        const toLabel = toAddrLower ? labelsMap.get(toAddrLower) : null;

        // If label exists, use bold label. Otherwise, use shortened address in backticks.
        const fromDisplay = fromLabel
            ? `**${fromLabel}**`
            : (tx.from ? `\`${shortenAddress(tx.from)}\`` : '`N/A`'); // Use backticks ` `

        const toDisplay = toLabel
            ? `**${toLabel}**`
            : (tx.to ? `\`${shortenAddress(tx.to)}\`` : '`N/A`'); // Use backticks ` `

        // Construct the line string using the specific BTC format
        // Format: • TxLinkMarkdown: Amount ETH (~Amount USD) From: FromDisplay -> To: ToDisplay
        const line = `• ${txLinkMarkdown}: **${valueEthFormatted}** (*~${valueUsdFormatted}*) From: ${fromDisplay} -> To: ${toDisplay}\n`;

        // Check length limit before adding
        if (listString.length + line.length > MAX_FIELD_VALUE_LENGTH) {
             listString += `\n*... (list shortened due to length limit)*`;
             break;
        }
        listString += line;
        lines++;
    }
    return listString || "`Error formatting list`";
}


/**
 * Sends an aggregated Whale Transaction report for a block batch to Discord.
 * @param {object} reportData - Aggregated whale data for the batch.
 */
async function sendWhaleReportToDiscord(reportData) {
    if (!config.discordWhaleWebhookUrl) { /* handle missing config */ return; }
    if (!reportData || !reportData.topWhales || reportData.totalWhaleTxCount === 0) { /* handle no data */ return; }

    // Define blockRangeString here based on reportData
    const blockRangeString = reportData.startBlock === reportData.endBlock
            ? `Block #${formatNumber(reportData.startBlock)}`
            : `Blocks #${formatNumber(reportData.startBlock)} - #${formatNumber(reportData.endBlock)}`;

    console.log(`[DiscordWhaleSender] Preparing whale report for ${blockRangeString}...`);

    try {
        const {
            startBlock, endBlock, batchTimestamp, totalWhaleTxCount,
            totalWhaleValueWei, topWhales, priceInfo, labelsMap
        } = reportData;

        const currentEthPrice = priceInfo ? priceInfo.usd : null;
        const title = `🚨🚨🚨 ETH Whale Alert Summary: ${blockRangeString} (${formatNumber(totalWhaleTxCount)} TXs)`;

        const totalValueEthStr = formatWeiToETH(totalWhaleValueWei);
        let totalValueUsdStr = 'N/A USD';
        if (currentEthPrice !== null) {
             try {
                 const totalValueWeiBigInt = BigInt(totalWhaleValueWei);
                 const totalValueEthNum = Number(totalValueWeiBigInt * 10000n / WEI_IN_ETH) / 10000;
                 totalValueUsdStr = formatUSD(totalValueEthNum * currentEthPrice);
             } catch(e) { console.error("[WhaleFormat] Error calculating total USD value:", e); totalValueUsdStr = 'Error USD';}
        }
        const description = `Detected **${formatNumber(totalWhaleTxCount)}** large transfer(s) (>= ${config.whaleThresholdEth} ETH) in ${blockRangeString}.\nTotal value: **${totalValueEthStr}** (*~${totalValueUsdStr}*)`;

        // Use the strictly formatted list function
        const whaleListStr = formatWhaleList(topWhales, labelsMap, currentEthPrice);

        const embed = {
            title: title,
            description: description,
            color: 0xff0000, // Red color
            fields: [
                {
                    name: `Largest Transactions (up to ${config.topNWhalesToShow})`,
                    value: whaleListStr.substring(0, MAX_FIELD_VALUE_LENGTH),
                    inline: false,
                },
            ],
            timestamp: new Date(batchTimestamp).toISOString(),
            footer: {
                text: `ETH Whale Monitor | ${new Date().toLocaleString('en-CA', { timeZone: 'Asia/Ho_Chi_Minh' })} ICT`
            }
        };

        const discordPayload = { embeds: [embed] };
        console.log(`[DiscordWhaleSender] Sending ETH Whale Alert report for ${blockRangeString} to configured webhook...`);

        await axios.post(config.discordWhaleWebhookUrl, discordPayload, {
            headers: { 'Content-Type': 'application/json' },
            timeout: 15000
        });
        console.log(`[DiscordWhaleSender] Successfully sent ETH Whale Alert report for ${blockRangeString}.`);

    } catch (error) {
        console.error(`[DiscordWhaleSender] Failed to send ETH Whale Alert report for ${blockRangeString}:`, error.message);
        // ... (rest of error handling, using blockRangeString) ...
        if (error.response) {
             console.error(`[DiscordWhaleSender] Whale Alert Discord API Response Status: ${error.response.status}`);
             console.error(`[DiscordWhaleSender] Whale Alert Discord API Response Keys:`, Object.keys(error.response.data || {}));
             console.error(`[DiscordWhaleSender] Whale Alert Discord API Response Message (if available):`, error.response.data?.message);
        } else if (error.request) { console.error("[DiscordWhaleSender] Whale Alert: No response received from Discord."); }
        else { console.error("[DiscordWhaleSender] Whale Alert Error details:", error.stack); }
    }
}

module.exports = {
    sendWhaleReportToDiscord
};
