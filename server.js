// server.js
require('dotenv').config();
const express = require('express');
const config = require('./config');
const { getMongoClient, closeDatabaseConnection } = require('./services/dbConnection');
const { getEthPriceWithTrend } = require('./utils/priceFetcher');
const { sendEthTransactionReportToDiscord } = require('./services/discordSender'); // Will send aggregated CEX data

const app = express();

// --- Middleware ---
app.use(express.json({ limit: '50mb' })); // Increased limit for batched payloads
app.use((req, res, next) => {
    console.log(`[${new Date().toISOString()}] ${req.method} ${req.url}`);
    next();
});

// --- Helper Functions ---
function safeHexToNumber(hex) { /* ... as before ... */
    if (typeof hex !== 'string' || !hex.startsWith('0x')) { return 0; }
    try {
        if (!/^(0x)?[0-9a-fA-F]+$/.test(hex)) { return 0; }
        const num = parseInt(hex, 16);
        return isNaN(num) ? 0 : num;
    } catch (e) { console.warn(`Could not convert hex "${hex}" to Number:`, e.message); return 0; }
}
function safeHexToBigInt(hex) { /* ... as before ... */
    if (typeof hex !== 'string' || !hex.startsWith('0x')) { return 0n; }
    try {
        if (!/^(0x)?[0-9a-fA-F]+$/.test(hex)) { return 0n; }
        return BigInt(hex);
    } catch (e) { console.warn(`Could not convert hex "${hex}" to BigInt:`, e.message); return 0n; }
}

// --- Routes ---
app.get('/', (req, res) => {
    res.status(200).send('ETH CEX Flow Webhook Receiver is running.'); // Updated message
});

app.post('/quicknode-webhook', async (req, res) => {
    const payload = req.body;
    console.log(`[Server] Received webhook payload.`);

    // --- Acknowledge QuickNode Immediately ---
    res.status(200).send('Webhook payload received, processing started');

    // --- Process Asynchronously ---
    setImmediate(async () => {
        let mongoClient = null;
        let db = null;
        let priceData = null;
        let allTransactions = [];
        let minBlock = Infinity;
        let maxBlock = 0;
        let lastTimestampMs = 0;
        // CEX Aggregation Variables
        const cexFlowsByLabel = new Map(); // Map<cex_label, { inflow: BigInt, outflow: BigInt }>
        let totalCexInflowBatch = 0n;
        let totalCexOutflowBatch = 0n;
        let totalTxCountInBatch = 0; // Count total TXs processed

        try {
            // --- Determine Payload Structure (Single vs. Batch) ---
            let batchItems = [];
            if (Array.isArray(payload)) {
                 batchItems = payload;
                 console.log(`[Server] Detected batched payload with ${batchItems.length} items.`);
            } else if (payload && payload.whaleTransactions) {
                 batchItems = [payload];
                 console.log(`[Server] Detected single payload item.`);
            } else {
                console.warn("[Server] Received payload in unexpected format. Cannot process.", JSON.stringify(payload).substring(0, 500));
                return;
            }

            // --- Extract All Transactions and Basic Batch Info ---
            for (const item of batchItems) {
                if (item && Array.isArray(item.whaleTransactions)) {
                    item.whaleTransactions.forEach(tx => {
                        allTransactions.push(tx);
                        const blockNum = tx.block;
                        const timestamp = tx.timestamp_ms;
                        if (blockNum < minBlock) minBlock = blockNum;
                        if (blockNum > maxBlock) maxBlock = blockNum;
                        if (timestamp > lastTimestampMs) lastTimestampMs = timestamp;
                    });
                }
            }
            totalTxCountInBatch = allTransactions.length; // Total TXs from filter

            if (allTransactions.length === 0) {
                console.log("[Server] No transactions found in the batch payload.");
                return;
            }
            console.log(`[Server] Aggregated ${totalTxCountInBatch} transactions from blocks ${minBlock} to ${maxBlock}.`);

            // --- Fetch ETH Price ---
            priceData = await getEthPriceWithTrend();
            if (priceData === null) { console.warn(`[Server] Batch ${minBlock}-${maxBlock}: Could not fetch ETH price.`); }

            // --- Collect Addresses and Fetch Labels ---
            const allAddresses = new Set();
            allTransactions.forEach(tx => {
                if (tx.from) allAddresses.add(tx.from.toLowerCase());
                if (tx.to) allAddresses.add(tx.to.toLowerCase());
            });
            const uniqueAddresses = Array.from(allAddresses);
            const addressLabels = new Map(); // Map<lowercase_address, label>

            if (uniqueAddresses.length > 0) {
                console.log(`[Server] Batch ${minBlock}-${maxBlock}: Connecting to MongoDB to fetch ${uniqueAddresses.length} labels...`);
                try {
                    mongoClient = await getMongoClient();
                    db = mongoClient.db(config.dbName);
                    const labelsCollection = db.collection(config.labelCollection);
                    console.log(`[Server] Batch ${minBlock}-${maxBlock}: Fetching labels...`);
                    // Query using original case if needed, map lowercase later
                    const cursor = labelsCollection.find(
                        { address: { $in: uniqueAddresses.map(a => a) } }, // Adjust query if needed
                        { projection: { _id: 0, address: 1, label: 1 } }
                    );
                    const labelsData = await cursor.toArray();
                    labelsData.forEach(item => {
                        if (item.address && item.label) {
                            addressLabels.set(item.address.toLowerCase(), item.label);
                        }
                    });
                    console.log(`[Server] Batch ${minBlock}-${maxBlock}: Fetched ${addressLabels.size} labels.`);
                } catch (dbError) {
                    console.error(`[Server] Batch ${minBlock}-${maxBlock}: MongoDB Error fetching labels:`, dbError);
                }
            }

            // --- Process Transactions for CEX Flows ---
            console.log(`[Server] Batch ${minBlock}-${maxBlock}: Analyzing transactions for CEX flows...`);
            allTransactions.forEach(tx => {
                const fromAddr = tx.from?.toLowerCase();
                const toAddr = tx.to?.toLowerCase();
                const valueWei = safeHexToBigInt(tx.value_wei || tx.value || '0x0');

                if (valueWei === 0n) return; // Skip zero-value transfers

                const fromLabel = fromAddr ? addressLabels.get(fromAddr) : null;
                const toLabel = toAddr ? addressLabels.get(toAddr) : null;

                const fromIsCex = fromLabel && config.knownCexKeywords.has(fromLabel.toLowerCase());
                const toIsCex = toLabel && config.knownCexKeywords.has(toLabel.toLowerCase());

                // --- Aggregate CEX Flows ---
                if (fromIsCex && !toIsCex) { // CEX Outflow
                    if (!cexFlowsByLabel.has(fromLabel)) {
                        cexFlowsByLabel.set(fromLabel, { inflow: 0n, outflow: 0n });
                    }
                    cexFlowsByLabel.get(fromLabel).outflow += valueWei;
                    totalCexOutflowBatch += valueWei;
                } else if (!fromIsCex && toIsCex) { // CEX Inflow
                    if (!cexFlowsByLabel.has(toLabel)) {
                        cexFlowsByLabel.set(toLabel, { inflow: 0n, outflow: 0n });
                    }
                    cexFlowsByLabel.get(toLabel).inflow += valueWei;
                    totalCexInflowBatch += valueWei;
                } else if (fromIsCex && toIsCex && fromLabel !== toLabel) { // CEX to CEX Transfer
                    // Record as outflow from sender CEX
                    if (!cexFlowsByLabel.has(fromLabel)) {
                        cexFlowsByLabel.set(fromLabel, { inflow: 0n, outflow: 0n });
                    }
                    cexFlowsByLabel.get(fromLabel).outflow += valueWei;
                    totalCexOutflowBatch += valueWei;

                    // Record as inflow to receiver CEX
                    if (!cexFlowsByLabel.has(toLabel)) {
                        cexFlowsByLabel.set(toLabel, { inflow: 0n, outflow: 0n });
                    }
                    cexFlowsByLabel.get(toLabel).inflow += valueWei;
                    totalCexInflowBatch += valueWei;
                }
                // Ignore non-CEX to non-CEX and CEX to same CEX transfers for this report
            });

            console.log(`[Server] Batch ${minBlock}-${maxBlock}: CEX flow aggregation complete. Found ${cexFlowsByLabel.size} involved CEX labels.`);
            console.log(`[Server] Batch ${minBlock}-${maxBlock}: Total CEX Inflow: ${totalCexInflowBatch} Wei, Total CEX Outflow: ${totalCexOutflowBatch} Wei.`);


            // --- Prepare Final Report Data Object (Aggregated CEX Data) ---
            const cexFlowsArray = Array.from(cexFlowsByLabel.entries()).map(([label, flows]) => ({
                label: label,
                inflow: flows.inflow.toString(), // Convert BigInt to string for sending
                outflow: flows.outflow.toString(), // Convert BigInt to string
                totalFlow: (flows.inflow + flows.outflow).toString() // Calculate total flow as string
            }));

            const finalReportData = {
                startBlock: minBlock,
                endBlock: maxBlock,
                batchTimestamp: lastTimestampMs,
                totalCexInflowWei: totalCexInflowBatch.toString(), // Send totals as strings
                totalCexOutflowWei: totalCexOutflowBatch.toString(),
                cexFlows: cexFlowsArray, // Array of aggregated flows per CEX label
                txCountAnalyzed: totalTxCountInBatch, // Total TXs received from filter
                priceInfo: priceData
            };

            // --- Send Aggregated CEX Report to Discord ---
            // Only send if there was actual CEX flow detected
            if (cexFlowsArray.length > 0 || totalCexInflowBatch > 0n || totalCexOutflowBatch > 0n) {
                console.log(`[Server] Batch ${minBlock}-${maxBlock}: Sending aggregated CEX report to Discord...`);
                await sendEthTransactionReportToDiscord(finalReportData); // Pass the aggregated data
            } else {
                console.log(`[Server] Batch ${minBlock}-${maxBlock}: No CEX flow detected. Skipping Discord report.`);
            }

        } catch (error) {
            console.error(`[Server] Unhandled error processing webhook batch:`, error);
        } finally {
             // Optional: Close DB connection if opened, though typically managed globally
             console.log(`[Server] Finished processing batch for blocks ${minBlock}-${maxBlock}.`);
        }
    }); // End setImmediate
});

// --- Basic Error Handler ---
app.use((err, req, res, next) => {
    console.error("[Server] Express Error Handler:", err.stack);
    res.status(500).send('Something broke!');
});

// --- Start Server & Graceful Shutdown ---
const server = app.listen(config.port, () => { /* ... as before ... */
    console.log(`[Server] ETH CEX Flow Webhook Receiver listening on port ${config.port}`);
    getMongoClient().catch(err => { console.error("[Server] Initial MongoDB connection failed on startup. Exiting.", err); process.exit(1); });
});
const shutdown = async (signal) => { /* ... as before ... */
    console.log(`[Server] ${signal} received...`);
    server.close(async () => { console.log('[Server] HTTP server closed.'); await closeDatabaseConnection(); console.log('[Server] Exiting process.'); process.exit(0); });
    setTimeout(async () => { console.error('[Server] Force shutdown due to timeout.'); await closeDatabaseConnection(); process.exit(1); }, 10000);
};
process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));
