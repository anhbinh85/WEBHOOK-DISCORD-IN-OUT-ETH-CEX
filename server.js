// // server.js
// // --- Core Dependencies ---
// require('dotenv').config(); // Load .env variables first
// const express = require('express');
// const util = require('util'); // For advanced object inspection if needed

// // --- Configuration ---
// const config = require('./config'); // Load all app configuration

// // --- Service Imports ---
// const { getMongoClient, closeDatabaseConnection } = require('./services/dbConnection');
// const { processWhaleTransactions } = require('./services/whaleProcessor'); // Handles whale logic
// const { sendWhaleReportToDiscord } = require('./services/discordWhaleSender'); // Handles whale Discord formatting/sending
// // Import the original CEX sender function (assuming it's still in discordSender.js or similar)
// const { sendEthTransactionReportToDiscord } = require('./services/discordSender'); // Original CEX sender

// // --- Utility Imports ---
// const { getEthPriceWithTrend } = require('./utils/priceFetcher'); // Fetches ETH price

// // --- Express App Setup ---
// const app = express();

// // --- Middleware ---
// // Increase payload size limit (adjust if necessary, but 50mb is generous)
// app.use(express.json({ limit: '50mb' }));
// // Basic request logging middleware
// app.use((req, res, next) => {
//     console.log(`[${new Date().toISOString()}] ${req.method} ${req.url}`);
//     next();
// });

// // --- Helper Functions ---
// /**
//  * Safely converts a hex string (like "0x...") to a BigInt.
//  * Returns 0n if the input is invalid or conversion fails.
//  * @param {string | null | undefined} hex
//  * @returns {bigint}
//  */
// function safeHexToBigInt(hex) {
//     if (typeof hex !== 'string' || !hex.startsWith('0x')) {
//         return 0n;
//     }
//     try {
//         if (!/^(0x)?[0-9a-fA-F]+$/.test(hex)) { return 0n; }
//         return BigInt(hex);
//     } catch (e) {
//         console.warn(`[Server] Could not convert hex "${hex}" to BigInt:`, e.message);
//         return 0n;
//     }
// }

// /**
//  * Safely converts a hex string (like "0x...") to a Number.
//  * Returns 0 if the input is invalid or conversion fails.
//  * @param {string | null | undefined} hex
//  * @returns {number}
//  */
// function safeHexToNumber(hex) {
//     if (typeof hex !== 'string' || !hex.startsWith('0x')) { return 0; }
//     try {
//         if (!/^(0x)?[0-9a-fA-F]+$/.test(hex)) { return 0; }
//         const num = parseInt(hex, 16);
//         return isNaN(num) ? 0 : num;
//     } catch (e) {
//         console.warn(`[Server] Could not convert hex "${hex}" to Number:`, e.message);
//         return 0;
//     }
// }
// // --- End Helper Functions ---


// // --- Routes ---

// // Basic health check route
// app.get('/', (req, res) => {
//     res.status(200).send('ETH CEX Flow & Whale Webhook Receiver is running.'); // Updated message
// });

// // Main webhook endpoint for QuickNode stream data
// app.post('/quicknode-webhook', async (req, res) => {
//     const payload = req.body;
//     const payloadType = Array.isArray(payload) ? 'array' : typeof payload;
//     const payloadSize = payload ? JSON.stringify(payload).length : 0;
//     console.log(`[Server] Received webhook payload. Type: ${payloadType}, Approx Size: ${payloadSize} bytes.`);

//     // *** ADDED: Log the received payload ***
//     // Use util.inspect for better object formatting, especially large ones.
//     // Set depth to a reasonable number (e.g., 5) to avoid excessive output. Use null for infinite depth (careful!).
//     console.log("[Server] Inspecting Full Received Payload (depth: 5):");
//     console.log(util.inspect(payload, { showHidden: false, depth: 5, colors: false }));
//     // *** END ADDED LOG ***

//     // Basic validation of the incoming payload
//     if (!payload || (payloadType === 'object' && Object.keys(payload).length === 0 && !Array.isArray(payload)) || (Array.isArray(payload) && payload.length === 0)) {
//          console.warn("[Server] Received empty or potentially invalid payload structure.");
//          res.status(200).send('Webhook payload received but appears empty or invalid, check source.');
//          return;
//     }

//     // --- Acknowledge QuickNode Immediately ---
//     res.status(200).send('Webhook payload received, processing started');
//     console.log(`[Server] Sent 200 OK acknowledgment to QuickNode.`);

//     // --- Process the Payload Asynchronously ---
//     setImmediate(async () => {
//         const processingStartTime = Date.now();
//         console.log(`[Server] Starting asynchronous processing...`);

//         // Initialize variables for this batch processing run
//         let mongoClient = null;
//         let db = null;
//         let priceData = null;
//         let allTransactions = []; // Holds all valid transactions from the payload batch
//         let minBlock = Infinity;
//         let maxBlock = 0;
//         let lastTimestampMs = 0; // Timestamp of the latest block/tx in the batch

//         // CEX Aggregation Variables
//         const cexFlowsByLabel = new Map(); // Map<cex_label, { inflow: BigInt, outflow: BigInt }>
//         let totalCexInflowBatch = 0n;
//         let totalCexOutflowBatch = 0n;

//         try {
//             // --- 1. Extract Transactions from Payload ---
//             let batchItems = [];
//             if (Array.isArray(payload)) {
//                  batchItems = payload;
//                  console.log(`[Server] Detected batched payload with ${batchItems.length} items.`);
//             } else if (payload && payload.whaleTransactions) { // Key from your filter function
//                  batchItems = [payload];
//                  console.log(`[Server] Detected single payload item.`);
//             } else {
//                 console.warn("[Server] Received payload in unexpected format. Cannot process.", JSON.stringify(payload).substring(0, 500));
//                 return;
//             }

//             for (const item of batchItems) {
//                 if (item && Array.isArray(item.whaleTransactions)) {
//                     item.whaleTransactions.forEach(tx => {
//                         if (tx && tx.block && tx.timestamp_ms && tx.txHash && tx.value_wei && tx.from) {
//                             allTransactions.push(tx);
//                             const blockNum = tx.block;
//                             const timestamp = tx.timestamp_ms;
//                             if (blockNum < minBlock) minBlock = blockNum;
//                             if (blockNum > maxBlock) maxBlock = blockNum;
//                             if (timestamp > lastTimestampMs) lastTimestampMs = timestamp;
//                         } else {
//                             console.warn(`[Server] Skipping invalid transaction object received from filter:`, tx ? tx.txHash : 'Data missing');
//                         }
//                     });
//                 } else {
//                      console.warn(`[Server] Skipping item in batch due to missing or non-array 'whaleTransactions'. Item keys: ${Object.keys(item || {}).join(', ')}`);
//                 }
//             }
//             const totalTxCountInPayload = allTransactions.length;

//             if (totalTxCountInPayload === 0) {
//                  console.log("[Server] No valid transactions found in the batch payload. Exiting async processing.");
//                  return;
//             }
//             const blockRangeString = (minBlock === Infinity || maxBlock === 0)
//                 ? 'Block N/A'
//                 : (minBlock === maxBlock ? `Block #${minBlock}` : `Blocks #${minBlock}-${maxBlock}`);
//             console.log(`[Server] Extracted ${totalTxCountInPayload} transactions from ${blockRangeString}. Last Timestamp: ${lastTimestampMs}`);

//             // --- 2. Fetch Current ETH Price ---
//             console.log(`[Server] Batch ${blockRangeString}: Attempting to fetch ETH price...`);
//             priceData = await getEthPriceWithTrend();
//             if (priceData) {
//                 console.log(`[Server] Batch ${blockRangeString}: ETH Price fetched successfully: $${priceData.usd}`);
//             } else {
//                 console.warn(`[Server] Batch ${blockRangeString}: Could not fetch ETH price. USD values will be unavailable.`);
//             }

//             // --- 3. Fetch Address Labels from MongoDB ---
//             const allAddresses = new Set();
//             allTransactions.forEach(tx => {
//                 if (tx.from) allAddresses.add(tx.from.toLowerCase());
//                 if (tx.to) allAddresses.add(tx.to.toLowerCase());
//             });
//             const uniqueAddresses = Array.from(allAddresses);
//             const addressLabels = new Map(); // Map: <lowercase_address, label>

//             if (uniqueAddresses.length > 0) {
//                  console.log(`[Server] Batch ${blockRangeString}: Need to fetch labels for ${uniqueAddresses.length} unique addresses.`);
//                  try {
//                       mongoClient = await getMongoClient();
//                       db = mongoClient.db(config.dbName);
//                       const labelsCollection = db.collection(config.labelCollection);
//                       console.log(`[Server] Batch ${blockRangeString}: Using labels collection: ${config.dbName}.${config.labelCollection}`);
//                       console.log(`[Server] Batch ${blockRangeString}: Fetching labels from DB...`);
//                       const cursor = labelsCollection.find(
//                           { address: { $in: uniqueAddresses.map(a => a) } },
//                           { projection: { _id: 0, address: 1, label: 1 } }
//                       );
//                       const dbStartTime = Date.now();
//                       const labelsData = await cursor.toArray();
//                       const dbEndTime = Date.now();
//                       console.log(`[Server] Batch ${blockRangeString}: MongoDB find query took ${dbEndTime - dbStartTime} ms.`);
//                       labelsData.forEach(item => {
//                           if (item.address && item.label) {
//                               addressLabels.set(item.address.toLowerCase(), item.label);
//                           }
//                       });
//                       console.log(`[Server] Batch ${blockRangeString}: Fetched ${addressLabels.size} labels from DB.`);
//                  } catch (dbError) {
//                       console.error(`[Server] Batch ${blockRangeString}: MongoDB Error during label fetching:`, dbError.message);
//                       console.warn(`[Server] Batch ${blockRangeString}: Proceeding without address labels due to DB error.`);
//                  }
//             } else {
//                  console.log(`[Server] Batch ${blockRangeString}: No unique addresses found. Skipping label fetching.`);
//             }

//             // --- 4. Process Transactions for CEX Flows ---
//             console.log(`[Server] Batch ${blockRangeString}: Analyzing ${totalTxCountInPayload} transactions for CEX flows...`);
//             allTransactions.forEach(tx => {
//                 const fromAddr = tx.from?.toLowerCase();
//                 const toAddr = tx.to?.toLowerCase();
//                 const valueWei = safeHexToBigInt(tx.value_wei || tx.value || '0x0');
//                 if (valueWei === 0n) return;
//                 const fromLabel = fromAddr ? addressLabels.get(fromAddr) : null;
//                 const toLabel = toAddr ? addressLabels.get(toAddr) : null;
//                 const fromIsCex = fromLabel && config.knownCexKeywords.has(fromLabel.toLowerCase());
//                 const toIsCex = toLabel && config.knownCexKeywords.has(toLabel.toLowerCase());

//                 if (fromIsCex && !toIsCex) {
//                     if (!cexFlowsByLabel.has(fromLabel)) cexFlowsByLabel.set(fromLabel, { inflow: 0n, outflow: 0n });
//                     cexFlowsByLabel.get(fromLabel).outflow += valueWei;
//                     totalCexOutflowBatch += valueWei;
//                 } else if (!fromIsCex && toIsCex) {
//                     if (!cexFlowsByLabel.has(toLabel)) cexFlowsByLabel.set(toLabel, { inflow: 0n, outflow: 0n });
//                     cexFlowsByLabel.get(toLabel).inflow += valueWei;
//                     totalCexInflowBatch += valueWei;
//                 } else if (fromIsCex && toIsCex && fromLabel !== toLabel) {
//                     if (!cexFlowsByLabel.has(fromLabel)) cexFlowsByLabel.set(fromLabel, { inflow: 0n, outflow: 0n });
//                     cexFlowsByLabel.get(fromLabel).outflow += valueWei;
//                     totalCexOutflowBatch += valueWei;
//                     if (!cexFlowsByLabel.has(toLabel)) cexFlowsByLabel.set(toLabel, { inflow: 0n, outflow: 0n });
//                     cexFlowsByLabel.get(toLabel).inflow += valueWei;
//                     totalCexInflowBatch += valueWei;
//                 }
//             });
//             console.log(`[Server] Batch ${blockRangeString}: CEX flow aggregation complete. Found ${cexFlowsByLabel.size} involved CEX labels.`);
//             console.log(`[Server] Batch ${blockRangeString}: Total CEX Inflow: ${totalCexInflowBatch} Wei, Total CEX Outflow: ${totalCexOutflowBatch} Wei.`);

//             // --- 5. Process for Whale Transactions ---
//             const {
//                 totalWhaleTxCount,
//                 totalWhaleValueWei,
//                 topWhales
//             } = processWhaleTransactions(allTransactions);

//             // --- 6. Send CEX Report ---
//             const cexFlowsArray = Array.from(cexFlowsByLabel.entries()).map(([label, flows]) => ({
//                 label: label,
//                 inflow: flows.inflow.toString(),
//                 outflow: flows.outflow.toString(),
//                 totalFlow: (flows.inflow + flows.outflow).toString()
//             }));

//             if (cexFlowsArray.length > 0 || totalCexInflowBatch > 0n || totalCexOutflowBatch > 0n) {
//                  const finalCexReportData = {
//                     startBlock: (minBlock === Infinity) ? 0 : minBlock,
//                     endBlock: maxBlock,
//                     batchTimestamp: lastTimestampMs,
//                     totalCexInflowWei: totalCexInflowBatch.toString(),
//                     totalCexOutflowWei: totalCexOutflowBatch.toString(),
//                     cexFlows: cexFlowsArray,
//                     txCountAnalyzed: totalTxCountInPayload,
//                     priceInfo: priceData
//                 };
//                 console.log(`[Server] Batch ${blockRangeString}: Sending aggregated CEX report...`);
//                 await sendEthTransactionReportToDiscord(finalCexReportData);
//             } else {
//                 console.log(`[Server] Batch ${blockRangeString}: No CEX flow detected. Skipping CEX report.`);
//             }

//             // --- 7. Send Whale Alert Report ---
//             if (totalWhaleTxCount > 0) {
//                 const whaleReportData = {
//                     startBlock: (minBlock === Infinity) ? 0 : minBlock,
//                     endBlock: maxBlock,
//                     batchTimestamp: lastTimestampMs,
//                     totalWhaleTxCount: totalWhaleTxCount,
//                     totalWhaleValueWei: totalWhaleValueWei.toString(),
//                     topWhales: topWhales.map(tx => ({
//                         txHash: tx.txHash,
//                         value_wei: tx.valueWeiParsed.toString(),
//                         from: tx.from,
//                         to: tx.to
//                     })),
//                     priceInfo: priceData,
//                     labelsMap: addressLabels
//                 };
//                 console.log(`[Server] Batch ${blockRangeString}: Sending Whale Alert report (${totalWhaleTxCount} whales)...`);
//                 await sendWhaleReportToDiscord(whaleReportData);
//             } else {
//                 console.log(`[Server] Batch ${blockRangeString}: No whale transactions found meeting the threshold >= ${config.whaleThresholdEth} ETH. Skipping Whale Alert report.`);
//             }

//             // --- 8. Store Batch Summary in MongoDB ---
//             try {
//                 if (!mongoClient) {
//                     mongoClient = await getMongoClient();
//                 }
//                 if (!db) {
//                     db = mongoClient.db(config.dbName);
//                 }
//                 const monitoringCollectionName = process.env.ETH_CEX_MONITORING_COLLECTION_NAME || 'ETH_CEX_MONITORING';
//                 const monitoringCollection = db.collection(monitoringCollectionName);
//                 const batchDoc = {
//                     startBlock: (minBlock === Infinity) ? 0 : minBlock,
//                     endBlock: maxBlock,
//                     batchTimestamp: lastTimestampMs,
//                     processedAt: new Date(),
//                     totalCexInflowWei: totalCexInflowBatch.toString(),
//                     totalCexOutflowWei: totalCexOutflowBatch.toString(),
//                     cexFlows: cexFlowsArray,
//                     txCountAnalyzed: totalTxCountInPayload,
//                     priceInfo: priceData,
//                     totalWhaleTxCount: totalWhaleTxCount,
//                     totalWhaleValueWei: totalWhaleValueWei.toString(),
//                     topWhales: topWhales.map(tx => ({
//                         txHash: tx.txHash,
//                         value_wei: tx.valueWeiParsed.toString(),
//                         from: tx.from,
//                         to: tx.to
//                     }))
//                 };
//                 await monitoringCollection.insertOne(batchDoc);
//                 console.log(`[Server] Batch ${blockRangeString}: Batch summary stored in MongoDB collection '${monitoringCollectionName}'.`);
//             } catch (mongoStoreErr) {
//                 console.error(`[Server] Batch ${blockRangeString}: Error storing batch summary in MongoDB:`, mongoStoreErr.message);
//             }

//         } catch (error) {
//             const blockRangeString = (minBlock === Infinity || maxBlock === 0) ? 'N/A' : `${minBlock}-${maxBlock}`;
//             console.error(`[Server] Unhandled error during asynchronous processing for blocks ${blockRangeString}:`, error.message);
//             console.error(error.stack);
//         } finally {
//              const processingEndTime = Date.now();
//              const blockRangeString = (minBlock === Infinity || maxBlock === 0) ? 'N/A' : `${minBlock}-${maxBlock}`;
//              console.log(`[Server] Finished asynchronous processing for blocks ${blockRangeString}. Duration: ${processingEndTime - processingStartTime} ms.`);
//         }
//     }); // End setImmediate asynchronous block
// }); // End POST /quicknode-webhook route


// // --- Generic Error Handler Middleware ---
// app.use((err, req, res, next) => {
//     console.error("[Server] Express Error Handler Caught Error:", err.message);
//     console.error(err.stack);
//     res.status(500).send('Internal Server Error');
// });

// // --- Start Server & Handle Graceful Shutdown ---
// const server = app.listen(config.port, () => {
//     console.log(`[Server] ETH Webhook Receiver listening on port ${config.port}`);
//     console.log(`[Server] Loaded Config - DB Name: ${config.dbName}, Label Collection: ${config.labelCollection}`);
//     console.log(`[Server] Whale Alert Config - Threshold: ${config.whaleThresholdEth} ETH, Webhook Set: ${!!config.discordWhaleWebhookUrl}`);
//     console.log(`[Server] CEX Report Config - Webhook Set: ${!!config.discordWebhookUrl}, Top N: ${config.topNCexEntriesToShow}`);

//     console.log("[Server] Attempting initial MongoDB connection on startup...");
//     getMongoClient().catch(err => {
//          console.error("[Server] FATAL: Initial MongoDB connection failed on startup. Exiting.", err.message);
//          process.exit(1);
//     });
// });

// // Graceful shutdown logic
// const shutdown = async (signal) => {
//     console.log(`[Server] ${signal} received... Starting graceful shutdown.`);
//     server.close(async () => {
//         console.log('[Server] HTTP server closed.');
//         await closeDatabaseConnection();
//         console.log('[Server] Exiting process gracefully.');
//         process.exit(0);
//     });
//     setTimeout(async () => {
//         console.error('[Server] Graceful shutdown timeout exceeded (10s). Forcing exit.');
//         await closeDatabaseConnection();
//         process.exit(1);
//     }, 10000);
// };

// process.on('SIGINT', () => shutdown('SIGINT'));
// process.on('SIGTERM', () => shutdown('SIGTERM'));

// server.js
// --- Core Dependencies ---
require('dotenv').config(); // Load .env variables first
const express = require('express');
const util = require('util'); // For advanced object inspection if needed

// --- Configuration ---
const config = require('./config'); // Load all app configuration

// --- Service Imports ---
const { getMongoClient, closeDatabaseConnection } = require('./services/dbConnection');
const { processWhaleTransactions } = require('./services/whaleProcessor'); // Handles whale logic
const { sendWhaleReportToDiscord } = require('./services/discordWhaleSender'); // Handles whale Discord formatting/sending
// Import the original CEX sender function (assuming it's still in discordSender.js or similar)
const { sendEthTransactionReportToDiscord } = require('./services/discordSender'); // Original CEX sender

// --- Utility Imports ---
const { getEthPriceWithTrend } = require('./utils/priceFetcher'); // Fetches ETH price

// --- Express App Setup ---
const app = express();

// --- Middleware ---
// Increase payload size limit (adjust if necessary, but 50mb is generous)
app.use(express.json({ limit: '50mb' }));
// Basic request logging middleware
app.use((req, res, next) => {
    console.log(`[${new Date().toISOString()}] ${req.method} ${req.url}`);
    next();
});

// --- Helper Functions ---
/**
 * Safely converts a hex string (like "0x...") to a Number.
 * Returns 0 if the input is invalid or conversion fails.
 * @param {string | null | undefined} hex
 * @returns {number}
 */
function safeHexToNumber(hex) {
    if (typeof hex !== 'string' || !hex.startsWith('0x')) { return 0; }
    try {
        if (!/^(0x)?[0-9a-fA-F]+$/.test(hex)) { return 0; }
        const num = parseInt(hex, 16); // Parse as base 16
        return isNaN(num) ? 0 : num; // Return 0 if result is Not-a-Number
    } catch (e) {
        console.warn(`[Server] Could not convert hex "${hex}" to Number:`, e.message);
        return 0;
    }
}
// Note: safeHexToBigInt is removed as value_wei is expected as a decimal string now.
// --- End Helper Functions ---


// --- Routes ---

// Basic health check route
app.get('/', (req, res) => {
    res.status(200).send('ETH CEX Flow & Whale Webhook Receiver is running.'); // Updated message
});

// Main webhook endpoint for QuickNode stream data
app.post('/quicknode-webhook', async (req, res) => {
    const payload = req.body;
    const payloadType = Array.isArray(payload) ? 'array' : typeof payload;
    const payloadSize = payload ? JSON.stringify(payload).length : 0;
    console.log(`[Server] Received webhook payload. Type: ${payloadType}, Approx Size: ${payloadSize} bytes.`);

    // Log received payload for debugging
    console.log("[Server] Inspecting Full Received Payload (depth: 5):");
    console.log(util.inspect(payload, { showHidden: false, depth: 5, colors: false }));

    // Basic validation of the incoming payload
    if (!payload || (payloadType === 'object' && Object.keys(payload).length === 0 && !Array.isArray(payload)) || (Array.isArray(payload) && payload.length === 0)) {
         console.warn("[Server] Received empty or potentially invalid payload structure.");
         res.status(200).send('Webhook payload received but appears empty or invalid, check source.');
         return; // Stop processing this request
    }

    // --- Acknowledge QuickNode Immediately ---
    res.status(200).send('Webhook payload received, processing started');
    console.log(`[Server] Sent 200 OK acknowledgment to QuickNode.`);

    // --- Process the Payload Asynchronously ---
    setImmediate(async () => {
        const processingStartTime = Date.now();
        console.log(`[Server] Starting asynchronous processing...`);

        // Initialize variables for this batch processing run
        let mongoClient = null;
        let db = null;
        let priceData = null;
        let allTransactions = []; // Holds all valid transactions from the payload batch
        let minBlock = Infinity;
        let maxBlock = 0;
        let lastTimestampMs = 0; // Timestamp of the latest block/tx in the batch

        // CEX Aggregation Variables
        const cexFlowsByLabel = new Map(); // Map<cex_label, { inflow: BigInt, outflow: BigInt }>
        let totalCexInflowBatch = 0n;
        let totalCexOutflowBatch = 0n;

        try {
            // --- 1. Extract Transactions from Payload ---
            let batchItems = [];
            if (Array.isArray(payload)) {
                 batchItems = payload;
                 console.log(`[Server] Detected batched payload with ${batchItems.length} items.`);
            } else if (payload && payload.whaleTransactions) { // Key from your filter function
                 batchItems = [payload];
                 console.log(`[Server] Detected single payload item.`);
            } else {
                console.warn("[Server] Received payload in unexpected format. Cannot process.", JSON.stringify(payload).substring(0, 500));
                return; // Exit async processing
            }

            // Extract all transactions from the batch items
            for (const item of batchItems) {
                // Use the key 'whaleTransactions' which your filter function returns
                if (item && Array.isArray(item.whaleTransactions)) {
                    item.whaleTransactions.forEach(tx => {
                        // Basic validation: ensure essential fields exist from the filter output
                        if (tx && tx.block && tx.timestamp_ms && tx.txHash && tx.value_wei && tx.from) {
                            allTransactions.push(tx); // Add the validated transaction
                            // Update block range and timestamp tracking
                            const blockNum = tx.block;
                            const timestamp = tx.timestamp_ms;
                            if (blockNum < minBlock) minBlock = blockNum;
                            if (blockNum > maxBlock) maxBlock = blockNum;
                            if (timestamp > lastTimestampMs) lastTimestampMs = timestamp;
                        } else {
                            console.warn(`[Server] Skipping invalid transaction object received from filter (missing required fields):`, tx ? tx.txHash : 'Data missing');
                        }
                    });
                } else {
                     console.warn(`[Server] Skipping item in batch due to missing or non-array 'whaleTransactions'. Item keys: ${Object.keys(item || {}).join(', ')}`);
                }
            }
            const totalTxCountInPayload = allTransactions.length; // Total valid transactions received

            // Exit if no valid transactions were extracted
            if (totalTxCountInPayload === 0) {
                 console.log("[Server] No valid transactions found in the batch payload after extraction. Exiting async processing.");
                 return;
            }

            // Determine block range string for logging
            const blockRangeString = (minBlock === Infinity || maxBlock === 0)
                ? 'Block N/A'
                : (minBlock === maxBlock ? `Block #${minBlock}` : `Blocks #${minBlock}-${maxBlock}`);
            console.log(`[Server] Extracted ${totalTxCountInPayload} transactions from ${blockRangeString}. Last Timestamp: ${lastTimestampMs}`);

            // --- 2. Fetch Current ETH Price ---
            console.log(`[Server] Batch ${blockRangeString}: Attempting to fetch ETH price...`);
            priceData = await getEthPriceWithTrend(); // Uses priceFetcher utility
            if (priceData) {
                console.log(`[Server] Batch ${blockRangeString}: ETH Price fetched successfully: $${priceData.usd}`);
            } else {
                console.warn(`[Server] Batch ${blockRangeString}: Could not fetch ETH price. USD values will be unavailable.`);
            }

            // --- 3. Fetch Address Labels from MongoDB ---
            const allAddresses = new Set(); // Use a Set to automatically handle duplicates
            allTransactions.forEach(tx => {
                // Add 'from' and 'to' addresses (if they exist) to the set
                if (tx.from) allAddresses.add(tx.from.toLowerCase());
                if (tx.to) allAddresses.add(tx.to.toLowerCase()); // 'to' can be null for contract creation
            });
            const uniqueAddresses = Array.from(allAddresses); // Convert Set to Array for querying
            const addressLabels = new Map(); // Map to store: <lowercase_address, label>

            if (uniqueAddresses.length > 0) {
                 console.log(`[Server] Batch ${blockRangeString}: Need to fetch labels for ${uniqueAddresses.length} unique addresses.`);
                 try {
                      // Get MongoDB client connection
                      mongoClient = await getMongoClient(); // Uses dbConnection service
                      db = mongoClient.db(config.dbName);
                      const labelsCollection = db.collection(config.labelCollection);
                      console.log(`[Server] Batch ${blockRangeString}: Using labels collection: ${config.dbName}.${config.labelCollection}`);
                      console.log(`[Server] Batch ${blockRangeString}: Fetching labels from DB...`);

                      // Query the database for labels matching the addresses found in the batch
                      const cursor = labelsCollection.find(
                          // Query using the array of unique addresses (ensure case sensitivity matches DB if needed)
                          { address: { $in: uniqueAddresses.map(a => a) } },
                          // Only project the necessary fields
                          { projection: { _id: 0, address: 1, label: 1 } }
                      );

                      const dbStartTime = Date.now(); // Time the query
                      const labelsData = await cursor.toArray(); // Execute the query
                      const dbEndTime = Date.now();
                      console.log(`[Server] Batch ${blockRangeString}: MongoDB find query took ${dbEndTime - dbStartTime} ms.`);

                      // Populate the addressLabels map using lowercase addresses as keys
                      labelsData.forEach(item => {
                          if (item.address && item.label) {
                              addressLabels.set(item.address.toLowerCase(), item.label);
                          }
                      });
                      console.log(`[Server] Batch ${blockRangeString}: Fetched ${addressLabels.size} labels from DB.`);

                 } catch (dbError) {
                      // Log database errors but allow processing to continue without labels
                      console.error(`[Server] Batch ${blockRangeString}: MongoDB Error during label fetching:`, dbError.message);
                      console.warn(`[Server] Batch ${blockRangeString}: Proceeding without address labels due to DB error.`);
                 }
                 // Note: MongoDB connection is typically managed globally and closed on shutdown
            } else {
                 console.log(`[Server] Batch ${blockRangeString}: No unique addresses found in transactions. Skipping label fetching.`);
            }

            // --- 4. Process Transactions for CEX Flows (Corrected Value Parsing + Debug Logging) ---
            console.log(`[Server] Batch ${blockRangeString}: Analyzing ${totalTxCountInPayload} transactions for CEX flows...`);
            // Log the CEX keywords being used for this batch
            console.log(`[CEX DEBUG] Using known CEX keywords:`, Array.from(config.knownCexKeywords));

            allTransactions.forEach((tx, index) => {
                const fromAddr = tx.from?.toLowerCase();
                const toAddr = tx.to?.toLowerCase();

                // *** CORRECTED CEX VALUE PARSING ***
                // Directly convert the decimal string `tx.value_wei` to BigInt
                let valueWei = 0n;
                if (typeof tx.value_wei === 'string' && /^\d+$/.test(tx.value_wei)) { // Check if it's a string of digits
                    try {
                        valueWei = BigInt(tx.value_wei);
                    } catch (e) {
                        console.warn(`[CEX DEBUG TX ${index+1}] Could not convert value_wei string "${tx.value_wei}" to BigInt:`, e.message);
                        // valueWei remains 0n
                    }
                } else if (tx.value_wei) { // Log if value_wei exists but isn't a valid string
                     console.warn(`[CEX DEBUG TX ${index+1}] Invalid value_wei format: Expected decimal string, got ${typeof tx.value_wei} (${tx.value_wei})`);
                     // valueWei remains 0n
                }
                // *** END CORRECTION ***

                // Log basic info for each transaction being checked
                console.log(`\n[CEX DEBUG TX ${index+1}/${totalTxCountInPayload}] Hash: ${tx.txHash}`);
                console.log(`[CEX DEBUG TX ${index+1}] From: ${tx.from}, To: ${tx.to}, Value (Wei): ${valueWei.toString()}`); // Log the PARSED valueWei

                // Skip zero-value transactions for CEX flow analysis
                if (valueWei === 0n) {
                    console.log(`[CEX DEBUG TX ${index+1}] Skipped: Zero value.`);
                    return;
                }

                // Get labels using lowercase addresses
                const fromLabel = fromAddr ? addressLabels.get(fromAddr) : null;
                const toLabel = toAddr ? addressLabels.get(toAddr) : null;

                // Log labels found for this transaction
                console.log(`[CEX DEBUG TX ${index+1}] From Label: ${fromLabel || 'None'}, To Label: ${toLabel || 'None'}`);

                // Prepare lowercase labels for keyword checking
                const fromLabelLower = fromLabel?.toLowerCase();
                const toLabelLower = toLabel?.toLowerCase();

                // Check if labels match known CEX keywords
                const fromIsCex = fromLabelLower && config.knownCexKeywords.has(fromLabelLower);
                const toIsCex = toLabelLower && config.knownCexKeywords.has(toLabelLower);

                // Log the results of the CEX checks
                console.log(`[CEX DEBUG TX ${index+1}] From is CEX? ${fromIsCex} (Checked '${fromLabelLower}' against keywords)`);
                console.log(`[CEX DEBUG TX ${index+1}] To is CEX? ${toIsCex} (Checked '${toLabelLower}' against keywords)`);

                // Aggregate CEX Flows based on checks
                if (fromIsCex && !toIsCex) {
                    console.log(`[CEX DEBUG TX ${index+1}] ===> CEX Outflow detected from ${fromLabel}`); // Log detection
                    if (!cexFlowsByLabel.has(fromLabel)) cexFlowsByLabel.set(fromLabel, { inflow: 0n, outflow: 0n });
                    cexFlowsByLabel.get(fromLabel).outflow += valueWei;
                    totalCexOutflowBatch += valueWei;
                } else if (!fromIsCex && toIsCex) {
                    console.log(`[CEX DEBUG TX ${index+1}] ===> CEX Inflow detected to ${toLabel}`); // Log detection
                    if (!cexFlowsByLabel.has(toLabel)) cexFlowsByLabel.set(toLabel, { inflow: 0n, outflow: 0n });
                    cexFlowsByLabel.get(toLabel).inflow += valueWei;
                    totalCexInflowBatch += valueWei;
                } else if (fromIsCex && toIsCex && fromLabel !== toLabel) {
                    console.log(`[CEX DEBUG TX ${index+1}] ===> CEX-to-CEX detected from ${fromLabel} to ${toLabel}`); // Log detection
                    // Record as outflow from sender CEX
                    if (!cexFlowsByLabel.has(fromLabel)) cexFlowsByLabel.set(fromLabel, { inflow: 0n, outflow: 0n });
                    cexFlowsByLabel.get(fromLabel).outflow += valueWei;
                    totalCexOutflowBatch += valueWei;
                    // Record as inflow to receiver CEX
                    if (!cexFlowsByLabel.has(toLabel)) cexFlowsByLabel.set(toLabel, { inflow: 0n, outflow: 0n });
                    cexFlowsByLabel.get(toLabel).inflow += valueWei;
                    totalCexInflowBatch += valueWei;
                } else {
                    // Log why it wasn't counted as CEX flow
                    if (fromIsCex && toIsCex && fromLabel === toLabel) {
                        console.log(`[CEX DEBUG TX ${index+1}] Skipped CEX: Transfer within the same CEX (${fromLabel}).`);
                    } else if (!fromIsCex && !toIsCex) {
                        console.log(`[CEX DEBUG TX ${index+1}] Skipped CEX: Neither From nor To is a known CEX.`);
                    } else {
                         // This case should ideally not happen if logic above is correct
                         console.log(`[CEX DEBUG TX ${index+1}] Skipped CEX: Unhandled case (FromCEX: ${fromIsCex}, ToCEX: ${toIsCex}).`);
                    }
                }
            }); // End CEX forEach loop
            console.log(`[Server] Batch ${blockRangeString}: CEX flow aggregation complete. Found ${cexFlowsByLabel.size} involved CEX labels.`);
            console.log(`[Server] Batch ${blockRangeString}: Total CEX Inflow: ${totalCexInflowBatch} Wei, Total CEX Outflow: ${totalCexOutflowBatch} Wei.`);

            // --- 5. Process for Whale Transactions ---
            // Delegate whale identification, filtering, and sorting to the whaleProcessor service
            const {
                totalWhaleTxCount,
                totalWhaleValueWei, // This is a BigInt
                topWhales           // Array of top N whale tx objects
            } = processWhaleTransactions(allTransactions); // Pass the same transaction list

            // --- 6. Send CEX Report ---
            // Prepare data for the CEX report sender
            const cexFlowsArray = Array.from(cexFlowsByLabel.entries()).map(([label, flows]) => ({
                label: label,
                inflow: flows.inflow.toString(),
                outflow: flows.outflow.toString(),
                totalFlow: (flows.inflow + flows.outflow).toString() // Calculate total flow for sorting/display if needed
            }));

            // Check if there's CEX data to report and if the CEX webhook is configured
            if ((cexFlowsArray.length > 0 || totalCexInflowBatch > 0n || totalCexOutflowBatch > 0n) && config.discordWebhookUrl) {
                 const finalCexReportData = {
                    startBlock: (minBlock === Infinity) ? 0 : minBlock,
                    endBlock: maxBlock,
                    batchTimestamp: lastTimestampMs,
                    totalCexInflowWei: totalCexInflowBatch.toString(),
                    totalCexOutflowWei: totalCexOutflowBatch.toString(),
                    cexFlows: cexFlowsArray, // Pass the aggregated flows per CEX
                    txCountAnalyzed: totalTxCountInPayload, // Total TXs received in payload
                    priceInfo: priceData
                };
                 console.log(`[Server] Batch ${blockRangeString}: Preparing to send aggregated CEX report...`);
                 // Call the CEX sender function
                 await sendEthTransactionReportToDiscord(finalCexReportData);
            } else {
                console.log(`[Server] Batch ${blockRangeString}: No CEX flow detected or CEX webhook URL not configured. Skipping CEX report.`);
            }

            // --- 7. Send Whale Alert Report ---
            // Check if whales were found and if the Whale webhook is configured
            if (totalWhaleTxCount > 0 && config.discordWhaleWebhookUrl) {
                // Prepare the data object required by the discordWhaleSender
                const whaleReportData = {
                    startBlock: (minBlock === Infinity) ? 0 : minBlock,
                    endBlock: maxBlock,
                    batchTimestamp: lastTimestampMs, // Use the latest timestamp from the batch
                    totalWhaleTxCount: totalWhaleTxCount,
                    totalWhaleValueWei: totalWhaleValueWei.toString(), // Convert BigInt total to string
                    // Map top whales: ensure valueWei is stringified, pass only essential fields
                    topWhales: topWhales.map(tx => ({
                        txHash: tx.txHash,
                        value_wei: tx.valueWeiParsed.toString(), // Use the parsed BigInt, convert to string
                        from: tx.from,
                        to: tx.to
                        // Add other fields from tx if needed by the sender
                    })),
                    priceInfo: priceData, // Pass fetched price data (can be null)
                    labelsMap: addressLabels // Pass the fetched labels map
                };

                console.log(`[Server] Batch ${blockRangeString}: Preparing to send Whale Alert report (${totalWhaleTxCount} whales)...`);
                // Call the dedicated whale sender function
                await sendWhaleReportToDiscord(whaleReportData);
            } else {
                console.log(`[Server] Batch ${blockRangeString}: No whale transactions found meeting the threshold >= ${config.whaleThresholdEth} ETH or Whale webhook URL not configured. Skipping Whale Alert report.`);
            }

            // --- 8. Store Batch Summary in MongoDB ---
            try {
                if (!mongoClient) {
                    mongoClient = await getMongoClient();
                }
                if (!db) {
                    db = mongoClient.db(config.dbName);
                }
                const monitoringCollectionName = process.env.ETH_CEX_MONITORING_COLLECTION_NAME || 'ETH_CEX_MONITORING';
                const monitoringCollection = db.collection(monitoringCollectionName);
                const batchDoc = {
                    startBlock: (minBlock === Infinity) ? 0 : minBlock,
                    endBlock: maxBlock,
                    batchTimestamp: lastTimestampMs,
                    processedAt: new Date(),
                    totalCexInflowWei: totalCexInflowBatch.toString(),
                    totalCexOutflowWei: totalCexOutflowBatch.toString(),
                    cexFlows: cexFlowsArray,
                    txCountAnalyzed: totalTxCountInPayload,
                    priceInfo: priceData,
                    totalWhaleTxCount: totalWhaleTxCount,
                    totalWhaleValueWei: totalWhaleValueWei.toString(),
                    topWhales: topWhales.map(tx => ({
                        txHash: tx.txHash,
                        value_wei: tx.valueWeiParsed.toString(),
                        from: tx.from,
                        to: tx.to
                    }))
                };
                await monitoringCollection.insertOne(batchDoc);
                console.log(`[Server] Batch ${blockRangeString}: Batch summary stored in MongoDB collection '${monitoringCollectionName}'.`);
            } catch (mongoStoreErr) {
                console.error(`[Server] Batch ${blockRangeString}: Error storing batch summary in MongoDB:`, mongoStoreErr.message);
            }

        } catch (error) {
            // Catch any unhandled errors during the asynchronous processing
            const blockRangeString = (minBlock === Infinity || maxBlock === 0) ? 'N/A' : `${minBlock}-${maxBlock}`;
            console.error(`[Server] Unhandled error during asynchronous processing for blocks ${blockRangeString}:`, error.message);
            console.error(error.stack); // Log the full stack trace for debugging
        } finally {
             // Log completion and duration of the asynchronous processing for this batch
             const processingEndTime = Date.now();
             const blockRangeString = (minBlock === Infinity || maxBlock === 0) ? 'N/A' : `${minBlock}-${maxBlock}`;
             console.log(`[Server] Finished asynchronous processing for blocks ${blockRangeString}. Duration: ${processingEndTime - processingStartTime} ms.`);
        }
    }); // End setImmediate asynchronous block
}); // End POST /quicknode-webhook route


// --- Generic Error Handler Middleware ---
// Catches errors passed via next(err) or thrown synchronously in route handlers
app.use((err, req, res, next) => {
    console.error("[Server] Express Error Handler Caught Error:", err.message);
    console.error(err.stack); // Log the full stack trace
    // Avoid sending detailed errors to the client in production
    res.status(500).send('Internal Server Error');
});

// --- Start Server & Handle Graceful Shutdown ---
const server = app.listen(config.port, () => {
    console.log(`[Server] ETH Webhook Receiver listening on port ${config.port}`);
    // Log key configuration values on startup for verification
    console.log(`[Server] Loaded Config - DB Name: ${config.dbName}, Label Collection: ${config.labelCollection}`);
    console.log(`[Server] Whale Alert Config - Threshold: ${config.whaleThresholdEth} ETH, Webhook Set: ${!!config.discordWhaleWebhookUrl}`);
    console.log(`[Server] CEX Report Config - Webhook Set: ${!!config.discordWebhookUrl}, Top N: ${config.topNCexEntriesToShow}`);

    // Attempt initial MongoDB connection on startup to catch connection issues early
    console.log("[Server] Attempting initial MongoDB connection on startup...");
    getMongoClient().catch(err => {
         console.error("[Server] FATAL: Initial MongoDB connection failed on startup. Exiting.", err.message);
         process.exit(1); // Exit if database connection fails initially
    });
});

// Graceful shutdown logic
const shutdown = async (signal) => {
    console.log(`[Server] ${signal} received... Starting graceful shutdown.`);
    // Stop accepting new connections
    server.close(async () => {
        console.log('[Server] HTTP server closed.');
        // Close the database connection
        await closeDatabaseConnection(); // Ensure DB connection is closed
        console.log('[Server] Exiting process gracefully.');
        process.exit(0); // Exit successfully
    });

    // Force shutdown if graceful shutdown takes too long
    setTimeout(async () => {
        console.error('[Server] Graceful shutdown timeout exceeded (10s). Forcing exit.');
        await closeDatabaseConnection(); // Attempt close even on force exit
        process.exit(1); // Exit with error code
    }, 10000); // 10 second timeout
};

// Listen for termination signals
process.on('SIGINT', () => shutdown('SIGINT')); // CTRL+C
process.on('SIGTERM', () => shutdown('SIGTERM')); // kill command







