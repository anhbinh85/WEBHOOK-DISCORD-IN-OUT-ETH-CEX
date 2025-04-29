// src/services/dbConnection.js
// Using the exact code provided by the user.
const { MongoClient, ServerApiVersion } = require('mongodb');
const config = require('../config'); // Use centralized config

const MONGO_CONNECT_TIMEOUT_MS = 20000;

let client = null;
let clientPromise = null;

function getMongoClient() {
    if (client && clientPromise) {
        // console.log("[DBConnection] Returning existing client promise.");
        return clientPromise;
    }

    // --- Connection Details ---
    // Uses config names expected by this specific version of the file
    const username = config.mongoUsername;
    const password = config.mongoPassword;
    const cluster = config.mongoCluster;
    // Uses dbName from config OR defaults to "admin" for ping
    const dbNameForPing = config.dbName || "admin";

    if (!username || !password || !cluster) {
        console.error("FATAL: MongoDB connection details (USERNAME, PASSWORD, CLUSTER) missing in config/env");
        process.exit(1); // Exits if essential config is missing
    }

    // Encodes username and password as per the provided file's code line
    const encodedUsername = encodeURIComponent(username);
    const encodedPassword = encodeURIComponent(password);
    // Constructs URI using encoded credentials
    const uri = `mongodb+srv://${encodedUsername}:${encodedPassword}@${cluster}/?retryWrites=true&w=majority&appName=Cluster0`;
    const safeUri = uri.substring(0, uri.indexOf('://') + 3) + '******:******@' + uri.substring(uri.indexOf('@') + 1);

    console.log(`[DBConnection] Initializing new MongoDB connection to: ${safeUri}`);

    // MongoClient options exactly as in the original file provided
    // NOTE: authSource and authMechanism are NOT included here, matching the user's file.
    const clientOptions = {
        serverApi: {
            version: ServerApiVersion.v1,
            strict: false, // Set to false as per the original file's comment/intent
            deprecationErrors: true
        },
        connectTimeoutMS: MONGO_CONNECT_TIMEOUT_MS,
        serverSelectionTimeoutMS: MONGO_CONNECT_TIMEOUT_MS
        // authSource and authMechanism are intentionally omitted to match the user's file
    };

    client = new MongoClient(uri, clientOptions);

    console.log("[DBConnection] Attempting connection...");
    // Connection and error handling logic exactly as in the original file
    clientPromise = client.connect()
        .then(connectedClient => {
            client = connectedClient;
            console.log("[DBConnection] MongoDB Client Connected!");
            // Pings dbName from config OR 'admin'
            return client.db(dbNameForPing).command({ ping: 1 })
                .then(() => {
                    console.log(`[DBConnection] MongoDB ping successful on database "${dbNameForPing}".`);
                    return client;
                });
        })
        .catch(err => {
            // Uses the simpler error logging from the original file
            console.error("FATAL: MongoDB connection failed:", err);
            client = null;
            clientPromise = null;
            process.exit(1); // Exits on connection failure
        });

    return clientPromise;
}

// closeDatabaseConnection function exactly as in the original file
async function closeDatabaseConnection() {
    if (client) {
        console.log("[DBConnection] Closing MongoDB connection...");
        try {
            await client.close();
            client = null;
            clientPromise = null;
            console.log("[DBConnection] MongoDB connection closed.");
        } catch (e) {
            console.error("[DBConnection] Error closing MongoDB connection:", e);
        }
    } else {
         console.log("[DBConnection] No active MongoDB connection to close.");
    }
}

module.exports = {
    getMongoClient,
    closeDatabaseConnection
};
