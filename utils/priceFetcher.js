// utils/priceFetcher.js
const axios = require('axios');
const config = require('../config'); // Use config for API ID

/**
 * Fetches the current price and 24h change percentage of Ethereum in USD
 * using the CoinGecko /coins/markets endpoint.
 * @returns {Promise<{usd: number, change24h: number}|null>}
 * Object with price data, or null if fetching fails.
 */
async function getEthPriceWithTrend() {
    const coinId = config.priceApiId || 'ethereum'; // Get ID from config or default
    console.log(`[PriceFetcher] Fetching ETH price and 24h trend for ID: ${coinId}...`);
    const apiUrl = `https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&ids=${coinId}`;

    try {
        const response = await axios.get(apiUrl, { timeout: 8000 }); // Increased timeout slightly

        if (response.data && Array.isArray(response.data) && response.data.length > 0) {
            const ethData = response.data[0];
            const currentPrice = ethData.current_price;
            const change24h = ethData.price_change_percentage_24h;

            if (typeof currentPrice === 'number' && typeof change24h === 'number') {
                console.log(`[PriceFetcher] Success. ETH Price: $${currentPrice}, 24h Change: ${change24h.toFixed(2)}%`);
                return {
                    usd: currentPrice,
                    change24h: change24h
                };
            } else {
                 console.error("[PriceFetcher] Failed to parse price/change fields from ETH API response:", ethData);
                 return null;
            }
        } else {
            console.error("[PriceFetcher] Unexpected API response structure for ETH:", response.data);
            return null;
        }
    } catch (error) {
        console.error("[PriceFetcher] Error fetching ETH price/trend:", error.message);
        if (error.response) {
            console.error(`[PriceFetcher] API Response Status: ${error.response.status}`);
            // Avoid logging potentially large/sensitive data in production
            // console.error(`[PriceFetcher] API Response Data:`, JSON.stringify(error.response.data, null, 2));
        } else if (error.request) {
            console.error("[PriceFetcher] No response received from API.");
        } else {
            console.error('[PriceFetcher] Error details:', error);
        }
        return null; // Indicate failure
    }
}

module.exports = {
    // Export with a clear name
    getEthPriceWithTrend
};
