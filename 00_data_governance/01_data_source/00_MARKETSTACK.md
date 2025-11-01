Step 2: API Endpoints

There are several API endpoints to choose from:

End-of-Day Data: Get daily stock market data.
Intraday Data: Get intraday and real-time market data.
Tickers: Get information about stock ticker symbols.
Exchanges: Get infotmation about all supported exchanges.
Currencies: Get information about all supported currencies.
Timezones: Get information about all supported timezones.
Base URL: API requests start out with the following base URL:

http://api.marketstack.com/v2/


Make API Request: Let's try making a few simple API requests for the end-of-day, intraday and static API endpoints. Take a look at the box below and click the API requests to open them in your browser.

Make API Requestrequired and optional
EODINTRADAYTICKERSEXCHANGESCURRENCIESTIMEZONES

// End-of-Day Data API Endpoint

http://api.marketstack.com/v2/eod
    ? access_key = YOUR_ACCESS_KEY
    & symbols = AAPL
    
// optional parameters: 

    & sort = DESC
    & date_from = YYYY-MM-DD
    & date_to = YYYY-MM-DD
    & limit = 100
    & offset = 0
    

To learn more about API requests and parameters, please refer to the API Documentation.

Step 3: Integrate into your application

This was barely scratching the surface of the marketstack API. For specific integration guides and code examples, please have a look at the API's Documentation.

Should you require assistance of any kind, please contact our support team.



FREE

$0
No hidden fees
100 Requests / mo
no Support
End-of-Day Data
1 Year History
Splits & Dividends
Stock Tickers Info
2700+ Stock Exchanges Info
Currencies & Timezones
HTTPS Encryption