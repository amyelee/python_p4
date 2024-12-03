import alpaca_trade_api as tradeapi
import time
import requests
import math

# == Exceptions == 
class AlpacaDataFetcherException(Exception):
    def __init__(self, message):            
        super().__init__(message)

class PlaceOrderException(Exception):
    def __init__(self, message):            
        super().__init__(message)

class AlpacaDataFetcher:
    # Class to fetch latest bars data from Alpaca API
    def __init__(self, api, api_key, api_secret, symbol):
        self.api_key = api_key
        self.api_secret = api_secret
        self.symbol = symbol
        self.api = api
        self.headers = {
            "accept": "application/json",
            "APCA-API-KEY-ID": api_key,
            "APCA-API-SECRET-KEY": api_secret
        }
        self.prices_fetched = 0

    def fetch_latest_bars(self):
        url = f"https://data.alpaca.markets/v2/stocks/bars/latest?symbols={self.symbol}&feed=iex"
        response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            data = response.json()
            return data['bars'][self.symbol]
        else:
            raise AlpacaDataFetcherException(f"Failed to fetch data: {response.status_code}")

class DualEWMASignal:
    # class to generate trading signals based on dual EWMA crossover strategy
    def __init__(self, short_ewma_span, long_ewma_span):
        """
        Initialize dual EWMA crossover strategy.
        Params:
            alpha_fast: Smoothing factor for fast EWMA.
            alpha_slow: Smoothing factor for slow EWMA.
        """
        self.fast_ewma = None
        self.slow_ewma = None
        self.alpha_fast = 2 / (long_ewma_span + 1)
        self.alpha_slow = 2 / (short_ewma_span + 1)

    def update(self, price):
        """
        Update fast and slow EWMA and generate trading signals.
        Params:
            price: New price data point.
        Returns: 
            Trading signal (-1 = Sell, 0 = Hold, 1 = Buy).
        """
        # fast EWMA
        if self.fast_ewma is None:
            self.fast_ewma = price
        else:
            self.fast_ewma = (self.alpha_fast * price) + ((1 - self.alpha_fast) * self.fast_ewma)

        # slow ewma
        if self.slow_ewma is None:
            self.slow_ewma = price
        else:
            self.slow_ewma = (self.alpha_slow * price) + ((1 - self.alpha_slow) * self.slow_ewma)

        # Generate trading signals
        if self.fast_ewma > self.slow_ewma:
            return 1 # buy
        elif self.fast_ewma < self.slow_ewma:
            return -1 # sell
        else:
            return 0  # hold

class AlpacaTrader:
    def __init__(self, api, initial_account_value, initial_buying_power):
        self.api = api
        self.account_value = initial_account_value
        self.buying_power = initial_buying_power
        self.cash = initial_account_value
        self.positions = 0

    def get_order_price(self, order):
        """
        Get the average filled price of an order.
        Params:
            order: Order object.
        Returns: 
            Average filled price.
        """
        order_status = self.api.get_order(order.id)
        if order_status.status == 'filled':
            return float(order_status.filled_avg_price)

    def place_buy_order(self, symbol, qty):
        """
        Place a buy (market) order for the specified symbol and quantity.
        Params:
            symbol: Stock symbol to buy.
            qty: Quantity of stock to buy.
        """
        try:
            order = self.api.submit_order(
                symbol=symbol,
                qty=qty,
                side='buy',
                type='market',
                time_in_force='gtc'
            )
        except:
            raise PlaceOrderException("Failed to place buy order.")
        else:
            price = self.get_order_price(order)
            cost = qty * price
            self.cash -= cost
            self.positions += qty
            self. account_value = self.cash + (self.positions * price)
            self.buying_power -=  cost

    def place_sell_order(self, symbol, qty):
        """
        Place a sell (market) order for the specified symbol and quantity.
        Params:
            symbol: Stock symbol to sell.
            qty: Quantity of stock to sell.
        """
        try:
            order = self.api.submit_order(
                symbol=symbol,
                qty=qty,
                side='sell',
                type='market',
                time_in_force='gtc'
            )
        except:
            raise PlaceOrderException("Failed to place sell order.")
        else:
            price = self.get_order_price(order)
            self.cash += qty * price
            self.positions -= qty
            self.account_value = self.cash + (self.positions * price)
            self.buying_power += qty * price

    def eod_liquidate(self, symbol):
        """
        Liquidate all positions at the end of the day.
        """
        if self.positions > 0:
            self.place_sell_order(symbol, self.positions)


if __name__ == '__main__':
    API_KEY = 'PKAS5FD8FYDEEC3PSTN0'
    API_SECRET = 'xG9nPePOlf52cg4sfQRbYwq25ZW4nduBMypp8Y3V'
    BASE_URL = 'https://paper-api.alpaca.markets'
    API = tradeapi.REST(API_KEY, API_SECRET, base_url=BASE_URL, api_version='v2')

    SYMBOL = 'TSLA'
    SHORT_EWMA_SPAN = 5
    LONG_EWMA_SPAN = 10

    fetcher = AlpacaDataFetcher(API, API_KEY, API_SECRET, SYMBOL)
    signal_generator = DualEWMASignal(short_ewma_span=SHORT_EWMA_SPAN, long_ewma_span=LONG_EWMA_SPAN)
    trader = AlpacaTrader(API, initial_account_value=100_000, initial_buying_power=200_000)

    while True:
        try:
            # Get Current time
            current_time = time.localtime()
            # Liquidate all positions at the end of the day
            if (current_time.tm_hour == 14) and (current_time.tm_min >= 55):
                trader.eod_liquidate(SYMBOL)
                break
            else:
                # Get the last close price
                latest_bars = fetcher.fetch_latest_bars()
                if latest_bars:
                    # generate signals/update ewma
                    fetcher.prices_fetched += 1
                    latest_close_price = latest_bars['c']
                    print(f"Latest close price: {latest_close_price}")
                    signal = signal_generator.update(latest_close_price)
                    print(signal_generator.fast_ewma , signal_generator.slow_ewma)
                    # Wait for enough data to calculate the EWMA and generate signals
                    if (fetcher.prices_fetched > LONG_EWMA_SPAN):
                        # Execute trades based on signals
                        if signal == 1:
                            # Place Buy order
                            quantity = (0.2*trader.buying_power) // latest_close_price
                            trader.place_buy_order(SYMBOL, quantity)
                            print("Buy order placed at {}".format(latest_close_price))
                        if signal == -1:
                            # Place Sell order
                            quantity = math.floor(0.2*trader.positions)
                            if quantity > 0:
                                trader.place_sell_order(SYMBOL, quantity)
                                print("Sell order placed at {}".format(latest_close_price))

            # wait for next minbar
            time.sleep(62)

        except (AlpacaDataFetcherException, PlaceOrderException) as e:
            print(e)

        except Exception:
            print("An unexpected error occurred.")
            break
