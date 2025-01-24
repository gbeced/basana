# Define the Config class to store the bot's settings
from enum import Enum

from basana import Pair


class GridTypes(Enum):
    SIMPLE = 1  # Simple grid


# Define the Config class to store the bot's settings
class Config:
    # Define the pair
    PAIR = Pair("BTC", "USDT")

    ##################################################
    # STRATEGY
    ##################################################
    # Bollinger Band
    BBAND_PERIOD = 30
    BBAND_STANDARD_DEVIATION = 2

    ##################################################
    # POSITION MANAGER
    ##################################################
    GRID_TYPE = GridTypes.SIMPLE

    # Define the number of grid lines to use for buying and selling
    GRID_SIZE = 5

    # Define the size of each grid step in Pips
    GRID_STEP_SIZE = 20000000

    HEDGING = True  # Take hedging orders when an order is filled
    MARTINGALE_SIZES = True  # Multiply position size by line number in grid (from 1 to GRID_SIZE)
    DEFAULT_POSITION_SIZE = 0.03

    ##################################################
    # BACKTEST
    ##################################################
    initial_balance = 10000
    loan_interest_percentage = 7
    loan_min_interest = 0.01
    loan_interest_period = 365
    loan_margin_requirement = 0.5
    base_symbol_precision = 8
    quote_symbol_precision = 2
    bars_csv_path = "binance_btcusdt_day.csv"
    bars_period = "1d"
