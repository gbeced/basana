# Basana
#
# Copyright 2022 Gabriel Martin Becedillas Ruiz
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from decimal import Decimal
import asyncio
import logging

from rich.text import Text
from textual import binding
from textual import containers
from textual import widgets
from textual.app import App

from basana.external.binance import exchange as binance_exchange
from binance_order_book_mirror import OrderBookUpdater
import basana as bs


class WriteToLogWidget(logging.Handler):
    """
    Custom logging handler that writes to a Log widget.
    """

    def __init__(self, log: widgets.Log):
        super().__init__()
        self._log = log

    def emit(self, record):
        msg = self.format(record)
        self._log.write_line(msg)
        # Force refresh because there is an update bug when using max_lines.
        self._log.refresh()


class OrderBookStatsApp(App):
    CSS = """
    #obook {
        width: auto;
        min-width: 40;
    }
    """

    BINDINGS = [
        binding.Binding(key="q", action="quit", description="Quit the app"),
    ]

    def __init__(self, dispatcher: bs.EventDispatcher, mirror: OrderBookUpdater):
        super().__init__()
        self._dispatcher = dispatcher
        self._dispatcher_task = None
        self._mirror = mirror
        self._top_n = 1000  # Must be <= 5000 for Binance.
        self._cum_depth_pct = Decimal("0.1")

    def compose(self):
        obook = widgets.DataTable(id="obook", show_cursor=False, zebra_stripes=True)
        basic_stats = widgets.DataTable(id="basic_stats", show_cursor=False, show_header=False)
        depth_and_volume_stats = widgets.DataTable(id="depth_and_volume_stats", show_cursor=False)
        imbalance_and_assimetry_stats = widgets.DataTable(
            id="imbalance_and_assimetry", show_cursor=False, show_header=False
        )
        logs = widgets.Log(id="logs", highlight=True, max_lines=100)

        stats = containers.Vertical(
            widgets.Collapsible(basic_stats, title="Basic", collapsed=False),
            widgets.Collapsible(depth_and_volume_stats, title="Depth and Volume", collapsed=False),
            widgets.Collapsible(imbalance_and_assimetry_stats, title="Imbalance and Asymmetry", collapsed=False),
            widgets.Collapsible(logs, title="Logs", collapsed=False),
        )

        yield containers.Horizontal(obook, stats)
        yield widgets.Footer()

    async def on_ready(self):
        # Create and configure the log handler that will forward logs to the widget.
        logs = self.query_one("#logs", expect_type=widgets.Log)
        handler = WriteToLogWidget(logs)
        handler.setFormatter(logging.Formatter("[%(asctime)s %(levelname)s] %(message)s"))
        # Add handler to root logger to capture all logs
        root_logger = logging.getLogger()
        root_logger.addHandler(handler)
        root_logger.setLevel(logging.INFO)

        # Init tables.
        table_columns = {
            "#obook": [
                (obook_text("Size", "bid"), "bid_size"),
                (obook_text("Bid", "bid"), "bid_price"),
                (obook_text("Ask", "ask"), "ask_price"),
                (obook_text("Size", "ask"), "ask_size"),
            ],
            "#basic_stats": ["description", "value"],
            "#depth_and_volume_stats": ["", "bid", "ask"],
            "#imbalance_and_assimetry": ["description", "value"],
        }
        for table_id, columns in table_columns.items():
            table = self.query_one(table_id, expect_type=widgets.DataTable)
            table.add_columns(*columns)

        # Update widgets periodically.
        self.set_interval(0.1, self._update_widgets)

        # Run dispatcher in background.
        if self._dispatcher_task is None:
            self._dispatcher_task = asyncio.create_task(self._dispatcher.run())

    def action_quit(self):
        self._dispatcher.stop()
        self.exit()

    def _generate_stats(self):
        stats = {}

        bids = self._mirror.order_book.bids
        asks = self._mirror.order_book.asks
        if bids and asks:
            spread = asks[0][0] - bids[0][0]
            mid_price = (asks[0][0] + bids[0][0]) / 2
            stats["spread"] = spread
            stats["mid_price"] = mid_price

            top_bids = self._mirror.order_book.bids[:self._top_n]
            top_asks = self._mirror.order_book.asks[:self._top_n]
            top_bids_depth = sum(bid[1] for bid in top_bids)
            top_asks_depth = sum(ask[1] for ask in top_asks)
            top_bids_vwap = sum(bid[0] * bid[1] for bid in top_bids) / top_bids_depth
            top_asks_vwap = sum(ask[0] * ask[1] for ask in top_asks) / top_asks_depth
            bids_cum_depth = cumulative_depth_at_distance(bids, mid_price, self._cum_depth_pct)
            asks_cum_depth = cumulative_depth_at_distance(asks, mid_price, self._cum_depth_pct)
            stats["top_bids_depth"] = top_bids_depth
            stats["top_asks_depth"] = top_asks_depth
            stats["top_bids_vwap"] = top_bids_vwap
            stats["top_asks_vwap"] = top_asks_vwap
            stats["bids_cum_depth"] = bids_cum_depth
            stats["asks_cum_depth"] = asks_cum_depth

            stats["imbalance"] = (top_bids_depth - top_asks_depth) / (top_bids_depth + top_asks_depth)

        return stats

    def _update_widgets(self):
        self._update_obook()

        stats = self._generate_stats()
        self._update_basic_stats(stats)
        self._update_depth_and_volume_stats(stats)
        self._update_imbalance_and_assimetry(stats)

    def _update_obook(self):
        table_max = 200  # Limit number of rows to display avoid performance issues.
        table = self.query_one("#obook", expect_type=widgets.DataTable)
        bids = self._mirror.order_book.bids[:table_max]
        asks = self._mirror.order_book.asks[:table_max]

        def build_row(row: int):
            bid_size = obook_text(str(bids[row][1]), "bid") if row < len(bids) else ""
            bid_price = obook_text(str(bids[row][0]), "bid") if row < len(bids) else ""
            ask_price = obook_text(str(asks[row][0]), "ask") if row < len(asks) else ""
            ask_size = obook_text(str(asks[row][1]), "ask") if row < len(asks) else ""
            return (bid_size, bid_price, ask_price, ask_size)

        current_rows = len(table.rows)
        final_rows = max(len(bids), len(asks))

        # Update existing rows but exclude the ones that will be removed.
        for i in range(min(current_rows, final_rows)):
            row = build_row(i)
            table.update_cell(str(i), "bid_size", row[0])
            table.update_cell(str(i), "bid_price", row[1])
            table.update_cell(str(i), "ask_price", row[2])
            table.update_cell(str(i), "ask_size", row[3])

        # Add or remove rows as necessary.
        if final_rows > current_rows:
            for i in range(final_rows - current_rows):
                row_i = current_rows + i
                table.add_row(*build_row(row_i), key=str(row_i))
        else:
            while (len(table.rows) > final_rows):
                table.remove_row(str(len(table.rows) - 1))

    def _update_basic_stats(self, stats: dict):
        rows = [
            ["Mid Price", "mid_price"],
            ["Bid-Ask Spread", "spread"],
            ["Lag", "lag"],
        ]
        values = {}
        values["spread"] = format_stat(stats, "spread", "{:.8f}")
        values["mid_price"] = format_stat(stats, "mid_price", "{:.8f}")
        values["lag"] = "N/A" if self._mirror.order_book.last_updated is None else \
            f"{(bs.utc_now() - self._mirror.order_book.last_updated).total_seconds():.3f} s"

        self._update_table("#basic_stats", rows, values)

    def _update_depth_and_volume_stats(self, stats: dict):
        rows = [
            [f"Top {self._top_n} Depth", "top_bids_depth", "top_asks_depth"],
            [f"Top {self._top_n} VWAP", "top_bids_vwap", "top_asks_vwap"],
            [f"Cumulative Depth at Distance ({self._cum_depth_pct} %)", "bids_cum_depth", "asks_cum_depth"],
        ]
        values = {}
        values["top_bids_depth"] = format_stat(stats, "top_bids_depth", "{:.8f}")
        values["top_asks_depth"] = format_stat(stats, "top_asks_depth", "{:.8f}")
        values["bids_cum_depth"] = format_stat(stats, "bids_cum_depth", "{:.8f}")
        values["asks_cum_depth"] = format_stat(stats, "asks_cum_depth", "{:.8f}")
        values["top_bids_vwap"] = format_stat(stats, "top_bids_vwap", "{:.8f}")
        values["top_asks_vwap"] = format_stat(stats, "top_asks_vwap", "{:.8f}")
        self._update_table("#depth_and_volume_stats", rows, values)

    def _update_imbalance_and_assimetry(self, stats: dict):
        rows = [
            [f"Top {self._top_n} Imbalance", "imbalance"],
        ]
        values = {}
        values["imbalance"] = format_stat(stats, "imbalance", "{:.8f}")
        self._update_table("#imbalance_and_assimetry", rows, values)

    def _update_table(self, table_id: str, rows: list, values: dict):
        table = self.query_one(table_id, expect_type=widgets.DataTable)
        table.clear()
        for row in rows:
            desc, *keys = row
            table.add_row(desc, *[values[key] for key in keys])


def obook_text(value: str, side: str) -> Text:
    return Text(value, style="green" if side == "bid" else "red")


def cumulative_depth_at_distance(levels: list, mid: Decimal, threshold_pct: Decimal):
    """
    Calculate the cumulative volume within a price distance from the mid price.
    """
    threshold = mid * Decimal(threshold_pct) / 100
    cum_vol = Decimal(0)
    for p, v in levels:
        if abs(p - mid) > threshold:
            break
        cum_vol += v
    return cum_vol


def format_stat(stats: dict, key: str, fmt: str) -> str:
    stat = stats.get(key)
    return fmt.format(stat) if stat is not None else "N/A"


async def main():
    event_dispatcher = bs.realtime_dispatcher()
    exchange = binance_exchange.Exchange(event_dispatcher)
    pair = bs.Pair("BTC", "USDT")
    mirror = OrderBookUpdater(pair, exchange)
    app = OrderBookStatsApp(event_dispatcher, mirror)

    await app.run_async()


if __name__ == "__main__":
    asyncio.run(main())
