import json
import unittest

from generate_day_trading_ratio import asset_type, market_stats


class FormulaTests(unittest.TestCase):
    def test_market_stats_uses_average_of_buy_and_sell(self):
        result = market_stats(["1,000", "10", "400", "40", "600", "60"])
        self.assertEqual(result["daytrade_value"], 500)
        self.assertEqual(result["turnover"], 1000)
        self.assertEqual(result["ratio"], 50)

    def test_asset_classification(self):
        self.assertEqual(asset_type("2330", "台積電"), "stock")
        self.assertEqual(asset_type("0050", "元大台灣50"), "etf")
        self.assertEqual(asset_type("9103", "美德醫療-DR"), "other")


class DatasetTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with open("day_trading_ratio_latest.json", encoding="utf-8") as handle:
            cls.data = json.load(handle)

    def test_combined_market_is_amount_weighted(self):
        markets = self.data["markets"]
        expected = (
            markets["twse"]["daytrade_value"] + markets["tpex"]["daytrade_value"]
        ) / (markets["twse"]["turnover"] + markets["tpex"]["turnover"]) * 100
        self.assertAlmostEqual(markets["combined"]["ratio"], expected, places=2)

    def test_stock_ratios_match_amounts(self):
        for stock in self.data["stocks"]:
            expected = stock["daytrade_value"] / stock["turnover"] * 100
            self.assertAlmostEqual(stock["ratio"], expected, places=2, msg=stock["code"])

    def test_latest_series_matches_cards(self):
        latest = self.data["series"][-1]
        self.assertEqual(latest["date"], self.data["as_of"])
        self.assertEqual(latest["combined_ratio"], self.data["markets"]["combined"]["ratio"])


if __name__ == "__main__":
    unittest.main()
