"""Unit tests for the ETL pipeline transform layer."""
import sys, os
import pytest
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from scripts.transform import clean


@pytest.fixture
def sample_df():
    return pd.DataFrame({
        "order_id":     ["O1", "O2", "O3", "O1"],
        "product_id":   ["P1", "P2", "P3", "P1"],
        "customer_id":  ["C1", "C2", "C3", "C1"],
        "customer_name":["Alice", "Bob", "Carol", "Alice"],
        "region":       ["East", "West", "East", "East"],
        "product_name": ["Prod A", "Prod B", "Prod C", "Prod A"],
        "category":     ["Tech", "Office", "Tech", "Tech"],
        "sub_category": ["Phones", "Paper", "Laptops", "Phones"],
        "sales":        [100.0, None, 200.0, 100.0],
        "profit":       [20.0, 10.0, None, 20.0],
        "discount":     [0.1, 0.2, None, 0.1],
        "quantity":     [2, 3, 1, 2],
        "order_date":   ["2023-01-01", "2023-01-02", "2023-01-03", "2023-01-01"],
        "ship_date":    ["2023-01-05", "2023-01-06", "2023-01-07", "2023-01-05"],
    })


class TestClean:
    def test_drops_null_sales(self, sample_df):
        assert clean(sample_df)["sales"].isnull().sum() == 0

    def test_drops_null_profit(self, sample_df):
        assert clean(sample_df)["profit"].isnull().sum() == 0

    def test_fills_null_discount(self, sample_df):
        result = clean(sample_df)
        assert result["discount"].isnull().sum() == 0

    def test_removes_duplicates(self, sample_df):
        result = clean(sample_df)
        assert result.duplicated(subset=["order_id", "product_id"]).sum() == 0

    def test_derives_order_month(self, sample_df):
        result = clean(sample_df)
        assert "order_month" in result.columns and result["order_month"].notna().all()

    def test_derives_order_year(self, sample_df):
        result = clean(sample_df)
        assert "order_year" in result.columns and result["order_year"].notna().all()

    def test_derives_profit_margin(self, sample_df):
        result = clean(sample_df)
        assert "profit_margin" in result.columns
        row = result[result["order_id"] == "O1"].iloc[0]
        assert abs(row["profit_margin"] - round(row["profit"] / row["sales"], 4)) < 0.001

    def test_zero_sales_margin(self, sample_df):
        sample_df.loc[0, "sales"] = 0.0
        result = clean(sample_df)
        row = result[result["order_id"] == "O1"]
        if not row.empty:
            assert row.iloc[0]["profit_margin"] == 0.0
