from collections import OrderedDict

import polars as pl
import pytest
from inline_snapshot import snapshot

import probe


def to_dict(x) -> dict:
    return {
        k: sorted(v) for k, v in OrderedDict(x.to_dict(as_series=False)).items()
    }


@pytest.fixture
def sample_df():
    return pl.read_csv(
        "data/sales_data.csv",
    ).lazy()


def test_highest_revenue_date(sample_df):
    query = "What date had the highest total sales revenue?"
    df = sample_df
    result = probe.ask(df, query, max_retries=0)
    assert to_dict(result.result) == snapshot({"date": ["2023-09-28T03:00:00.000000"], "total_sales": [18464.4]})


def test_top_salespeople(sample_df):
    query = "Who are the top 3 salespeople by total revenue?"
    df = sample_df
    result = probe.ask(df, query, max_retries=0)
    assert to_dict(result.result) == snapshot({
    "salesperson_name": ["Emma Kumar", "James Roy", "Karen Le"],
    "total_revenue": [147749.73, 181779.21, 255723.09],
})


def test_best_performing_product(sample_df):
    query = "What is the best performing product by revenue and what is its total revenue?"
    df = sample_df
    result = probe.ask(df, query, max_retries=0)
    assert to_dict(result.result) == snapshot({"product": ["Mouse"], "total_revenue": [618559.63]})


def test_average_order_value(sample_df):
    query = "What is the average order value by customer segment?"
    df = sample_df
    result = probe.ask(df, query, max_retries=0)
    assert to_dict(result.result) == snapshot({
    "customer_segment": ["Consumer", "Enterprise", "Government", "Small Business"],
    "avg_order_value": [
        4557.576499999999,
        4832.8763445378145,
        5067.34864978903,
        5162.970075471699,
    ],
})


def test_return_rate_by_product(sample_df):
    query = "What is the return rate for each product?"
    df = sample_df
    result = probe.ask(df, query, max_retries=0)
    assert to_dict(result.result) == snapshot({
    "product": [
        "Desktop",
        "Headphones",
        "Keyboard",
        "Laptop",
        "Monitor",
        "Mouse",
        "Printer",
        "Smartphone",
        "Speaker",
        "Tablet",
    ],
    "return_rate": [
        3.9215686274509802,
        4.123711340206185,
        4.166666666666666,
        4.716981132075472,
        5.154639175257731,
        5.813953488372093,
        6.122448979591836,
        6.363636363636363,
        7.476635514018691,
        10.891089108910892,
    ],
})


def test_country_revenue_share(sample_df):
    query = "What is the revenue share percentage by country?"
    df = sample_df
    result = probe.ask(df, query, max_retries=0)
    assert to_dict(result.result) == snapshot()


def test_top_selling_products_by_region(sample_df):
    query = "What is the top selling product in each region by quantity?"
    df = sample_df
    result = probe.ask(df, query, max_retries=0)
    assert to_dict(result.result) == snapshot({
    "region": ["Central", "East", "North", "South", "West"],
    "product": ["Headphones", "Mouse", "Smartphone", "Speaker", "Tablet"],
    "total_quantity": [123, 130, 156, 174, 188],
})


def test_sales_channel_performance(sample_df):
    query = "What is the performance of each sales channel by revenue and order count?"
    df = sample_df
    result = probe.ask(df, query, max_retries=0)
    assert to_dict(result.result) == snapshot({
    "sales_channel": ["Direct Sales", "Distributor", "Online", "Retail Store"],
    "total_revenue": [
        1169673.5199999998,
        1205687.4999999993,
        1215539.8499999996,
        1292417.2900000005,
    ],
    "total_orders": [243, 243, 250, 264],
})


def test_monthly_sales_growth(sample_df):
    query = "What is the month-over-month sales growth rate?"
    df = sample_df
    result = probe.ask(df, query, max_retries=0)
    assert to_dict(result.result) == snapshot()


def test_customer_segment_profitability(sample_df):
    query = "What is the average profit margin by customer segment?"
    df = sample_df
    result = probe.ask(df, query, max_retries=0)
    assert to_dict(result.result) == snapshot({
    "customer_segment": ["Consumer", "Enterprise", "Government", "Small Business"],
    "avg_profit_margin": [
        511.69019230769237,
        519.6541509433963,
        548.4919409282701,
        622.3910504201681,
    ],
})


def test_shipping_method_distribution(sample_df):
    query = "What is the distribution of shipping methods by order count?"
    df = sample_df
    result = probe.ask(df, query, max_retries=0)
    assert to_dict(result.result) == snapshot({
    "shipping_method": ["Express", "Next Day", "Pickup", "Standard"],
    "order_count": [223, 240, 252, 285],
})


def test_high_value_customers(sample_df):
    query = "Who are the top 5 customers by total purchase value?"
    df = sample_df
    result = probe.ask(df, query, max_retries=0)
    assert to_dict(result.result) == snapshot({
    "customer_id": [
        "CUST-00035",
        "CUST-00222",
        "CUST-00235",
        "CUST-00373",
        "CUST-00398",
    ],
    "total_purchase_value": [
        41246.86,
        41482.02,
        43385.71000000001,
        44549.979999999996,
        44982.68,
    ],
})


def test_product_category_performance(sample_df):
    query = "What is the performance of each product category by revenue and profit margin?"
    df = sample_df
    result = probe.ask(df, query, max_retries=0)
    assert to_dict(result.result) == snapshot({
    "category": ["Accessories", "Electronics", "Peripherals"], "total_revenue": [1558633.1899999997, 1649468.5, 1675216.4699999997], "avg_profit_margin": [-11.96010461986105, -11.959526352224149, -11.834545333263625]})


def test_payment_method_usage(sample_df):
    query = (
        "What is the usage distribution of payment methods by value and count?"
    )
    df = sample_df
    result = probe.ask(df, query, max_retries=0)
    assert to_dict(result.result) == snapshot({
    "payment_method": ["Bank Transfer", "Cash", "Credit Card", "Debit Card", "PayPal"],
    "total_value": [
        0.18001058730934705,
        0.192536680837523,
        0.19407897846246414,
        0.19821854900398297,
        0.23515520438668286,
    ],
    "total_count": [0.185, 0.191, 0.195, 0.197, 0.232],
})
