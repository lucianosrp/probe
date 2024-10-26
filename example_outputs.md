```pycon

>>> df = pl.scan_csv("data/sales_data.csv")
>>> query = "What's the average time between repeat purchases for each customer segment?"
out = ask(df, query, print_query=True, print_code=True, print_output=True)

What's the average time between repeat purchases for each customer segment?
-------
Here's how long customers typically wait between purchases across different segments:

- Enterprise customers make repeat purchases most frequently, averaging 463 days between orders
- Small Business customers follow with about 473 days between purchases
- Government entities wait approximately 500 days between orders
- Consumer segment has the longest interval at 519 days between purchases

This suggests that Enterprise and Small Business customers are more regular buyers, while Consumers tend to make purchases less frequently.


-------

(
    df.with_columns(
        pl.col("date").str.strptime(pl.Datetime, format="%Y-%m-%dT%H:%M:%S.000000").alias("parsed_date")
    )
    .sort("parsed_date")
    .group_by(["customer_segment", "customer_id"])
    .agg([
        (
            pl.col("parsed_date").diff()
            .mean()
            .dt.total_days()
            .alias("avg_days_between_purchases")
        )
    ])
    .group_by("customer_segment")
    .agg([
        pl.col("avg_days_between_purchases")
        .mean()
        .round(2)
        .alias("segment_avg_days_between_purchases")
    ])
    .sort("segment_avg_days_between_purchases", descending=True)
)


-------

 shape: (4, 2)
┌──────────────────┬────────────────────────────────────┐
│ customer_segment ┆ segment_avg_days_between_purchases │
│ ---              ┆ ---                                │
│ str              ┆ f64                                │
╞══════════════════╪════════════════════════════════════╡
│ Consumer         ┆ 519.25                             │
│ Government       ┆ 499.83                             │
│ Small Business   ┆ 472.86                             │
│ Enterprise       ┆ 463.45                             │
└──────────────────┴────────────────────────────────────┘
```



```

What is the month-over-month sales growth rate?
-------
Looking at the month-over-month growth rates from 2020 to 2023, there have been significant fluctuations in sales performance. The highest growth rate was recorded in November 2022 with a 165.5% increase, while the lowest was in June 2022 with a -64.9% decline.

Some notable patterns include:
- March 2023 showed strong growth of 141.5%
- January 2021 had a substantial increase of 158.8%
- Most recent months in 2023 showed moderate growth rates, with November 2023 at 15.7%

The growth rates have been quite volatile throughout the period, suggesting significant seasonal variations and possibly other external factors affecting sales performance. While there are periods of strong positive growth, there are also considerable contractions, making it important to understand the underlying factors driving these fluctuations.


-------

df.with_columns(
    pl.col("date").str.strptime(pl.Datetime, format="%Y-%m-%dT%H:%M:%S.000000").dt.date().alias("parsed_date")
).group_by([
    pl.col("parsed_date").dt.year().alias("year"),
    pl.col("parsed_date").dt.month().alias("month")
]).agg(
    pl.col("final_amount").sum().alias("monthly_sales")
).with_columns([
    (
        (pl.col("monthly_sales") - pl.col("monthly_sales").shift(1)) /
        pl.col("monthly_sales").shift(1) * 100
    ).alias("mom_growth_rate")
]).select(["year", "month", "monthly_sales", "mom_growth_rate"])
```
