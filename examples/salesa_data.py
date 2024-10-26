import polars as pl

import probe

data = pl.scan_csv("../data/sales_data.csv")
output = probe.ask(data, "How many products are being sold?")

# Will print something like this:
""""
    Here are the top 5 sales representatives ranked by their total sales:
        1. Karen Le leads with $255,723.09 in sales
        2. Emma Kumar with $181,779.21
        3. James Roy with $147,749.73
        4. Omar Cohen with $145,889.03
        5. Ming Nielsen with $142,345.37
    Karen Le significantly outperforms her colleagues, generating nearly $74,000 more in sales than the second-best performer, Emma
    Kumar.
"""

# You can access the raw python code here:
output.code
# <LazyFrame at 0x7F33E830BF20>

# Raw output from the LLM:
output.code_str
"""
df.with_columns(
    pl.col("final_amount").sum().over("salesperson_name").alias("total_sales")
).select([
    "salesperson_name",
    "total_sales"
]).unique().sort(
    "total_sales", descending=True
).limit(5)
"""
