import random
from datetime import datetime, timedelta

import numpy as np
import polars as pl

# Set random seed for reproducibility
np.random.seed(42)


def generate_name():
    first_names = [
        "James",
        "Mary",
        "John",
        "Patricia",
        "Robert",
        "Jennifer",
        "Michael",
        "Linda",
        "William",
        "Elizabeth",
        "David",
        "Barbara",
        "Richard",
        "Susan",
        "Joseph",
        "Jessica",
        "Thomas",
        "Sarah",
        "Charles",
        "Karen",
        "Wei",
        "Li",
        "Ming",
        "Yan",
        "Yuki",
        "Hiroshi",
        "Akiko",
        "Juan",
        "Maria",
        "Carlos",
        "Sofia",
        "Luis",
        "Anna",
        "Pavel",
        "Elena",
        "Ahmed",
        "Fatima",
        "Omar",
        "Isabella",
        "Emma",
        "Noah",
        "Olivia",
        "Liam",
        "Ava",
        "Ethan",
        "Mia",
        "Lucas",
        "Sophia",
        "Mason",
        "Charlotte",
    ]

    last_names = [
        "Smith",
        "Johnson",
        "Williams",
        "Brown",
        "Jones",
        "Garcia",
        "Miller",
        "Davis",
        "Rodriguez",
        "Martinez",
        "Chen",
        "Wang",
        "Li",
        "Zhang",
        "Liu",
        "Sato",
        "Suzuki",
        "Takahashi",
        "Kim",
        "Lee",
        "Patel",
        "Kumar",
        "Singh",
        "Sharma",
        "Nguyen",
        "Tran",
        "Le",
        "Andersen",
        "Nielsen",
        "Jensen",
        "Schmidt",
        "Mueller",
        "Fischer",
        "Weber",
        "Santos",
        "Silva",
        "Oliveira",
        "Kowalski",
        "Nowak",
        "Ivanov",
        "Popov",
        "Smirnov",
        "Ahmed",
        "Ali",
        "Mohamed",
        "Cohen",
        "Levy",
        "Martin",
        "Roy",
        "Tremblay",
    ]

    return f"{random.choice(first_names)} {random.choice(last_names)}"


# Country and state data
countries_states = {
    "United States": [
        "California",
        "Texas",
        "Florida",
        "New York",
        "Illinois",
        "Pennsylvania",
        "Ohio",
        "Georgia",
        "Michigan",
        "North Carolina",
    ],
    "Canada": [
        "Ontario",
        "Quebec",
        "British Columbia",
        "Alberta",
        "Manitoba",
        "Saskatchewan",
        "Nova Scotia",
        "New Brunswick",
    ],
    "United Kingdom": ["England", "Scotland", "Wales", "Northern Ireland"],
    "Australia": [
        "New South Wales",
        "Victoria",
        "Queensland",
        "Western Australia",
        "South Australia",
        "Tasmania",
    ],
    "Germany": [
        "Bavaria",
        "North Rhine-Westphalia",
        "Baden-Württemberg",
        "Lower Saxony",
        "Hesse",
        "Saxony",
    ],
}

# Generate 1000 rows of data
n_rows = 1000

# Generate dates
end_date = datetime(2024, 1, 1)
start_date = end_date - timedelta(days=365 * 4)
dates = random.sample(
    [
        start_date + timedelta(hours=i)
        for i in range(int((end_date - start_date).total_seconds() / 3600))
    ],
    n_rows,
)

# Define possible values for categorical fields
products = [
    "Laptop",
    "Smartphone",
    "Tablet",
    "Desktop",
    "Monitor",
    "Printer",
    "Keyboard",
    "Mouse",
    "Headphones",
    "Speaker",
]
categories = ["Electronics", "Accessories", "Peripherals"]
regions = ["North", "South", "East", "West", "Central"]
payment_methods = [
    "Credit Card",
    "Debit Card",
    "PayPal",
    "Bank Transfer",
    "Cash",
]
customer_segments = ["Consumer", "Small Business", "Enterprise", "Government"]
sales_channels = ["Online", "Retail Store", "Direct Sales", "Distributor"]
shipping_methods = ["Standard", "Express", "Next Day", "Pickup"]

# Generate unique salespeople and customers
n_salespeople = 50
n_customers = 500

salespeople_data = {
    "salesperson_id": [f"SP-{i:03d}" for i in range(1, n_salespeople + 1)],
    "salesperson_name": [generate_name() for _ in range(n_salespeople)],
}
salespeople_df = pl.DataFrame(salespeople_data)

customers_data = {
    "customer_id": [f"CUST-{i:05d}" for i in range(1, n_customers + 1)],
    "customer_name": [generate_name() for _ in range(n_customers)],
    "country": np.random.choice(list(countries_states.keys()), n_customers),
}
customers_df = pl.DataFrame(customers_data)

# Add states based on countries
customers_df = customers_df.with_columns(
    pl.Series(
        "state",
        [
            np.random.choice(countries_states[country])
            for country in customers_df["country"]
        ],
    )
)

# Generate main sales data
data = {
    "order_id": [f"ORD-{i:06d}" for i in range(1, n_rows + 1)],
    "date": dates,
    "product": np.random.choice(products, n_rows),
    "category": np.random.choice(categories, n_rows),
    "quantity": np.random.randint(1, 11, n_rows),
    "unit_price": np.random.uniform(50, 2000, n_rows).round(2),
    "discount_percentage": np.random.choice([0, 5, 10, 15, 20], n_rows),
    "region": np.random.choice(regions, n_rows),
    "payment_method": np.random.choice(payment_methods, n_rows),
    "customer_segment": np.random.choice(customer_segments, n_rows),
    "sales_channel": np.random.choice(sales_channels, n_rows),
    "shipping_method": np.random.choice(shipping_methods, n_rows),
    "customer_id": [
        f"CUST-{i:05d}" for i in np.random.randint(1, n_customers + 1, n_rows)
    ],
    "salesperson_id": [
        f"SP-{i:03d}" for i in np.random.randint(1, n_salespeople + 1, n_rows)
    ],
}

# Create main DataFrame
df = pl.DataFrame(data)

# Join with customer and salesperson data
df = df.join(customers_df, on="customer_id", how="left")
df = df.join(salespeople_df, on="salesperson_id", how="left")

# Calculate derived fields
df = df.with_columns(
    [
        (
            (
                pl.col("unit_price")
                * pl.col("quantity")
                * pl.col("discount_percentage")
                / 100
            )
            .round(2)
            .alias("discount_amount")
        ),
        (
            (pl.col("unit_price") * pl.col("quantity"))
            .round(2)
            .alias("subtotal")
        ),
    ]
)

df = df.with_columns(
    [
        (
            (pl.col("subtotal") - pl.col("discount_amount"))
            .round(2)
            .alias("total_amount")
        ),
        (
            pl.when(pl.col("shipping_method") == "Standard")
            .then(10)
            .when(pl.col("shipping_method") == "Express")
            .then(25)
            .when(pl.col("shipping_method") == "Next Day")
            .then(50)
            .otherwise(0)
            .alias("shipping_cost")
        ),
    ]
)

df = df.with_columns(
    [
        (
            (pl.col("total_amount") + pl.col("shipping_cost"))
            .round(2)
            .alias("final_amount")
        ),
        pl.Series(
            "returned", np.random.choice([True, False], n_rows, p=[0.05, 0.95])
        ),
    ]
)


# Sort by date
df = df.sort("date")

# Save to CSV
df.write_csv("sales_data.csv")

# Display first few rows and data info
print("\nFirst few rows:")
print(df.head())
print("\nDataset Info:")
print(df.schema)
