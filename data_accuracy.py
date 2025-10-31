
from pyspark.sql.functions import col

def validate_total_price():
    """
    This cell creates a sample DataFrame of order records and validates that total_price equals quantity multiplied by unit_price for each row.
    It reports any failures by printing the mismatched records.
    """
    # Sample order data
    data = [
        ("O001", 2, 50, 100),    # Valid
        ("O002", 3, 30, 70),     # Invalid: 3*30=90, not 80
        ("O003", 1, 200, 200)    # Valid
    ]
    columns = ["order_id", "quantity", "unit_price", "total_price"]
    df = spark.createDataFrame(data, columns)

    # Find rows where total_price does not match quantity * unit_price
    invalid_rows = df.filter(
        col("total_price") != col("quantity") * col("unit_price")
    )

    if invalid_rows.count() == 0:
        print("All order records have accurate total_price values.")
    else:
        print("Order records with inaccurate total_price values:")
        display(invalid_rows)

validate_total_price()
