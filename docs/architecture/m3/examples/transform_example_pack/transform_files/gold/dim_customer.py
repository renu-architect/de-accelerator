def transform(inputs, params, ctx):
    """inputs: {object_code: DataFrame}; return the Gold dataframe. Runs from a reviewed, checksum-verified file."""
    c = inputs["customers"].where("_is_current AND NOT _is_deleted")
    return c.selectExpr("xxhash64(customer_id) AS customer_key", "customer_id", "email", "country", "_valid_from AS valid_from")
