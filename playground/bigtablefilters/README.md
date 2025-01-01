1. Row Filters:

a. `RowKeyRegexFilter`:
- What it does: Filters rows based on their row key using a regular expression pattern
- Example use: Finding all rows where the key contains a specific customer ID pattern

b. `RowSampleFilter`:
- What it does: Randomly samples a percentage of rows
- Example use: Getting a statistical sample of your data, like sampling 10% of transactions for analysis

c. `PassAllFilter`:
- What it does: Lets all rows pass through (essentially no filtering)
- Example use: When you want to explicitly show you're not filtering rows

d. `BlockAllFilter`:
- What it does: Blocks all rows from passing through
- Example use: Testing or when you temporarily want to block all data access

2. Column Family Filters:

a. `FamilyNameRegexFilter`:
- What it does: Filters column families based on their names using regex
- Example use: Selecting only specific types of data columns, like only 'payment_info' column family

3. Column Qualifier Filters:

a. `ColumnQualifierRegexFilter`:
- What it does: Filters columns based on their names using regex
- Example use: Getting only columns that match certain naming patterns, like 'amount_*'

b. `ColumnRangeFilter`:
- What it does: Selects columns within a specified range of names
- Example use: Getting columns between 'timestamp_start' and 'timestamp_end'

4. Value Filters:

a. `ValueRegexFilter`:
- What it does: Filters cells based on their value using regex
- Example use: Finding cells containing specific text patterns

b. `ValueRangeFilter`:
- What it does: Selects cells with values in a specified range
- Example use: Finding transactions with amounts between $100 and $1000

5. Timestamp Filters:

a. `TimestampRangeFilter`:
- What it does: Filters cells based on their timestamp range
- Example use: Getting data modifications made between specific dates

b. `TimestampRangeFilter.add_range`:
- What it does: Adds a timestamp range to filter
- Example use: Specifying multiple time periods to include

6. Composite Filters:

a. `RowFilterChain`:
- What it does: Combines multiple filters with AND logic
- Example use: Finding rows that match multiple conditions simultaneously (e.g., specific customer AND specific date range)

b. `RowFilterUnion`:
- What it does: Combines multiple filters with OR logic
- Example use: Finding rows that match any of several conditions (e.g., either high-value transactions OR flagged transactions)

c. `ConditionalRowFilter`:
- What it does: Applies different filters based on whether a condition is met
- Example use: If a transaction is marked as 'high-value', apply additional filtering rules

7. Transformation Filters:

a. `CellsPerRowOffsetFilter`:
- What it does: Skips a specified number of cells per row
- Example use: Pagination or skipping older versions of data

b. `CellsPerRowLimitFilter`:
- What it does: Limits the number of cells returned per row
- Example use: Getting only the most recent N versions of each cell

c. `StripValueTransformer`:
- What it does: Removes the value from matched cells
- Example use: When you only need to check for existence of cells without reading their values

d. `ApplyLabelFilter`:
- What it does: Adds labels to matched cells
- Example use: Marking specific cells for later processing or identification

These filters can be combined to create complex filtering logic. For example, you might want to:
- Find high-value transactions (ValueRangeFilter)
- From specific customers (RowKeyRegexFilter)
- In the last month (TimestampRangeFilter)
- And only get the latest version of each cell (CellsPerRowLimitFilter)