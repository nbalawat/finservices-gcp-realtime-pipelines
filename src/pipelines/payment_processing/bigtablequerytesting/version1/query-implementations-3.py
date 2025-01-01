class AnalyticsQueries:
    """Implementations of analytics-focused queries for each table structure"""

    @staticmethod
    async def date_range_customer_activity():
        """Find all customers who have transacted over a given period"""
        
        # Implementation for payments_by_date (transaction_date#transaction_type#customerId)
        async def using_date_table(table, start_date: str, end_date: str) -> List[str]:
            start_key = start_date
            end_key = f"{end_date}\uff\uff"
            
            row_set = RowSet()
            row_set.add_row_range(start_key=start_key, end_key=end_key)
            
            customer_ids = set()
            async for row in table.read_rows(row_set=row_set):
                key_parts = row.row_key.decode('utf-8').split('#')
                if len(key_parts) > 2:
                    customer_ids.add(key_parts[2])
            return list(customer_ids)

        # Implementation for payments_by_customer (customerId#transaction_date#transaction_type)
        async def using_customer_table(table, start_date: str, end_date: str) -> List[str]:
            customer_ids = set()
            async for row in table.read_rows():
                key_parts = row.row_key.decode('utf-8').split('#')
                if len(key_parts) > 1:
                    if start_date <= key_parts[1] <= end_date:
                        customer_ids.add(key_parts[0])
            return list(customer_ids)

        # Implementation for payments_by_transaction (transaction_id#customerId#transaction_date)
        async def using_transaction_table(table, start_date: str, end_date: str) -> List[str]:
            customer_ids = set()
            async for row in table.read_rows():
                key_parts = row.row_key.decode('utf-8').split('#')
                if len(key_parts) > 2:
                    if start_date <= key_parts[2] <= end_date:
                        customer_ids.add(key_parts[1])
            return list(customer_ids)

        # Implementation for payments_by_id (transaction_id)
        async def using_id_table(table, start_date: str, end_date: str) -> List[str]:
            customer_ids = set()
            async for row in table.read_rows():
                cell_values = row.cells["cf1"]
                transaction_date = cell_values["transaction_date"][0].value.decode('utf-8')
                if start_date <= transaction_date <= end_date:
                    customer_ids.add(cell_values["customer_id"][0].value.decode('utf-8'))
            return list(customer_ids)

        # Implementation for payments_by_id_date (transaction_id#transaction_date)
        async def using_id_date_table(table, start_date: str, end_date: str) -> List[str]:
            start_key = f"#{start_date}"
            end_key = f"#{end_date}\uff\uff"
            
            customer_ids = set()
            async for row in table.read_rows():
                row_key = row.row_key.decode('utf-8')
                date_part = row_key.split('#')[1] if '#' in row_key else None
                if date_part and start_date <= date_part <= end_date:
                    cell_values = row.cells["cf1"]
                    customer_ids.add(cell_values["customer_id"][0].value.decode('utf-8'))
            return list(customer_ids)

        return {
            "payments_by_date": using_date_table,
            "payments_by_customer": using_customer_table,
            "payments_by_transaction": using_transaction_table,
            "payments_by_id": using_id_table,
            "payments_by_id_date": using_id_date_table
        }

    @staticmethod
    async def payment_type_analysis():
        """Total number of transactions by payment type over a period"""
        
        # Implementation for payments_by_date (transaction_date#transaction_type#customerId)
        async def using_date_table(table, start_date: str, end_date: str, 
                                 payment_type: str) -> Dict[str, int]:
            start_key = f"{start_date}#{payment_type}"
            end_key = f"{end_date}#{payment_type}\uff\uff"
            
            row_set = RowSet()
            row_set.add_row_range(start_key=start_key, end_key=end_key)
            
            count = 0
            async for row in table.read_rows(row_set=row_set):
                count += 1
            return {"transaction_count": count}

        # Implementation for payments_by_customer (customerId#transaction_date#transaction_type)
        async def using_customer_table(table, start_date: str, end_date: str, 
                                     payment_type: str) -> Dict[str, int]:
            count = 0
            async for row in table.read_rows():
                key_parts = row.row_key.decode('utf-8').split('#')
                if (len(key_parts) > 2 and 
                    start_date <= key_parts[1] <= end_date and 
                    key_parts[2] == payment_type):
                    count += 1
            return {"transaction_count": count}

        # Implementation for payments_by_transaction (transaction_id#customerId#transaction_date)
        async def using_transaction_table(table, start_date: str, end_date: str, 
                                        payment_type: str) -> Dict[str, int]:
            count = 0
            async for row in table.read_rows():
                key_parts = row.row_key.decode('utf-8').split('#')
                if len(key_parts) > 2:
                    transaction_date = key_parts[2]
                    if start_date <= transaction_date <= end_date:
                        cell_values = row.cells["cf1"]
                        if cell_values["payment_type"][0].value.decode('utf-8') == payment_type:
                            count += 1
            return {"transaction_count": count}

        # Implementation for payments_by_id (transaction_id)
        async def using_id_table(table, start_date: str, end_date: str, 
                                payment_type: str) -> Dict[str, int]:
            count = 0
            async for row in table.read_rows():
                cell_values = row.cells["cf1"]
                transaction_date = cell_values["transaction_date"][0].value.decode('utf-8')
                if (start_date <= transaction_date <= end_date and 
                    cell_values["payment_type"][0].value.decode('utf-8') == payment_type):
                    count += 1
            return {"transaction_count": count}

        # Implementation for payments_by_id_date (transaction_id#transaction_date)
        async def using_id_date_table(table, start_date: str, end_date: str, 
                                     payment_type: str) -> Dict[str, int]:
            start_key = f"#{start_date}"
            end_key = f"#{end_date}\uff\uff"
            
            count = 0
            async for row in table.read_rows():
                row_key = row.row_key.decode('utf-8')
                if '#' in row_key:
                    date_part = row_key.split('#')[1]
                    if start_date <= date_part <= end_date:
                        cell_values = row.cells["cf1"]
                        if cell_values["payment_type"][0].value.decode('utf-8') == payment_type:
                            count += 1
            return {"transaction_count": count}

        return {
            "payments_by_date": using_date_table,
            "payments_by_customer": using_customer_table,
            "payments_by_transaction": using_transaction_table,
            "payments_by_id": using_id_table,
            "payments_by_id_date": using_id_date_table
        }