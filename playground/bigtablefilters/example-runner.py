async def run_practical_examples():
    """Run and analyze practical filter examples"""
    
    # Initialize BigTable client and tables
    instance = await create_bigtable_client()
    tables = {
        'payments_by_customer': instance.table('payments_by_customer'),
        'payments_by_date': instance.table('payments_by_date'),
        'payments_by_transaction': instance.table('payments_by_transaction'),
        'payments_by_id': instance.table('payments_by_id'),
        'payments_by_id_date': instance.table('payments_by_id_date')
    }
    
    examples = PracticalFilterExamples()
    
    # Run examples for each table
    results = {
        'payments_by_customer': await examples.payments_by_customer_examples(tables['payments_by_customer']),
        'payments_by_date': await examples.payments_by_date_examples(tables['payments_by_date']),
        'payments_by_transaction': await examples.payments_by_transaction_examples(tables['payments_by_transaction']),
        'payments_by_id': await examples.payments_by_id_examples(tables['payments_by_id'])
    }
    
    # Print results
    print("\nPractical Filter Examples Results")
    print("=" * 80)
    
    for table_name, table_results in results.items():
        print(f"\n{table_name}:")
        print("-" * 40)
        
        for example_name, example_result in table_results.items():
            print(f"\n{example_name}:")
            print(f"Description: {example_result['description']}")
            print(f"Matched rows: {example_result['matched_rows']}")
            
            if example_result['matched_rows'] > 0:
                print("Sample data:")
                for i, row in enumerate(example_result['sample_data'][:3], 1):
                    print(f"  Row {i}: {row['row_key']}")
                    for col_family, columns in row['data'].items():
                        for col, val in columns.items():
                            print(f"    {col}: {val}")
    
    return results

async def analyze_filter_performance():
    """Analyze and compare filter performance across tables"""
    
    # Run practical examples
    results = await run_practical_examples()
    
    # Analyze results
    print("\nFilter Performance Analysis")
    print("=" * 80)
    
    for table_name, table_results in results.items():
        print(f"\n{table_name}:")
        print("-" * 40)
        
        total_queries = len(table_results)
        successful_queries = sum(1 for r in table_results.values() if r['matched_rows'] > 0)
        avg_matches = sum(r['matched_rows'] for r in table_results.values()) / total_queries
        
        print(f"Total queries: {total_queries}")
        print(f"Successful queries: {successful_queries}")
        print(f"Success rate: {(successful_queries/total_queries)*100:.1f}%")
        print(f"Average matches per query: {avg_matches:.1f}")
        
        # Most effective queries
        most_effective = max(
            table_results.items(),
            key=lambda x: x[1]['matched_rows']
        )
        print(f"Most effective query: {most_effective[0]} ({most_effective[1]['matched_rows']} matches)")

if __name__ == "__main__":
    # Run examples and analysis
    asyncio.run(analyze_filter_performance())