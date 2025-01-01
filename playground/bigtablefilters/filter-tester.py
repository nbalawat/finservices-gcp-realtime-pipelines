async def test_filters_on_table(table_name: str, table) -> Dict[str, Any]:
    """Test all filters on a specific table"""
    explorer = BigtableFilterExplorer(table)
    
    # Run all filter tests
    results = {
        "table_name": table_name,
        "row_key_regex_results": await explorer.test_row_key_regex_filter(),
        "value_range_results": await explorer.test_value_range_filter(),
        "column_filter_results": await explorer.test_column_filters(),
        "composite_filter_results": await explorer.test_composite_filters(),
        "timestamp_filter_results": await explorer.test_timestamp_filters(),
        "transformation_filter_results": await explorer.test_transformation_filters()
    }
    
    return results

async def run_filter_tests():
    """Run tests on all tables"""
    # Initialize BigTable client and tables
    instance = await create_bigtable_client()
    tables = {
        'payments_by_customer': instance.table('payments_by_customer'),
        'payments_by_date': instance.table('payments_by_date'),
        'payments_by_transaction': instance.table('payments_by_transaction'),
        'payments_by_id': instance.table('payments_by_id'),
        'payments_by_id_date': instance.table('payments_by_id_date')
    }
    
    all_results = {}
    for table_name, table in tables.items():
        print(f"\nTesting filters on table: {table_name}")
        print("=" * 80)
        
        results = await test_filters_on_table(table_name, table)
        all_results[table_name] = results
        
        # Print summary for this table
        print(f"\nFilter Test Results for {table_name}:")
        for filter_type, filter_results in results.items():
            if filter_type != "table_name":
                print(f"\n{filter_type}:")
                for test in filter_results:
                    print(f"  - {test['test_case']}:")
                    if "error" in test:
                        print(f"    Error: {test['error']}")
                    else:
                        print(f"    Matched rows: {test['matched_rows']}")
                        if test['matched_rows'] > 0:
                            print(f"    Sample row keys: {[row['row_key'] for row in test['sample_rows']]}")
        
        print("\n" + "=" * 80)
    
    return all_results

def analyze_filter_effectiveness(all_results: Dict[str, Any]) -> None:
    """Analyze and print filter effectiveness across tables"""
    print("\nFilter Effectiveness Analysis")
    print("=" * 80)
    
    for table_name, results in all_results.items():
        print(f"\n{table_name}:")
        
        # Analyze each filter type
        for filter_type, filter_results in results.items():
            if filter_type != "table_name":
                successful_tests = sum(1 for test in filter_results if "error" not in test)
                total_tests = len(filter_results)
                
                if successful_tests > 0:
                    avg_matches = sum(
                        test.get("matched_rows", 0) 
                        for test in filter_results 
                        if "matched_rows" in test
                    ) / successful_tests
                else:
                    avg_matches = 0
                
                print(f"\n  {filter_type}:")
                print(f"    - Success rate: {successful_tests}/{total_tests} ({(successful_tests/total_tests)*100:.1f}%)
                print(f"    - Average matches per test: {avg_matches:.1f}")
                print(f"    - Most effective test: {max((test['matched_rows'] for test in filter_results if 'matched_rows' in test), default=0)} matches")

if __name__ == "__main__":
    # Run all filter tests
    all_results = asyncio.run(run_filter_tests())
    
    # Analyze results
    analyze_filter_effectiveness(all_results)