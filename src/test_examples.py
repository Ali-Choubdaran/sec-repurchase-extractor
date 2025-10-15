"""
Test examples for SEC Repurchase Data Extractor
These examples ensure functionality is maintained during refactoring
"""

TEST_FILING_URLS = [
    "https://www.sec.gov/Archives/edgar/data/789019/000156459022035087/msft-10q_20220930.htm",
    "https://www.sec.gov/Archives/edgar/data/320193/000032019322000070/aapl-20220625.htm"
]

def test_all_examples():
    """Test all examples to ensure functionality is maintained"""
    from main import RepurchaseExtractor
    import pandas as pd

    for i, url in enumerate(TEST_FILING_URLS, 1):
        print(f"\n{'='*80}")
        print(f"TEST {i}: {url}")
        print(f"{'='*80}")
        
        try:
            result = RepurchaseExtractor(url)
            flow_dic, df_output = result
            
            # Print flow_dic (metadata) in a readable format
            print(f"\n📊 EXTRACTION METADATA:")
            print(f"{'─'*50}")
            for key, value in flow_dic.items():
                print(f"  {key}: {value}")
            
            # Print the extracted dataframe in a nice format
            print(f"\n📋 EXTRACTED DATA ({len(df_output)} rows):")
            print(f"{'─'*50}")
            
            # Configure pandas display for better readability
            pd.set_option('display.max_columns', None)
            pd.set_option('display.width', None)
            pd.set_option('display.max_colwidth', 30)
            
            if not df_output.empty:
                print(df_output.to_string(index=True))
            else:
                print("  No data extracted")
                
            print(f"\n✅ SUCCESS: {len(df_output)} rows extracted")
            
        except Exception as e:
            print(f"\n❌ ERROR: {e}")
            import traceback
            traceback.print_exc()
        
        print(f"\n{'='*80}")

if __name__ == "__main__":
    test_all_examples()
