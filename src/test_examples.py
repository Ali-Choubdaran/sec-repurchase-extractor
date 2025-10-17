"""
Test examples for SEC Repurchase Data Extractor
These examples ensure functionality is maintained during refactoring
"""

TEST_FILING_URLS = [
    "https://www.sec.gov/ix?doc=/Archives/edgar/data/1393612/000139361224000032/dfs-20240331.htm"

]

def test_all_examples():
    """Test all examples to ensure functionality is maintained"""
    import warnings
    import pandas as pd
    
    # Suppress warnings for cleaner output
    warnings.filterwarnings('ignore', category=FutureWarning)
    warnings.filterwarnings('ignore', category=pd.errors.SettingWithCopyWarning)
    warnings.filterwarnings('ignore', category=UserWarning)
    
    from main import RepurchaseExtractor

    for i, url in enumerate(TEST_FILING_URLS, 1):
        print(f"\n{'='*80}")
        print(f"TEST {i}: {url}")
        print(f"{'='*80}")
        
        try:
            extractor = RepurchaseExtractor(url)
            extractor.extract()  # No return value - updates instance variables
            
            # Access instance variables directly
            extraction_metadata = extractor.extraction_metadata
            repurchase_data = extractor.repurchase_data
            
            # Print extraction_metadata in a readable format
            print(f"\n📊 EXTRACTION METADATA:")
            print(f"{'─'*50}")
            for key, value in extraction_metadata.items():
                print(f"  {key}: {value}")
            
            # Print the extracted dataframe in a nice format
            print(f"\n📋 EXTRACTED DATA ({len(repurchase_data)} rows):")
            print(f"{'─'*50}")
            
            # Configure pandas display for better readability
            pd.set_option('display.max_columns', None)
            pd.set_option('display.width', None)
            pd.set_option('display.max_colwidth', 30)
            
            if not repurchase_data.empty:
                print(repurchase_data.to_string(index=True))
            else:
                print("  No data extracted")
            
            # Save repurchase_data to pickle file for detailed examination
            import pickle
            pickle_filename = f"repurchase_data_test_{i}.pkl"
            with open(pickle_filename, 'wb') as f:
                pickle.dump(repurchase_data, f)
            print(f"\n💾 SAVED: {pickle_filename} for detailed examination")
                
            print(f"\n✅ SUCCESS: {len(repurchase_data)} rows extracted")
            
        except Exception as e:
            print(f"\n❌ ERROR: {e}")
            import traceback
            traceback.print_exc()
        
        print(f"\n{'='*80}")

if __name__ == "__main__":
    test_all_examples()
