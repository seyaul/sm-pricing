# tests/test_upc_normalization.py
import sys
import pandas as pd
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent.parent  # Go up from tests/ to sm-pricing/
sys.path.insert(0, str(project_root))

print(f"📁 Project root: {project_root}")
print(f"📁 Current file: {Path(__file__)}")

# Now we can import from graph_ingestion package
try:
    # First, make sure graph_ingestion is a package
    graph_init = project_root / "graph_ingestion" / "__init__.py"
    if not graph_init.exists():
        print("⚠️  Creating missing __init__.py file...")
        graph_init.touch()
    
    from graph_ingestion.email_processor import VendorFileProcessor
    print("✅ Successfully imported VendorFileProcessor")
except ImportError as e:
    print(f"❌ Import error: {e}")
    print("\nDebugging info:")
    print(f"Python path: {sys.path}")
    
    graph_dir = project_root / "graph_ingestion"
    print(f"Graph ingestion directory exists: {graph_dir.exists()}")
    if graph_dir.exists():
        print("Files in graph_ingestion:")
        for f in graph_dir.glob("*"):
            print(f"  - {f.name}")
    sys.exit(1)

def test_upc_normalization():
    """Test UPC normalization with various formats"""
    
    processor = VendorFileProcessor()
    
    # Test cases for UPC normalization
    test_cases = [
        # Standard UPCs
        ("12345678901", "12345678901"),     # 11 digits - keep as is
        ("123456789012", "12345678901"),    # 12 digits - remove check digit
        ("0123456789012", "12345678901"),   # Leading zero + 12 digits
        ("00123456789012", "12345678901"),  # Multiple leading zeros
        
        # With hyphens
        ("123-456-78901", "12345678901"),   # Remove hyphens
        ("0-123-456-78901", "12345678901"), # Leading zero + hyphens
        
        # Edge cases
        ("", None),                         # Empty string
        (None, None),                       # None value
        ("   ", None),                      # Whitespace only
        ("ABC123", "ABC123"),               # Non-numeric
        ("123ABC456", "123ABC456"),         # Mixed alphanumeric
        
        # Real examples from your files
        ("827048021008", "827048021008"),   # HANA format
        ("23547300518", "23547300518"),     # KEHE format
        ("000-75925-30120", "759253012"),   # UNFI format with leading zeros
    ]
    
    print("\n🧪 Testing UPC Normalization")
    print("=" * 50)
    
    for input_upc, expected in test_cases:
        result = processor.normalize_upc(input_upc)
        status = "✅" if result == expected else "❌"
        print(f"{status} Input: {input_upc!r:20} → {result!r:15} (expected: {expected!r})")
    
    print("\n" + "=" * 50)
    return processor

def find_test_files():
    """Find test files in various possible locations"""
    test_files = [
        "STM 070125.xlsx",                                    # Should be HANA
        "STREETS DC27 August Pricing File 6.30.2025.xlsx",   # Should be KEHE  
        "OSA X STM - JULY LIST.xlsx",                         # Should be OSA (note the dash)
        "Dashboard 7.3.25.xlsx",                              # Should be RAINFOREST
        "Sold Qty & PL1.xlsx",                                # Should be ECRS (note the &)
        "J44PBM01-CSV02.HA1LFO20689620.CSV",                  # Should be UNFI (note the dash)
    ]
    
    # Possible locations for test files
    possible_dirs = [
        project_root / "tests" / "test_files",
        project_root / "test_files", 
        project_root,
        project_root / "graph_ingestion" / "tests" / "test_files",
        Path(__file__).parent / "test_files"
    ]
    
    print(f"\n📂 Searching for test files...")
    for test_dir in possible_dirs:
        if test_dir.exists():
            files_found = []
            for filename in test_files:
                if (test_dir / filename).exists():
                    files_found.append(filename)
            
            if files_found:
                print(f"✅ Found {len(files_found)} files in: {test_dir}")
                for f in files_found:
                    print(f"   - {f}")
                return test_dir, files_found
        else:
            print(f"❌ Directory doesn't exist: {test_dir}")
    
    print("❌ No test files found in any location")
    return None, []

def test_file_detection_local():
    """Test file detection with your local sample files"""
    
    processor = VendorFileProcessor()
    test_dir, found_files = find_test_files()
    
    if not test_dir or not found_files:
        print("\n❌ Cannot test file detection - no test files found")
        return
    
    print("\n🔍 Testing File Detection")
    print("=" * 50)
    
    for filename in found_files:
        file_path = test_dir / filename
        detected_type = processor.detect_file_type(file_path)
        print(f"📄 {filename:45} → {detected_type or 'UNKNOWN'}")

def test_data_extraction():
    """Test data extraction from each file type"""
    
    processor = VendorFileProcessor()
    test_dir, found_files = find_test_files()
    
    if not test_dir or not found_files:
        print("\n❌ Cannot test data extraction - no test files found")
        return {}
    
    # File mapping (filename → expected vendor type)
    # Updated to match your actual filenames
    file_mapping = {
        "STM 070125.xlsx": "HANA",
        "STREETS DC27 August Pricing File 6.30.2025.xlsx": "KEHE",
        "OSA X STM - JULY LIST.xlsx": "OSA",  # Note the dash
        "Dashboard 7.3.25.xlsx": "RAINFOREST",
        "Sold Qty & PL1.xlsx": "ECRS",  # Note the &
        "J44PBM01-CSV02.HA1LFO20689620.CSV": "UNFI",  # Note the dash
    }
    
    print("\n📊 Testing Data Extraction")
    print("=" * 50)
    
    extracted_data = {}
    
    for filename in found_files:
        if filename in file_mapping:
            vendor_type = file_mapping[filename]
            file_path = test_dir / filename
            
            try:
                data = processor.extract_vendor_data(file_path, vendor_type)
                if data is not None:
                    extracted_data[vendor_type] = data
                    print(f"✅ {vendor_type:12} → {len(data):5,} products extracted")
                    
                    # Show sample UPCs and their normalized versions
                    sample = data[['upc', 'upc_normalized']].head(3)
                    for _, row in sample.iterrows():
                        print(f"     UPC: {row['upc']} → {row['upc_normalized']}")
                else:
                    print(f"❌ {vendor_type:12} → EXTRACTION FAILED")
            except Exception as e:
                print(f"❌ {vendor_type:12} → ERROR: {str(e)[:50]}...")
    
    return extracted_data

def test_matching_logic(extracted_data):
    """Test the matching logic between ECRS and vendor files"""
    
    if not extracted_data or "ECRS" not in extracted_data:
        print("\n❌ Cannot test matching - ECRS data not available")
        return
        
    print("\n🔗 Testing UPC Matching Logic")
    print("=" * 50)
    
    # Get ECRS data as base
    ecrs_data = extracted_data["ECRS"]
    print(f"📊 ECRS base dataset: {len(ecrs_data):,} products")
    
    # Test matching with each vendor
    for vendor_name, vendor_data in extracted_data.items():
        if vendor_name == "ECRS":
            continue
            
        # Find matches
        ecrs_upcs = set(ecrs_data['upc_normalized'].dropna())
        vendor_upcs = set(vendor_data['upc_normalized'].dropna())
        
        matches = ecrs_upcs.intersection(vendor_upcs)
        match_rate = len(matches) / len(ecrs_upcs) * 100 if len(ecrs_upcs) > 0 else 0
        
        print(f"🎯 {vendor_name:12} → {len(matches):4,} matches ({match_rate:5.1f}% of ECRS)")
        
        # Show some example matches
        if matches:
            sample_matches = list(matches)[:3]
            for upc in sample_matches:
                ecrs_item = ecrs_data[ecrs_data['upc_normalized'] == upc].iloc[0]
                vendor_item = vendor_data[vendor_data['upc_normalized'] == upc].iloc[0]
                brand = ecrs_item.get('brand', 'N/A')
                item = ecrs_item.get('item', 'N/A')
                item_short = item[:30] if isinstance(item, str) else 'N/A'
                print(f"     Match: {upc} → {brand} {item_short}")

def main():
    """Run all tests"""
    print("🚀 Starting UPC Processing Tests")
    
    # Test 1: UPC Normalization
    processor = test_upc_normalization()
    
    # Test 2: File Detection  
    test_file_detection_local()
    
    # Test 3: Data Extraction
    extracted_data = test_data_extraction()
    
    # Test 4: Matching Logic
    test_matching_logic(extracted_data)
    
    print("\n🎉 All tests completed!")

if __name__ == "__main__":
    main()