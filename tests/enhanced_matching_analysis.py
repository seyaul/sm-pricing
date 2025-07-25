# tests/upc_normalization_tester.py
import sys
import pandas as pd
import numpy as np
from pathlib import Path
from collections import defaultdict
import re
from typing import Set, Dict, List, Tuple, Optional
import json

# Add the project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Create __init__.py if missing
graph_init = project_root / "graph_ingestion" / "__init__.py"
if not graph_init.exists():
    graph_init.touch()

from graph_ingestion.email_processor import VendorFileProcessor

class UPCNormalizationTester:
    """Test both old and new UPC normalization methods side-by-side"""
    
    def __init__(self):
        # Initialize the original processor
        self.original_processor = VendorFileProcessor()
        
    def original_normalize_upc(self, upc) -> Optional[str]:
        """
        Your CURRENT normalization method (from the conservative approach)
        """
        if pd.isna(upc) or upc is None:
            return None
            
        # Convert to string and strip whitespace
        upc_str = str(upc).strip()
        
        if not upc_str:
            return None
        
        # Remove ALL decimal points and everything after them
        if '.' in upc_str:
            upc_str = upc_str.split('.')[0]
        
        # Remove hyphens and spaces
        upc_clean = re.sub(r'[-\s]', '', upc_str)
        
        # Handle non-numeric UPCs (return as-is for manual review)
        if not upc_clean.isdigit():
            return upc_clean
        
        # Remove leading zeros (padding removal)
        upc_clean = upc_clean.lstrip('0')
        
        # If all zeros, return None
        if not upc_clean:
            return None
            
        # Return as string - NO CHECK DIGIT REMOVAL
        return upc_clean

    def new_normalize_upc(self, upc) -> Optional[str]:
        """
        PROPOSED new normalization method with check digit removal and short UPC filtering
        """
        if pd.isna(upc) or upc is None:
            return None
            
        # Convert to string and strip whitespace
        upc_str = str(upc).strip()
        
        if not upc_str:
            return None
        
        # Remove ALL decimal points and everything after them
        if '.' in upc_str:
            upc_str = upc_str.split('.')[0]
        
        # Remove hyphens and spaces
        upc_clean = re.sub(r'[-\s]', '', upc_str)
        
        # Handle non-numeric UPCs
        if not upc_clean.isdigit():
            return upc_clean
        
        # Remove leading zeros (padding removal)
        upc_clean = upc_clean.lstrip('0')
        
        # If all zeros or empty, return None
        if not upc_clean:
            return None
        
        # NEW: FILTER OUT SHORT UPCs (≤5 digits = company-specific codes)
        if len(upc_clean) <= 5:
            return None
            
        # NEW: SMART CHECK DIGIT REMOVAL based on validated analysis
        if len(upc_clean) == 13:  # EAN-13 → remove check digit → 12 digits
            return upc_clean[:12]
        elif len(upc_clean) == 12:  # UPC-A → remove check digit → 11 digits  
            return upc_clean[:11]
        elif len(upc_clean) == 8:   # UPC-E → keep as-is
            return upc_clean
        else:  # 6-11 digits → keep as-is (valid UPC lengths)
            return upc_clean

    def extract_vendor_data_with_both_methods(self, file_path: Path, vendor_type: str) -> Dict[str, pd.DataFrame]:
        """
        Extract data using BOTH normalization methods for comparison
        """
        try:
            config = self.original_processor.vendor_columns[vendor_type]
            
            if vendor_type == "UNFI":
                df = pd.read_csv(file_path, header=config.get("header_row", 2), dtype=str)
            elif vendor_type == "RAINFOREST":
                df = pd.read_excel(
                    file_path, 
                    sheet_name=config["sheet"], 
                    header=config["header_row"],
                    dtype=str
                )
            else:
                df = pd.read_excel(file_path, dtype=str)
            
            # Extract relevant columns
            upc_col = config["upc_col"]
            cost_col = config.get("cost_col")
            
            if vendor_type == "ECRS":
                # ECRS is the base file, extract more columns
                base_df = pd.DataFrame({
                    'upc': df.iloc[:, upc_col].astype(str),
                    'category': df.iloc[:, config["category_col"]].astype(str),
                    'brand': df.iloc[:, config["brand_col"]].astype(str),
                    'item': df.iloc[:, config["item_col"]].astype(str),
                    'avg_price': pd.to_numeric(df.iloc[:, config["price_col"]], errors='coerce')
                })
                base_df['product_name'] = base_df['brand'].astype(str) + ' ' + base_df['item'].astype(str)
            else:
                # Vendor files - extract UPC, unit cost, and product name
                base_df = pd.DataFrame({
                    'upc': df.iloc[:, upc_col].astype(str),
                    'unit_cost': pd.to_numeric(df.iloc[:, cost_col], errors='coerce') if cost_col is not None else None
                })
                
                # Add product name based on vendor-specific columns
                if vendor_type == "HANA":
                    base_df['product_name'] = df.iloc[:, 1].astype(str)  # "Item Name" column
                elif vendor_type == "KEHE":
                    base_df['product_name'] = df.iloc[:, 2].astype(str) + ' ' + df.iloc[:, 3].astype(str)  # "BRAND" + "DESC"
                elif vendor_type == "RAINFOREST":
                    base_df['product_name'] = df.iloc[:, 3].astype(str) + ' ' + df.iloc[:, 4].astype(str)  # "Manufacturer Name" + "Item Description"
                elif vendor_type == "OSA":
                    base_df['product_name'] = df.iloc[:, 1].astype(str)  # "DESCRIPTION"
                elif vendor_type == "UNFI":
                    base_df['product_name'] = df.iloc[:, 5].astype(str) + ' ' + df.iloc[:, 6].astype(str)  # "BRAND" + "DESCRIPTION"
                else:
                    base_df['product_name'] = 'Unknown'
            
            # Create TWO versions with different normalizations
            old_df = base_df.copy()
            new_df = base_df.copy()
            
            # Apply old normalization
            old_df['upc_normalized'] = old_df['upc'].apply(self.original_normalize_upc)
            old_df = old_df.dropna(subset=['upc_normalized'])
            
            # Apply new normalization
            new_df['upc_normalized'] = new_df['upc'].apply(self.new_normalize_upc)
            new_df = new_df.dropna(subset=['upc_normalized'])
            
            return {
                'old_method': old_df,
                'new_method': new_df
            }
            
        except Exception as e:
            print(f"❌ Error extracting data from {file_path}: {e}")
            return {'old_method': pd.DataFrame(), 'new_method': pd.DataFrame()}

    def compare_normalization_methods(self, sample_upcs: List[str]) -> pd.DataFrame:
        """
        Compare old vs new normalization on a set of sample UPCs
        """
        results = []
        
        for upc in sample_upcs:
            old_result = self.original_normalize_upc(upc)
            new_result = self.new_normalize_upc(upc)
            
            results.append({
                'original_upc': upc,
                'old_normalized': old_result,
                'new_normalized': new_result,
                'same_result': old_result == new_result,
                'old_length': len(str(old_result)) if old_result else 0,
                'new_length': len(str(new_result)) if new_result else 0,
                'filtered_by_new': old_result is not None and new_result is None
            })
        
        return pd.DataFrame(results)

    def test_matching_improvements(self, extracted_data: Dict[str, Dict[str, pd.DataFrame]]) -> Dict:
        """
        Test how much the new normalization improves matching rates
        """
        if "ECRS" not in extracted_data:
            return {"error": "No ECRS data available"}
        
        results = {
            'old_method': {'total_matches': 0, 'vendor_matches': {}},
            'new_method': {'total_matches': 0, 'vendor_matches': {}},
            'improvement': {}
        }
        
        # Get ECRS UPCs for both methods
        ecrs_old = set(extracted_data["ECRS"]['old_method']['upc_normalized'].dropna())
        ecrs_new = set(extracted_data["ECRS"]['new_method']['upc_normalized'].dropna())
        
        print(f"\n📊 MATCHING COMPARISON:")
        print(f"ECRS UPCs - Old method: {len(ecrs_old):,}")
        print(f"ECRS UPCs - New method: {len(ecrs_new):,}")
        print(f"ECRS difference: {len(ecrs_new) - len(ecrs_old):+,}")
        
        for vendor_name, vendor_data in extracted_data.items():
            if vendor_name == "ECRS":
                continue
                
            # Old method matches
            vendor_old = set(vendor_data['old_method']['upc_normalized'].dropna())
            old_matches = len(ecrs_old.intersection(vendor_old))
            
            # New method matches
            vendor_new = set(vendor_data['new_method']['upc_normalized'].dropna())
            new_matches = len(ecrs_new.intersection(vendor_new))
            
            # Store results
            results['old_method']['vendor_matches'][vendor_name] = {
                'vendor_upcs': len(vendor_old),
                'matches': old_matches,
                'match_rate': old_matches / len(vendor_old) * 100 if len(vendor_old) > 0 else 0
            }
            
            results['new_method']['vendor_matches'][vendor_name] = {
                'vendor_upcs': len(vendor_new),
                'matches': new_matches,
                'match_rate': new_matches / len(vendor_new) * 100 if len(vendor_new) > 0 else 0
            }
            
            results['improvement'][vendor_name] = {
                'additional_matches': new_matches - old_matches,
                'improvement_rate': ((new_matches - old_matches) / old_matches * 100) if old_matches > 0 else 0
            }
            
            results['old_method']['total_matches'] += old_matches
            results['new_method']['total_matches'] += new_matches
            
            print(f"\n{vendor_name}:")
            print(f"  Old: {old_matches:,} matches ({old_matches / len(vendor_old) * 100:.1f}% of {len(vendor_old):,} UPCs)")
            print(f"  New: {new_matches:,} matches ({new_matches / len(vendor_new) * 100:.1f}% of {len(vendor_new):,} UPCs)")
            print(f"  Improvement: {new_matches - old_matches:+,} matches ({((new_matches - old_matches) / old_matches * 100) if old_matches > 0 else 0:+.1f}%)")
        
        total_improvement = results['new_method']['total_matches'] - results['old_method']['total_matches']
        print(f"\n🎯 TOTAL IMPROVEMENT:")
        print(f"  Old total matches: {results['old_method']['total_matches']:,}")
        print(f"  New total matches: {results['new_method']['total_matches']:,}")
        print(f"  Additional matches: {total_improvement:+,}")
        print(f"  Overall improvement: {(total_improvement / results['old_method']['total_matches'] * 100) if results['old_method']['total_matches'] > 0 else 0:+.1f}%")
        
        return results

    def run_comprehensive_test(self) -> Dict:
        """
        Run comprehensive A/B test of old vs new normalization
        """
        print("🧪 UPC NORMALIZATION A/B TEST")
        print("=" * 60)
        
        # Step 1: Test sample UPCs
        print("\n📋 Step 1: Sample UPC Normalization Comparison")
        sample_upcs = [
            # From your CSV analysis
            "5210007082", "52100070827",  # UNFI check digit pattern
            "4900055821", "49000558210",  # Another UNFI pattern
            "4066", "406",                # Short UPCs (should be filtered)
            "6000", "600",                # More short UPCs
            
            # Standard test cases
            "123456789012", "1234567890123",  # Check digit removal
            "000-123456789012",               # Leading zeros + hyphens
            "123456789012.0",                 # Decimal removal
        ]
        
        comparison_df = self.compare_normalization_methods(sample_upcs)
        
        print("\nNormalization Comparison:")
        for _, row in comparison_df.iterrows():
            status = "✅" if row['same_result'] else "🔄"
            filtered = " [FILTERED]" if row['filtered_by_new'] else ""
            print(f"{status} {row['original_upc']!r:15} → Old: {row['old_normalized']!r:12} | New: {row['new_normalized']!r:12}{filtered}")
        
        # Step 2: Test with real data files
        print(f"\n📋 Step 2: Real Data Extraction & Matching Test")
        
        # Find test files
        test_files = [
            ("STM 070125.xlsx", "HANA"),
            ("STREETS DC27 August Pricing File 6.30.2025.xlsx", "KEHE"),
            ("OSA X STM - JULY LIST.xlsx", "OSA"), 
            ("Dashboard 7.3.25.xlsx", "RAINFOREST"),
            ("Sold Qty & PL1.xlsx", "ECRS"),
            ("J44PBM01-CSV02.HA1LFO20689620.CSV", "UNFI"),
        ]
        
        possible_dirs = [
            project_root / "tests" / "test_files",
            project_root / "test_files", 
            project_root,
        ]
        
        test_dir = None
        for test_dir_candidate in possible_dirs:
            if test_dir_candidate.exists():
                files_found = sum(1 for filename, _ in test_files if (test_dir_candidate / filename).exists())
                if files_found > 0:
                    test_dir = test_dir_candidate
                    break
        
        if not test_dir:
            print("❌ No test files found")
            return {"error": "No test files found"}
        
        # Extract data with both methods
        extracted_data = {}
        
        for filename, vendor_type in test_files:
            file_path = test_dir / filename
            if file_path.exists():
                print(f"Processing {vendor_type}...")
                data = self.extract_vendor_data_with_both_methods(file_path, vendor_type)
                if len(data['old_method']) > 0 and len(data['new_method']) > 0:
                    extracted_data[vendor_type] = data
                    print(f"  ✅ Old method: {len(data['old_method']):,} products")
                    print(f"  ✅ New method: {len(data['new_method']):,} products")
                    print(f"  📊 Difference: {len(data['new_method']) - len(data['old_method']):+,}")
        
        # Step 3: Compare matching performance
        if extracted_data:
            matching_results = self.test_matching_improvements(extracted_data)
            
            # Save detailed results
            output_dir = project_root / "ab_test_results"
            output_dir.mkdir(exist_ok=True)
            
            # Save comparison DataFrame
            comparison_df.to_csv(output_dir / "upc_normalization_comparison.csv", index=False)
            
            # Save matching results
            with open(output_dir / "matching_improvement_results.json", 'w') as f:
                json.dump(matching_results, f, indent=2)
            
            print(f"\n💾 Results saved to: {output_dir}")
            
            return {
                "normalization_comparison": comparison_df,
                "matching_results": matching_results,
                "test_summary": {
                    "total_old_matches": matching_results['old_method']['total_matches'],
                    "total_new_matches": matching_results['new_method']['total_matches'],
                    "improvement": matching_results['new_method']['total_matches'] - matching_results['old_method']['total_matches']
                }
            }
        else:
            print("❌ No data extracted for comparison")
            return {"error": "No data extracted"}

def main():
    """Run the A/B test"""
    tester = UPCNormalizationTester()
    results = tester.run_comprehensive_test()
    
    if "error" not in results:
        summary = results["test_summary"]
        print(f"\n🎉 A/B TEST COMPLETE!")
        print(f"📊 Old method total matches: {summary['total_old_matches']:,}")
        print(f"📊 New method total matches: {summary['total_new_matches']:,}")
        print(f"📈 Additional matches gained: {summary['improvement']:+,}")
        
        if summary['improvement'] > 0:
            print(f"✅ NEW METHOD PERFORMS BETTER!")
            print(f"💡 Recommendation: Update your normalization method")
        else:
            print(f"⚠️  Results are mixed - review detailed analysis")
    
    print(f"\n📁 Check the 'ab_test_results' directory for detailed comparison files")

if __name__ == "__main__":
    main()