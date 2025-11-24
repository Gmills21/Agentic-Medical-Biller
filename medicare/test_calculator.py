"""Interactive test script for Medicare Price Calculator.

This script allows you to test the calculator with custom CPT codes and ZIP codes.
"""

from medicare_price_calculator import get_medicare_price


def test_single_calculation(cpt_code: str, zip_code: str):
    """Test a single CPT code and ZIP code combination."""
    try:
        price = get_medicare_price(cpt_code, zip_code)
        print(f"\n{'='*60}")
        print(f"CPT Code: {cpt_code}")
        print(f"ZIP Code: {zip_code}")
        print(f"Medicare Price: ${price:,.2f}")
        print(f"{'='*60}\n")
        return price
    except Exception as e:
        print(f"\n{'='*60}")
        print(f"ERROR calculating price for CPT {cpt_code} in ZIP {zip_code}")
        print(f"Error: {type(e).__name__}: {e}")
        print(f"{'='*60}\n")
        return None


def test_multiple_calculations():
    """Test multiple CPT/ZIP combinations."""
    test_cases = [
        ("99285", "00601"),  # Emergency dept visit - high, Puerto Rico
        ("99213", "10001"),  # Office visit level 3, New York
        ("99214", "33139"),  # Office visit level 4, Florida
        ("36415", "90210"),  # Routine venipuncture, California
        ("99285", "10001"),  # Emergency dept visit - high, New York
    ]
    
    print("\n" + "="*60)
    print("TESTING MULTIPLE CPT/ZIP COMBINATIONS")
    print("="*60)
    
    results = []
    for cpt, zip_code in test_cases:
        price = test_single_calculation(cpt, zip_code)
        results.append((cpt, zip_code, price))
    
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    print(f"{'CPT Code':<10} {'ZIP Code':<10} {'Price':<15} {'Status'}")
    print("-" * 60)
    for cpt, zip_code, price in results:
        status = "[OK] Success" if price is not None else "[FAIL] Failed"
        price_str = f"${price:,.2f}" if price is not None else "N/A"
        print(f"{cpt:<10} {zip_code:<10} {price_str:<15} {status}")
    print("="*60 + "\n")


def interactive_mode():
    """Interactive mode to test custom CPT codes and ZIP codes."""
    print("\n" + "="*60)
    print("MEDICARE PRICE CALCULATOR - INTERACTIVE MODE")
    print("="*60)
    print("Enter CPT codes and ZIP codes to calculate prices.")
    print("Type 'quit' or 'exit' to stop.\n")
    
    while True:
        try:
            cpt_code = input("Enter CPT/HCPCS code (or 'quit' to exit): ").strip()
            if cpt_code.lower() in ['quit', 'exit', 'q']:
                print("Goodbye!")
                break
            
            if not cpt_code:
                print("Please enter a valid CPT code.")
                continue
            
            zip_code = input("Enter ZIP code: ").strip()
            if not zip_code:
                print("Please enter a valid ZIP code.")
                continue
            
            test_single_calculation(cpt_code, zip_code)
            
        except KeyboardInterrupt:
            print("\n\nGoodbye!")
            break
        except Exception as e:
            print(f"Unexpected error: {e}\n")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        # Command line mode: python test_calculator.py <cpt_code> <zip_code>
        if len(sys.argv) == 3:
            cpt_code = sys.argv[1]
            zip_code = sys.argv[2]
            test_single_calculation(cpt_code, zip_code)
        elif sys.argv[1] == "--multiple":
            # Test multiple cases
            test_multiple_calculations()
        else:
            print("Usage:")
            print("  python test_calculator.py <cpt_code> <zip_code>")
            print("  python test_calculator.py --multiple")
            print("  python test_calculator.py  (for interactive mode)")
    else:
        # Interactive mode
        interactive_mode()

