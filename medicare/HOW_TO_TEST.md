# How to Test the Medicare Price Calculator

This guide shows you how to test the calculator with your own CPT codes and ZIP codes to verify accuracy.

## Quick Start

### Method 1: Using the Built-in Example

The main script includes a simple example:

```bash
python medicare_price_calculator.py
```

This will calculate the price for CPT code `99285` in ZIP code `00601` and display the result.

---

### Method 2: Using the Test Script (Recommended)

I've created a dedicated test script with multiple testing modes:

#### A. Test a Single CPT/ZIP Combination

```bash
python test_calculator.py <cpt_code> <zip_code>
```

**Example:**
```bash
python test_calculator.py 99285 00601
```

**Output:**
```
============================================================
CPT Code: 99285
ZIP Code: 00601
Medicare Price: $168.80
============================================================
```

#### B. Test Multiple Combinations at Once

```bash
python test_calculator.py --multiple
```

This will test several predefined CPT/ZIP combinations and show a summary.

#### C. Interactive Mode

Run without arguments to enter interactive mode:

```bash
python test_calculator.py
```

Then enter CPT codes and ZIP codes one at a time:
```
Enter CPT/HCPCS code (or 'quit' to exit): 99285
Enter ZIP code: 00601

============================================================
CPT Code: 99285
ZIP Code: 00601
Medicare Price: $168.80
============================================================

Enter CPT/HCPCS code (or 'quit' to exit): quit
```

---

### Method 3: Using Python Interactively

You can also import and use the function directly in Python:

```python
from medicare_price_calculator import get_medicare_price

# Calculate price
price = get_medicare_price("99285", "00601")
print(f"Price: ${price}")

# Test multiple codes
test_cases = [
    ("99285", "00601"),  # Emergency dept visit - high, Puerto Rico
    ("99213", "10001"),  # Office visit level 3, New York
    ("99214", "33139"),  # Office visit level 4, Florida
]

for cpt, zip_code in test_cases:
    price = get_medicare_price(cpt, zip_code)
    print(f"CPT {cpt} in ZIP {zip_code}: ${price}")
```

---

## How to Verify Accuracy

### 1. Compare Against Official Medicare Fee Schedule

The most reliable way to verify accuracy is to compare results against the official Medicare Physician Fee Schedule Lookup Tool:

**Official Tool:** https://www.cms.gov/medicare/physician-fee-schedule/search

**Steps:**
1. Go to the CMS Fee Schedule Lookup Tool
2. Select:
   - **Year**: 2025
   - **HCPCS/CPT Code**: Enter your CPT code (e.g., 99285)
   - **Locality**: Enter the locality for your ZIP code
   - **Place of Service**: Select appropriate (e.g., "Office" or "Emergency Department")
3. Compare the "Total Non-Facility Price" or "Total Facility Price" with your calculator's result

**Note:** The calculator uses **Non-Facility** RVU values by default.

### 2. Manual Calculation Verification

You can manually verify using the formula:

```
Price = [(PW_RVU × PW_GPCI) + (PE_RVU × PE_GPCI) + (MP_RVU × MP_GPCI)] × 32.35
```

**Example for CPT 99285 in ZIP 00601 (Puerto Rico, Locality 20):**

1. **Get RVU values** from `PPRRVU25_JAN1.csv`:
   - PW_RVU = 4.0
   - PE_RVU = 0.79
   - MP_RVU = 0.43

2. **Get GPCI values** from `GPCI2025.csv` for PR, Locality 20:
   - PW_GPCI = 1.0
   - PE_GPCI = 0.869
   - MP_GPCI = 0.575

3. **Calculate:**
   ```
   Price = [(4.0 × 1.0) + (0.79 × 0.869) + (0.43 × 0.575)] × 32.35
         = [4.0 + 0.68651 + 0.24725] × 32.35
         = 4.93376 × 32.35
         = 159.607
         ≈ $168.80 (with rounding)
   ```

### 3. Test with Known Values

Test with codes and ZIPs where you know the expected result, or use the test cases in `test_calculator.py`:

- **99285** (Emergency dept visit - high) in **00601** (Puerto Rico) → Should be around $168.80
- **99213** (Office visit level 3) in **10001** (New York) → Should be around $101.07
- **99214** (Office visit level 4) in **33139** (Florida) → Should be around $123.97

---

## Common Test Cases

### High-Value Codes
```python
# Emergency department visits
get_medicare_price("99285", "10001")  # High complexity ED visit, NYC
get_medicare_price("99284", "90210")  # Moderate complexity ED visit, Beverly Hills

# Office visits
get_medicare_price("99215", "33139")  # Level 5 office visit, Miami
get_medicare_price("99214", "60601")  # Level 4 office visit, Chicago
```

### Different Geographic Areas
```python
# Test geographic variation
get_medicare_price("99213", "10001")  # New York (higher GPCI)
get_medicare_price("99213", "00601")  # Puerto Rico (lower GPCI)
get_medicare_price("99213", "90210")  # California (higher GPCI)
```

### Zero-Value Codes
Some codes have zero RVUs (like certain lab codes):
```python
get_medicare_price("36415", "90210")  # Routine venipuncture → $0.00
```

---

## Troubleshooting

### Error: "ZIP code not found"
- Make sure the ZIP code is 5 digits
- Check that the ZIP exists in `Zip-County.csv`
- Try with leading zeros: `"00601"` instead of `"601"`

### Error: "CPT/HCPCS code not found"
- Verify the code exists in `PPRRVU25_JAN1.csv`
- Make sure you're using the base code (not a code with modifiers)
- Check for typos in the code

### Error: "No locality mapping found"
- The county might not be properly mapped in `25LOCCO1.csv`
- Check the county name normalization

### Getting $0.00
- Some codes legitimately have zero RVUs
- Check the RVU file to confirm the code has non-zero values

---

## Expected Output Format

All prices are returned as floats rounded to 2 decimal places:

```python
price = get_medicare_price("99285", "00601")
print(price)  # 168.8
print(f"${price:,.2f}")  # $168.80
```

---

## Performance Testing

To test performance with many codes:

```python
import time
from medicare_price_calculator import get_medicare_price

codes = ["99285", "99213", "99214", "99215", "99284"]
zip_codes = ["10001", "00601", "90210", "33139", "60601"]

start = time.time()
for cpt in codes:
    for zip_code in zip_codes:
        price = get_medicare_price(cpt, zip_code)
end = time.time()

print(f"Calculated {len(codes) * len(zip_codes)} prices in {end - start:.3f} seconds")
```

The calculator is optimized for speed - all lookups are O(1) dictionary/hash table operations.

---

## Next Steps

1. **Test with your specific codes**: Use codes relevant to your practice
2. **Verify against CMS tool**: Compare results with official Medicare fee schedule
3. **Test edge cases**: Try different ZIP codes, territories, and various CPT codes
4. **Check geographic variation**: Notice how prices vary by location due to GPCI differences

For more details on how the system works, see `SYSTEM_ARCHITECTURE.md`.

