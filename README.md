# SEC Repurchase Data Extractor

A sophisticated Python tool for extracting and structuring share repurchase data from SEC 10-Q and 10-K filings. Unlike tools that over-process data, this extractor preserves maximum information and provides researchers with the flexibility to tailor their analysis based on specific research designs.

## Overview

This tool transforms complex SEC filing tables into structured, research-ready data while preserving all contextual information. It handles the messy reality of how companies report repurchase activities, including multiple transaction types, missing data scenarios, and varying table structures.

## Key Features

- **Information Preservation**: Captures all data without loss, including missing value context
- **Multi-Table Support**: Handles filings with multiple repurchase programs or transaction types
- **Flexible Output**: Three complementary data sources for comprehensive analysis
- **Research-Grade Quality**: Maintains data integrity for academic and professional research
- **Real-World Robustness**: Handles edge cases and complex filing structures

## Installation

```bash
git clone https://github.com/Ali-Choubdaran/sec-repurchase-extractor.git
cd sec-repurchase-extractor
pip install -r requirements.txt
```

## Quick Start

```python
from src.main import RepurchaseExtractor

# Initialize extractor with SEC filing URL
extractor = RepurchaseExtractor("https://www.sec.gov/ix?doc=/Archives/edgar/data/1393612/000139361224000032/dfs-20240331.htm")

# Extract data (updates instance variables)
extractor.extract()

# Access the three key outputs
soup_before = extractor.soup_before    # HTML before table
repurchase_data = extractor.repurchase_data       # Structured table data
soup_after = extractor.soup_after      # HTML after table (footnotes)
```

## Understanding the Output

The extractor provides three complementary data sources that work together to give you complete information:

### 1. `self.soup_before` - Context Before Table

Contains the HTML text immediately preceding the repurchase table, often including introductory information about the company's repurchase activities.

### 2. `self.repurchase_data` - Structured Table Data

The core structured data extracted from the repurchase table. This is where the magic happens.

### 3. `self.soup_after` - Footnotes and Additional Context

Contains the HTML text immediately following the table, typically including crucial footnotes about repurchase programs, authorization details, and program terms.

## Real-World Example: Discover Financial Services (DFS)

Let's examine how the tool processes a complex real-world example from Discover Financial Services' Q1 2024 filing.

### Raw SEC Filing Data

![Discover Financial Services Repurchase Table](https://via.placeholder.com/800x600/ffffff/000000?text=SEC+Filing+Table+Screenshot)

_Note: This would show the actual SEC filing table from the DFS 10-Q form_

### Extracted Structured Data

The raw table above is transformed into `self.repurchase_data`. Here's how to interpret the key components:

#### Row Types (`id` column)

- **`id = 1, 2, 3`**: Monthly intervals (January, February, March 2024)
- **`id = 4`**: Total row for the quarter (if present in original)

#### Standardized Column Names

The tool automatically converts numeric column names to meaningful labels:

- **`row_label`**: Row description (periods, program types, etc.)
- **`tot_shares`**: Total number of shares purchased (in thousands)
- **`avg_price`**: Average price paid per share (no unit conversion)
- **`prog_shares`**: Shares purchased under public programs (in thousands)
- **`remaining_auth`**: Remaining authorization under programs (in millions)

#### Dollar vs. Share Indicators

Additional columns indicate whether values represent dollars or shares:

- **`tot_shares_dollar`**: 1 if `tot_shares` is in dollars, 0 if shares
- **`avg_price_dollar`**: 1 if `avg_price` is in dollars, 0 if shares
- **`prog_shares_dollar`**: 1 if `prog_shares` is in dollars, 0 if shares
- **`remaining_auth_dollar`**: 1 if `remaining_auth` is in dollars, 0 if shares

#### Multi-Table Support (`table_id` column)

The DFS example demonstrates a sophisticated scenario where the company reports two types of transactions:

- **`table_id = 1`**: "Repurchase program" transactions
- **`table_id = 2`**: "Employee transactions" (compensation-related)

This design allows researchers to:

- Analyze only public repurchase programs (filter `table_id = 1`)
- Study employee compensation transactions (filter `table_id = 2`)
- Combine both for total repurchase activity
- Compare different transaction types

#### Program Quality Scoring (`table_score` column)

The `table_score` helps identify the most relevant repurchase programs:

```python
def label_score(text):
    score = 0
    if 'repurchase' in text: score += 1
    if 'program' in text: score += 1
    if 'open' in text: score += 1
    if 'employe' in text: score -= 1
    if 'transaction' in text: score -= 1
    # ... more scoring rules
    return score
```

Higher scores indicate more likely public repurchase programs, with open market repurchases scoring highest.

### Contextual Analysis: Program Tracking

The real power emerges when combining `self.repurchase_data` with `self.soup_after`:

#### From `soup_after.text`:

```
(1)In April 2023, our Board of Directors approved a new share repurchase program
authorizing the purchase of up to $2.7 billion of our outstanding shares of
common stock through June 30, 2024. This share repurchase authorization replaced
our prior $4.2 billion share repurchase program.
```

#### From `repurchase_data`:

- Remaining authorization at March 31, 2024: $2,225,091,655
- Program completion rate: (2.7B - 2.225B) / 2.7B = 17.6%

This enables researchers to track program completion rates and link repurchase activity directly to program authorizations.

## Data Structure Details

### Standard Column Mapping

Most repurchase tables contain four core columns:

1. **Total Number of Shares Purchased**: Total shares bought back
2. **Average Price Paid Per Share**: Average price for those shares
3. **Shares Purchased Under Public Programs**: Shares from announced programs only
4. **Maximum Remaining Value**: Remaining authorization under programs

### Missing Value Notations

The tool preserves information about why data is missing using specific notations:

- **`!o`**: Not disclosed (company chose not to report)
- **`!u`**: Unavailable (data not accessible)
- **`!e`**: Error in extraction
- **`!f`**: Formatting issue
- **`!n`**: Not applicable
- **`!c`**: Calculation error
- **`NaN`**: Standard missing value

These notations help researchers understand the context of missing data and make informed decisions about data usage.

### Date Handling

- **`beg_date`/`end_date`**: Precise date ranges for each period
- **Monthly intervals**: SEC-required monthly breakdowns
- **Quarter totals**: Aggregated quarterly figures

## Advanced Usage Examples

### Filtering by Transaction Type

```python
# Get only public repurchase programs
public_programs = repurchase_data[repurchase_data['table_id'] == 1]

# Get only employee transactions
employee_transactions = repurchase_data[repurchase_data['table_id'] == 2]
```

### Program Completion Analysis

```python
# Extract program details from footnotes
import re
footnote_text = extractor.soup_after.text

# Find authorization amount
auth_match = re.search(r'up to \$(\d+\.?\d*)\s*billion', footnote_text)
if auth_match:
    authorized_amount = float(auth_match.group(1)) * 1e9

# Calculate completion rate
remaining_balance = repurchase_data[repurchase_data['id'] == 4]['remaining_auth'].iloc[0]  # Total row, remaining_auth column
completion_rate = (authorized_amount - remaining_balance) / authorized_amount
```

### Standardized Units

The tool automatically converts all values to standardized units:

- **`tot_shares`**: Always in thousands of shares
- **`avg_price`**: No unit conversion (keeps original precision)
- **`prog_shares`**: Always in thousands of shares  
- **`remaining_auth`**: Always in millions of dollars/shares

No manual unit conversion is needed - the data is ready for analysis!

### Working with Different Units

If you need to convert to different units:

```python
# Convert to actual shares (from thousands)
actual_shares = repurchase_data['tot_shares'] * 1000

# Convert to billions (from millions)
remaining_billions = repurchase_data['remaining_auth'] / 1000

# Convert to actual dollars (from millions)
remaining_dollars = repurchase_data['remaining_auth'] * 1e6
```

## Research Applications

This tool enables sophisticated research applications:

1. **Program Tracking**: Monitor completion rates of announced repurchase programs
2. **Cross-Company Analysis**: Compare repurchase patterns across firms
3. **Market Impact Studies**: Analyze price effects of repurchase activities
4. **Regulatory Compliance**: Track adherence to SEC reporting requirements
5. **Financial Modeling**: Build models using granular repurchase data

## Error Handling

The tool provides detailed error information through the `extraction_metadata`:

```python
# Check extraction status
if extractor.extraction_metadata.get('self_term_re') == 'not_3_monthly_intervals':
    print("Could not identify 3 monthly intervals")
elif extractor.extraction_metadata.get('error_term_re'):
    print(f"Error: {extractor.extraction_metadata['error_term_re']}")
else:
    print("Extraction successful")
```

## Contributing

We welcome contributions! Please see our contributing guidelines for details on:

- Adding new missing value notations
- Improving scoring algorithms
- Enhancing date parsing capabilities
- Adding support for new filing formats

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Citation

If you use this tool in your research, please cite:

```bibtex
@software{sec_repurchase_extractor,
  title={SEC Repurchase Data Extractor},
  author={SEC Repurchase Data Extractor Team},
  year={2024},
  url={https://github.com/Ali-Choubdaran/sec-repurchase-extractor}
}
```

## Support

For questions, issues, or feature requests, please open an issue on GitHub or contact the development team.
