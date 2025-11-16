import pandas as pd
import os
import glob
from bs4 import BeautifulSoup

def convert_html_to_csv():
    """Convert HTML files (disguised as .xls) to CSV files"""
    
    # Define directories
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    download_dir = os.path.join(project_root, "data", "downloads")
    batch_dir = os.path.join(project_root, "data", "batch", "ssrn")
    
    # Create batch directory if it doesn't exist
    os.makedirs(batch_dir, exist_ok=True)
    
    print(f"Converting HTML files from: {download_dir}")
    print(f"Saving CSV files to: {batch_dir}")
    
    # Find all .xls files (which are actually HTML)
    html_files = glob.glob(os.path.join(download_dir, "*.xls"))
    
    if not html_files:
        print("No .xls files found in downloads directory.")
        return
    
    print(f"Found {len(html_files)} HTML files to convert...")
    
    converted_count = 0
    error_count = 0
    
    for html_file in html_files:
        try:
            # Read HTML file
            with open(html_file, 'r', encoding='utf-8') as file:
                html_content = file.read()
            
            # Parse HTML with BeautifulSoup
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Find the main data table
            table = soup.find('table')
            if not table:
                print(f"✗ No table found in {os.path.basename(html_file)}")
                error_count += 1
                continue
            
            # Extract table data
            rows = []
            for tr in table.find_all('tr'):
                row = []
                for td in tr.find_all(['td', 'th']):
                    # Get text content and clean it
                    cell_text = td.get_text(strip=True)
                    # Replace non-breaking spaces and clean up
                    cell_text = cell_text.replace('\xa0', '').replace('&nbsp;', '')
                    row.append(cell_text)
                if row:  # Only add non-empty rows
                    rows.append(row)
            
            if not rows:
                print(f"✗ No data rows found in {os.path.basename(html_file)}")
                error_count += 1
                continue
            
            # Create DataFrame
            df = pd.DataFrame(rows)
            
            # Generate CSV filename
            base_name = os.path.basename(html_file)
            csv_name = os.path.splitext(base_name)[0] + '.csv'
            csv_path = os.path.join(batch_dir, csv_name)
            
            # Save as CSV
            df.to_csv(csv_path, index=False, header=False)
            
            print(f"✓ Converted: {base_name} -> {csv_name} ({len(rows)} rows)")
            converted_count += 1
            
        except Exception as e:
            print(f"✗ Error converting {os.path.basename(html_file)}: {str(e)}")
            error_count += 1
    
    print(f"\nConversion completed:")
    print(f"  Successfully converted: {converted_count} files")
    print(f"  Errors: {error_count} files")
    print(f"  CSV files saved to: {batch_dir}")

if __name__ == "__main__":
    convert_html_to_csv()
    
