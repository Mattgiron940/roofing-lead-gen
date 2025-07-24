import csv
import os
from datetime import datetime

def save_results_to_csv(scraped_data, filename='leads.csv'):
    """
    Save scraped data to CSV file with unified field handling
    """
    if not scraped_data:
        print("No data to save")
        return
    
    # Collect all unique fieldnames from all records
    all_fields = set()
    for record in scraped_data:
        all_fields.update(record.keys())
    
    # Convert to sorted list for consistent CSV column order
    fieldnames = sorted(list(all_fields))
    
    # Write to CSV
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        
        # Write each row, handling missing fields gracefully
        for record in scraped_data:
            # Fill missing fields with empty string
            complete_record = {field: record.get(field, '') for field in fieldnames}
            writer.writerow(complete_record)
    
    print(f"Successfully saved {len(scraped_data)} records to {filename}")