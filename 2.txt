!pip install pypdf pandas

import os
import re
import pandas as pd
from datetime import datetime
from google.colab import drive
from pypdf import PdfReader

# Mount Google Drive
drive.mount('/content/drive')

import time
start = time.time()


# Base directory for PDFs
base_dir = "/content/drive/My Drive/Colab Notebooks/20250716_CR data/docs/Companies Ordinance pdfs fix"

# Lists to store DataFrames
all_dfs = []  # For main table data
summary_data = []  # For summary table

# Function to check if PDF contains the phrase and extract text per page
def contains_phrase(pdf_path):
    try:
        with open(pdf_path, 'rb') as file:
            reader = PdfReader(file)
            pages_text = []
            sections_found = set()
            for page in reader.pages:

                page_text = page.extract_text()
                if page_text:
                    # (CHECK!)
                    # print(page_text)
                    
                    # Check for 'struck\s*off' or 'deregister'
                    if re.search(r'struck\s*off|deregister|剔\s*除|解\s*散', page_text, re.IGNORECASE):
                        # Remove spaces for section pattern matching
                        text_no_spaces = ''.join(page_text.split())
                        # Find section pattern like section{number}...) within 10 chars of 'section'
                        section_match = re.search(r'section\d+\([^)]{0,10}\)', text_no_spaces, re.IGNORECASE)
                        if section_match:
                            sections_found.add(section_match.group(0))
                    pages_text.append(page_text)
            has_phrase = bool(sections_found)
            return has_phrase, pages_text, list(sections_found) if sections_found else ['None']
    except Exception as e:
        print(f"Error reading {pdf_path}: {e}")
        return False, [], ['None']

def extract_table_regex(pages_text, folder_date, gn_number, sections_found):
    try:
        dfs = []
        total_rows = 0
        unique_cis = set()

        for page_text in pages_text:
            # Find all occurrences of 'after the date here of' or '公\s*告' in this page
            matches = list(re.finditer(r'(?i:after\s*the\s*date\s*here\s*of)|公\s*告', page_text))
            if matches:
                # Extract text after the last match
                last_match = matches[-1]
                page_text = page_text[last_match.end():]  # Extract text after the last matched phrase
            else:
                # If neither phrase is found, process the entire page text
                pass

            # Step 1: Find all BRNs with their start and end positions
            brn_pattern = r'(?:^[A-Za-z]{0,2}\s*)?(\d{5,11})(?=\s|$)'
            brn_matches = [(match.group(1), match.start(), match.end()) for match in re.finditer(brn_pattern, page_text, re.MULTILINE)]

            # Create DataFrame for BRNs
            brn_df = pd.DataFrame(brn_matches, columns=['BRN', 'start_pos', 'end_pos'])

            # Step 2: Extract text between BRNs or for the last BRN's line
            name_data = []
            for i, row in brn_df.iterrows():
                start_pos = row['end_pos']
                # Determine end position: next BRN's start_pos or end of text
                end_pos = brn_df['start_pos'].iloc[i + 1] if i + 1 < len(brn_df) else len(page_text)

                # Extract text segment
                text_segment = page_text[start_pos:end_pos]

                # Check if this is the last BRN
                is_last_brn = i + 1 == len(brn_df)

                if is_last_brn:
                    # For the last BRN, process only its line
                    line_end = re.search(r'\n|$', text_segment)
                    if line_end:
                        text_segment = text_segment[:line_end.start()]
                else:
                    # For non-last BRNs, reduce multiple lines to a single line
                    text_segment = re.sub(r'\n', ' ', text_segment)
                    # (CHECK!) Print the reduced text for debugging
                    # print(f"Reduced text for BRN {row['BRN']}: {text_segment}")

                # Stop at '公司註冊處處長' or 'Registrar of Companies'
                stop_match = re.search(r'(公\s*司\s*註\s*冊\s*處\s*處\s*長|Registrar\s*of\s*Companies)', text_segment, re.IGNORECASE)
                if stop_match:
                    text_segment = text_segment[:stop_match.start()]

                # Extract English and Chinese names
                # English name: all non-Chinese characters at the start (optional)
                english_name_match = re.match(r'^\s*([^\u4e00-\u9fff]*)', text_segment)
                english_name = english_name_match.group(1).strip() if english_name_match and english_name_match.group(1) else ''
                
                # Chinese name: Chinese characters (and parentheses) after the English name (optional)
                # Search only after the end of the English name
                search_start = english_name_match.end() if english_name_match else 0
                chinese_name_match = re.search(r'[\u4e00-\u9fff\(\)]+', text_segment[search_start:])
                chinese_name = chinese_name_match.group(0).strip() if chinese_name_match else ''

                name_data.append({'BRN': row['BRN'], 'ENGLISH_NAME': english_name, 'CHINESE_NAME': chinese_name})

            # Create DataFrame for names
            name_df = pd.DataFrame(name_data)

            # Step 3: Left join BRNs with names
            df = brn_df[['BRN']].merge(name_df, on='BRN', how='left')

            # Step 4: Clean up and process DataFrame
            if not df.empty:
                # Clean up columns
                df['BRN'] = df['BRN'].str.strip()
                df['ENGLISH_NAME'] = df['ENGLISH_NAME'].str.strip().fillna('')
                df['CHINESE_NAME'] = df['CHINESE_NAME'].str.strip().replace('公司註冊處處長', '', regex=False).fillna('')

                # Validate BRN: Ensure it's numeric and 5-11 digits
                df = df[df['BRN'].str.match(r'^\d{5,11}$')]

                # Remove duplicate BRNs, keeping the first occurrence
                df = df.drop_duplicates(subset=['BRN'], keep='first')

                # Add PUBLISHED_DATE, GOV_NOTICE_NUM, and SECTION
                df['PUBLISHED_DATE'] = datetime.strptime(folder_date, '%Y%m%d').strftime('%Y-%m-%d')
                df['GOV_NOTICE_NUM'] = gn_number
                df['SECTION'] = ', '.join(sections_found)  # Combine all sections found

                # Append to dfs list
                dfs.append(df)

                # Update total_rows and unique_cis
                total_rows += len(df)
                unique_cis.update(df['BRN'])


        if dfs:
            combined_df = pd.concat(dfs, ignore_index=True)
            return combined_df, total_rows, len(unique_cis)
        return None, 0, 0
    except Exception as e:
        print(f"Error in regex extraction: {e}")
        return None, 0, 0

# Iterate through folders
for folder_name in os.listdir(base_dir):
    folder_path = os.path.join(base_dir, folder_name)
    if os.path.isdir(folder_path) and re.match(r'\d{8}', folder_name):  # Validate folder name (yyyymmdd)
        for filename in os.listdir(folder_path):
            if filename.startswith('GN_') and filename.endswith('.pdf'):
                gn_number = filename.replace('GN_', '').replace('.pdf', '')
                pdf_path = os.path.join(folder_path, filename)
                print(f"Checking PDF: {pdf_path}")

                # Check for phrase and get text per page
                has_phrase, pages_text, sections_found = contains_phrase(pdf_path)

                # Add to summary with PUBLISHED_DATE and GOV_NOTICE_NUM
                summary_entry = {
                    'Publish Date': datetime.strptime(folder_name, '%Y%m%d').strftime('%Y-%m-%d'),
                    'GN Number': gn_number,
                    'Passed contains_phrase': has_phrase,
                    'Passed extract_table_regex': True,  # Always True since only regex is used
                    'Mapping Method': 'PdfReader' if has_phrase else 'None',
                    'Mapped Cnt': 0,
                    'Mapped Distinct Count': 0,
                    'Section': ', '.join(sections_found),
                    'Item Screened': pdf_path
                }

                if has_phrase:
                    df, row_count, unique_ci_count = extract_table_regex(pages_text, folder_name, gn_number, sections_found)
                    if df is not None:
                        all_dfs.append(df)
                        summary_entry['Mapped Cnt'] = row_count
                        summary_entry['Mapped Distinct Cnt'] = unique_ci_count

                summary_data.append(summary_entry)


# Combine main DataFrame
if all_dfs:
    main_df = pd.concat(all_dfs, ignore_index=True)
    # Sort by PUBLISHED_DATE and GOV_NOTICE_NUM
    main_df = main_df.sort_values(by=['PUBLISHED_DATE', 'GOV_NOTICE_NUM'])
    print("\nExtracted Tables from PDFs:")
    # print(main_df.to_string(index=False))
else:
    print("\nNo tables extracted from PDFs.")

# Create and print summary DataFrame
summary_df = pd.DataFrame(summary_data)
summary_df = summary_df.sort_values(by=['Item Screened'])
print("\nSummary of PDF Processing:")
# print(summary_df.to_string(index=False))

output_dir = '/content/drive/My Drive/Colab Notebooks/20250716_CR data/output'

main_df.to_excel(f'{output_dir}/main_output.xlsx', sheet_name='Sheet1', index=False, engine='openpyxl')
summary_df.to_excel(f'{output_dir}/summary_df.xlsx', sheet_name='Sheet1', index=False, engine='openpyxl')
print('Done.')
end = time.time()
print((end-start)/60)
