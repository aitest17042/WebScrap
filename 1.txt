import requests
from bs4 import BeautifulSoup
import re
import os
import time
import pandas as pd
from datetime import datetime
from urllib.parse import urljoin
from google.colab import drive

# Mount Google Drive
drive.mount('/content/drive')

start = time.time()

# Base URL template
base_url = "https://www.gld.gov.hk/egazette/english/gazette/volume.php?year={year}&vol={vol}&no={no}&extra=0&type=0"

# Headers to mimic a browser
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

# Function to download PDFs for "Companies Ordinance" notices and return notice numbers
def download_companies_ordinance_pdfs(url, publish_date):
    # Convert publish date to yyyymmdd for folder name
    date_obj = datetime.strptime(publish_date, '%d/%m/%Y')
    folder_date = date_obj.strftime('%Y%m%d')
    target_dir = f"/content/drive/My Drive/Colab Notebooks/20250716_CR data/docs/Companies Ordinance pdfs/{folder_date}"
    os.makedirs(target_dir, exist_ok=True)

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        table = soup.find('table')  # Adjust selector if needed

        notices = []
        if table:
            rows = table.find_all('tr')[1:]  # Skip header
            for row in rows:
                cols = row.find_all('td')
                if len(cols) >= 3:
                    notice_number = cols[0].text.strip()
                    subject_cell = cols[1]
                    link = subject_cell.find('a')
                    if link and 'href' in link.attrs:
                        subject_text = link.text.strip()
                        link_url = urljoin(url, link['href'])
                        if re.search(r'Companies Ordinance', subject_text, re.IGNORECASE):
                            notices.append({
                                'notice_number': notice_number,
                                'subject': subject_text,
                                'url': link_url
                            })

        # Sort notices by Government Notice Number
        notices.sort(key=lambda x: int(x['notice_number']) if x['notice_number'].isdigit() else x['notice_number'])

        # Download PDFs
        for notice in notices:
            print(f"Downloading PDF for Notice {notice['notice_number']}: {notice['subject']} ({url})")
            try:
                pdf_response = requests.get(notice['url'], headers=headers, timeout=10)
                pdf_response.raise_for_status()
                filename = os.path.join(target_dir, f"GN_{notice['notice_number']}.pdf")
                with open(filename, 'wb') as f:
                    f.write(pdf_response.content)
                print(f"Saved: {filename}")
            except Exception as e:
                print(f"Failed to download {notice['url']}: {e}")

        print(f"\nFound {len(notices)} notices containing 'Companies Ordinance' in {url}")
        # Return list of notice numbers
        return [notice['notice_number'] for notice in notices]
    except Exception as e:
        print(f"Error processing {url}: {e}")
        return []

# Function to extract publish date
def extract_publish_date(url):
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            text = soup.get_text()
            match = re.search(r'Gazette  Published on [A-Za-z]+, (\d{2}/\d{2}/\d{4}),', text)
            if match:
                return match.group(1)  # e.g., 14/02/2025
            else:
                print(f"No publish date found in {url}")
                return None
        else:
            print(f"Page not accessible: {url} (Status: {response.status_code})")
            return None
    except Exception as e:
        print(f"Error accessing {url}: {e}")
        return None

# List to store table data
data = []

# Iterate through specified years, volumes, and numbers
for year, vol, max_no in [(2024, 28, 52), (2025, 29, 28)]:
    for no in range(1, max_no + 1):
        url = base_url.format(year=year, vol=vol, no=no)
        print(f"\nChecking: {url}")
        publish_date = extract_publish_date(url)
        if publish_date:
            # Convert date to yyyy-mm-dd for DataFrame
            date_obj = datetime.strptime(publish_date, '%d/%m/%Y')
            formatted_date = date_obj.strftime('%Y-%m-%d')
            # Get notice numbers
            notice_numbers = download_companies_ordinance_pdfs(url, publish_date)
            data.append({
                'Publish Date': formatted_date,
                'Link': url,
                'Volume': vol,
                'Num': no,
                'GN Number': ', '.join(notice_numbers) if notice_numbers else 'None'
            })
        time.sleep(1)  # Avoid rate-limiting


end = time.time()
print((end-start)/60)

# Create pandas DataFrame
df = pd.DataFrame(data)

# Sort by Publish Date
df = df.sort_values(by='Publish Date')

# Print the DataFrame
if not df.empty:
    print("\nGazette Issues Table:")
    print(df.to_string(index=False))
else:
    print("No gazette issues found.")


df['GN Number'] = df['GN Number'].str.split(', ')
df_expanded = df.explode('GN Number').reset_index(drop=True)

output_dir = '/content/drive/My Drive/Colab Notebooks/20250716_CR data/output'
df_expanded.to_excel(f'{output_dir}/Downloaded PDFs.xlsx', index=False, engine='openpyxl')
