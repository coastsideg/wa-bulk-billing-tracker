"""
WA GP Bulk Billing Tracker - BY STATE ELECTORATE (FIXED - CORRECT URLs)
Scrapes healthdirect.gov.au using correct URL format and maps to electorates
"""

import requests
from bs4 import BeautifulSoup
import json
import csv
from datetime import datetime
from time import sleep
import random
from collections import defaultdict
import sys
import re

class WABulkBillingByElectorate:
    def __init__(self, electorate_csv_path):
        self.base_url = "https://www.healthdirect.gov.au"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        self.results = []
        self.suburb_postcode_list = []
        self.electorate_mapping = self.load_electorate_mapping(electorate_csv_path)
        
    def load_electorate_mapping(self, csv_path):
        """Load the electorate-suburb-postcode mapping"""
        mapping = defaultdict(list)
        suburb_list = []
        
        try:
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    electorate = row['Electorate']
                    suburb = row['Suburb']
                    postcode = row['Postcode']
                    
                    mapping[postcode].append({
                        'electorate': electorate,
                        'suburb': suburb
                    })
                    
                    suburb_list.append({
                        'suburb': suburb,
                        'postcode': postcode,
                        'electorate': electorate
                    })
            
            self.suburb_postcode_list = suburb_list
            print(f"✓ Loaded {len(suburb_list)} suburb-postcode combinations")
            print(f"✓ Covering {len(set(s['electorate'] for s in suburb_list))} electorates")
            return mapping
        except Exception as e:
            print(f"ERROR: Could not load CSV file: {e}")
            print(f"Make sure 'WA_Electorates_Suburbs_Postcodes.csv' is in the same folder!")
            sys.exit(1)
    
    def get_electorate_from_postcode_suburb(self, postcode, suburb):
        """Find electorate based on postcode and suburb"""
        if postcode not in self.electorate_mapping:
            return "Unknown"
        
        # Normalize suburb name for matching
        suburb_normalized = suburb.lower().strip()
        
        for entry in self.electorate_mapping[postcode]:
            if entry['suburb'].lower().strip() == suburb_normalized:
                return entry['electorate']
        
        # If exact match not found, return first electorate for that postcode
        return self.electorate_mapping[postcode][0]['electorate']
    
    def normalize_suburb_for_url(self, suburb):
        """Convert suburb name to URL format (lowercase, hyphens)"""
        # Remove special characters, convert to lowercase, replace spaces with hyphens
        suburb_url = suburb.lower().strip()
        suburb_url = re.sub(r'[^\w\s-]', '', suburb_url)  # Remove special chars except space and hyphen
        suburb_url = re.sub(r'[\s]+', '-', suburb_url)  # Replace spaces with hyphens
        return suburb_url
    
    def search_gp_clinics(self, suburb, postcode, electorate):
        """Search for GP clinics in a specific suburb"""
        suburb_url = self.normalize_suburb_for_url(suburb)
        print(f"Searching {suburb} ({postcode}) - {electorate}...")
        
        # Correct URL format: /search/{suburb}-{postcode}-{state}/gp-general-practice/788007007
        search_url = f"{self.base_url}/australian-health-services/search/{suburb_url}-{postcode}-wa/gp-general-practice/788007007"
        
        try:
            response = requests.get(search_url, headers=self.headers, timeout=15)
            
            # If 404, skip this suburb
            if response.status_code == 404:
                print(f"  No data available for {suburb}")
                return
            
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find all clinic links
            clinic_links = []
            
            # Look for links to individual clinic pages
            for link in soup.find_all('a', href=True):
                href = link['href']
                if '/healthcare-service/' in href and '/gp-general-practice/' in href:
                    if not href.startswith('http'):
                        href = self.base_url + href
                    if href not in clinic_links:
                        clinic_links.append(href)
            
            print(f"  Found {len(clinic_links)} clinics")
            
            for clinic_url in clinic_links:
                try:
                    # Get clinic details
                    clinic_data = self.get_clinic_details(clinic_url, postcode, suburb, electorate)
                    if clinic_data:
                        self.results.append(clinic_data)
                        
                    # Be polite - random delay
                    sleep(random.uniform(0.5, 1.5))
                    
                except Exception as e:
                    print(f"    Error processing clinic: {e}")
                    continue
            
        except requests.exceptions.HTTPError as e:
            if '404' not in str(e):
                print(f"  HTTP error for {suburb}: {e}")
        except requests.exceptions.RequestException as e:
            print(f"  Network error for {suburb}: {e}")
        except Exception as e:
            print(f"  Error searching {suburb}: {e}")
    
    def get_clinic_details(self, url, search_postcode, search_suburb, electorate):
        """Get detailed information about a specific clinic"""
        try:
            response = requests.get(url, headers=self.headers, timeout=15)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Extract clinic name
            name_elem = soup.find('h1') or soup.find('h2')
            clinic_name = name_elem.get_text(strip=True) if name_elem else "Unknown"
            
            # Extract address
            address_text = ""
            address_elem = soup.find('address')
            if address_elem:
                address_text = address_elem.get_text(strip=True)
            else:
                # Try to find address in other ways
                for elem in soup.find_all(['p', 'div']):
                    text = elem.get_text()
                    if 'WA' in text and any(char.isdigit() for char in text):
                        address_text = text.strip()
                        break
            
            # Extract suburb and postcode
            suburb = search_suburb
            postcode = search_postcode
            
            if address_text:
                # Try to extract postcode from address
                pc_match = re.search(r'\b(\d{4})\b', address_text)
                if pc_match:
                    postcode = pc_match.group(1)
                
                # Try to extract suburb (word before WA)
                suburb_match = re.search(r'([A-Z][A-Z\s]+)\s+WA\s+\d{4}', address_text)
                if suburb_match:
                    suburb = suburb_match.group(1).strip()
            
            # Extract phone
            phone = "N/A"
            phone_elem = soup.find('a', href=re.compile(r'tel:'))
            if phone_elem:
                phone = phone_elem.get_text(strip=True)
            else:
                # Try to find phone number in text
                phone_match = re.search(r'(\d{2}\s?\d{4}\s?\d{4})', soup.get_text())
                if phone_match:
                    phone = phone_match.group(1)
            
            # CHECK FOR BULK BILLING STATUS
            page_text = soup.get_text().lower()
            
            # Look for bulk billing indicators
            is_bulk_billed = False
            billing_status = "Unknown"
            
            if 'bulk billing only' in page_text:
                is_bulk_billed = True
                billing_status = "100% Bulk Billed"
            elif '100% bulk bill' in page_text or 'fully bulk billed' in page_text:
                is_bulk_billed = True
                billing_status = "100% Bulk Billed"
            elif 'bulk bills all patients' in page_text:
                is_bulk_billed = True
                billing_status = "100% Bulk Billed"
            elif 'mixed billing' in page_text:
                billing_status = "Mixed Billing"
            elif 'fees apply' in page_text or 'private billing' in page_text:
                billing_status = "Private Billing"
            
            clinic_data = {
                'name': clinic_name,
                'address': address_text if address_text else "Unknown",
                'suburb': suburb,
                'postcode': postcode,
                'electorate': electorate,
                'phone': phone,
                'url': url,
                'is_bulk_billed': is_bulk_billed,
                'billing_status': billing_status,
                'last_checked': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            if is_bulk_billed:
                print(f"    ✓ BULK BILLED: {clinic_name}")
            
            return clinic_data
            
        except Exception as e:
            print(f"    Error getting clinic details: {e}")
            return None
    
    def save_results(self, filename='wa_bulk_billing_by_electorate.json'):
        """Save results organized by electorate"""
        
        # Remove duplicates (same clinic URL)
        unique_results = {}
        for clinic in self.results:
            url = clinic.get('url')
            if url not in unique_results:
                unique_results[url] = clinic
        
        self.results = list(unique_results.values())
        
        # Organize by electorate
        by_electorate = defaultdict(list)
        bulk_billed_count = 0
        
        for clinic in self.results:
            electorate = clinic.get('electorate', 'Unknown')
            by_electorate[electorate].append(clinic)
            if clinic.get('is_bulk_billed'):
                bulk_billed_count += 1
        
        # Create summary statistics
        electorate_stats = {}
        for electorate, clinics in by_electorate.items():
            bulk_billed = [c for c in clinics if c.get('is_bulk_billed')]
            electorate_stats[electorate] = {
                'total_clinics': len(clinics),
                'bulk_billed_clinics': len(bulk_billed),
                'percentage': round(len(bulk_billed) / len(clinics) * 100, 1) if clinics else 0
            }
        
        output = {
            'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'summary': {
                'total_clinics': len(self.results),
                'bulk_billed_clinics': bulk_billed_count,
                'electorates_covered': len(by_electorate)
            },
            'by_electorate': dict(by_electorate),
            'electorate_stats': electorate_stats
        }
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        
        print(f"\n✓ Saved results to {filename}")
        print(f"  - {len(by_electorate)} electorates")
        print(f"  - {len(self.results)} total clinics")
        print(f"  - {bulk_billed_count} bulk-billed clinics ({round(bulk_billed_count/len(self.results)*100, 1)}%)")
        
        return filename
    
    def run(self):
        """Main execution"""
        print("=" * 70)
        print("WA GP BULK BILLING TRACKER - BY ELECTORATE")
        print("=" * 70)
        print(f"Searching {len(self.suburb_postcode_list)} suburb-postcode combinations...\n")
        
        # Search each suburb-postcode combination
        for i, entry in enumerate(self.suburb_postcode_list, 1):
            print(f"[{i}/{len(self.suburb_postcode_list)}] ", end="")
            self.search_gp_clinics(
                entry['suburb'],
                entry['postcode'],
                entry['electorate']
            )
            sleep(random.uniform(1, 3))  # Be nice to the server
        
        print(f"\n{'=' * 70}")
        print(f"SCAN COMPLETE")
        print(f"={'=' * 70}")
        
        # Save results
        if self.results:
            self.save_results()
        else:
            print("\n⚠️  No clinics found. The website may have blocked requests.")
            print("Try running again later or using a VPN.")
        
        print(f"\n✓ Done!")

if __name__ == "__main__":
    # Path to your electorate CSV file
    csv_path = "WA_Electorates_Suburbs_Postcodes.csv"
    
    tracker = WABulkBillingByElectorate(csv_path)
    tracker.run()
