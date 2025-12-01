"""
WA GP Bulk Billing Tracker - STATE + FEDERAL ELECTORATES
Scrapes healthdirect.gov.au and maps to BOTH state and federal electoral districts
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

class WABulkBillingDualElectorates:
    def __init__(self, state_csv_path, federal_csv_path):
        self.base_url = "https://www.healthdirect.gov.au"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        self.results = []
        
        # Load both state and federal mappings
        self.state_mapping, self.state_suburb_list = self.load_state_mapping(state_csv_path)
        self.federal_mapping = self.load_federal_mapping(federal_csv_path)
        
    def load_state_mapping(self, csv_path):
        """Load the STATE electorate-suburb-postcode mapping"""
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
                        'state_electorate': electorate
                    })
            
            print(f"✓ Loaded STATE mapping: {len(suburb_list)} suburb-postcode combinations")
            print(f"✓ Covering {len(set(s['state_electorate'] for s in suburb_list))} state electorates")
            return mapping, suburb_list
        except Exception as e:
            print(f"ERROR: Could not load state CSV file: {e}")
            sys.exit(1)
    
    def load_federal_mapping(self, csv_path):
        """Load the FEDERAL electorate-suburb-postcode mapping"""
        mapping = defaultdict(list)
        
        try:
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    federal_electorate = row['Federal_Electorate']
                    suburb = row['Suburb']
                    postcode = row['Postcode']
                    
                    # Key by postcode + suburb for exact matching
                    key = f"{postcode}_{suburb.lower().strip()}"
                    mapping[key] = federal_electorate
            
            print(f"✓ Loaded FEDERAL mapping: {len(mapping)} suburb-postcode combinations")
            print(f"✓ Covering {len(set(mapping.values()))} federal electorates\n")
            return mapping
        except Exception as e:
            print(f"WARNING: Could not load federal CSV file: {e}")
            print("Continuing with state electorates only...")
            return {}
    
    def get_state_electorate(self, postcode, suburb):
        """Find STATE electorate based on postcode and suburb"""
        if postcode not in self.state_mapping:
            return "Unknown"
        
        suburb_normalized = suburb.lower().strip()
        
        for entry in self.state_mapping[postcode]:
            if entry['suburb'].lower().strip() == suburb_normalized:
                return entry['electorate']
        
        # If exact match not found, return first electorate for that postcode
        return self.state_mapping[postcode][0]['electorate']
    
    def get_federal_electorate(self, postcode, suburb):
        """Find FEDERAL electorate based on postcode and suburb"""
        key = f"{postcode}_{suburb.lower().strip()}"
        return self.federal_mapping.get(key, "Unknown")
    
    def normalize_suburb_for_url(self, suburb):
        """Convert suburb name to URL format"""
        suburb_url = suburb.lower().strip()
        suburb_url = re.sub(r'[^\w\s-]', '', suburb_url)
        suburb_url = re.sub(r'[\s]+', '-', suburb_url)
        return suburb_url
    
    def search_gp_clinics(self, suburb, postcode, state_electorate):
        """Search for GP clinics in a specific suburb"""
        suburb_url = self.normalize_suburb_for_url(suburb)
        print(f"Searching {suburb} ({postcode}) - State: {state_electorate}...")
        
        search_url = f"{self.base_url}/australian-health-services/search/{suburb_url}-{postcode}-wa/gp-general-practice/788007007"
        
        try:
            response = requests.get(search_url, headers=self.headers, timeout=15)
            
            if response.status_code == 404:
                print(f"  No data available")
                return
            
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find all clinic links
            clinic_links = []
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
                    clinic_data = self.get_clinic_details(clinic_url, postcode, suburb, state_electorate)
                    if clinic_data:
                        self.results.append(clinic_data)
                    sleep(random.uniform(0.5, 1.5))
                except Exception as e:
                    print(f"    Error processing clinic: {e}")
                    continue
            
        except requests.exceptions.HTTPError as e:
            if '404' not in str(e):
                print(f"  HTTP error: {e}")
        except Exception as e:
            print(f"  Error: {e}")
    
    def get_clinic_details(self, url, search_postcode, search_suburb, state_electorate):
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
            
            # Extract suburb and postcode
            suburb = search_suburb
            postcode = search_postcode
            
            if address_text:
                pc_match = re.search(r'\b(\d{4})\b', address_text)
                if pc_match:
                    postcode = pc_match.group(1)
                
                suburb_match = re.search(r'([A-Z][A-Z\s]+)\s+WA\s+\d{4}', address_text)
                if suburb_match:
                    suburb = suburb_match.group(1).strip()
            
            # Get FEDERAL electorate
            federal_electorate = self.get_federal_electorate(postcode, suburb)
            
            # Extract phone
            phone = "N/A"
            phone_elem = soup.find('a', href=re.compile(r'tel:'))
            if phone_elem:
                phone = phone_elem.get_text(strip=True)
            
            # CHECK FOR BULK BILLING
            page_text = soup.get_text().lower()
            
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
                'state_electorate': state_electorate,
                'federal_electorate': federal_electorate,
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
            print(f"    Error getting details: {e}")
            return None
    
    def save_results(self, filename='wa_bulk_billing_by_electorate.json'):
        """Save results organized by BOTH state and federal electorates"""
        
        # Remove duplicates
        unique_results = {}
        for clinic in self.results:
            url = clinic.get('url')
            if url not in unique_results:
                unique_results[url] = clinic
        
        self.results = list(unique_results.values())
        
        # Organize by STATE electorate
        by_state = defaultdict(list)
        # Organize by FEDERAL electorate
        by_federal = defaultdict(list)
        
        bulk_billed_count = 0
        
        for clinic in self.results:
            state_elect = clinic.get('state_electorate', 'Unknown')
            federal_elect = clinic.get('federal_electorate', 'Unknown')
            
            by_state[state_elect].append(clinic)
            by_federal[federal_elect].append(clinic)
            
            if clinic.get('is_bulk_billed'):
                bulk_billed_count += 1
        
        # Create STATE statistics
        state_stats = {}
        for electorate, clinics in by_state.items():
            bulk_billed = [c for c in clinics if c.get('is_bulk_billed')]
            state_stats[electorate] = {
                'total_clinics': len(clinics),
                'bulk_billed_clinics': len(bulk_billed),
                'percentage': round(len(bulk_billed) / len(clinics) * 100, 1) if clinics else 0
            }
        
        # Create FEDERAL statistics
        federal_stats = {}
        for electorate, clinics in by_federal.items():
            bulk_billed = [c for c in clinics if c.get('is_bulk_billed')]
            federal_stats[electorate] = {
                'total_clinics': len(clinics),
                'bulk_billed_clinics': len(bulk_billed),
                'percentage': round(len(bulk_billed) / len(clinics) * 100, 1) if clinics else 0
            }
        
        output = {
            'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'summary': {
                'total_clinics': len(self.results),
                'bulk_billed_clinics': bulk_billed_count,
                'state_electorates_covered': len(by_state),
                'federal_electorates_covered': len(by_federal)
            },
            'by_state_electorate': dict(by_state),
            'by_federal_electorate': dict(by_federal),
            'state_electorate_stats': state_stats,
            'federal_electorate_stats': federal_stats
        }
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        
        print(f"\n✓ Saved results to {filename}")
        print(f"  - {len(by_state)} state electorates")
        print(f"  - {len(by_federal)} federal electorates")
        print(f"  - {len(self.results)} total clinics")
        print(f"  - {bulk_billed_count} bulk-billed clinics")
        
        return filename
    
    def run(self):
        """Main execution"""
        print("=" * 70)
        print("WA GP BULK BILLING TRACKER - STATE + FEDERAL ELECTORATES")
        print("=" * 70)
        print(f"Searching {len(self.state_suburb_list)} suburb-postcode combinations...\n")
        
        # Search each suburb
        for i, entry in enumerate(self.state_suburb_list, 1):
            print(f"[{i}/{len(self.state_suburb_list)}] ", end="")
            self.search_gp_clinics(
                entry['suburb'],
                entry['postcode'],
                entry['state_electorate']
            )
            sleep(random.uniform(1, 3))
        
        print(f"\n{'=' * 70}")
        print(f"SCAN COMPLETE")
        print(f"={'=' * 70}")
        
        if self.results:
            self.save_results()
        else:
            print("\n⚠️  No clinics found.")
        
        print(f"\n✓ Done!")

if __name__ == "__main__":
    # Paths to your CSV files
    state_csv = "WA_Electorates_Suburbs_Postcodes.csv"
    federal_csv = "WA_Federal_Electorates_Suburbs_Postcodes.csv"
    
    tracker = WABulkBillingDualElectorates(state_csv, federal_csv)
    tracker.run()
