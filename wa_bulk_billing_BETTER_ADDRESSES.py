"""
WA GP Bulk Billing Tracker - IMPROVED ADDRESS EXTRACTION
Better address scraping from healthdirect.gov.au
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

class WABulkBillingImprovedAddresses:
    def __init__(self, state_csv_path, federal_csv_path):
        self.base_url = "https://www.healthdirect.gov.au"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        self.results = []
        
        self.state_mapping, self.state_suburb_list = self.load_state_mapping(state_csv_path)
        self.federal_mapping = self.load_federal_mapping(federal_csv_path)
        
    def load_state_mapping(self, csv_path):
        """Load STATE electorate mapping"""
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
        """Load FEDERAL electorate mapping"""
        mapping = defaultdict(list)
        
        try:
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    federal_electorate = row['Federal_Electorate']
                    suburb = row['Suburb']
                    postcode = row['Postcode']
                    
                    key = f"{postcode}_{suburb.lower().strip()}"
                    mapping[key] = federal_electorate
            
            print(f"✓ Loaded FEDERAL mapping: {len(mapping)} suburb-postcode combinations")
            print(f"✓ Covering {len(set(mapping.values()))} federal electorates\n")
            return mapping
        except Exception as e:
            print(f"WARNING: Could not load federal CSV: {e}")
            return {}
    
    def get_state_electorate(self, postcode, suburb):
        """Find STATE electorate"""
        if postcode not in self.state_mapping:
            return "Unknown"
        
        suburb_normalized = suburb.lower().strip()
        
        for entry in self.state_mapping[postcode]:
            if entry['suburb'].lower().strip() == suburb_normalized:
                return entry['electorate']
        
        return self.state_mapping[postcode][0]['electorate']
    
    def get_federal_electorate(self, postcode, suburb):
        """Find FEDERAL electorate"""
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
    
    def extract_address_better(self, soup):
        """IMPROVED address extraction with multiple methods"""
        
        # METHOD 1: Look for <address> tag
        address_elem = soup.find('address')
        if address_elem:
            address_text = address_elem.get_text(strip=True)
            # Clean up the address
            address_text = ' '.join(address_text.split())  # Remove extra whitespace
            if len(address_text) > 10 and 'WA' in address_text:
                return address_text
        
        # METHOD 2: Look for text containing street patterns
        # Common patterns: "123 Main Street", "Unit 5, 123 Main St"
        for elem in soup.find_all(['p', 'div', 'span']):
            text = elem.get_text(strip=True)
            # Look for patterns like: number + street name + WA + postcode
            if re.search(r'\d+\s+[A-Z][a-zA-Z\s]+(?:Street|St|Road|Rd|Avenue|Ave|Drive|Dr|Boulevard|Blvd|Lane|Ln|Court|Ct|Terrace|Tce|Way|Highway|Hwy|Place|Pl|Crescent|Cres).*WA\s+\d{4}', text, re.IGNORECASE):
                address_text = ' '.join(text.split())
                return address_text
        
        # METHOD 3: Look for "Get directions" link or similar
        directions_link = soup.find('a', string=re.compile(r'Get directions|View map|Map', re.IGNORECASE))
        if directions_link:
            # Try to find address near this link
            parent = directions_link.find_parent(['div', 'section', 'article'])
            if parent:
                text = parent.get_text(strip=True)
                # Extract address-like text
                match = re.search(r'(?:Address[:\s]*)?([^·]+WA\s+\d{4})', text, re.IGNORECASE)
                if match:
                    return match.group(1).strip()
        
        # METHOD 4: Look for structured data (schema.org markup)
        structured_data = soup.find_all('script', type='application/ld+json')
        for script in structured_data:
            try:
                data = json.loads(script.string)
                if isinstance(data, dict):
                    if 'address' in data:
                        addr = data['address']
                        if isinstance(addr, dict):
                            street = addr.get('streetAddress', '')
                            locality = addr.get('addressLocality', '')
                            region = addr.get('addressRegion', '')
                            postal = addr.get('postalCode', '')
                            if street and locality:
                                return f"{street}, {locality} {region} {postal}".strip()
            except:
                continue
        
        # METHOD 5: Look for any text with WA and 4-digit postcode
        all_text = soup.get_text()
        # Find the most address-like text
        matches = re.findall(r'([^\.]+?\d+[^\.]*?WA\s+\d{4})', all_text, re.IGNORECASE)
        for match in matches:
            match = match.strip()
            # Filter out obviously wrong matches (too short, no street name, etc.)
            if len(match) > 15 and any(keyword in match.lower() for keyword in ['street', 'st', 'road', 'rd', 'avenue', 'ave', 'drive', 'dr']):
                return ' '.join(match.split())
        
        return None
    
    def get_clinic_details(self, url, search_postcode, search_suburb, state_electorate):
        """Get detailed clinic information with IMPROVED address extraction"""
        try:
            response = requests.get(url, headers=self.headers, timeout=15)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Extract clinic name
            name_elem = soup.find('h1') or soup.find('h2')
            clinic_name = name_elem.get_text(strip=True) if name_elem else "Unknown"
            
            # IMPROVED ADDRESS EXTRACTION
            address_text = self.extract_address_better(soup)
            
            # Extract suburb and postcode from address if we got one
            suburb = search_suburb
            postcode = search_postcode
            
            if address_text:
                # Extract postcode
                pc_match = re.search(r'\b(\d{4})\b', address_text)
                if pc_match:
                    postcode = pc_match.group(1)
                
                # Extract suburb (word(s) before WA)
                suburb_match = re.search(r'([A-Z][A-Z\s]+)\s+WA\s+\d{4}', address_text)
                if suburb_match:
                    suburb = suburb_match.group(1).strip()
            else:
                # Fallback: just use suburb and postcode
                address_text = f"{search_suburb}, WA {search_postcode}"
            
            # Get federal electorate
            federal_electorate = self.get_federal_electorate(postcode, suburb)
            
            # Extract phone
            phone = "N/A"
            phone_elem = soup.find('a', href=re.compile(r'tel:'))
            if phone_elem:
                phone = phone_elem.get_text(strip=True)
            else:
                # Try to find phone number in text
                phone_match = re.search(r'(?:Phone[:\s]*)?(\d{2}\s?\d{4}\s?\d{4})', soup.get_text())
                if phone_match:
                    phone = phone_match.group(1)
            
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
                'address': address_text,
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
                if address_text and len(address_text) > 20:
                    print(f"      Address: {address_text[:50]}...")
            
            return clinic_data
            
        except Exception as e:
            print(f"    Error getting details: {e}")
            return None
    
    def save_results(self, filename='wa_bulk_billing_by_electorate.json'):
        """Save results"""
        
        # Remove duplicates
        unique_results = {}
        for clinic in self.results:
            url = clinic.get('url')
            if url not in unique_results:
                unique_results[url] = clinic
        
        self.results = list(unique_results.values())
        
        # Organize by state and federal
        by_state = defaultdict(list)
        by_federal = defaultdict(list)
        
        bulk_billed_count = 0
        
        for clinic in self.results:
            state_elect = clinic.get('state_electorate', 'Unknown')
            federal_elect = clinic.get('federal_electorate', 'Unknown')
            
            by_state[state_elect].append(clinic)
            by_federal[federal_elect].append(clinic)
            
            if clinic.get('is_bulk_billed'):
                bulk_billed_count += 1
        
        # Create statistics
        state_stats = {}
        for electorate, clinics in by_state.items():
            bulk_billed = [c for c in clinics if c.get('is_bulk_billed')]
            state_stats[electorate] = {
                'total_clinics': len(clinics),
                'bulk_billed_clinics': len(bulk_billed),
                'percentage': round(len(bulk_billed) / len(clinics) * 100, 1) if clinics else 0
            }
        
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
        print("WA GP BULK BILLING TRACKER - IMPROVED ADDRESS EXTRACTION")
        print("=" * 70)
        print(f"Searching {len(self.state_suburb_list)} suburb-postcode combinations...\n")
        
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
    state_csv = "WA_Electorates_Suburbs_Postcodes.csv"
    federal_csv = "WA_Federal_Electorates_COMPLETE.csv"
    
    tracker = WABulkBillingImprovedAddresses(state_csv, federal_csv)
    tracker.run()
