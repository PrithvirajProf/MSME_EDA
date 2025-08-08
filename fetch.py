import requests
import json
import os
import gzip
import time
from typing import List, Set, Dict, Any
import logging
import math
import pandas as pd

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DistrictDataFetcher:
    def __init__(self, base_url: str, output_dir: str = "complete_district_data"):
        self.base_url = base_url
        self.output_dir = output_dir
        self.processed_records: Set[str] = set()
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)

    def generate_record_id(self, record: dict) -> str:
        """Generate unique ID for a record to check duplicates"""
        key_fields = ['District', 'State', 'EnterpriseName', 'CommunicationAddress']
        id_parts = []
        for field in key_fields:
            value = record.get(field, record.get(field.capitalize(), ''))
            id_parts.append(str(value).strip().lower())
        return '|'.join(id_parts)

    def is_duplicate_record(self, record: dict) -> bool:
        """Check if record already exists"""
        record_id = self.generate_record_id(record)
        if record_id in self.processed_records:
            return True
        self.processed_records.add(record_id)
        return False

    def get_total_records(self, district: str) -> int:
        """Get total number of records for a district"""
        url = self.base_url.replace("LATUR", district.upper()).replace("limit=1000", "limit=1")
        
        try:
            response = requests.get(url, timeout=30,verify=False)
            response.raise_for_status()
            data = response.json()
            total = data.get('total', 0)
            logger.info(f"Total records for {district}: {total}")
            return total
        except Exception as e:
            logger.error(f"Error getting total for {district}: {e}")
            return 0

    def fetch_all_district_data(self, district: str, batch_size: int = 1000, max_retries: int = 3) -> List[dict]:
        """Fetch ALL data for a district using pagination"""
        
        # First, get total number of records
        total_records = self.get_total_records(district)
        if total_records == 0:
            return []
        
        # Calculate number of batches needed
        total_batches = math.ceil(total_records / batch_size)
        logger.info(f"Fetching {total_records} records for {district} in {total_batches} batches")
        
        all_records = []
        
        for batch in range(total_batches):
            offset = batch * batch_size
            batch_records = self.fetch_batch(district, offset, batch_size, max_retries)
            
            if batch_records:
                # Filter duplicates
                unique_batch_records = []
                for record in batch_records:
                    if not self.is_duplicate_record(record):
                        unique_batch_records.append(record)
                
                all_records.extend(unique_batch_records)
                logger.info(f"Batch {batch + 1}/{total_batches}: Got {len(unique_batch_records)} unique records")
            else:
                logger.warning(f"No data received for batch {batch + 1}/{total_batches}")
            
            # Rate limiting between batches
            if batch < total_batches - 1:
                time.sleep(1)
        
        logger.info(f"Total unique records fetched for {district}: {len(all_records)}")
        return all_records

    def fetch_batch(self, district: str, offset: int, limit: int, max_retries: int = 3) -> List[dict]:
        """Fetch a single batch of data with offset"""
        
        # Modify URL to include offset
        base_url_with_offset = self.base_url.replace("limit=1000", f"limit={limit}&offset={offset}")
        url = base_url_with_offset.replace("LATUR", district.upper())
        
        for attempt in range(max_retries):
            try:
                response = requests.get(url, timeout=30,verify=False)
                response.raise_for_status()
                
                data = response.json()
                records = data.get('records', [])
                
                return records
                
            except requests.exceptions.RequestException as e:
                logger.error(f"Request error for {district} offset {offset} (attempt {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    logger.error(f"Failed to fetch batch for {district} after {max_retries} attempts")
                    return []
            
            except json.JSONDecodeError as e:
                logger.error(f"JSON decode error for {district} offset {offset}: {e}")
                return []
            
            except Exception as e:
                logger.error(f"Unexpected error for {district} offset {offset}: {e}")
                return []

    def save_data(self, district: str, data: List[dict], total_available: int) -> None:
        """Save data with metadata about completeness"""
        if not data:
            logger.warning(f"No data to save for {district}")
            return
        
        # Create metadata
        metadata = {
            'district': district,
            'total_records_available': total_available,
            'records_fetched': len(data),
            'fetch_completion': f"{len(data)}/{total_available} ({len(data)/total_available*100:.1f}%)" if total_available > 0 else "N/A",
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
            'data': data
        }
        
        filename = f"{district.lower()}_complete_data.json.gz"
        filepath = os.path.join(self.output_dir, filename)
        
        try:
            with gzip.open(filepath, 'wt', encoding='utf-8') as f:
                json.dump(metadata, f, separators=(',', ':'), ensure_ascii=False)
            
            file_size = os.path.getsize(filepath)
            logger.info(f"Saved {len(data)}/{total_available} records for {district} ({file_size} bytes)")
        except Exception as e:
            logger.error(f"Error saving data for {district}: {e}")

    def process_districts(self, districts: List[str], batch_size: int = 1000, delay: float = 2.0) -> None:
        """Process all districts with complete data fetching"""
        logger.info(f"Starting to process {len(districts)} districts with complete data fetching")
        
        summary = {}
        
        for i, district in enumerate(districts, 1):
            logger.info(f"Processing district {i}/{len(districts)}: {district}")
            
            try:
                # Clear processed records for each district to avoid cross-district conflicts
                district_processed_records = set()
                temp_processed = self.processed_records.copy()
                self.processed_records = district_processed_records
                
                total_available = self.get_total_records(district)
                all_data = self.fetch_all_district_data(district, batch_size)
                self.save_data(district, all_data, total_available)
                
                # Store summary
                summary[district] = {
                    'total_available': total_available,
                    'fetched': len(all_data),
                    'completion_rate': f"{len(all_data)/total_available*100:.1f}%" if total_available > 0 else "0%"
                }
                
                # Restore global processed records
                self.processed_records = temp_processed
                
                if i < len(districts):
                    time.sleep(delay)
            except Exception as e:
                logger.error(f"Error processing {district}: {e}")
                summary[district] = {'error': str(e)}
                continue
        
        # Print final summary
        self.print_final_summary(summary)

    def print_final_summary(self, summary: Dict[str, Dict[str, Any]]) -> None:
        """Print comprehensive summary"""
        logger.info("=" * 80)
        logger.info("FINAL PROCESSING SUMMARY")
        logger.info("=" * 80)
        
        total_available = 0
        total_fetched = 0
        
        for district, stats in summary.items():
            if 'error' in stats:
                logger.info(f"{district}: ERROR - {stats['error']}")
            else:
                available = stats['total_available']
                fetched = stats['fetched']
                completion = stats['completion_rate']
                
                total_available += available
                total_fetched += fetched
                
                logger.info(f"{district}: {fetched:,}/{available:,} records ({completion})")
        
        logger.info("-" * 80)
        logger.info(f"TOTAL: {total_fetched:,}/{total_available:,} records "
                   f"({total_fetched/total_available*100:.1f}% overall completion)")
        logger.info("=" * 80)

def main():
    # Configuration
    BASE_URL = "https://api.data.gov.in/resource/8b68ae56-84cf-4728-a0a6-1be11028dea7?api-key=579b464db66ec23bdd000001f85d5882df0744e4705730a6dae2f1c9&format=json&limit=1000&filters%5BDistrict%5D=LATUR"

    # Districts array - populate with your districts
    """
    DISTRICTS = ['WAYANAD', 'INDORE', 'TIRUVANNAMALAI', 
        'ERODE', 'SAMASTIPUR', 'KANNUR', 'TIRUCHIRAPPALLI', 'KORBA', 'AMRITSAR', 'MEERUT', 
        'VIRUDHUNAGAR', 'SIROHI', 'TIKAMGARH', 'VAISHALI', 'BHAVNAGAR', 'ANANTHAPUR', 'MOGA', 
        'SOLAPUR', 'PRAKASAM', 'JHARSUGUDA', 'SAGAR', 'FARIDABAD', 'FARRUKHABAD', 'WEST GODAVARI', 
        'DHARWAD', 'GORAKHPUR', 'CHITOOR', 'KRISHNA', 'YADGIR', 'PALGHAR', 'ALIGARH', 'AURANGABAD', 
        'RAJGARH', 'DURG', 'BHILWARA', 'DUNGARPUR', 'PALI', 'GONDA', 'NAN DED', 'JIND', 'RAJKOT', 'THIRUVALLUR', 
        'KRISHNAGIRI', 'CUDDALORE', 'BIDAR', 'SIWAN', 'PUDUKKOTTAI', 'BURHANPUR', 'HAVERI', 'BALRAMPUR', 'HAPUR', 
        'MADURAI', 'NALGONDA', 'AGRA', 'KHORDHA', 'DHARMAPURI', 'NASHIK', 'LUDHIANA', 'MURSHIDABAD', 'MANSA', 'UDUPI', 
        'TUTICORIN', 'GANJAM', 'ERNAKULAM', 'JAIPUR', 'RANGA REDDI', 'CHURU', 'AMBALA', 'NAMAKKAL', 'JABALPUR', 'CHANDRAPUR', 
        'BALOD BAZAR', 'UJJAIN', 'JODHPUR', 'KARNAL', 'JAJAPUR', 'SAMBHAL', 'SIKAR', 'BHADOHI', 'ANAND', 'KANCHIPURAM', 'DAVANGERE', 
        'KATNI', 'MAHESANA', 'SONIPAT', 'PATIALA', 'HINGOLI', 'AMETHI', 'KAMRUP METRO', 'VARANASI', 'TUMAKURU', 'YAMUNANAGAR', 
        'HOSHIARPUR', 'GUNTUR', 'SATARA', 'SIRSA', 'RAIGARH', 'BEED', 'BARGARH', 'THRISSUR', 'BEGUSARAI', 'BALANGIR', 'MANDSAUR'
        , 'GOALPARA', 'BARAMULLA', 'SONITPUR', 'AYODHYA', 'BENGALURU (RURAL)', 'AZAMGARH', 'BARMER', 'ALIPURDUAR', 'CHITTORGARH'
        , 'EAST GODAVARI', 'MUZAFFARPUR', 'CHHINDWARA', 'BANAS KANTHA', 'KASGANJ', 'DHAR', 'AJMER', 'HOSHANGABAD', 'GUNA', 'PURI'
        , 'NAGAUR', 'MORADABAD', 'BASTI', 'DHANBAD', 'ROHTAS', 'RAJSAMAND', 'OSMANABAD', 'BHARATPUR', 'SAHARANPUR', 'SIDDIPET', 
        'KALABURAGI', 'KOZHIKODE', 'BANKURA', 'AGAR MALWA', 'HISAR', 'BIKANER', 'SURENDRANAGAR', 'SURGUJA', 'MAHASAMUND', 'SIVASAGAR', 
        'SITAPUR', 'KOLAR', 'GOPALGANJ', 'VIKARABAD', 'JALGAON', 'DEHRADUN', 'AKOLA', 'JALORE', 'BAGALKOT', 'MANDYA', 'AMRAVATI', 'GODDA', 
        'RUPNAGAR', 'TEHRI GARHWAL', 'BAREILLY', 'MATHURA', 'PALAKKAD', 'DHENKANAL', 'HASSAN', 'VIJAYAPURA', 'PARBHANI', 'GANDHINAGAR', 
        'SHAJAPUR', 'RAMPUR', 'GHAZIABAD', 'BUNDI', 'GANDERBAL', 'KULLU', 'DAUSA', 'BULDHANA', 'RATLAM', 'GURDASPUR', 'UTTAR KANNAD', 'LATUR', 
        'GANGANAGAR', 'FATEHGARH SAHIB', 'MYSURU', 'PURNIA', 'KARIMNAGAR', 'KOLLAM', 'MORBI', 'PULWAMA', 'VIDISHA', 'KANNAUJ', 'NAGAON', 'BHADRADRI KOTHAGUDEM', 'TIRUNELVELI', 
        'SOLAN', 'ALAPPUZHA', 'KENDUJHAR', 'SUPAUL', 'CUTTACK', 'KARUR', 'TONK', 'TARN TARAN', 'SAHARSA', 'ARIYALUR', 'SHIVAMOGGA', 'SRI MUKTSAR SAHIB', 'KANKER', 'KISHTWAR', 
        'GAYA', 'JALPAIGURI', 'DIBRUGARH', 'DHULE', 'BOTAD', 'BARNALA', 'MADHEPURA', 'PONDICHERRY', 'MUNGER', 'RAMANAGARA', 'SINDHUDURG', 'KAUSHAMBI', 'SURYAPET', 'HOWRAH',
        'EAST SINGHBHUM', 'BILASPUR', 'NALANDA', 'WEST MEDINIPUR', 'THENI', 'JEHANABAD', 'SANT KABEER NAGAR', 'DEOGHAR', 'KAPURTHALA', 'KENDRAPARA', 'RAMANATHAPURAM',
        'KATHUA', 'MADHUBANI', 'SARAN', 'IMPHAL EAST', 'KATIHAR', 'DARRANG', 'PURBA BARDHAMAN', 'HARDOI', 'RAIPUR', 'SITAMARHI', 'AMBEDAKAR NAGAR', 'PRAYAGRAJ', 'KOHIMA',
        'LUNGLEI', 'NEEMUCH', 'RATNAGIRI', 'NAGARKURNOOL', 'PANNA', 'YANAM', 'SHAMLI', 'KANNIYAKUMARI', 'SHAHID BHAGAT SINGH NAGAR', 'CHIKKAMAGALURU', 'FATEHPUR', 'MANDI', 
        'KALAHANDI', 'ANGUL', 'MAHARAJGANJ', 'UNA', 'REWA', 'ETAH', 'MALAPPURAM', 'YADADRI BHUVANAGIRI', 'DEORIA', 'DADRA AND NAGAR HAVELI', 'JALNA', 'MORENA', 'ARARIA', 'BHARUCH', 
        'COOCHBEHAR', 'PILIBHIT', 'BALLIA', 'PURULIA', 'BHIWANI', 'NAINITAL', 'SHAHJAHANPUR', 'JAISALMER', 'BARPETA', 'SHIMLA', 'IMPHAL WEST', 'KUPWARA', 'GIRIDIH', 'GHAZIPUR',
        'KANGRA', 'KURUKSHETRA', 'LEH LADAKH', 'SATNA', 'YAVATMAL', 'FAZILKA', 'BANSWARA', 'CHIKBALLAPUR', 'Eluru', 'RAE BARELI', 'BALESHWAR', 'HAZARIBAG ', 'JAUNPUR', 'BIRBHUM', 
        'CHURACHANDPUR', 'NAVSARI', 'RAJOURI', 'MUZAFFARNAGAR', 'DEWAS', 'BIJNOR', 'KOTTAYAM', 'DUMKA', 'BAKSA', 'KHEDA', 'RAISEN', 'SEHORE', 'CHAMARAJNAGAR', 'LAKHIMPUR', 
        'SAHEBGANJ', 'EAST MEDINIPUR', 'VIJAYANAGAR', 'SRIKAKULAM', 'VELLORE', 'DAKSHIN DINAJPUR', 'BARABANKI', 'RAICHUR', 'MAHISAGAR', 'KAITHAL', 'NAYAGARH', 'KISHANGANJ',
        'AMROHA', 'PATHANAMTHIPTA', 'PAURI GARHWAL', 'NUAPADA', 'WARDHA', 'UNNAO', 'SANGRUR', 'CHHATARPUR', 'BULANDSHAHAR', 'DANTEWADA', 'KUSHINAGAR', 'HOOGHLY', 'FIROZABAD',
        'VILLUPURAM', 'SURAJPUR', 'PALWAL', 'KHAMMAM', 'PAKUR ', 'GUMLA ', 'DAMAN', 'BUXAR', 'KORAPUT', 'ADILABAD', 'WEST TRIPURA', 'ETAWAH', 'BONGAIGAON', 'RAMBAN', 
        'MAINPURI', 'BHANDARA', 'SEONI', 'BALAGHAT', 'MAU', 'BEMETARA', 'BHAGALPUR', 'NIZAMABAD', 'BASTAR', 'MAHABUBNAGAR', 'KAMAREDDY', 'REWARI', 'RI BHOI', 'BOUDH', 
        'DINDIGUL', 'HARIDWAR', 'GADCHIROLI', 'AIZAWL', 'JHANSI', 'LATEHAR', 'PALAMU', 'ALMORA', 'DODA', 'JHALAWAR', 'KABIRDHAM', 'SHAHDARA', 'NABARANGPUR', 'UTTAR DINAJPUR',
        'MALDA', 'NAWADA', 'CHAMPAWAT', 'FIROZEPUR', 'MAHE', 'PATHANKOT', 'PORBANDAR', 'DARBHANGA', 'KHAGARIA', 'WASHIM', 'NALBARI', 'GIR SOMNATH', 'ASHOKNAGAR', 'KULGAM',
        'BANDIPORA', 'UDHAM SINGH NAGAR', 'DHUBRI', 'SAMBA', 'CHITRADURGA', 'JOGULAMBA GADWAL', 'RAJNANDAGAON', 'BANKA', 'MAHENDRAGARH', 'PATAN', 'SABAR KANTHA', 'GAJAPATI',
        'DHEMAJI', 'SINGRAULI', 'BUDAUN', 'NARSINGHPUR', 'TINSUKIA', 'KANPUR DEHAT', 'Tirupati', 'MARIGAON', 'BALOD', 'PEDDAPALLI', 'WANAPARTHY', 'ARVALLI', 'UMARIA', 
        'MIRZAPUR', 'LOHARDAGA ', 'SAMBALPUR', 'SUNDARGARH', 'GARHWA ', 'NUH', 'EAST NIMAR', 'DINDORI', 'MAHABUBABAD', 'EAST SIANG', 'GADAG', 'HAMIRPUR', 'NAGAPATTINAM',
        'EAST KHASI HILLS', 'SIMDEGA ', 'Nandyal', 'NTR', 'CHANDAULI', 'SIRMAUR', 'Nirmal', 'Ranipet', 'FARIDKOT', 'PANCHKULA', 'LAKHISARAI', 'BARAN', 'MANDLA', 'NANDURBAR',
        'PERAMBALUR', 'AMRELI', 'SHIVPURI', 'CHITRAKOOT', 'SOUTH TRIPURA', 'DIMAPUR', 'GOMATI', 'JALAUN', 'BARWANI', 'WEST GARO HILLS', 'DATIA', 'SAWAI MADHOPUR', 'KODAGU',
        'KANGPOKPI', 'HARDA', 'SIDDHARTHNAGAR', 'Kakinada', 'Konaseema', 'UDHAMPUR', 'DAMOH', 'BISHNUPUR', 'SHEOHAR', 'KAIMUR (BHABUA)', 'DOHAD', 'BETUL', 'SHEIKHPURA', 
        'POONCH', 'NARMADA', 'JHABUA', 'DHOLPUR', 'SENAPATI', 'Gyalshing', 'DHAMTARI', 'KARAULI', 'KARAIKAL', 'KHAIRGARH CHHUIKHADAN GANDAI', 
        'MANENDRAGARH CHIRIMIRI BHARATPUR', 'FATEHABAD', 'SONBHADRA', 'LALITPUR', 'Didwana Kuchaman', 'THOUBAL', 'KANDHAMAL', 'HOJAI', 'JAGATSINGHPUR', 
        'MANCHERIAL', 'DEOGARH', 'CHHOTAUDEPUR', 'KONDAGAON', 'Jaipur Gramin', 'UTTARKASHI', 'Warangal', 'BAHRAICH', 'SUKMA', 'BANDA', 'ANUPPUR', 'SIDHI',
        'KODERMA ', 'BHIND', 'RUDRA PRAYAG', 'MAHOBA', 'JORHAT', 'Sri Sathya Sai', 'CHAMPHAI', 'NORTH TRIPURA', 'BAGHPAT', 'Anakapalli', 'Palnadu', 'RAYAGADA', 
        'JASHPUR', 'HAILAKANDI', 'MUNGELI', 'JANGOAN', 'MOKOKCHUNG', 'SOUTH WEST  GARO HILLS', 'REASI', 'CHAMBA', 'CHARAIDEO', 'Mulugu', 'LAHUL AND SPITI', 'CHAMOLI', 
        'KOMARAM BHEEM ASIFABAD', 'Soreng', 'SEPAHIJALA', 'MALKANGIRI  ', 'ARWAL', 'UDALGURI', 'DHALAI', 'SONEPUR', 'RAJANNA SIRCILLA', 'NIWARI', 'PHEK', 'Bapatla', 
        'Annamayya', 'Namchi', 'KHOWAI', 'Balotra', 'PAPUM PARE', 'Gangapurcity', 'Jagitial', 'KALIMPONG', 'UKHRUL', 'Parvathipuram Manyam', 'KARGIL', 'SAKTI', 
        'WEST KAMENG', 'CHIRANG', 'Pandhurna', 'SHEOPUR', 'EAST KAMENG', 'KHUNTI ', 'TAPI', 'LAKSHADWEEP DISTRICT', 'GARIABAND', 'KARBI-ANGELONG', 'KAKCHING', 
        'CHARKI DADRI', 'WEST KARBI ANGLONG', 'KOLASIB', 'NIKOBARS', 'MALERKOTLA', 'Khawzawl', 'JAMTARA', 'ALIRAJPUR', 'DIMA HASAO', 'Jhargram', 'DANG', 
        'Kotputli-Behror', 'KINNAUR', 'MAMIT', 'Anoopgarh', 'TAMULPUR', 'WEST SIANG', 'EAST JAINTIA HILLS', 'NARAYANPUR', 'LONGLENG', 'BIJAPUR', 
        'SOUTH SALMARA MANCACHAR', 'LAWNGTLAI', 'Beawar', 'TAWANG', 'SARANGARH BILAIGARH', 'EASTERN WEST KHASI HILLS', 'Chumoukedima', 'JIRIBAM', 
        'MON', 'WEST JAINTIA HILLS', 'Gaurella Pendra Marwahi', 'KRA DAADI', 'WEST KHASI HILLS', 'SOUTH GARO HILLS', 'WOKHA', 'MAJULI', 'PEREN', 'KURUNG KUMEY',
        'KAMJONG', 'CHANDEL', 'DIU', 'TUENSANG', 'Alluri Sitharama Raju', 'BAJALI', 'Hnahthial', 'Mangan', 'SERCHHIP', 'LOWER SUBANSIRI', 'NAMSAI', 
        'PAKKE KESSANG', 'Jodhpur Gramin', 'TAMENGLONG', 'EAST GARO HILLS', 'TENGNOUPAL', 'MOHLA MANPUR AMBAGARH CHOUKI', 'SAIHA', 'KAMLE', 'LONGDING',
        'Pakyong', 'CHANGLANG', 'LOWER SIANG', 'ZUNHEBOTO', 'NONEY', 'KIPHRIE', 'Kekri', 'TIRAP', 'Neem Ka Thana', 'Saitual', 'Khairthal-Tijara', 'Dudu', 
        'UPPER SUBANSIRI', 'LOHIT', 'Tseminyu', 'Sanchor', 'Deeg', 'NORTH GARO HILLS', 'Noklak']  # Add your district names here
    """
    

    DISTRICTS = ['LATUR']
    
    if not DISTRICTS:
        logger.error("Please populate the DISTRICTS list with district names")
        return
    
    # Initialize fetcher
    fetcher = DistrictDataFetcher(BASE_URL)
    
    # Process all districts with complete data fetching
    fetcher.process_districts(DISTRICTS, batch_size=1000, delay=2.0)

if __name__ == "__main__":
    main()