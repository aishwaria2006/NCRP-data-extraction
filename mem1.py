"""
NCRP Data Ingestion Module
Handles CSV, Excel, and PDF files with robust error handling and data validation
"""

import json
import re
import hashlib
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any
import pandas as pd
import PyPDF2
from dateutil import parser as date_parser
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class NCRPDataIngestionAgent:
    """Main agent for NCRP data ingestion with validation and cleaning"""
    
    def __init__(self):
        self.processed_complaints = set()  # For duplicate detection
        self.standard_schema = self._get_standard_schema()
        
    def _get_standard_schema(self) -> Dict:
        """Define the standard NCRP output schema"""
        return {
            # Primary identifiers
            "complaint_id": None,
            "acknowledgement_number": None,
            
            # DateTime fields (flattened for easy access)
            "date_time": None,  # Combined incident date and time
            "incident_datetime": None,
            "complaint_datetime": None,
            
            # Complainant details (flattened key fields)
            "complainant_name": None,
            "district": None,
            "crime_type": None,
            "platform": None,
            "amount_lost": 0.0,
            "status": "Under Process",
            
            # Detailed nested structure
            "complainant_details": {
                "name": None,
                "mobile": None,
                "email": None,
                "address": {
                    "street": None,
                    "house_no": None,
                    "colony": None,
                    "village_town": None,
                    "pincode": None,
                    "police_station": None,
                    "district": None,
                    "state": None
                }
            },
            "crime_details": {
                "complaint_type": None,
                "category": None,
                "sub_category": None,
                "description": None
            },
            "financial_details": {
                "total_fraud_amount": 0.0,
                "currency": "INR"
            },
            "transactions": [],
            "actions_taken": [],
            
            # Metadata
            "source_file": None,
            "metadata": {
                "processing_timestamp": None,
                "data_quality_score": 0.0,
                "validation_status": None,
                "is_duplicate": False
            }
        }
    
    def process_file(self, file_path: str) -> Dict:
        """
        Main entry point - detects file type and processes accordingly
        """
        try:
            file_path = Path(file_path)
            
            if not file_path.exists():
                raise FileNotFoundError(f"File not found: {file_path}")
            
            logger.info(f"Processing file: {file_path.name}")
            
            # Step 1: Detect file type and load
            file_extension = file_path.suffix.lower()
            
            if file_extension == '.csv':
                raw_data = self._load_csv(file_path)
            elif file_extension in ['.xlsx', '.xls']:
                raw_data = self._load_excel(file_path)
            elif file_extension == '.pdf':
                raw_data = self._load_pdf(file_path)
            else:
                raise ValueError(f"Unsupported file format: {file_extension}")
            
            # Step 2: Extract and structure data
            structured_data = self._extract_data(raw_data, file_path.name)
            
            # Step 3: Clean and normalize
            cleaned_data = self._clean_and_normalize(structured_data)
            
            # Step 4: Flatten key fields for easy access
            cleaned_data = self._flatten_key_fields(cleaned_data)
            
            # Step 5: Validate and quality check
            validated_data = self._validate_data(cleaned_data)
            
            # Step 6: Check for duplicates
            if self._is_duplicate(validated_data):
                logger.warning(f"Duplicate complaint detected: {validated_data['complaint_id']}")
                validated_data['metadata']['is_duplicate'] = True
            else:
                validated_data['metadata']['is_duplicate'] = False
                self._register_complaint(validated_data)
            
            # Step 7: Add processing metadata
            validated_data['metadata']['processing_timestamp'] = datetime.now().isoformat()
            validated_data['source_file'] = file_path.name
            
            logger.info(f"Successfully processed complaint: {validated_data['complaint_id']}")
            
            return {
                "status": "success",
                "data": validated_data,
                "summary": self._generate_summary(validated_data)
            }
            
        except Exception as e:
            logger.error(f"Error processing file: {str(e)}")
            return {
                "status": "error",
                "error": str(e),
                "error_type": type(e).__name__
            }
    
    def _load_csv(self, file_path: Path) -> pd.DataFrame:
        """Load CSV file with multiple encoding attempts"""
        encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
        
        for encoding in encodings:
            try:
                df = pd.read_csv(file_path, encoding=encoding)
                logger.info(f"CSV loaded successfully with {encoding} encoding")
                return df
            except UnicodeDecodeError:
                continue
        
        raise ValueError("Failed to load CSV with any supported encoding")
    
    def _load_excel(self, file_path: Path) -> pd.DataFrame:
        """Load Excel file"""
        try:
            df = pd.read_excel(file_path, sheet_name=0)
            logger.info("Excel file loaded successfully")
            return df
        except Exception as e:
            raise ValueError(f"Failed to load Excel file: {str(e)}")
    
    def _load_pdf(self, file_path: Path) -> Dict:
        """Extract text from PDF and parse"""
        try:
            with open(file_path, 'rb') as file:
                pdf_reader = PyPDF2.PdfReader(file)
                text = ""
                for page in pdf_reader.pages:
                    text += page.extract_text()
            
            logger.info("PDF text extracted successfully")
            return self._parse_pdf_text(text)
        except Exception as e:
            raise ValueError(f"Failed to load PDF file: {str(e)}")
    
    def _parse_pdf_text(self, text: str) -> Dict:
        """Parse extracted PDF text into structured format"""
        data = {}
        
        # Extract acknowledgement number
        ack_match = re.search(r'Acknowledgement Number\s*:\s*(\d+)', text)
        if ack_match:
            data['acknowledgement_number'] = ack_match.group(1)
        
        # Extract complaint ID (use ack number if no separate ID found)
        complaint_match = re.search(r'(?:Complaint.*?|ID.*?)(\d{10,})', text)
        if complaint_match:
            data['complaint_id'] = complaint_match.group(1)
        elif ack_match:
            data['complaint_id'] = ack_match.group(1)
        
        # Extract dates
        incident_date_match = re.search(r'Incident Date/Time\s*(\d{2}/\d{2}/\d{4})\s*(\d{2}\s*:\s*\d{2}(?:\s*:\s*\d{2})?(?:\s*[AP]M)?)', text, re.IGNORECASE)
        if incident_date_match:
            data['incident_date'] = incident_date_match.group(1)
            data['incident_time'] = incident_date_match.group(2).replace(' ', '')
        
        complaint_date_match = re.search(r'Complaint Date\s*(\d{2}/\d{1,2}/\d{4})', text)
        if complaint_date_match:
            data['complaint_date'] = complaint_date_match.group(1)
        
        # Extract complainant details
        name_match = re.search(r'Name\s*([^\n]+?)(?:\s*Mobile|\s*Email|\s*Street)', text)
        if name_match:
            name = name_match.group(1).strip()
            # Remove asterisks and clean
            name = re.sub(r'\*+', '', name).strip()
            data['name'] = name
        
        mobile_match = re.search(r'Mobile\s*(\d+)', text)
        if mobile_match:
            data['mobile'] = mobile_match.group(1)
        
        email_match = re.search(r'Email\s*([^\s]+@[^\s]+)', text)
        if email_match:
            data['email'] = email_match.group(1)
        
        # Extract address components
        street_match = re.search(r'Street Name\s*([^\n]+)', text)
        if street_match:
            data['street'] = street_match.group(1).strip()
        
        house_match = re.search(r'House No\s*([^\n]+?)(?:\s*Colony|\n)', text)
        if house_match:
            data['house_no'] = house_match.group(1).strip()
        
        colony_match = re.search(r'Colony\s*([^\n]+)', text)
        if colony_match:
            data['colony'] = colony_match.group(1).strip()
        
        village_match = re.search(r'Village/\s*Town\s*([^\n]+)', text)
        if village_match:
            data['village_town'] = village_match.group(1).strip()
        
        police_station_match = re.search(r'Police Station\s*([^\n]+)', text)
        if police_station_match:
            data['police_station'] = police_station_match.group(1).strip()
        
        district_match = re.search(r'District\s*([^\n]+?)(?:\s*State|\n)', text)
        if district_match:
            data['district'] = district_match.group(1).strip()
        
        state_match = re.search(r'State\s*([^\n]+)', text)
        if state_match:
            data['state'] = state_match.group(1).strip()
        
        pincode_match = re.search(r'Pincode\s*(\d+)', text)
        if pincode_match:
            data['pincode'] = pincode_match.group(1)
        
        # Extract crime details
        complaint_type_match = re.search(r'Complaint Type\s*:\s*([^\n]+)', text)
        if complaint_type_match:
            data['complaint_type'] = complaint_type_match.group(1).strip()
        
        category_match = re.search(r'Category of complaint\s*([^\n]+)', text)
        if category_match:
            data['category'] = category_match.group(1).strip()
        
        sub_category_match = re.search(r'Sub Category of Complaint\s*([^\n]+)', text)
        if sub_category_match:
            data['sub_category'] = sub_category_match.group(1).strip()
        
        description_match = re.search(r'Additional Information.*?Content\s*([^\n]+)', text)
        if description_match:
            data['description'] = description_match.group(1).strip()
        
        # Extract total fraud amount
        amount_match = re.search(r'Total Fraudulent Amount.*?:\s*([\d,]+\.?\d*)', text)
        if amount_match:
            data['total_fraud_amount'] = amount_match.group(1).replace(',', '')
        
        # Extract status
        status_match = re.search(r'Status\s*[:\-]?\s*([^\n]+)', text)
        if status_match:
            data['status'] = status_match.group(1).strip()
        else:
            # Check for "Under Process"
            if 'Under Process' in text:
                data['status'] = 'Under Process'
            elif 'Complaint Accepted' in text:
                data['status'] = 'Complaint Accepted'
        
        # Extract transactions
        data['transactions'] = self._extract_transactions_from_pdf(text)
        
        # Extract actions taken
        data['actions_taken'] = self._extract_actions_from_pdf(text)
        
        # Extract platform from transactions or sub-category
        data['platform'] = self._extract_platform(data, text)
        
        return data
    
    def _extract_platform(self, data: Dict, text: str) -> str:
        """Extract platform/payment method from available data"""
        # Check sub-category first
        sub_category = data.get('sub_category', '')
        if 'UPI' in sub_category:
            # Try to find specific platform in text
            platforms = ['PhonePe', 'Google Pay', 'GPay', 'Paytm', 'Amazon Pay', 'BHIM']
            for platform in platforms:
                if re.search(platform, text, re.IGNORECASE):
                    return platform
            return 'UPI'
        elif 'Card' in sub_category:
            return 'Card'
        elif 'Net Banking' in sub_category or 'Banking' in sub_category:
            return 'Net Banking'
        
        # Check transactions for bank names
        transactions = data.get('transactions', [])
        if transactions and len(transactions) > 0:
            first_trans = transactions[0]
            if 'bank' in first_trans:
                return first_trans['bank']
        
        # Check actions for beneficiary info
        actions = data.get('actions_taken', [])
        for action in actions:
            if action.get('beneficiary_bank'):
                return action['beneficiary_bank']
        
        return 'Unknown'
    
    def _extract_transactions_from_pdf(self, text: str) -> List[Dict]:
        """Extract transaction details from PDF text"""
        transactions = []
        
        # Find transaction section
        trans_section = re.search(r'Debited Transaction Details(.+?)(?:Action Taken by bank|Total Fraudulent Amount)', text, re.DOTALL)
        if not trans_section:
            return transactions
        
        trans_text = trans_section.group(1)
        
        # Extract bank name pattern
        bank_pattern = r'([\w\s]+Bank[^\n]*?)\s+(\d+)\s+([\d,]+)\s+(\d{2}/\d{2}/\d{4})'
        
        # Try to extract with bank name
        trans_matches = re.finditer(bank_pattern, trans_text)
        
        for match in trans_matches:
            transaction = {
                "bank": match.group(1).strip(),
                "account_number": match.group(2),
                "amount": float(match.group(3).replace(',', '')),
                "transaction_date": match.group(4),
                "status": "reported"
            }
            transactions.append(transaction)
        
        # If no matches with bank, try simpler pattern
        if not transactions:
            simple_pattern = r'(\d{10,})\s+([\d,]+)\s+(\d{2}/\d{2}/\d{4})'
            trans_matches = re.finditer(simple_pattern, trans_text)
            
            for match in trans_matches:
                transaction = {
                    "transaction_id": match.group(1),
                    "amount": float(match.group(2).replace(',', '')),
                    "transaction_date": match.group(3),
                    "status": "reported"
                }
                transactions.append(transaction)
        
        return transactions
    
    def _extract_actions_from_pdf(self, text: str) -> List[Dict]:
        """Extract action taken details from PDF text"""
        actions = []
        
        # Find action section
        action_section = re.search(r'Action Taken by bank(.+?)(?:Complaint Accepted|Action Taken\s*$|$)', text, re.DOTALL)
        if not action_section:
            return actions
        
        action_text = action_section.group(1)
        
        # Extract money transfer actions
        action_pattern = r'Money Transfer to\s+([^\n]+?)\s+(\d+)\s+([\d,]+)'
        action_matches = re.finditer(action_pattern, action_text)
        
        for match in action_matches:
            action = {
                "action_type": "Money Transfer",
                "beneficiary_bank": match.group(1).strip(),
                "account_number": match.group(2),
                "amount": float(match.group(3).replace(',', '')),
                "status": "transferred"
            }
            actions.append(action)
        
        # Extract hold actions
        hold_pattern = r'Transaction put on hold\s+([^\n]+?)\s+([\d,]+)'
        hold_matches = re.finditer(hold_pattern, action_text)
        
        for match in hold_matches:
            action = {
                "action_type": "Transaction Hold",
                "bank": match.group(1).strip(),
                "amount": float(match.group(2).replace(',', '')),
                "status": "on_hold"
            }
            actions.append(action)
        
        # Extract old transaction actions
        if 'Old Transaction' in action_text:
            old_matches = re.finditer(r'Old Transaction\s+([^\n]+)', action_text)
            for match in old_matches:
                action = {
                    "action_type": "Old Transaction",
                    "bank": match.group(1).strip(),
                    "status": "old_txn",
                    "remarks": "Please visit nearby branch"
                }
                actions.append(action)
        
        return actions
    
    def _extract_data(self, raw_data: Any, source_file: str) -> Dict:
        """Extract data into standard schema"""
        schema = self._get_standard_schema()
        schema['source_file'] = source_file
        
        if isinstance(raw_data, pd.DataFrame):
            # Handle CSV/Excel
            if len(raw_data) > 0:
                row = raw_data.iloc[0].to_dict()
                schema = self._map_dataframe_to_schema(row, source_file)
        elif isinstance(raw_data, dict):
            # Handle PDF parsed data
            schema = self._map_pdf_to_schema(raw_data, source_file)
        
        return schema
    
    def _map_dataframe_to_schema(self, row: Dict, source_file: str) -> Dict:
        """Map DataFrame row to standard schema"""
        schema = self._get_standard_schema()
        schema['source_file'] = source_file
        
        # Map fields with flexible column name matching
        column_mappings = {
            'complaint_id': ['complaint_id', 'complaintid', 'id', 'complaint_number', 'ack_no'],
            'acknowledgement_number': ['acknowledgement_number', 'ack_number', 'ack_no'],
            'name': ['name', 'complainant_name', 'complainant'],
            'mobile': ['mobile', 'phone', 'contact', 'mobile_number'],
            'email': ['email', 'email_id', 'complainant_email'],
            'district': ['district'],
            'state': ['state'],
            'crime_type': ['crime_type', 'sub_category', 'fraud_type'],
            'platform': ['platform', 'payment_platform', 'bank'],
            'amount': ['amount', 'amount_lost', 'total_amount', 'fraud_amount'],
            'status': ['status', 'complaint_status'],
        }
        
        for schema_key, possible_columns in column_mappings.items():
            for col in possible_columns:
                if col in row and pd.notna(row[col]):
                    value = str(row[col]).strip()
                    
                    if schema_key in ['name', 'mobile', 'email']:
                        schema['complainant_details'][schema_key] = value
                    elif schema_key == 'district':
                        schema['complainant_details']['address']['district'] = value
                    elif schema_key == 'state':
                        schema['complainant_details']['address']['state'] = value
                    elif schema_key == 'amount':
                        try:
                            schema['financial_details']['total_fraud_amount'] = float(value.replace(',', ''))
                        except:
                            pass
                    else:
                        schema[schema_key] = value
                    break
        
        return schema
    
    def _map_pdf_to_schema(self, data: Dict, source_file: str) -> Dict:
        """Map PDF parsed data to standard schema"""
        schema = self._get_standard_schema()
        schema['source_file'] = source_file
        
        # Map basic fields
        schema['complaint_id'] = data.get('complaint_id')
        schema['acknowledgement_number'] = data.get('acknowledgement_number')
        
        # Map dates
        if data.get('incident_date') and data.get('incident_time'):
            schema['incident_datetime'] = f"{data['incident_date']} {data['incident_time']}"
        elif data.get('incident_date'):
            schema['incident_datetime'] = data['incident_date']
        
        schema['complaint_datetime'] = data.get('complaint_date')
        
        # Map complainant details
        schema['complainant_details']['name'] = data.get('name')
        schema['complainant_details']['mobile'] = data.get('mobile')
        schema['complainant_details']['email'] = data.get('email')
        schema['complainant_details']['address']['street'] = data.get('street')
        schema['complainant_details']['address']['house_no'] = data.get('house_no')
        schema['complainant_details']['address']['colony'] = data.get('colony')
        schema['complainant_details']['address']['village_town'] = data.get('village_town')
        schema['complainant_details']['address']['police_station'] = data.get('police_station')
        schema['complainant_details']['address']['district'] = data.get('district')
        schema['complainant_details']['address']['state'] = data.get('state')
        schema['complainant_details']['address']['pincode'] = data.get('pincode')
        
        # Map crime details
        schema['crime_details']['complaint_type'] = data.get('complaint_type')
        schema['crime_details']['category'] = data.get('category')
        schema['crime_details']['sub_category'] = data.get('sub_category')
        schema['crime_details']['description'] = data.get('description')
        
        # Map financial details
        if data.get('total_fraud_amount'):
            try:
                schema['financial_details']['total_fraud_amount'] = float(data['total_fraud_amount'])
            except ValueError:
                schema['financial_details']['total_fraud_amount'] = 0.0
        
        # Map status
        schema['status'] = data.get('status', 'Under Process')
        
        # Map platform
        schema['platform'] = data.get('platform', 'Unknown')
        
        # Map transactions
        schema['transactions'] = data.get('transactions', [])
        
        # Map actions
        schema['actions_taken'] = data.get('actions_taken', [])
        
        return schema
    
    def _flatten_key_fields(self, data: Dict) -> Dict:
        """Flatten key fields to top level for easy access"""
        # Date time
        if data.get('incident_datetime'):
            data['date_time'] = data['incident_datetime']
        elif data.get('complaint_datetime'):
            data['date_time'] = data['complaint_datetime']
        
        # Complainant name
        if data['complainant_details'].get('name'):
            data['complainant_name'] = data['complainant_details']['name']
        
        # District
        if data['complainant_details']['address'].get('district'):
            data['district'] = data['complainant_details']['address']['district']
        
        # Crime type - use sub_category if available, otherwise category
        if data['crime_details'].get('sub_category'):
            data['crime_type'] = data['crime_details']['sub_category']
        elif data['crime_details'].get('category'):
            data['crime_type'] = data['crime_details']['category']
        
        # Amount lost
        if data['financial_details'].get('total_fraud_amount'):
            data['amount_lost'] = data['financial_details']['total_fraud_amount']
        
        return data
    
    def _clean_and_normalize(self, data: Dict) -> Dict:
        """Clean and normalize all data fields"""
        
        # Normalize dates
        if data.get('incident_datetime'):
            data['incident_datetime'] = self._normalize_date(data['incident_datetime'])
        
        if data.get('complaint_datetime'):
            data['complaint_datetime'] = self._normalize_date(data['complaint_datetime'])
        
        # Clean text fields
        if data['complainant_details']['name']:
            data['complainant_details']['name'] = self._clean_text(
                data['complainant_details']['name']
            )
        
        # Normalize mobile number
        if data['complainant_details']['mobile']:
            data['complainant_details']['mobile'] = self._normalize_mobile(
                data['complainant_details']['mobile']
            )
        
        # Normalize email
        if data['complainant_details']['email']:
            data['complainant_details']['email'] = self._normalize_email(
                data['complainant_details']['email']
            )
        
        # Clean address fields
        for key in data['complainant_details']['address']:
            if data['complainant_details']['address'][key]:
                data['complainant_details']['address'][key] = self._clean_text(
                    str(data['complainant_details']['address'][key])
                )
        
        # Normalize crime categories
        if data['crime_details']['category']:
            data['crime_details']['category'] = self._normalize_category(
                data['crime_details']['category']
            )
        
        if data['crime_details']['sub_category']:
            data['crime_details']['sub_category'] = self._normalize_category(
                data['crime_details']['sub_category']
            )
        
        # Normalize platform
        if data.get('platform'):
            data['platform'] = self._normalize_platform(data['platform'])
        
        # Normalize status
        if data.get('status'):
            data['status'] = self._normalize_status(data['status'])
        
        # Clean transactions
        for transaction in data['transactions']:
            if transaction.get('transaction_date'):
                transaction['transaction_date'] = self._normalize_date(
                    transaction['transaction_date']
                )
        
        return data
    
    def _normalize_date(self, date_str: str) -> Optional[str]:
        """Normalize date to ISO format YYYY-MM-DD HH:MM:SS or YYYY-MM-DD"""
        if not date_str:
            return None
        
        try:
            # Try parsing with dateutil
            parsed_date = date_parser.parse(str(date_str), dayfirst=True)
            
            # Check if time component exists
            if parsed_date.hour or parsed_date.minute or parsed_date.second:
                return parsed_date.strftime('%Y-%m-%d %H:%M:%S')
            else:
                return parsed_date.strftime('%Y-%m-%d')
        except:
            # Try manual parsing for DD/MM/YYYY format
            try:
                match = re.search(r'(\d{1,2})[/\-](\d{1,2})[/\-](\d{4})', str(date_str))
                if match:
                    day, month, year = match.groups()
                    
                    # Check for time component
                    time_match = re.search(r'(\d{1,2}):(\d{2})(?::(\d{2}))?', str(date_str))
                    if time_match:
                        hour = time_match.group(1).zfill(2)
                        minute = time_match.group(2)
                        second = time_match.group(3) if time_match.group(3) else '00'
                        return f"{year}-{month.zfill(2)}-{day.zfill(2)} {hour}:{minute}:{second}"
                    else:
                        return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
            except:
                pass
        
        logger.warning(f"Could not normalize date: {date_str}")
        return str(date_str)
    
    def _clean_text(self, text: str) -> str:
        """Clean and normalize text"""
        if not text:
            return ""
        
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', str(text).strip())
        
        # Remove special characters (keep alphanumeric and common punctuation)
        text = re.sub(r'[^\w\s\-.,@()]', '', text)
        
        return text.strip()
    
    def _normalize_mobile(self, mobile: str) -> str:
        """Normalize mobile number to standard format"""
        if not mobile:
            return ""
        
        # Extract only digits
        digits = re.sub(r'\D', '', str(mobile))
        
        # Remove country code if present
        if len(digits) > 10:
            digits = digits[-10:]
        
        return digits
    
    def _normalize_email(self, email: str) -> str:
        """Normalize email address"""
        if not email:
            return ""
        
        email = str(email).lower().strip()
        
        # Basic email validation
        if re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', email):
            return email
        
        logger.warning(f"Invalid email format: {email}")
        return email
    
    def _normalize_category(self, category: str) -> str:
        """Normalize crime category"""
        if not category:
            return ""
        
        category = str(category).strip()
        
        # Standardize common categories
        category_map = {
            'online financial fraud': 'Online Financial Fraud',
            'upi fraud': 'UPI Fraud',
            'cyber fraud': 'Cyber Fraud',
            'banking fraud': 'Banking Fraud',
            'card fraud': 'Card Fraud',
            'debit card fraud': 'Debit Card Fraud',
            'credit card fraud': 'Credit Card Fraud',
            'phishing': 'Phishing',
            'identity theft': 'Identity Theft',
            'social media fraud': 'Social Media Fraud',
        }
        
        return category_map.get(category.lower(), category.title())
    
    def _normalize_platform(self, platform: str) -> str:
        """Normalize platform/payment method"""
        if not platform:
            return "Unknown"
        
        platform = str(platform).strip()
        
        platform_map = {
            'phonepe': 'PhonePe',
            'phone pe': 'PhonePe',
            'google pay': 'Google Pay',
            'googlepay': 'Google Pay',
            'gpay': 'Google Pay',
            'paytm': 'Paytm',
            'amazon pay': 'Amazon Pay',
            'bhim': 'BHIM',
            'upi': 'UPI',
            'imps': 'IMPS',
            'neft': 'NEFT',
            'rtgs': 'RTGS',
            'net banking': 'Net Banking',
            'netbanking': 'Net Banking',
        }
        
        return platform_map.get(platform.lower(), platform)
    
    def _normalize_status(self, status: str) -> str:
        """Normalize complaint status"""
        if not status:
            return "Under Process"
        
        status = str(status).strip()
        
        status_map = {
            'under process': 'Under Process',
            'under enquiry': 'Under Enquiry',
            'under investigation': 'Under Investigation',
            'complaint accepted': 'Complaint Accepted',
            'complaint registered': 'Complaint Registered',
            'fir registered': 'FIR Registered',
            'closed': 'Closed',
            'resolved': 'Resolved',
            'pending': 'Pending',
        }
        
        return status_map.get(status.lower(), status)
    
    def _validate_data(self, data: Dict) -> Dict:
        """Validate data and calculate quality score"""
        quality_score = 0.0
        total_fields = 0
        filled_fields = 0
        
        # Define critical fields with their weights
        critical_fields = [
            ('complaint_id', data.get('complaint_id'), 2.0),
            ('complainant_name', data.get('complainant_name'), 2.0),
            ('mobile', data['complainant_details'].get('mobile'), 1.5),
            ('district', data.get('district'), 1.0),
            ('date_time', data.get('date_time'), 1.5),
            ('crime_type', data.get('crime_type'), 1.5),
            ('amount_lost', data.get('amount_lost'), 2.0),
            ('status', data.get('status'), 1.0),
        ]
        
        for field_name, value, weight in critical_fields:
            total_fields += weight
            if value and str(value).strip() and str(value).lower() not in ['none', 'unknown', '0', '0.0']:
                filled_fields += weight
            else:
                logger.warning(f"Missing or invalid critical field: {field_name}")
        
        # Calculate quality score
        if total_fields > 0:
            quality_score = (filled_fields / total_fields) * 100
        
        data['metadata']['data_quality_score'] = round(quality_score, 2)
        data['metadata']['validation_status'] = 'valid' if quality_score >= 60 else 'incomplete'
        
        # Additional validation checks
        validation_warnings = []
        
        # Check mobile number format
        mobile = data['complainant_details'].get('mobile')
        if mobile and len(mobile) != 10:
            validation_warnings.append("Mobile number is not 10 digits")
        
        # Check email format
        email = data['complainant_details'].get('email')
        if email and '@' not in email:
            validation_warnings.append("Invalid email format")
        
        # Check amount is positive
        amount = data.get('amount_lost', 0)
        if amount <= 0:
            validation_warnings.append("Amount lost is zero or negative")
        
        if validation_warnings:
            data['metadata']['validation_warnings'] = validation_warnings
        
        return data
    
    def _is_duplicate(self, data: Dict) -> bool:
        """Check if complaint is duplicate"""
        complaint_id = data.get('complaint_id')
        
        if complaint_id and complaint_id in self.processed_complaints:
            return True
        
        # Generate hash from key fields for secondary duplicate check
        hash_string = f"{data.get('complainant_name', '')}_" \
                     f"{data['complainant_details'].get('mobile', '')}_" \
                     f"{data.get('date_time', '')}_" \
                     f"{data.get('amount_lost', 0)}"
        
        complaint_hash = hashlib.md5(hash_string.encode()).hexdigest()
        
        if complaint_hash in self.processed_complaints:
            return True
        
        return False
    
    def _register_complaint(self, data: Dict):
        """Register complaint to prevent duplicates"""
        complaint_id = data.get('complaint_id')
        if complaint_id:
            self.processed_complaints.add(complaint_id)
        
        # Also register hash
        hash_string = f"{data.get('complainant_name', '')}_" \
                     f"{data['complainant_details'].get('mobile', '')}_" \
                     f"{data.get('date_time', '')}_" \
                     f"{data.get('amount_lost', 0)}"
        
        complaint_hash = hashlib.md5(hash_string.encode()).hexdigest()
        self.processed_complaints.add(complaint_hash)
    
    def _generate_summary(self, data: Dict) -> Dict:
        """Generate summary statistics"""
        return {
            "complaint_id": data.get('complaint_id'),
            "complainant_name": data.get('complainant_name'),
            "district": data.get('district'),
            "crime_type": data.get('crime_type'),
            "platform": data.get('platform'),
            "amount_lost": data.get('amount_lost', 0),
            "status": data.get('status'),
            "date_time": data.get('date_time'),
            "num_transactions": len(data.get('transactions', [])),
            "num_actions_taken": len(data.get('actions_taken', [])),
            "data_quality_score": data['metadata'].get('data_quality_score', 0),
            "validation_status": data['metadata'].get('validation_status', 'unknown'),
            "is_duplicate": data['metadata'].get('is_duplicate', False),
            "source_file": data.get('source_file')
        }
    
    def save_output(self, result: Dict, output_path: str):
        """Save processed data to JSON file"""
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(result, f, indent=2, ensure_ascii=False)
            logger.info(f"Output saved to: {output_path}")
        except Exception as e:
            logger.error(f"Error saving output: {str(e)}")
    
    def batch_process(self, file_paths: List[str], output_dir: str = "output"):
        """Process multiple files and save outputs"""
        Path(output_dir).mkdir(exist_ok=True)
        
        results = []
        for file_path in file_paths:
            result = self.process_file(file_path)
            results.append(result)
            
            if result['status'] == 'success':
                complaint_id = result['data'].get('complaint_id', 'unknown')
                output_file = f"{output_dir}/complaint_{complaint_id}.json"
                self.save_output(result, output_file)
        
        # Save batch summary
        summary = {
            "total_files": len(file_paths),
            "successful": sum(1 for r in results if r['status'] == 'success'),
            "failed": sum(1 for r in results if r['status'] == 'error'),
            "duplicates": sum(1 for r in results if r['status'] == 'success' and r['data']['metadata'].get('is_duplicate')),
            "results": [r['summary'] if r['status'] == 'success' else {'error': r.get('error')} for r in results]
        }
        
        self.save_output(summary, f"{output_dir}/batch_summary.json")
        return summary



def export_to_excel_append(result: dict, excel_path: str):
    """
    Append NCRP complaint output to Excel file
    """
    if result.get("status") != "success":
        raise ValueError("Cannot export failed result")

    data = result["data"]

    row = {
        "Complaint ID": data.get("complaint_id"),
        "Acknowledgement No": data.get("acknowledgement_number"),
        "Date & Time": data.get("date_time"),
        "Complainant Name": data.get("complainant_name"),
        "District": data.get("district"),
        "Crime Type": data.get("crime_type"),
        "Platform": data.get("platform"),
        "Amount Lost (INR)": data.get("amount_lost"),
        "Status": data.get("status"),
        "Data Quality Score": data["metadata"].get("data_quality_score"),
        "Source File": data.get("source_file")
    }

    df_new = pd.DataFrame([row])

    excel_path = Path(excel_path)

    if excel_path.exists():
        df_existing = pd.read_excel(excel_path)
        df_final = pd.concat([df_existing, df_new], ignore_index=True)
    else:
        df_final = df_new

    df_final.to_excel(excel_path, index=False)
    print(f"Complaint appended to Excel : {excel_path}")
import gspread
from google.oauth2.service_account import Credentials

def append_to_google_sheet(result: dict, sheet_name: str, creds_path: str):
    """
    Append NCRP complaint data to Google Sheet.
    Assumes duplicate check is already done.
    """

    if result.get("status") != "success":
        return

    data = result["data"]

    # Google Sheets authentication
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]

    creds = Credentials.from_service_account_file(
        creds_path, scopes=scopes
    )

    client = gspread.authorize(creds)

    sheet = client.open(sheet_name).sheet1  # First sheet

    # Prepare row (same structure as Excel)
    row = [
        data.get("complaint_id"),
        data.get("acknowledgement_number"),
        data.get("date_time"),
        data.get("complainant_name"),
        data.get("district"),
        data.get("crime_type"),
        data.get("platform"),
        data.get("amount_lost"),
        data.get("status"),
        data["metadata"].get("data_quality_score"),
        data.get("source_file")
    ]

    sheet.append_row(row, value_input_option="USER_ENTERED")

    print("Complaint appended to Google Sheet")


# Example usage
def run_member1_pipeline(
    input_dir="data/uploads",
    excel_output="ncrp_complaints.xlsx",
    json_output="output_ncrp_data.json",
    sheet_name="NCRP_Master_Sheet",
    creds_path="service_account.json"
):
    agent = NCRPDataIngestionAgent()

    input_dir = Path(input_dir)
    files = list(input_dir.glob("*"))

    if not files:
        raise Exception("No input files found in upload directory")

    results = []

    for file in files:
        result = agent.process_file(str(file))
        results.append(result)

        if result["status"] == "success":
            export_to_excel_append(result, excel_output)
            append_to_google_sheet(result, sheet_name, creds_path)

    agent.save_output(
        {
            "status": "success",
            "processed_files": len(files),
            "results": results
        },
        json_output
    )

    return results
if __name__ == "__main__":
    run_member1_pipeline()
