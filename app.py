from functools import lru_cache
import io
import os
import re
import shutil
import tempfile
from typing import Dict, List, Tuple
import uuid
import zipfile
import pandas as pd
import numpy as np
from flask import Flask, render_template, request, redirect, send_from_directory, url_for, flash, send_file, jsonify, session
from flask_sqlalchemy import SQLAlchemy
from flask_restful import Api, Resource
from werkzeug.utils import secure_filename
from rapidfuzz import process, fuzz
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from unidecode import unidecode
from datetime import datetime, timedelta
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import LabelEncoder
import joblib
import json
from flask_session import Session
import logging
from collections import Counter
import os
import openpyxl
from openpyxl.utils import get_column_letter


# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config['SECRET_KEY'] = 'crop-variety-system-secret-key-2024'
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///crop_variety.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024
app.config['SESSION_TYPE'] = 'filesystem'
app.config['SESSION_FILE_DIR'] = tempfile.mkdtemp()
app.config['SESSION_PERMANENT'] = False
app.config['SESSION_USE_SIGNER'] = True
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(hours=12)

db = SQLAlchemy(app)
api = Api(app)

# Initialize server-side session
Session(app)

# Create upload directories
os.makedirs(os.path.join(app.config['UPLOAD_FOLDER'], 'master'), exist_ok=True)
os.makedirs(os.path.join(app.config['UPLOAD_FOLDER'], 'country'), exist_ok=True)

# Database Models
class Variety(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    country = db.Column(db.String(100))
    crop = db.Column(db.String(100))
    botanical_name = db.Column(db.String(150))
    variety_release_name = db.Column(db.String(150), index=True)
    seed_type = db.Column(db.String(100))
    year_of_release = db.Column(db.String(50))
    releasing_entity = db.Column(db.String(200))
    maintainer = db.Column(db.String(200))
    production_altitude = db.Column(db.String(100))
    min_altitude = db.Column(db.String(50))
    max_altitude = db.Column(db.String(50))
    agroecological_zones = db.Column(db.String(200))
    maturity_days = db.Column(db.String(50))
    yield_mt_ha = db.Column(db.String(50))
    presence_in_regional_catalogue = db.Column(db.String(50))
    other_countries_of_release = db.Column(db.String(300))
    commercialising_companies = db.Column(db.String(300))
    commercialising_names = db.Column(db.String(300))
    special_attributes = db.Column(db.Text)
    licence_type = db.Column(db.String(100))
    maintenance_status = db.Column(db.String(100))
    commercialising_level = db.Column(db.String(100))
    disease_tolerant = db.Column(db.String(200))
    field_pest_resistant = db.Column(db.String(200))
    drought_tolerant = db.Column(db.String(50))
    storage_pest_resistant = db.Column(db.String(200))
    consumer_preference = db.Column(db.String(200))
    data_quality_score = db.Column(db.Float, default=4.5)
    last_verified = db.Column(db.DateTime)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class UploadLog(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    filename = db.Column(db.String(200))
    file_type = db.Column(db.String(50))
    record_count = db.Column(db.Integer)
    matching_accuracy = db.Column(db.Float)
    processing_time = db.Column(db.Float)
    uploaded_at = db.Column(db.DateTime, default=datetime.utcnow)

class MatchingLog(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    upload_id = db.Column(db.Integer, db.ForeignKey('upload_log.id'))
    variety_name = db.Column(db.String(150))
    matched_variety_id = db.Column(db.Integer, db.ForeignKey('variety.id'))
    confidence_score = db.Column(db.Float)
    matching_algorithm = db.Column(db.String(50))
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

# Add to your existing models in app.py
class FilterSession(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    session_id = db.Column(db.String(100), unique=True, nullable=False)
    filename = db.Column(db.String(200), nullable=False)
    original_data = db.Column(db.Text)  # Store as JSON
    column_mapping = db.Column(db.Text)  # Store as JSON
    filter_state = db.Column(db.Text)  # Store as JSON
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    expires_at = db.Column(db.DateTime, nullable=False)

class FilteredData(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    filter_session_id = db.Column(db.Integer, db.ForeignKey('filter_session.id'), nullable=False)
    column_name = db.Column(db.String(100), nullable=False)
    value = db.Column(db.String(500), nullable=False)
    record_count = db.Column(db.Integer, default=0)

# Data Cleaning Engine
class DataCleaningEngine:
    @staticmethod
    def clean_text(text):
        """Clean and standardize text data"""
        if pd.isna(text):
            return ""
        
        text = str(text).strip()
        text = re.sub(r'\s+', ' ', text)
        text = re.sub(r'[^\w\s\-\.\(\)]', '', text)
        text = unidecode(text).upper()
        return text.strip()
    
    @staticmethod
    def clean_numeric(value):
        """Clean numeric values"""
        if pd.isna(value):
            return None
        
        value = str(value).strip()
        value = re.sub(r'[^\d\.\-]', '', value)
        try:
            return float(value) if value else None
        except ValueError:
            return None
    
    @staticmethod
    def standardize_country_name(country):
        """Standardize country names"""
        country_mapping = {
            'USA': 'UNITED STATES',
            'U.S.A.': 'UNITED STATES',
            'UK': 'UNITED KINGDOM',
            'U.K.': 'UNITED KINGDOM',
        }
        clean_country = DataCleaningEngine.clean_text(country)
        return country_mapping.get(clean_country, clean_country)

# Advanced Matching Engine
class AdvancedMatchingEngine:
    def __init__(self):
        self.vectorizer = TfidfVectorizer(analyzer='char_wb', ngram_range=(2, 4), min_df=1)
        self.fitted = False
        self.variety_names = []
        self.clean_variety_names = []
    
    def fit(self, variety_names):
        """Fit TF-IDF vectorizer on variety names"""
        if not variety_names:
            self.fitted = False
            return
            
        # Clean and filter variety names
        self.variety_names = []
        self.clean_variety_names = []
        
        for name in variety_names:
            if name and str(name).strip():
                clean_name = DataCleaningEngine.clean_text(str(name))
                if clean_name and len(clean_name) >= 2:
                    self.variety_names.append(name)
                    self.clean_variety_names.append(clean_name)
        
        if self.clean_variety_names:
            try:
                self.vectorizer.fit(self.clean_variety_names)
                self.fitted = True
                logger.info(f"Matching engine fitted with {len(self.clean_variety_names)} variety names")
            except Exception as e:
                logger.error(f"Error fitting vectorizer: {str(e)}")
                self.fitted = False
        else:
            self.fitted = False
    
    def advanced_match(self, query, threshold=0.7):
        """Robust advanced matching with better error handling"""
        if not self.fitted or not query:
            return None, 0.0
        
        try:
            clean_query = DataCleaningEngine.clean_text(str(query))
            if not clean_query or len(clean_query.strip()) < 2:
                return None, 0.0
            
            # TF-IDF Cosine Similarity
            cosine_scores = [0] * len(self.clean_variety_names)
            try:
                query_vec = self.vectorizer.transform([clean_query])
                names_vec = self.vectorizer.transform(self.clean_variety_names)
                cosine_scores = cosine_similarity(query_vec, names_vec).flatten()
            except Exception as e:
                logger.warning(f"TF-IDF matching failed: {str(e)}")
            
            # Fuzzy matching with multiple strategies
            fuzzy_scores = []
            for name in self.clean_variety_names:
                # Use multiple fuzzy matching strategies
                ratio_score = fuzz.ratio(clean_query, name) / 100.0
                partial_score = fuzz.partial_ratio(clean_query, name) / 100.0
                token_score = fuzz.token_sort_ratio(clean_query, name) / 100.0
                
                # Use the best fuzzy score
                best_fuzzy = max(ratio_score, partial_score, token_score)
                fuzzy_scores.append(best_fuzzy)
            
            # Combine scores with weighted average
            combined_scores = []
            for cos_score, fuzzy_score in zip(cosine_scores, fuzzy_scores):
                # Give more weight to fuzzy matching for short queries
                if len(clean_query) < 5:
                    combined = (cos_score * 0.4) + (fuzzy_score * 0.6)
                else:
                    combined = (cos_score * 0.6) + (fuzzy_score * 0.4)
                combined_scores.append(combined)
            
            if combined_scores:
                best_idx = np.argmax(combined_scores)
                best_score = combined_scores[best_idx]
                
                if best_score >= threshold:
                    logger.debug(f"Match found: '{clean_query}' -> '{self.variety_names[best_idx]}' (score: {best_score:.3f})")
                    return self.variety_names[best_idx], best_score
            
            logger.debug(f"No match found for: '{clean_query}' (best score: {max(combined_scores) if combined_scores else 0:.3f})")
            return None, max(combined_scores) if combined_scores else 0.0
            
        except Exception as e:
            logger.error(f"Advanced matching error for query '{query}': {str(e)}")
            return None, 0.0

# Search Engine with Cascading and Global Modes
class SearchEngine:
    def __init__(self):
        self.search_modes = ['cascading', 'global']
        self.current_mode = 'cascading'
    
    def set_search_mode(self, mode):
        if mode in self.search_modes:
            self.current_mode = mode
            return True
        return False
    
    def cascading_search(self, query, filters=None):
        """Cascading search: Search in sequence of importance"""
        if not query:
            base_query = Variety.query
            if filters:
                for key, value in filters.items():
                    if value:
                        base_query = base_query.filter(getattr(Variety, key).ilike(f'%{value}%'))
            return base_query
        
        search_query = f"%{query}%"
        
        # Step 1: Variety names
        results = Variety.query.filter(Variety.variety_release_name.ilike(search_query))
        
        # Step 2: Crop types
        if results.count() == 0:
            results = Variety.query.filter(Variety.crop.ilike(search_query))
        
        # Step 3: Countries
        if results.count() == 0:
            results = Variety.query.filter(Variety.country.ilike(search_query))
        
        # Step 4: Other fields
        if results.count() == 0:
            results = Variety.query.filter(
                Variety.botanical_name.ilike(search_query) |
                Variety.releasing_entity.ilike(search_query) |
                Variety.special_attributes.ilike(search_query)
            )
        
        # Apply filters
        if filters:
            for key, value in filters.items():
                if value:
                    results = results.filter(getattr(Variety, key).ilike(f'%{value}%'))
        
        return results
    
    def global_search(self, query, filters=None):
        """Global search: Search across all relevant fields simultaneously"""
        if not query:
            base_query = Variety.query
            if filters:
                for key, value in filters.items():
                    if value:
                        base_query = base_query.filter(getattr(Variety, key).ilike(f'%{value}%'))
            return base_query
        
        search_query = f"%{query}%"
        
        # Build global search query
        results = Variety.query.filter(
            db.or_(
                Variety.variety_release_name.ilike(search_query),
                Variety.crop.ilike(search_query),
                Variety.country.ilike(search_query),
                Variety.botanical_name.ilike(search_query),
                Variety.releasing_entity.ilike(search_query),
                Variety.maintainer.ilike(search_query),
                Variety.agroecological_zones.ilike(search_query),
                Variety.special_attributes.ilike(search_query),
                Variety.disease_tolerant.ilike(search_query),
                Variety.field_pest_resistant.ilike(search_query),
                Variety.other_countries_of_release.ilike(search_query),
                Variety.commercialising_companies.ilike(search_query)
            )
        )
        
        # Apply filters
        if filters:
            for key, value in filters.items():
                if value:
                    results = results.filter(getattr(Variety, key).ilike(f'%{value}%'))
        
        return results
    
    def search(self, query, filters=None, mode=None):
        """Main search method with mode selection"""
        search_mode = mode or self.current_mode
        
        if search_mode == 'cascading':
            return self.cascading_search(query, filters)
        else:
            return self.global_search(query, filters)

# Data Cleaner
class DataCleaner:
    @staticmethod
    def clean_dataframe(df):
        """Comprehensive dataframe cleaning"""
        cleaned_df = df.copy()
        
        # Clean each column
        for col in cleaned_df.columns:
            if cleaned_df[col].dtype == 'object':
                cleaned_df[col] = cleaned_df[col].apply(DataCleaningEngine.clean_text)
        
        # Remove completely empty rows
        cleaned_df = cleaned_df.dropna(how='all')
        
        return cleaned_df
    
    @staticmethod
    def get_cleaning_report(original_df, cleaned_df):
        """Generate a cleaning report comparing original and cleaned data"""
        report = {
            'original_rows': len(original_df),
            'cleaned_rows': len(cleaned_df),
            'rows_removed': len(original_df) - len(cleaned_df),
            'columns_processed': len(original_df.columns),
            'cleaning_summary': {}
        }
        
        # Calculate changes for each column
        for col in original_df.columns:
            if col in cleaned_df.columns:
                original_non_null = original_df[col].count()
                cleaned_non_null = cleaned_df[col].count()
                report['cleaning_summary'][col] = {
                    'original_non_null': original_non_null,
                    'cleaned_non_null': cleaned_non_null,
                    'null_values_removed': original_non_null - cleaned_non_null
                }
        
        return report

# Initialize engines
matching_engine = AdvancedMatchingEngine()
search_engine = SearchEngine()

# Session storage
class SessionData:
    def __init__(self):
        self.country_file_path = None
        self.country_df = None
        self.cleaned_country_df = None
        self.column_mapping = None
        self.fields_to_fill = []
        self.matched_data = None
        self.matching_results = None

session_data = SessionData()

# Utility functions
def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ['xlsx', 'xls']

def get_master_columns():
    return [column.name for column in Variety.__table__.columns 
            if column.name not in ['id', 'created_at', 'updated_at', 'data_quality_score', 'last_verified']]

def auto_detect_columns(uploaded_columns):
    master_columns = get_master_columns()
    mappings = {}
    
    for uploaded_col in uploaded_columns:
        uploaded_lower = uploaded_col.lower()
        
        # Exact matches
        for master_col in master_columns:
            if uploaded_lower in master_col.lower() or master_col.lower() in uploaded_lower:
                mappings[uploaded_col] = master_col
                break
        
        # Fuzzy matching
        if uploaded_col not in mappings:
            best_match, score, _ = process.extractOne(uploaded_col, master_columns, scorer=fuzz.partial_ratio)
            if score > 70:
                mappings[uploaded_col] = best_match
    
    return mappings

# ENHANCED MATCHING HELPER FUNCTIONS

def find_variety_column(country_df, column_mapping):
    """Find the best column to use for variety name matching"""
    # Strategy 1: Check column mapping first
    for col, mapped_col in column_mapping.items():
        if mapped_col == 'variety_release_name':
            if col in country_df.columns:
                logger.info(f"Using mapped variety column: {col}")
                return col
    
    # Strategy 2: Look for columns with variety-related keywords
    variety_keywords = ['variety', 'name', 'cultivar', 'release', 'varietal', 'cultivo']
    for col in country_df.columns:
        col_lower = col.lower()
        if any(keyword in col_lower for keyword in variety_keywords):
            logger.info(f"Using keyword-matched variety column: {col}")
            return col
    
    # Strategy 3: Use the first column that has reasonable string data
    for col in country_df.columns:
        if country_df[col].notna().sum() > 0:  # At least some non-null values
            sample_values = country_df[col].dropna().head(5)
            if all(isinstance(val, str) and len(str(val).strip()) > 0 for val in sample_values if pd.notna(val)):
                logger.info(f"Using first valid string column: {col}")
                return col
    
    # Strategy 4: Fallback to first column
    fallback_col = country_df.columns[0] if len(country_df.columns) > 0 else None
    logger.info(f"Using fallback column: {fallback_col}")
    return fallback_col

def extract_variety_name(row, variety_column, column_mapping, all_columns):
    """Extract variety name with multiple fallback strategies"""
    # Strategy 1: Use the identified variety column
    if variety_column and variety_column in row.index:
        name = row[variety_column]
        if name and pd.notna(name) and str(name).strip():
            return str(name).strip()
    
    # Strategy 2: Look through mapped columns
    for col, mapped_col in column_mapping.items():
        if mapped_col in ['variety_release_name', 'crop', 'botanical_name']:
            if col in row.index and pd.notna(row[col]) and str(row[col]).strip():
                return str(row[col]).strip()
    
    # Strategy 3: Search all columns for potential variety names
    for col in all_columns:
        if col in row.index and pd.notna(row[col]) and str(row[col]).strip():
            value = str(row[col]).strip()
            # Heuristic: values that look like names (not numbers, not too long)
            if (len(value) > 2 and len(value) < 100 and 
                not value.replace('.', '').replace(',', '').replace(' ', '').isdigit()):
                return value
    
    return None

def find_matching_varieties_with_crop_verification(cleaned_name, country_crop, master_varieties):
    """
    Find matching varieties using CONCATENATED crop+variety key for exact matching.
    This ensures 100% accuracy by matching both crop AND variety together.
    """
    matches = []
    
    # Create the search key from country data
    country_key = create_crop_variety_key(country_crop, cleaned_name)
    
    # Extract crop and variety from the key for logging
    country_crop_clean = country_key.split('||')[0] if '||' in country_key else ''
    country_variety_clean = country_key.split('||')[1] if '||' in country_key else ''
    
    logger.info(f"Searching for concatenated key: '{country_key}'")
    
    # Special case: if no crop provided in country data
    if not country_crop or pd.isna(country_crop) or str(country_crop).strip() == '':
        logger.warning(f"No crop provided for variety '{cleaned_name}' - matching by name only (less reliable)")
        # Fallback to name-only matching
        for variety in master_varieties:
            if variety.variety_release_name:
                master_clean_name = DataCleaningEngine.clean_text(str(variety.variety_release_name))
                if master_clean_name == cleaned_name:
                    matches.append(variety)
                    logger.debug(f"Name-only match: {variety.variety_release_name} (crop: {variety.crop})")
        return matches
    
    # STRICT MODE: Match using concatenated crop+variety key
    for variety in master_varieties:
        if variety.variety_release_name:
            # Create master key
            master_key = create_crop_variety_key(variety.crop, variety.variety_release_name)
            
            # Exact key match
            if master_key == country_key:
                matches.append(variety)
                logger.info(f"✓ EXACT MATCH: '{country_key}' found in master catalog")
            else:
                # Log mismatches for debugging
                master_crop_clean = master_key.split('||')[0] if '||' in master_key else ''
                master_variety_clean = master_key.split('||')[1] if '||' in master_key else ''
                
                # Check if variety name matches but crop doesn't
                if master_variety_clean == country_variety_clean and master_crop_clean != country_crop_clean:
                    logger.debug(f"✗ CROP MISMATCH: Variety '{master_variety_clean}' exists but crop differs - Country: '{country_crop_clean}' vs Master: '{master_crop_clean}'")
    
    if not matches:
        logger.warning(f"✗ NO MATCH: '{country_key}' not found in master catalog")
    else:
        logger.info(f"Found {len(matches)} exact matches for '{country_key}'")
    
    return matches

def aggregate_variety_data(matched_varieties, fields_to_fill, original_country=None):
    """Aggregate data from multiple variety matches with enhanced logic for countries"""
    aggregated = {}
    
    for field in fields_to_fill:
        values = set()
        
        for variety in matched_varieties:
            if hasattr(variety, field):
                value = getattr(variety, field)
                if value and not pd.isna(value) and str(value).strip():
                    # Special handling for "other_countries_of_release"
                    if field == 'other_countries_of_release':
                        # Split by common delimiters and add individual countries
                        countries = re.split(r'[;,|]', str(value))
                        for country in countries:
                            clean_country = country.strip()
                            if clean_country:
                                values.add(clean_country)
                    else:
                        values.add(str(value).strip())
        
        # Enhanced logic for other_countries_of_release
        if field == 'other_countries_of_release':
            # Add countries from the matched varieties themselves
            for variety in matched_varieties:
                if variety.country and variety.country.strip():
                    values.add(variety.country.strip())
            
            # Remove the original country if provided (to avoid duplication)
            if original_country:
                original_clean = DataCleaningEngine.clean_text(original_country)
                values = {v for v in values if DataCleaningEngine.clean_text(v) != original_clean}
            
            # Remove any empty strings
            values = {v for v in values if v and v.strip()}
        
        # Convert set to appropriate format
        if values:
            if field == 'other_countries_of_release':
                aggregated[field] = '; '.join(sorted(values))
            else:
                # For other fields, use intelligent selection
                aggregated[field] = select_best_value(values, field, matched_varieties)
        else:
            aggregated[field] = None
    
    return aggregated

def select_best_value(values, field, matched_varieties=None):
    """Select the most appropriate value from multiple options with enhanced logic"""
    values_list = list(values)
    
    if not values_list:
        return None
    
    # For certain fields, prefer specific values
    if field == 'crop':
        # Return the most frequent crop, prioritizing exact matches with varieties
        if matched_varieties:
            # Count frequencies
            crop_counts = Counter(values_list)
            # If we have exact matches, prefer their crop
            exact_crops = [v.crop for v in matched_varieties if hasattr(v, 'crop') and v.crop in values_list]
            if exact_crops:
                exact_crop_counts = Counter(exact_crops)
                # Combine counts, giving extra weight to exact matches
                for crop in exact_crop_counts:
                    crop_counts[crop] += exact_crop_counts[crop] * 2
            return crop_counts.most_common(1)[0][0]
        else:
            return Counter(values_list).most_common(1)[0][0]
    
    elif field == 'country':
        # Return the most frequent country
        return Counter(values_list).most_common(1)[0][0]
    
    elif field in ['yield_mt_ha', 'maturity_days']:
        # Return the most common value, or highest if numeric
        try:
            numeric_values = [float(str(v).replace(',', '')) for v in values_list if str(v).replace(',', '').replace('.', '').isdigit()]
            if numeric_values:
                # For yield, prefer highest; for maturity, prefer most common
                if field == 'yield_mt_ha':
                    return str(max(numeric_values))
                else:
                    # For maturity, return the most frequent value
                    str_values = [str(v) for v in values_list]
                    return Counter(str_values).most_common(1)[0][0]
            else:
                return Counter(values_list).most_common(1)[0][0]
        except:
            return Counter(values_list).most_common(1)[0][0]
    
    elif field == 'other_countries_of_release':
        # Already handled in aggregate_variety_data
        return '; '.join(sorted(values_list))
    
    else:
        # Return the most frequent value
        return Counter(values_list).most_common(1)[0][0]

def fill_with_aggregated_data(result_df, idx, aggregated_data, fields_to_fill):
    """Fill dataframe with aggregated data from multiple matches"""
    fields_filled = 0
    
    for field in fields_to_fill:
        if field in aggregated_data and aggregated_data[field]:
            result_df.at[idx, field] = aggregated_data[field]
            fields_filled += 1
            logger.debug(f"Filled field '{field}' with aggregated value: {aggregated_data[field]}")
    
    return fields_filled

def create_crop_variety_key(crop, variety_name):
    """
    Create a unique concatenated key for crop + variety matching.
    Format: CROP||VARIETY (using || as separator to avoid conflicts)
    """
    if not crop or pd.isna(crop):
        crop = ""
    if not variety_name or pd.isna(variety_name):
        variety_name = ""
    
    # Clean both components
    clean_crop = DataCleaningEngine.clean_text(str(crop))
    clean_variety = DataCleaningEngine.clean_text(str(variety_name))
    
    # Create concatenated key with separator
    key = f"{clean_crop}||{clean_variety}"
    return key

def perform_matching(country_df, column_mapping, fields_to_fill):
    """
    Perform robust matching using CONCATENATED crop+variety keys for 100% accuracy.
    This ensures that varieties are only matched when BOTH crop AND variety name match exactly.
    """
    if country_df.empty:
        logger.warning("Country dataframe is empty")
        return country_df.copy()
    
    result_df = country_df.copy()
    
    # Add empty columns for fields to be filled
    for field in fields_to_fill:
        if field not in result_df.columns:
            result_df[field] = ''
    
    # Get all master varieties for matching
    master_varieties = Variety.query.all()
    if not master_varieties:
        logger.warning("No master varieties found in database")
        return result_df
    
    # Extract variety names AND create concatenated keys for matching engine
    variety_keys = []
    valid_master_varieties = []
    
    for v in master_varieties:
        if v.variety_release_name and str(v.variety_release_name).strip():
            # Create concatenated key for this variety
            key = create_crop_variety_key(v.crop, v.variety_release_name)
            variety_keys.append(key)
            valid_master_varieties.append(v)
    
    if not variety_keys:
        logger.warning("No valid variety keys found in master database")
        return result_df
    
    # Fit matching engine with concatenated keys
    try:
        matching_engine.fit(variety_keys)
        if not matching_engine.fitted:
            logger.error("Matching engine failed to fit")
            return result_df
        logger.info(f"Matching engine fitted with {len(variety_keys)} crop+variety concatenated keys")
    except Exception as e:
        logger.error(f"Error fitting matching engine: {str(e)}")
        return result_df
    
    # Find the best column for variety name matching
    variety_column = find_variety_column(country_df, column_mapping)
    
    if not variety_column:
        logger.error("Could not find a suitable variety column for matching")
        return result_df
    
    # Get original country from the data if available
    original_country = None
    if 'country' in column_mapping.values():
        country_cols = [k for k, v in column_mapping.items() if v == 'country']
        if country_cols and country_cols[0] in country_df.columns:
            # Take the first non-null country value
            country_values = country_df[country_cols[0]].dropna()
            if len(country_values) > 0:
                original_country = str(country_values.iloc[0]).strip()
    
    # Perform matching for each row using concatenated keys
    # REDUCED: Only store summary statistics, not all details
    match_stats = {
        'total_processed': 0,
        'successful_matches': 0,
        'failed_matches': 0,
        'crop_mismatches': 0,
        'no_crop_provided': 0,
        'name_only_matches': 0,
        # Store only first 20 details for preview, not all
        'details': [],
        'sample_size': 20
    }
    
    for idx, row in country_df.iterrows():
        match_stats['total_processed'] += 1
        
        # Get variety name
        variety_name = extract_variety_name(row, variety_column, column_mapping, country_df.columns)
        
        if not variety_name or pd.isna(variety_name) or str(variety_name).strip() == '':
            match_stats['failed_matches'] += 1
            # Only store first 20 details
            if len(match_stats['details']) < match_stats['sample_size']:
                match_stats['details'].append({
                    'row': int(idx),
                    'variety_name': str(variety_name) if variety_name else None,
                    'status': 'empty_name'
                })
            continue
        
        # Get crop from country data
        country_crop = None
        if 'crop' in column_mapping.values():
            crop_cols = [k for k, v in column_mapping.items() if v == 'crop']
            if crop_cols and crop_cols[0] in row.index:
                country_crop = row[crop_cols[0]]
        
        # Track if crop was provided
        if not country_crop or pd.isna(country_crop) or str(country_crop).strip() == '':
            match_stats['no_crop_provided'] += 1
            logger.warning(f"Row {idx}: No crop provided for variety '{variety_name}' - will use less reliable name-only matching")
        
        try:
            # Create concatenated key for matching
            cleaned_name = DataCleaningEngine.clean_text(str(variety_name))
            country_key = create_crop_variety_key(country_crop, cleaned_name)
            
            # Perform matching with the concatenated key
            matched_key, score = matching_engine.advanced_match(country_key, 1.0)
            
            if matched_key and score >= 1.0:
                # Find ALL matching varieties with this exact key
                all_matched_varieties = find_matching_varieties_with_crop_verification(
                    cleaned_name, country_crop, valid_master_varieties
                )
                
                if all_matched_varieties:
                    # Track if this was a name-only match (less reliable)
                    if not country_crop or pd.isna(country_crop):
                        match_stats['name_only_matches'] += 1
                    
                    # Aggregate data from matched varieties
                    aggregated_data = aggregate_variety_data(
                        all_matched_varieties, 
                        fields_to_fill, 
                        original_country
                    )
                    
                    # Fill fields with aggregated data
                    fields_filled = fill_with_aggregated_data(result_df, idx, aggregated_data, fields_to_fill)
                    
                    if fields_filled > 0:
                        match_stats['successful_matches'] += 1
                        
                        # Determine match type for reporting
                        match_type = 'name_only' if (not country_crop or pd.isna(country_crop)) else 'crop_and_variety'
                        
                        # REDUCED: Only store minimal details for first 20 records
                        if len(match_stats['details']) < match_stats['sample_size']:
                            match_stats['details'].append({
                                'row': int(idx),
                                'variety_name': str(variety_name)[:50],  # Truncate long names
                                'country_crop': str(country_crop)[:30] if country_crop else 'NOT PROVIDED',
                                'match_type': match_type,
                                'match_count': len(all_matched_varieties),
                                'status': 'success',
                                'confidence': float(score)
                            })
                        
                        logger.info(f"Row {idx}: ✓ Matched '{country_key}' to {len(all_matched_varieties)} varieties (score: {score:.3f}, type: {match_type})")
                    else:
                        match_stats['failed_matches'] += 1
                        if len(match_stats['details']) < match_stats['sample_size']:
                            match_stats['details'].append({
                                'row': int(idx),
                                'variety_name': str(variety_name)[:50],
                                'status': 'no_fields_filled'
                            })
                else:
                    # No matches found
                    if country_crop and not pd.isna(country_crop):
                        match_stats['crop_mismatches'] += 1
                        logger.warning(f"Row {idx}: ✗ CROP MISMATCH (searched for: '{country_key}')")
                    
                    match_stats['failed_matches'] += 1
                    if len(match_stats['details']) < match_stats['sample_size']:
                        match_stats['details'].append({
                            'row': int(idx),
                            'variety_name': str(variety_name)[:50],
                            'status': 'crop_mismatch' if country_crop else 'variety_not_found'
                        })
            else:
                # No match found at all
                match_stats['failed_matches'] += 1
                if len(match_stats['details']) < match_stats['sample_size']:
                    match_stats['details'].append({
                        'row': int(idx),
                        'variety_name': str(variety_name)[:50],
                        'status': 'no_match'
                    })
                
        except Exception as e:
            logger.error(f"Error matching row {idx}: {str(e)}")
            match_stats['failed_matches'] += 1
            if len(match_stats['details']) < match_stats['sample_size']:
                match_stats['details'].append({
                    'row': int(idx),
                    'variety_name': str(variety_name)[:50] if variety_name else None,
                    'status': 'error',
                    'error': str(e)[:100]  # Truncate error messages
                })
            continue
    
    # Log comprehensive matching statistics
    success_rate = (match_stats['successful_matches'] / match_stats['total_processed']) * 100 if match_stats['total_processed'] > 0 else 0
    logger.info("=" * 80)
    logger.info(f"MATCHING COMPLETED - CROP+VARIETY CONCATENATED KEY APPROACH")
    logger.info(f"Total processed: {match_stats['total_processed']}")
    logger.info(f"Successful matches: {match_stats['successful_matches']} ({success_rate:.1f}%)")
    logger.info(f"Failed matches: {match_stats['failed_matches']}")
    logger.info(f"Crop mismatches: {match_stats['crop_mismatches']}")
    logger.info(f"Records without crop data: {match_stats['no_crop_provided']}")
    logger.info(f"Name-only matches (less reliable): {match_stats['name_only_matches']}")
    logger.info("=" * 80)
    
    # Store ONLY summary stats in session, not full details
    session['match_stats'] = {
        'total_processed': match_stats['total_processed'],
        'successful_matches': match_stats['successful_matches'],
        'failed_matches': match_stats['failed_matches'],
        'crop_mismatches': match_stats['crop_mismatches'],
        'no_crop_provided': match_stats['no_crop_provided'],
        'name_only_matches': match_stats['name_only_matches'],
        'success_rate': success_rate,
        'details': match_stats['details'][:20]  # Only first 20
    }
    
    return result_df

# API Resources
class VarietyAPI(Resource):
    def get(self, variety_id=None):
        if variety_id:
            variety = Variety.query.get_or_404(variety_id)
            return {
                'id': variety.id,
                'variety_release_name': variety.variety_release_name,
                'crop': variety.crop,
                'country': variety.country,
                'year_of_release': variety.year_of_release,
                'botanical_name': variety.botanical_name,
                'releasing_entity': variety.releasing_entity,
                'special_attributes': variety.special_attributes
            }
        else:
            page = request.args.get('page', 1, type=int)
            per_page = request.args.get('per_page', 50, type=int)
            query = request.args.get('q', '')
            
            varieties = Variety.query
            if query:
                varieties = varieties.filter(
                    Variety.variety_release_name.ilike(f'%{query}%') |
                    Variety.crop.ilike(f'%{query}%')
                )
            
            varieties = varieties.paginate(page=page, per_page=per_page, error_out=False)
            
            return {
                'varieties': [{
                    'id': v.id,
                    'variety_release_name': v.variety_release_name,
                    'crop': v.crop,
                    'country': v.country,
                    'year_of_release': v.year_of_release
                } for v in varieties.items],
                'total': varieties.total,
                'pages': varieties.pages,
                'current_page': page
            }

class SearchAPI(Resource):
    def get(self):
        """Advanced search API endpoint"""
        query = request.args.get('q', '')
        search_mode = request.args.get('mode', 'cascading')
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 50, type=int)
        
        # Build filters
        filters = {}
        filter_fields = ['crop', 'country', 'seed_type', 'year_of_release']
        
        for field in filter_fields:
            value = request.args.get(field)
            if value:
                filters[field] = value
        
        # Set search mode
        search_engine.set_search_mode(search_mode)
        
        # Perform search
        results = search_engine.search(query, filters if filters else None, search_mode)
        
        # Paginate results
        paginated_results = results.paginate(page=page, per_page=per_page, error_out=False)
        
        return {
            'query': query,
            'search_mode': search_mode,
            'filters': filters,
            'results': [{
                'id': v.id,
                'variety_release_name': v.variety_release_name,
                'crop': v.crop,
                'country': v.country,
                'year_of_release': v.year_of_release,
                'botanical_name': v.botanical_name,
                'releasing_entity': v.releasing_entity
            } for v in paginated_results.items],
            'pagination': {
                'total': paginated_results.total,
                'pages': paginated_results.pages,
                'current_page': page,
                'per_page': per_page
            }
        }
    
class AttributeExtractor:
    """Extract and classify traits from special_attributes text with improved context-aware logic"""
    
    # Class-level constants for better memory efficiency
    TRAITS = ('disease_tolerant', 'field_pest_resistant', 'drought_tolerant', 
              'storage_pest_resistant', 'consumer_preference')
    
    def __init__(self):
        # Optimized keyword dictionary for performance
        self.trait_keywords = {
            'disease_tolerant': {
                "YES": [
                    "resistant to", "tolerant to", "resistance to", "tolerance to",
                    "resistant against", "tolerant against", "immune to",
                    "disease resistant", "disease tolerance", "disease resistance",
                    "resistant to disease", "resistant to diseases", "tolerance to disease",
                    "tolerance to diseases", "resistance to disease", "resistance to diseases",
                    "mosaic disease", "cmd", "cassava mosaic", "late blight",
                    "downy mildew", "maize streak", "msv", "rust", "blight",
                    "bacterial blight", "fusarium", "wilt", "anthracnose",
                    "rosette", "septoria", "leaf spot", "curvularia",
                    "streak virus", "brown blotch", "follar diseases"
                ],
                "NO": [
                    "susceptible to", "susceptible to disease", "susceptible to diseases",
                    "prone to", "prone to disease", "prone to diseases",
                    "vulnerable to", "vulnerable to disease", "vulnerable to diseases",
                    "not resistant", "no resistance", "disease prone"
                ]
            },
            'field_pest_resistant': {
                "YES": [
                    "pest resistant", "pest resistance", "resistant to pest", "resistant to pests",
                    "resistance to pest", "resistance to pests", "resistant against pest",
                    "resistant against pests", "insect resistant", "insect resistance",
                    "resistant to insect", "resistant to insects", "field pest",
                    "stem borer", "fall armyworm", "thrips", "aphid","aphids"
                     "alectra", "weevil", "nematode",
                    "legume pod borer", "field insect"
                ],
                "NO": [
                    "pest susceptible", "insect damage", "pest prone",
                    "no pest resistance", "susceptible to pest", "susceptible to pests"
                ]
            },
            'drought_tolerant': {
                "YES": [
                    "drought tolerant", "drought tolerance", "drought resistant",
                    "drought resistance", "water stress", "dry condition",
                    "low rainfall", "water efficient", "dry season",
                    "leaf retention in dry season", "dry environment"
                ],
                "NO": [
                    "water loving", "high water requirement", "drought susceptible",
                    "not drought tolerant"
                ]
            },
            'storage_pest_resistant': {
                "YES": [
                    "storage pest resistant", "weevil resistant", "weevil resistance",
                    "storage resistant", "post-harvest pest resistant", "keeps well",
                    "good storage", "long storage", "bruchid resistance",
                    "store well", "storage quality", "resistant to storage pest"
                ],
                "NO": [
                    "poor storage", "storage susceptible", "weevil damage",
                    "short shelf life", "storage pest damage"
                ]
            },
            'consumer_preference': {
                "YES": [
                    "consumer preference", "preferred", "popular", "accepted",
                    "appealing", "attractive", "good quality", "excellent quality",
                    "high quality", "premium quality", "good for", "suitable for",
                    "good taste", "good flavor", "palatable", "cooking quality",
                    "mealiness", "poundable", "fast cooking", "good for gari",
                    "good for fufu", "good for flour", "good for pap",
                    "attractive color", "shining", "creamy", "large seed",
                    "big cob", "good appearance","high yielding","high biomass","early maturity"
                ],
                "NO": [
                    "poor quality", "unacceptable", "not preferred", "poor taste",
                    "bitter", "low quality","moderate yielding"
                ]
            }
        }
        
        # Negation words that flip positive to negative
        self.negation_words = [
            'not', 'no', 'non', 'without', 'lacks', 'lacking', 'never',
            'rarely', 'hardly', 'barely', 'insufficient', 'inadequate',
            'poor', 'low', 'weak', 'limited'
        ]
        
        # Compile negation pattern
        self.negation_pattern = re.compile(
            r'\b(?:' + '|'.join(re.escape(w) for w in self.negation_words) + r')\b',
            re.IGNORECASE
        )
        
        # Pre-compile optimized patterns
        self._compile_optimized_patterns()
        
        # Cache for default NO response
        self._default_response = {trait: 'NO' for trait in self.TRAITS}
    
    def _compile_optimized_patterns(self):
        """Compile patterns with proper escaping and ordering"""
        self.compiled_patterns = {}
        
        for trait, categories in self.trait_keywords.items():
            self.compiled_patterns[trait] = {}
            
            for category in ["NO", "YES"]:
                keywords = categories[category]
                if not keywords:
                    self.compiled_patterns[trait][category] = None
                    continue
                
                # Sort by length (longest first) to match most specific phrases
                sorted_keywords = sorted(keywords, key=len, reverse=True)
                
                # Create patterns with capture groups for context analysis
                patterns = []
                for kw in sorted_keywords:
                    # Pattern captures negation context (30 chars before keyword)
                    pattern = re.compile(
                        r'(.{0,30})\b' + re.escape(kw) + r'\b',
                        re.IGNORECASE
                    )
                    patterns.append((kw, pattern))
                
                self.compiled_patterns[trait][category] = patterns
    
    @staticmethod
    def _safe_convert_to_string(text):
        """Optimized string conversion"""
        if text is None:
            return ""
        if isinstance(text, str):
            return text
        try:
            return str(text)
        except:
            return ""
    
    @lru_cache(maxsize=10000)
    def _normalize_and_classify_all(self, text: str) -> Tuple[str, str, str, str, str]:
        """
        Single-pass normalization and classification for all traits.
        Returns tuple of results for all 5 traits.
        Cache entire result for identical inputs.
        """
        if not text or not text.strip():
            return ('NO', 'NO', 'NO', 'NO', 'NO')
        
        clean_text = text.lower().strip()
        
        # Process all traits
        results = []
        for trait in self.TRAITS:
            results.append(self._context_aware_classify(clean_text, trait))
        
        return tuple(results)
    
    def extract_traits(self, text, confidence_threshold=0.25):
        """
        High-performance trait extraction with context-aware classification
        
        Args:
            text: Input text to extract traits from
            confidence_threshold: Unused parameter, kept for interface compatibility
            
        Returns:
            Dict mapping trait names to 'YES' or 'NO'
        """
        safe_text = self._safe_convert_to_string(text)
        
        # Get cached results as tuple
        results_tuple = self._normalize_and_classify_all(safe_text)
        
        # Convert tuple to dict
        return dict(zip(self.TRAITS, results_tuple))
    
    def _context_aware_classify(self, text: str, trait: str) -> str:
        """
        Context-aware classification that considers:
        1. Proximity of negation words to keywords
        2. Explicit negative keywords (higher priority)
        3. Positive keywords without negation context
        4. Context window analysis for generic terms
        5. Score-based approach when both positive and negative signals exist
        
        Args:
            text: Normalized text to search
            trait: Trait name to classify
            
        Returns:
            'YES' or 'NO'
        """
        if trait not in self.compiled_patterns:
            return "NO"
        
        patterns_dict = self.compiled_patterns[trait]
        
        # Track matches with context
        no_matches = []
        yes_matches = []
        
        # Check NO patterns (explicit negative indicators)
        no_patterns = patterns_dict.get("NO")
        if no_patterns:
            for keyword, pattern in no_patterns:
                matches = pattern.finditer(text)
                for match in matches:
                    context = match.group(1) if match.lastindex else ""
                    no_matches.append({
                        'keyword': keyword,
                        'position': match.start(),
                        'context': context,
                        'is_explicit': True  # Explicit negative keyword
                    })
        
        # Check YES patterns (positive indicators)
        yes_patterns = patterns_dict.get("YES")
        if yes_patterns:
            for keyword, pattern in yes_patterns:
                matches = pattern.finditer(text)
                for match in matches:
                    context = match.group(1) if match.lastindex else ""
                    
                    # Check if negation word appears in context (within 30 chars before)
                    has_negation = bool(self.negation_pattern.search(context))
                    
                    if has_negation:
                        # Positive keyword negated = treat as negative
                        no_matches.append({
                            'keyword': keyword,
                            'position': match.start(),
                            'context': context,
                            'is_explicit': False  # Implicit negative (negated positive)
                        })
                    else:
                        # For generic terms, validate with context window
                        is_valid = self._validate_generic_match(keyword, match, text, trait)
                        
                        if is_valid:
                            # Genuine positive match
                            yes_matches.append({
                                'keyword': keyword,
                                'position': match.start(),
                                'context': context
                            })
        
        # Decision logic based on match analysis
        return self._resolve_classification(no_matches, yes_matches)
    
    def _validate_generic_match(self, keyword: str, match, text: str, trait: str) -> bool:
        """
        Validate generic keywords like "resistant to" by checking nearby context.
        
        For example:
        - "resistant to pests" should match field_pest_resistant
        - "resistant to disease" should match disease_tolerant
        - "resistant to drought" should match drought_tolerant
        
        Args:
            keyword: The matched keyword
            match: The regex match object
            text: Full text being analyzed
            trait: Current trait being classified
            
        Returns:
            True if match is valid for this trait, False otherwise
        """
        # Generic keywords that need context validation
        generic_keywords = {
            'resistant to', 'tolerant to', 'resistance to', 'tolerance to',
            'resistant against', 'tolerant against', 'susceptible to',
            'prone to', 'vulnerable to'
        }
        
        if keyword not in generic_keywords:
            return True  # Specific keywords always valid
        
        # Get 50 characters after the keyword for context
        match_end = match.end()
        context_window = text[match_end:match_end + 50].lower()
        
        # Define trait-specific context words
        trait_contexts = {
            'disease_tolerant': ['disease', 'diseases', 'blight', 'mosaic', 'wilt', 
                                 'mildew', 'rust', 'streak', 'virus', 'fungus'],
            'field_pest_resistant': ['field pest', 'field pests', 'borer', 
                                     'armyworm', 'thrips', 'aphid','beetle','mite','beetles','mites'],
            'drought_tolerant': ['drought', 'water', 'dry', 'rainfall', 'moisture'],
            'storage_pest_resistant': ['storage','storage pest','storage pests', 'weevil','rodents','beetle','mite','beetles','mites', 'bruchid', 'post-harvest'],
            'consumer_preference': ['high yielding','high','early','early maturity']  # Not applicable
        }
        
        if trait not in trait_contexts:
            return True
        
        context_words = trait_contexts[trait]
        if not context_words:
            return True  # No validation needed
        
        # Check if any context word appears in the window
        for context_word in context_words:
            if context_word in context_window:
                return True
        
        # Special case: if no specific context found, reject generic match
        # This prevents "resistant to" from matching everything
        return False
    
    def _resolve_classification(self, no_matches: List[Dict], yes_matches: List[Dict]) -> str:
        """
        Resolve classification when both positive and negative signals exist
        
        Priority rules:
        1. Explicit negative keywords (NO category) have highest priority
        2. If only negated positives, return NO
        3. If positive matches outweigh negative (by 2:1 ratio), return YES
        4. Otherwise, default to NO (conservative)
        
        Args:
            no_matches: List of negative matches with context
            yes_matches: List of positive matches with context
            
        Returns:
            'YES' or 'NO'
        """
        # No evidence = NO
        if not no_matches and not yes_matches:
            return "NO"
        
        # Only positive evidence = YES
        if yes_matches and not no_matches:
            return "YES"
        
        # Only negative evidence = NO
        if no_matches and not yes_matches:
            return "NO"
        
        # Both positive and negative evidence - need to resolve conflict
        
        # Count explicit negatives (from NO keyword category)
        explicit_negatives = sum(1 for m in no_matches if m.get('is_explicit', False))
        
        # If any explicit negative keywords, prioritize them
        if explicit_negatives > 0:
            # But if positive evidence strongly outweighs (3:1), trust positive
            if len(yes_matches) >= explicit_negatives * 3:
                return "YES"
            return "NO"
        
        # All negatives are implicit (negated positives)
        # Compare counts: positive must outweigh by 2:1
        if len(yes_matches) >= len(no_matches) * 2:
            return "YES"
        
        # Default to NO when uncertain (conservative approach)
        return "NO"
    
    def batch_extract_traits(self, texts: List, confidence_threshold=0.05) -> List[Dict[str, str]]:
        """
        Optimized batch processing
        
        Args:
            texts: List of texts to process
            confidence_threshold: Unused parameter, kept for interface compatibility
            
        Returns:
            List of trait dictionaries
        """
        # Pre-convert all texts to strings to optimize caching
        safe_texts = [self._safe_convert_to_string(text) for text in texts]
        
        # Process with cached normalization and classification
        return [dict(zip(self.TRAITS, self._normalize_and_classify_all(text))) 
                for text in safe_texts]
    
    def clear_cache(self):
        """Clear LRU cache to free memory if needed"""
        self._normalize_and_classify_all.cache_clear()
    
    def get_cache_info(self):
        """Get cache statistics for monitoring"""
        return self._normalize_and_classify_all.cache_info()


# Maintain backward compatibility
attribute_extractor = AttributeExtractor()
# Enhanced DataCleaner with attribute extraction
class EnhancedDataCleaner(DataCleaner):
    """Enhanced data cleaner with attribute extraction capability"""
    
    @staticmethod
    def extract_attributes_from_dataframe(df, special_attributes_column='special_attributes'):
        """
        Extract traits from special_attributes column and add as new columns
        """
        # Create a copy to avoid modifying the original
        result_df = df.copy()
        
        # First, validate the special_attributes_column exists
        if special_attributes_column not in result_df.columns:
            logger.warning(f"Special attributes column '{special_attributes_column}' not found. Available columns: {list(result_df.columns)}")
            # Initialize trait columns with 'NO' and return
            trait_columns = list(attribute_extractor.trait_keywords.keys())
            for trait in trait_columns:
                result_df[trait] = 'NO'
            return result_df
        
        # Initialize trait columns with 'NO'
        trait_columns = list(attribute_extractor.trait_keywords.keys())
        for trait in trait_columns:
            result_df[trait] = 'NO'
        
        # Process each row with comprehensive error handling
        processed_count = 0
        error_count = 0
        
        for idx in range(len(result_df)):
            try:
                # Safely get the row data
                row = result_df.iloc[idx]
                
                # Safely get the special attributes value
                special_attrs_value = row[special_attributes_column]
                
                # Extract traits using the robust attribute extractor
                traits = attribute_extractor.extract_traits(special_attrs_value)
                
                # Update the trait columns
                for trait, value in traits.items():
                    if trait in trait_columns:
                        result_df.at[result_df.index[idx], trait] = value
                
                processed_count += 1
                
            except Exception as e:
                error_count += 1
                logger.warning(f"Error processing row {idx} for attribute extraction: {str(e)}")
                # Continue with other rows even if one fails
        
        logger.info(f"Attribute extraction completed: {processed_count} records processed, {error_count} errors")
        
        return result_df
    
    @staticmethod
    def get_attribute_extraction_report(df, special_attributes_column='special_attributes'):
        """
        Generate report on attribute extraction results with native Python types
        """
        trait_columns = list(attribute_extractor.trait_keywords.keys())
        report = {
            'total_records': int(len(df)),
            'records_with_special_attributes': 0,
            'trait_distribution': {},
            'extraction_summary': {
                'successful_records': 0,
                'failed_records': 0
            }
        }
        
        # Calculate records with special attributes
        try:
            if special_attributes_column in df.columns:
                valid_count = 0
                for value in df[special_attributes_column]:
                    # Check if value exists and has content
                    if pd.notna(value) and value is not None:
                        if isinstance(value, str) and value.strip():
                            valid_count += 1
                        elif isinstance(value, (int, float)) and not pd.isna(value):
                            valid_count += 1
                        elif value:  # Any other truthy value
                            valid_count += 1
                report['records_with_special_attributes'] = valid_count
        except Exception as e:
            logger.warning(f"Error counting special attributes: {str(e)}")
        
        # Calculate distribution for each trait
        for trait in trait_columns:
            if trait in df.columns:
                try:
                    yes_count = int((df[trait] == 'YES').sum())
                    no_count = int(len(df) - yes_count)
                    yes_percentage = float((yes_count / len(df)) * 100) if len(df) > 0 else 0.0
                    
                    report['trait_distribution'][trait] = {
                        'yes_count': yes_count,
                        'no_count': no_count,
                        'yes_percentage': yes_percentage
                    }
                except Exception as e:
                    logger.warning(f"Error calculating distribution for {trait}: {str(e)}")
                    report['trait_distribution'][trait] = {
                        'yes_count': 0,
                        'no_count': int(len(df)),
                        'yes_percentage': 0.0
                    }
        
        return report
    
# Update the master import function to include attribute extraction
def process_master_import_with_attributes(df, column_mapping, filename):
    """Process master data import with automatic attribute extraction"""
    try:
        # Clear existing data
        Variety.query.delete()
        
        # Check if we have special_attributes column mapped
        special_attrs_mapped = False
        special_attrs_source_col = None
        
        for uploaded_col, db_col in column_mapping.items():
            if db_col == 'special_attributes':
                special_attrs_mapped = True
                special_attrs_source_col = uploaded_col
                break
        
        # Extract attributes if special_attributes column is available
        if special_attrs_mapped and special_attrs_source_col in df.columns:
            logger.info("Extracting traits from special_attributes column...")
            df = EnhancedDataCleaner.extract_attributes_from_dataframe(df, special_attrs_source_col)
            
            # Generate extraction report
            extraction_report = EnhancedDataCleaner.get_attribute_extraction_report(df, special_attrs_source_col)
            logger.info(f"Attribute extraction completed: {extraction_report}")
        
        # Import data with column mapping
        records_imported = 0
        skipped_records = 0
        
        for idx, row in df.iterrows():
            # Skip empty rows
            if row.isnull().all():
                skipped_records += 1
                continue
                
            variety_data = {}
            
            # Map data using column mapping
            for uploaded_col, db_col in column_mapping.items():
                if uploaded_col in df.columns:
                    value = row[uploaded_col]
                    # Convert pandas NaN to None
                    if pd.isna(value):
                        value = None
                    variety_data[db_col] = value
            
            # Also include extracted trait columns
            trait_columns = list(attribute_extractor.trait_keywords.keys())
            for trait_col in trait_columns:
                if trait_col in df.columns and pd.notna(row[trait_col]):
                    variety_data[trait_col] = row[trait_col]
            
            # Skip if essential data is missing
            if not variety_data.get('variety_release_name') and not variety_data.get('crop'):
                skipped_records += 1
                continue
            
            # Create variety instance
            variety = Variety(**variety_data)
            db.session.add(variety)
            records_imported += 1
            
            # Commit in batches to avoid memory issues
            if records_imported % 100 == 0:
                db.session.commit()
        
        # Final commit
        db.session.commit()
        
        # Log upload
        upload_log = UploadLog(
            filename=filename,
            file_type='master',
            record_count=records_imported
        )
        db.session.add(upload_log)
        db.session.commit()
        
        # Update stats
        stats = {
            'total_varieties': Variety.query.count(),
            'recent_uploads': UploadLog.query.filter_by(file_type='master').order_by(UploadLog.uploaded_at.desc()).limit(5).all()
        }
        
        flash(f'Successfully imported {records_imported} varieties with automatic trait extraction! {skipped_records} records were skipped.', 'success')
        return render_template('upload_master.html', stats=stats)
        
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error in process_master_import_with_attributes: {str(e)}")
        flash(f'Error importing data: {str(e)}', 'error')
        return redirect(url_for('upload_master'))
    


@app.route('/api/preview_columns', methods=['POST'])
def api_preview_columns():
    """API endpoint to preview columns from uploaded file"""
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400
    
    try:
        # Read the file
        df = pd.read_excel(file)
        
        # Analyze columns
        columns_info = []
        for col in df.columns:
            unique_count = df[col].nunique()
            non_null_count = df[col].notna().sum()
            completeness = (non_null_count / len(df)) * 100
            
            # Get sample values (first 3 non-null values)
            sample_values = df[col].dropna().head(3).astype(str).tolist()
            
            columns_info.append({
                'name': col,
                'unique_values': unique_count,
                'completeness': round(completeness, 1),
                'sample_values': sample_values
            })
        
        # Sort by completeness and reasonable group count
        columns_info.sort(key=lambda x: (-x['completeness'], x['unique_values']))
        
        return jsonify({
            'total_columns': len(df.columns),
            'total_rows': len(df),
            'columns': columns_info
        })
        
    except Exception as e:
        logger.error(f"Error previewing columns: {str(e)}")
        return jsonify({'error': str(e)}), 500

# Add new API endpoint for attribute extraction
@app.route('/api/extract_attributes', methods=['POST'])
def api_extract_attributes():
    """API endpoint for extracting attributes from text"""
    data = request.get_json()
    
    if not data or 'text' not in data:
        return jsonify({'error': 'No text provided'}), 400
    
    text = data['text']
    confidence_threshold = data.get('confidence_threshold', 0.3)
    
    traits = attribute_extractor.extract_traits(text, confidence_threshold)
    
    return jsonify({
        'input_text': text,
        'confidence_threshold': confidence_threshold,
        'traits': traits
    })
    

# Add a route to test attribute extraction
@app.route('/test_attribute_extraction', methods=['GET', 'POST'])
def test_attribute_extraction():
    """Test page for attribute extraction"""
    if request.method == 'POST':
        text = request.form.get('text', '')
        confidence_threshold = float(request.form.get('confidence_threshold', 0.3))
        
        traits = attribute_extractor.extract_traits(text, confidence_threshold)
        
        return render_template('test_attribute_extraction.html',
                             input_text=text,
                             confidence_threshold=confidence_threshold,
                             traits=traits,
                             trait_keywords=attribute_extractor.trait_keywords)
    
    return render_template('test_attribute_extraction.html',
                         input_text='',
                         traits={},
                         trait_keywords=attribute_extractor.trait_keywords)

# Update the main process_master_import function to use the enhanced version
def process_master_import(df, column_mapping, filename):
    """Main master import function - now with attribute extraction"""
    return process_master_import_with_attributes(df, column_mapping, filename)
# Register API endpoints
api.add_resource(VarietyAPI, '/api/varieties', '/api/varieties/<int:variety_id>')
api.add_resource(SearchAPI, '/api/search')

def map_your_headers(df):
    """Perfect mapping for your specific headers"""
    exact_mapping = {
        'COUNTRY': 'country',
        'CROP': 'crop', 
        'BOTANICAL NAME': 'botanical_name',
        'VARIETY RELEASE NAME': 'variety_release_name',
        'SEED TYPE': 'seed_type',
        'YEAR OF RELEASE': 'year_of_release',
        'RELEASING ENTITY/OWNER': 'releasing_entity',
        'MAINTAINER': 'maintainer',
        'PRODUCTION ALTITUDE (MASL)': 'production_altitude',
        'MINIMUM PRODUCTION ALTITUDE (MASL)': 'min_altitude',
        'MAXIMUM PRODUCTION ALTITUDE (MASL)': 'max_altitude',
        'RECOMMENDED AGROECOLOGICAL ZONES': 'agroecological_zones',
        'MATURITY (DAYS)': 'maturity_days',
        'YIELD (MT/HA)': 'yield_mt_ha',
        'PRESENCE IN REGIONAL CATALOGUE (COMESA, SADC, ECOWAS)': 'presence_in_regional_catalogue',
        'OTHER COUNTRIES OF RELEASE': 'other_countries_of_release',
        'COMMERCIALISING COMPANIES': 'commercialising_companies',
        'COMMERCIALISING NAMES': 'commercialising_names',
        'SPECIAL ATTRIBUTES /DUS & VCU (AT THE TIME OF RELEASE)': 'special_attributes',
        'TYPE OF LICENCE (EXCLUSIVE, NON-EXCLUSIVE, SEMI-EXCLUSIVE, PROPRIETARY, NOT LICENSED, LICENCE NOT REQUIRED)': 'licence_type',
        'MAINTENANCE STATUS (ACTIVELY MAINTAINED, SPORADICALLY MAINTAINED, NOT MAINTAINED, RETIRED)': 'maintenance_status',
        'COMMERCIALISING LEVEL (FULL, LIMITED, EMERGING, NOT COMMERCIALISED)': 'commercialising_level',
        'DISEASE TOLERANT': 'disease_tolerant',
        'FIELD PEST RESISTANT': 'field_pest_resistant', 
        'DROUGHT TOLERANT': 'drought_tolerant',
        'STORAGE PEST RESISTANT': 'storage_pest_resistant',
        'CONSUMER PREFERENCE': 'consumer_preference'
    }
    
    column_mapping = {}
    for uploaded_col in df.columns:
        # Exact match with your headers
        if uploaded_col in exact_mapping:
            column_mapping[uploaded_col] = exact_mapping[uploaded_col]
        else:
            # Fallback to fuzzy matching for any variations
            best_match, score, _ = process.extractOne(uploaded_col, exact_mapping.keys(), scorer=fuzz.partial_ratio)
            if score > 80:
                column_mapping[uploaded_col] = exact_mapping[best_match]
    
    return column_mapping

# ===== ROUTES =====

@app.route('/')
def index():
    """Home page with dashboard"""
    stats = {
        'total_varieties': Variety.query.count(),
        'recent_uploads': UploadLog.query.order_by(UploadLog.uploaded_at.desc()).limit(5).all()
    }
    return render_template('index.html', stats=stats, variety_count=Variety.query.count())

@app.route('/upload_master', methods=['GET', 'POST'])
def upload_master():
    """Upload master reference catalog with perfect column mapping"""
    # Get stats for the template
    stats = {
        'total_varieties': Variety.query.count(),
        'recent_uploads': UploadLog.query.filter_by(file_type='master').order_by(UploadLog.uploaded_at.desc()).limit(5).all()
    }
    
    if request.method == 'POST':
        if 'file' not in request.files:
            flash('No file selected', 'error')
            return render_template('upload_master.html', stats=stats)
        
        file = request.files['file']
        if file.filename == '':
            flash('No file selected', 'error')
            return render_template('upload_master.html', stats=stats)
        
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            filepath = os.path.join(app.config['UPLOAD_FOLDER'], 'master', filename)
            file.save(filepath)
            
            try:
                # Read Excel file
                df = pd.read_excel(filepath)
                
                # Use the existing mapping function
                column_mapping = map_your_headers(df)
                
                # Check if we have the essential columns mapped
                essential_columns = ['variety_release_name', 'crop', 'country', 'year_of_release']
                mapped_essential = [col for col in essential_columns if col in column_mapping.values()]
                
                if len(mapped_essential) < len(essential_columns):
                    # Show mapping interface for missing essentials
                    uploaded_columns = df.columns.tolist()
                    return render_template('map_master_columns.html',
                                         filename=filename,
                                         uploaded_columns=uploaded_columns,
                                         column_mapping=column_mapping,
                                         essential_columns=essential_columns,
                                         missing_essential=[col for col in essential_columns if col not in column_mapping.values()],
                                         master_columns=get_master_columns())
                
                # If all essential columns are mapped, proceed with import
                return process_master_import(df, column_mapping, filename)
                
            except Exception as e:
                logger.error(f"Error processing master file: {str(e)}")
                flash(f'Error processing file: {str(e)}', 'error')
                return render_template('upload_master.html', stats=stats)
    
    return render_template('upload_master.html', stats=stats,variety_count=Variety.query.count())

@app.route('/upload_country', methods=['GET', 'POST'])
def upload_country():
    """Upload country catalog for matching"""
    if request.method == 'POST':
        if 'file' not in request.files:
            flash('No file selected', 'error')
            return redirect(request.url)
        
        file = request.files['file']
        if file.filename == '':
            flash('No file selected', 'error')
            return redirect(request.url)
        
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            filepath = os.path.join(app.config['UPLOAD_FOLDER'], 'country', filename)
            file.save(filepath)
            
            try:
                # Read and clean the data
                df = pd.read_excel(filepath)
                cleaned_df = DataCleaner.clean_dataframe(df)
                
                # FIXED: Store file path only, not dataframe in session
                session['country_file_path'] = filepath
                session['country_filename'] = filename
                
                # Store in SessionData class (in-memory, not session)
                session_data.country_file_path = filepath
                session_data.country_df = df
                session_data.cleaned_country_df = cleaned_df
                
                # Use map_your_headers for initial column mapping
                initial_mapping = map_your_headers(cleaned_df)
                auto_mappings = auto_detect_columns(cleaned_df.columns.tolist())
                combined_mappings = {**auto_mappings, **initial_mapping}
                
                flash('File uploaded successfully! Please review column mappings.', 'success')
                return render_template('map_columns.html', 
                                     columns=cleaned_df.columns.tolist(), 
                                     auto_mappings=combined_mappings,
                                     master_columns=get_master_columns(),
                                     cleaning_report=DataCleaner.get_cleaning_report(df, cleaned_df))
                
            except Exception as e:
                flash(f'Error processing file: {str(e)}', 'error')
                return redirect(request.url)
    
    return render_template('upload_country.html', variety_count=Variety.query.count())

def calculate_quality_stats(data, columns):
    """Calculate data quality statistics"""
    if not data or not columns:
        return {
            'completion_rate': 0.0,
            'filled_fields': 0,
            'total_fields': 0,
            'empty_fields': 0
        }
    
    total_cells = len(data) * len(columns)
    filled_cells = 0
    
    for row in data:
        for column in columns:
            value = row.get(column)
            if value is not None and value != '' and str(value).strip() != '':
                filled_cells += 1
    
    completion_rate = round((filled_cells / total_cells) * 100, 1) if total_cells > 0 else 0.0
    
    return {
        'completion_rate': completion_rate,
        'filled_fields': filled_cells,
        'total_fields': total_cells,
        'empty_fields': total_cells - filled_cells
    }

@app.route('/map_columns', methods=['POST','GET'])
def map_columns():
    """Handle column mapping and start matching process"""
    if session_data.cleaned_country_df is None:
        flash('Please upload a file first', 'error')
        return redirect(url_for('upload_country'))
    
    column_mapping = {}
    fields_to_fill = []
    
    for key, value in request.form.items():
        if key.startswith('map_'):
            col_name = key[4:]
            if value:
                column_mapping[col_name] = value
        
        if key.startswith('fill_') and request.form.get(key) == 'on':
            fields_to_fill.append(key[5:])
    
    session_data.column_mapping = column_mapping
    session_data.fields_to_fill = fields_to_fill
    
    # Perform matching and filling
    try:
        matched_df = perform_matching(session_data.cleaned_country_df, column_mapping, fields_to_fill)
        session_data.matched_data = matched_df.to_dict('records')
        
        # Calculate some stats for the template
        total_rows = len(matched_df)
        columns = matched_df.columns.tolist()
        preview_data = session_data.matched_data[:10]  # First 10 rows for preview
        
        # Calculate quality statistics
        quality_stats = calculate_quality_stats(session_data.matched_data, columns)
        
        # Get match statistics from session
        match_stats = session.get('match_stats', None)
        
        return render_template('match_preview.html', 
                             data=preview_data,
                             total_rows=total_rows,
                             columns=columns,
                             quality_stats=quality_stats,
                             match_stats=match_stats)
    
    except Exception as e:
        logger.error(f"Error during matching: {str(e)}", exc_info=True)
        flash(f'Error during matching: {str(e)}', 'error')
        return redirect(url_for('upload_country'))

    
@app.route('/match_preview')
def match_preview():
    """Show matching preview"""
    if session_data.matched_data is None:
        flash('No matched data available. Please upload and map a file first.', 'error')
        return redirect(url_for('upload_country'))
    
    preview_data = session_data.matched_data[:10]
    all_columns = session_data.cleaned_country_df.columns.tolist() + session_data.fields_to_fill
    
    # Calculate quality statistics
    quality_stats = calculate_quality_stats(session_data.matched_data, all_columns)
    
    # Get match statistics from session
    match_stats = session.get('match_stats', None)
    
    return render_template('match_preview.html',
                         data=preview_data,
                         total_rows=len(session_data.matched_data),
                         columns=all_columns,
                         quality_stats=quality_stats,
                         match_stats=match_stats)

@app.route('/export')
def export_data():
    """Export matched data as Excel"""
    if session_data.matched_data is None:
        flash('No data to export', 'error')
        return redirect(url_for('index'))
    
    try:
        df = pd.DataFrame(session_data.matched_data)
        export_path = os.path.join(app.config['UPLOAD_FOLDER'], 'country', 'matched_output.xlsx')
        df.to_excel(export_path, index=False)
        
        # Log the export
        upload_log = UploadLog(
            filename='matched_output.xlsx',
            file_type='export',
            record_count=len(df)
        )
        db.session.add(upload_log)
        db.session.commit()
        
        return send_file(export_path, as_attachment=True, download_name='matched_varieties.xlsx')
    
    except Exception as e:
        flash(f'Error exporting file: {str(e)}', 'error')
        return redirect(url_for('index'))

@app.route('/search')
def search():
    """Advanced search page with cascading/global modes"""
    query = request.args.get('q', '')
    search_mode = request.args.get('mode', 'cascading')
    crop_filter = request.args.get('crop', '')
    country_filter = request.args.get('country', '')
    seed_type_filter = request.args.get('seed_type', '')
    year_filter = request.args.get('year', '')
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 20, type=int)
    
    # Set search mode
    search_engine.set_search_mode(search_mode)
    
    # Build filters
    filters = {}
    if crop_filter:
        filters['crop'] = crop_filter
    if country_filter:
        filters['country'] = country_filter
    if seed_type_filter:
        filters['seed_type'] = seed_type_filter
    if year_filter:
        filters['year_of_release'] = year_filter
    
    # Perform search
    results = search_engine.search(query, filters if filters else None, search_mode)
    
    # Get unique values for filter dropdowns
    crops = db.session.query(Variety.crop).distinct().filter(Variety.crop.isnot(None)).all()
    countries = db.session.query(Variety.country).distinct().filter(Variety.country.isnot(None)).all()
    seed_types = db.session.query(Variety.seed_type).distinct().filter(Variety.seed_type.isnot(None)).all()
    years = db.session.query(Variety.year_of_release).distinct().filter(Variety.year_of_release.isnot(None)).order_by(Variety.year_of_release.desc()).all()
    
    # Paginate results
    varieties_pagination = results.paginate(
        page=page, 
        per_page=per_page, 
        error_out=False
    )
    
    varieties = varieties_pagination.items
    
    # Calculate range display values
    start_item = (page - 1) * per_page + 1
    end_item = min(page * per_page, varieties_pagination.total) if varieties_pagination else 0
    
    return render_template('search.html', 
                         varieties=varieties,
                         pagination=varieties_pagination,
                         query=query,
                         search_mode=search_mode,
                         crops=[c[0] for c in crops],
                         countries=[c[0] for c in countries],
                         seed_types=[s[0] for s in seed_types],
                         years=[y[0] for y in years],
                         current_filters={
                             'crop': crop_filter,
                             'country': country_filter,
                             'seed_type': seed_type_filter,
                             'year': year_filter
                         },
                         page=page,
                         per_page=per_page,
                         start_item=start_item,
                         end_item=end_item,variety_count=Variety.query.count())

@app.route('/view/<int:id>')
def view_variety(id):
    """View detailed variety information"""
    variety = Variety.query.get_or_404(id)
    return render_template('variety_detail.html', variety=variety,variety_count=Variety.query.count())

@app.route('/data-quality')
def data_quality():
    """Data quality dashboard with comprehensive statistics"""
    try:
        total_varieties = Variety.query.count()
        
        if total_varieties == 0:
            # Database is empty
            flash('No varieties in the database. Please upload master catalog data first.', 'warning')
            return render_template('data_quality.html',
                                 total_varieties=0,
                                 completeness={},
                                 recent_uploads=[],
                                 variety_count=0,
                                 crop_distribution={},
                                 country_distribution={},
                                 year_distribution={},
                                 has_data=False)
        
        # Calculate field completeness
        fields = [
            'variety_release_name', 'crop', 'country', 'year_of_release', 
            'botanical_name', 'seed_type', 'releasing_entity', 'maintainer',
            'maturity_days', 'yield_mt_ha', 'agroecological_zones',
            'special_attributes', 'disease_tolerant', 'field_pest_resistant',
            'drought_tolerant', 'storage_pest_resistant', 'consumer_preference'
        ]
        
        completeness = {}
        for field in fields:
            try:
                # Count non-null AND non-empty values
                non_null_count = Variety.query.filter(
                    getattr(Variety, field).isnot(None),
                    getattr(Variety, field) != '',
                    getattr(Variety, field) != 'NO'  # Exclude default 'NO' values for trait fields
                ).count()
                
                completeness[field] = {
                    'count': non_null_count,
                    'percentage': round((non_null_count / total_varieties) * 100, 1) if total_varieties > 0 else 0
                }
            except Exception as e:
                logger.error(f"Error calculating completeness for {field}: {str(e)}")
                completeness[field] = {'count': 0, 'percentage': 0}
        
        # Get crop distribution
        crop_distribution = {}
        try:
            crops = db.session.query(
                Variety.crop, 
                db.func.count(Variety.id)
            ).filter(
                Variety.crop.isnot(None),
                Variety.crop != ''
            ).group_by(Variety.crop).order_by(db.func.count(Variety.id).desc()).limit(10).all()
            
            crop_distribution = {crop: count for crop, count in crops}
        except Exception as e:
            logger.error(f"Error getting crop distribution: {str(e)}")
        
        # Get country distribution
        country_distribution = {}
        try:
            countries = db.session.query(
                Variety.country, 
                db.func.count(Variety.id)
            ).filter(
                Variety.country.isnot(None),
                Variety.country != ''
            ).group_by(Variety.country).order_by(db.func.count(Variety.id).desc()).limit(10).all()
            
            country_distribution = {country: count for country, count in countries}
        except Exception as e:
            logger.error(f"Error getting country distribution: {str(e)}")
        
        # Get year distribution
        year_distribution = {}
        try:
            years = db.session.query(
                Variety.year_of_release, 
                db.func.count(Variety.id)
            ).filter(
                Variety.year_of_release.isnot(None),
                Variety.year_of_release != ''
            ).group_by(Variety.year_of_release).order_by(Variety.year_of_release.desc()).limit(10).all()
            
            year_distribution = {year: count for year, count in years}
        except Exception as e:
            logger.error(f"Error getting year distribution: {str(e)}")
        
        # Recent uploads
        recent_uploads = UploadLog.query.order_by(UploadLog.uploaded_at.desc()).limit(10).all()
        
        # Calculate overall data quality score
        total_completeness = sum(c['percentage'] for c in completeness.values())
        avg_completeness = total_completeness / len(completeness) if completeness else 0
        
        # Quality insights
        quality_insights = []
        if avg_completeness < 30:
            quality_insights.append({
                'level': 'critical',
                'message': 'Data completeness is critically low. Consider enriching your dataset.'
            })
        elif avg_completeness < 60:
            quality_insights.append({
                'level': 'warning',
                'message': 'Data completeness can be improved. Focus on filling key fields.'
            })
        else:
            quality_insights.append({
                'level': 'success',
                'message': 'Good data quality! Continue maintaining your catalog.'
            })
        
        # Find fields with lowest completeness
        sorted_completeness = sorted(completeness.items(), key=lambda x: x[1]['percentage'])
        low_completeness_fields = [
            {'field': field, 'percentage': data['percentage']} 
            for field, data in sorted_completeness[:5] 
            if data['percentage'] < 50
        ]
        
        if low_completeness_fields:
            quality_insights.append({
                'level': 'info',
                'message': f"Low completeness in: {', '.join([f['field'] for f in low_completeness_fields[:3]])}"
            })
        
        return render_template('data_quality.html',
                             total_varieties=total_varieties,
                             completeness=completeness,
                             recent_uploads=recent_uploads,
                             variety_count=total_varieties,
                             crop_distribution=crop_distribution,
                             country_distribution=country_distribution,
                             year_distribution=year_distribution,
                             avg_completeness=round(avg_completeness, 1),
                             quality_insights=quality_insights,
                             low_completeness_fields=low_completeness_fields,
                             has_data=True)
    
    except Exception as e:
        logger.error(f"Error in data_quality route: {str(e)}", exc_info=True)
        flash(f'Error loading data quality dashboard: {str(e)}', 'error')
        return render_template('data_quality.html',
                             total_varieties=0,
                             completeness={},
                             recent_uploads=[],
                             variety_count=0,
                             crop_distribution={},
                             country_distribution={},
                             year_distribution={},
                             has_data=False)
    
# Add this to your app.py

@app.route('/split_data', methods=['GET', 'POST'])
def split_data():
    """Automatically create sheets from uploaded file based on selected column"""
    if request.method == 'POST':
        if 'file' not in request.files:
            flash('No file selected', 'error')
            return redirect(request.url)
        
        file = request.files['file']
        if file.filename == '':
            flash('No file selected', 'error')
            return redirect(request.url)
        
        if file and allowed_file(file.filename):
            try:
                # Read the uploaded file
                df = pd.read_excel(file)
                
                # Get splitting parameters from form
                split_column = request.form.get('split_column')
                output_type = request.form.get('output_type', 'single_file')
                file_prefix = request.form.get('file_prefix', '').strip()
                include_summary = request.form.get('include_summary') == 'on'
                min_records_per_sheet = int(request.form.get('min_records_per_sheet', 1))
                
                # Validate split column exists
                if not split_column:
                    flash('Please select a column to split by', 'error')
                    return redirect(request.url)
                
                if split_column not in df.columns:
                    flash(f"Column '{split_column}' not found in the file", 'error')
                    return redirect(request.url)
                
                # Use existing column mapping function to get friendly names
                column_mapping = map_your_headers(df)
                split_column_friendly = column_mapping.get(split_column, split_column)
                
                # Clean the split column - handle missing values
                df_clean = df.copy()
                df_clean[split_column] = df_clean[split_column].fillna('Unknown')
                
                # Get unique values for splitting and their actual counts
                group_counts = {}
                valid_groups = []
                
                for value in df_clean[split_column].unique():
                    count = len(df_clean[df_clean[split_column] == value])
                    if count >= min_records_per_sheet:
                        group_counts[value] = count
                        valid_groups.append(value)
                
                if not valid_groups:
                    flash(f"No groups found with at least {min_records_per_sheet} records", 'error')
                    return redirect(request.url)
                
                # Generate sheets based on output type
                if output_type == 'single_file':
                    output_path = generate_single_file_sheets(
                        df_clean, split_column, valid_groups, 
                        file_prefix, include_summary, split_column_friendly
                    )
                    download_name = f"{file_prefix}_{split_column_friendly}_split_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
                else:
                    output_path = generate_multiple_files(
                        df_clean, split_column, valid_groups,
                        file_prefix, split_column_friendly
                    )
                    download_name = f"{file_prefix}_{split_column_friendly}_split_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"
                
                # Store in session for download
                session['split_output_path'] = output_path
                session['split_download_name'] = download_name
                session['split_stats'] = {
                    'total_groups': len(valid_groups),
                    'total_records': len(df),
                    'split_column': split_column_friendly,
                    'output_type': output_type,
                    'file_prefix': file_prefix,
                    'groups': valid_groups,
                    'group_counts': group_counts  # Add actual counts for each group
                }
                
                flash(f'Successfully created {len(valid_groups)} sheets from {split_column_friendly}!', 'success')
                return render_template('split_results.html',
                                    stats=session['split_stats'],
                                    groups=valid_groups,
                                    group_counts=group_counts,  # Pass counts to template
                                    output_type=output_type)
                
            except Exception as e:
                logger.error(f"Error splitting data: {str(e)}")
                flash(f'Error processing file: {str(e)}', 'error')
                return redirect(request.url)
    
    # GET request - show upload form
    return render_template('split_data.html', variety_count=Variety.query.count())

def generate_single_file_sheets(df, split_column, values, file_prefix, include_summary, split_column_friendly):
    """Generate a single Excel file with multiple sheets"""
    output_dir = os.path.join(app.config['UPLOAD_FOLDER'], 'split_output')
    os.makedirs(output_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_filename = f"{file_prefix}_{split_column_friendly}_split_{timestamp}.xlsx"
    output_path = os.path.join(output_dir, output_filename)
    
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        # Create sheets for each value
        for value in values:
            # Filter data for this specific value - returns ALL columns for matching rows
            sheet_data = df[df[split_column] == value]
            
            # Generate sheet name using prefix + value
            if file_prefix:
                sheet_name = f"{file_prefix}_{value}"
            else:
                sheet_name = f"{value}"
            
            # Make sheet name safe for Excel
            sheet_name = generate_safe_sheet_name(sheet_name)
            
            # Write ALL data (not just filtered column) to the sheet
            sheet_data.to_excel(writer, sheet_name=sheet_name, index=False)
        
        # Add summary sheet if requested
        if include_summary:
            summary_data = generate_summary(df, split_column, values, file_prefix)
            summary_data.to_excel(writer, sheet_name='Summary', index=False)
    
    return output_path

def generate_multiple_files(df, split_column, values, file_prefix, split_column_friendly):
    """Generate multiple Excel files (one per group) and return as zip"""
    output_dir = os.path.join(app.config['UPLOAD_FOLDER'], 'split_output', f"split_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
    os.makedirs(output_dir, exist_ok=True)
    
    for value in values:
        # Filter data for this specific value - returns ALL columns for matching rows
        sheet_data = df[df[split_column] == value]
        
        # Generate filename using prefix + value
        if file_prefix:
            filename = f"{file_prefix}_{value}.xlsx"
        else:
            filename = f"{value}.xlsx"
        
        # Make filename safe
        filename = generate_safe_filename(filename)
        
        filepath = os.path.join(output_dir, filename)
        
        # Write ALL data to the individual file
        sheet_data.to_excel(filepath, index=False)
    
    # Create zip file
    zip_path = f"{output_dir}.zip"
    with zipfile.ZipFile(zip_path, 'w') as zipf:
        for root, dirs, files in os.walk(output_dir):
            for file in files:
                file_path = os.path.join(root, file)
                arcname = os.path.relpath(file_path, output_dir)
                zipf.write(file_path, arcname)
    
    # Clean up individual files
    shutil.rmtree(output_dir)
    
    return zip_path

def generate_summary(df, split_column, values, file_prefix):
    """Generate summary statistics for the split operation"""
    summary_data = []
    
    for value in values:
        # Get ALL data for this group
        group_data = df[df[split_column] == value]
        
        # Calculate comprehensive statistics
        total_columns = len(df.columns)
        filled_cells = group_data.count().sum()
        total_cells = len(group_data) * total_columns
        
        summary_data.append({
            'Group Name': f"{file_prefix}_{value}" if file_prefix else value,
            'Record Count': len(group_data),
            'Total Data Cells': total_cells,
            'Filled Cells': filled_cells,
            'Data Completeness (%)': round((filled_cells / total_cells) * 100, 1) if total_cells > 0 else 0,
            'Columns with 100% Data': (group_data.count() == len(group_data)).sum(),
            'Split Column': split_column,
            'File Prefix': file_prefix if file_prefix else 'None'
        })
    
    return pd.DataFrame(summary_data)

def generate_safe_sheet_name(name, max_length=31):
    """Generate Excel-safe sheet name"""
    # Remove invalid characters
    safe_name = re.sub(r'[\\/*?\[\]:]', '', str(name))
    
    # Truncate if too long
    if len(safe_name) > max_length:
        safe_name = safe_name[:max_length-3] + '...'
    
    return safe_name

def generate_safe_filename(name):
    """Generate filesystem-safe filename"""
    # Remove invalid characters
    safe_name = re.sub(r'[<>:"/\\|?*]', '', str(name))
    
    # Replace spaces with underscores
    safe_name = safe_name.replace(' ', '_')
    
    return safe_name

@app.route('/download_split_data')
def download_split_data():
    """Download the generated split data"""
    file_path = session.get('split_output_path')
    download_name = session.get('split_download_name', 'split_data.xlsx')
    
    if not file_path or not os.path.exists(file_path):
        flash('No split data found. Please generate data first.', 'error')
        return redirect(url_for('split_data'))
    
    try:
        if file_path.endswith('.zip'):
            mimetype = 'application/zip'
        else:
            mimetype = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        
        return send_file(
            file_path,
            as_attachment=True,
            download_name=download_name,
            mimetype=mimetype
        )
    except Exception as e:
        flash(f'Error downloading file: {str(e)}', 'error')
        return redirect(url_for('split_data'))

# Helper function to get column suggestions for the template
def get_column_suggestions(df):
    """Get suggested columns for splitting based on data characteristics"""
    suggestions = []
    
    for col in df.columns:
        unique_count = df[col].nunique()
        total_count = len(df)
        
        # Good candidates for splitting have multiple values but not too many
        if 2 <= unique_count <= 50:  # Reasonable number of groups
            completeness = (df[col].notna().sum() / total_count) * 100
            suggestions.append({
                'column': col,
                'unique_values': unique_count,
                'completeness': round(completeness, 1),
                'sample_values': df[col].dropna().unique()[:5].tolist()  # First 5 non-null values
            })
    
    # Sort by completeness and reasonable group count
    suggestions.sort(key=lambda x: (-x['completeness'], x['unique_values']))
    return suggestions[:10]  # Return top 10 suggestions

@app.route('/advanced_filter', methods=['GET', 'POST'])
def advanced_filter():
    """Advanced Excel-like filtering interface using database"""
    
    # Check if user wants to start fresh (from "New File" button or URL parameter)
    if request.args.get('new') == 'true' or request.form.get('action') == 'new_file':
        # Clear the filter session
        session_id = session.get('filter_session_id')
        if session_id:
            try:
                # Delete filter session and associated data from database
                filter_session = FilterSession.query.filter_by(session_id=session_id).first()
                if filter_session:
                    # Delete associated filtered data first
                    FilteredData.query.filter_by(filter_session_id=filter_session.id).delete()
                    # Then delete the session
                    db.session.delete(filter_session)
                    db.session.commit()
            except Exception as e:
                logger.error(f"Error clearing filter session: {str(e)}")
        
        # Clear session variables
        session.pop('filter_session_id', None)
        flash('Started new filter session. Please upload a new file.', 'info')
        return redirect(url_for('advanced_filter'))

    if request.method == 'POST':
        if 'file' not in request.files:
            flash('No file selected', 'error')
            return redirect(request.url)
        
        file = request.files['file']
        if file.filename == '':
            flash('No file selected', 'error')
            return redirect(request.url)
        
        if file and allowed_file(file.filename):
            try:
                # Clear any existing session first
                existing_session_id = session.get('filter_session_id')
                if existing_session_id:
                    filter_session = FilterSession.query.filter_by(session_id=existing_session_id).first()
                    if filter_session:
                        FilteredData.query.filter_by(filter_session_id=filter_session.id).delete()
                        db.session.delete(filter_session)
                        db.session.commit()
                
                # Read and process the new file
                df = pd.read_excel(file)
                df_clean = DataCleaner.clean_dataframe(df)
                column_mapping = map_your_headers(df_clean)
                
                # Generate unique session ID
                import uuid
                session_id = str(uuid.uuid4())
                
                # Store data in database
                filter_session = store_data_in_database(session_id, df_clean, column_mapping, file.filename)
                
                # Build filter interface data
                filter_data = build_filter_data_from_db(filter_session)
                
                # Store session ID in Flask session
                session['filter_session_id'] = session_id
                
                flash(f'File uploaded successfully! {len(df_clean)} records loaded.', 'success')
                return render_template('advanced_filter.html', 
                                    filter_data=filter_data,
                                    filter_state={})
                
            except Exception as e:
                logger.error(f"Error processing file: {str(e)}", exc_info=True)
                flash(f'Error processing file: {str(e)}', 'error')
                return redirect(request.url)
    
    # GET request - check if we have an active session
    session_id = session.get('filter_session_id')
    if session_id:
        try:
            filter_session = FilterSession.query.filter_by(session_id=session_id).first()
            if filter_session and filter_session.expires_at > datetime.utcnow():
                filter_data = build_filter_data_from_db(filter_session)
                filter_state = json.loads(filter_session.filter_state) if filter_session.filter_state else {}
                
                return render_template('advanced_filter.html',
                                    filter_data=filter_data,
                                    filter_state=filter_state)
            else:
                # Session expired or not found
                session.pop('filter_session_id', None)
                flash('Your filter session has expired. Please upload your file again.', 'info')
        except Exception as e:
            logger.error(f"Error loading session: {str(e)}")
            session.pop('filter_session_id', None)
            flash('Error loading your session. Please upload your file again.', 'error')
    
    return render_template('advanced_filter.html', 
                         filter_data=None,
                         filter_state={})
def cleanup_expired_sessions():
    """Clean up expired filter sessions from database"""
    try:
        expired_sessions = FilterSession.query.filter(
            FilterSession.expires_at < datetime.utcnow()
        ).all()
        
        for session in expired_sessions:
            # Delete associated filtered data
            FilteredData.query.filter_by(filter_session_id=session.id).delete()
            # Delete the session
            db.session.delete(session)
        
        db.session.commit()
        logger.info(f"Cleaned up {len(expired_sessions)} expired filter sessions")
        
    except Exception as e:
        logger.error(f"Error cleaning up expired sessions: {str(e)}")
        db.session.rollback()

# You can call this function periodically or in app startup
@app.before_request
def before_request():
    """Run before each request"""
    # Clean up expired sessions occasionally (1% of requests)
    import random
    if random.random() < 0.01:
        cleanup_expired_sessions()

def store_data_in_database(session_id, df, column_mapping, filename):
    """Store uploaded data in database tables"""
    # Convert data to JSON for storage
    data_json = df.to_json(orient='records')
    mapping_json = json.dumps(column_mapping)
    
    # Calculate expiration (24 hours from now)
    expires_at = datetime.utcnow() + timedelta(hours=24)
    
    # Create filter session
    filter_session = FilterSession(
        session_id=session_id,
        filename=filename,
        original_data=data_json,
        column_mapping=mapping_json,
        filter_state='{}',
        expires_at=expires_at
    )
    
    db.session.add(filter_session)
    db.session.commit()
    
    # Pre-calculate distinct values for each column and store them
    for col_name in list(df.columns):
        try:
            distinct_values = df[col_name].dropna().unique().tolist()
            distinct_values = [str(val) for val in distinct_values if val not in ['', 'nan', 'NaN', None] and str(val).strip()]
            distinct_values.sort()
            
            for value in distinct_values[:100]:  # Store first 100 values
                filtered_data = FilteredData(
                    filter_session_id=filter_session.id,
                    column_name=col_name,
                    value=value,
                    record_count=len(df[df[col_name].astype(str) == value])
                )
                db.session.add(filtered_data)
        except Exception as e:
            logger.error(f"Error processing column '{col_name}': {str(e)}")
            continue
    
    db.session.commit()
    return filter_session


def build_filter_data_from_db(filter_session):
    """Build filter data structure from database"""
    # Get distinct values for each column from database
    columns_data = {}
    
    # Get all unique column names for this session
    distinct_columns = db.session.query(FilteredData.column_name).filter_by(
        filter_session_id=filter_session.id
    ).distinct().all()
    
    column_mapping = json.loads(filter_session.column_mapping)
    
    for col_tuple in distinct_columns:
        col_name = col_tuple[0]
        
        # Get all values for this column
        values_query = FilteredData.query.filter_by(
            filter_session_id=filter_session.id,
            column_name=col_name
        ).order_by(FilteredData.value).all()
        
        values = [item.value for item in values_query]
        
        columns_data[col_name] = {
            'friendly_name': column_mapping.get(col_name, col_name),
            'values': values,
            'total_values': len(values)
        }
    
    return {
        'filename': filter_session.filename,
        'columns': columns_data,
        'total_records': len(json.loads(filter_session.original_data))
    }


@app.route('/advanced_filter/apply', methods=['POST'])
def apply_advanced_filters():
    """Apply filters and return matching record count"""
    session_id = session.get('filter_session_id')
    if not session_id:
        return jsonify({'error': 'No active filter session'}), 400
    
    try:
        filters = request.get_json()
        
        # Get filter session
        filter_session = FilterSession.query.filter_by(session_id=session_id).first()
        if not filter_session:
            return jsonify({'error': 'Filter session not found'}), 404
        
        # Update filter state in database
        filter_session.filter_state = json.dumps(filters)
        db.session.commit()
        
        # Load original data
        original_data = json.loads(filter_session.original_data)
        df = pd.DataFrame(original_data)
        
        # Apply filters
        if filters:
            mask = pd.Series([True] * len(df))
            for column, selected_values in filters.items():
                if selected_values and len(selected_values) > 0:
                    # Convert both to strings for comparison
                    column_mask = df[column].astype(str).isin([str(v) for v in selected_values])
                    mask = mask & column_mask
            
            filtered_df = df[mask]
            total_matching = len(filtered_df)
        else:
            total_matching = len(df)
        
        return jsonify({
            'success': True,
            'total_matching': total_matching,
            'total_original': len(df),
            'filters_applied': sum(1 for f in filters.values() if f)
        })
        
    except Exception as e:
        logger.error(f"Error applying filters: {str(e)}", exc_info=True)
        return jsonify({'error': str(e)}), 500


@app.route('/advanced_filter/export', methods=['POST'])
def export_advanced_filter():
    """Export filtered data to Excel"""
    session_id = session.get('filter_session_id')
    if not session_id:
        flash('No active filter session', 'error')
        return redirect(url_for('advanced_filter'))
    
    try:
        # Get filter session
        filter_session = FilterSession.query.filter_by(session_id=session_id).first()
        if not filter_session:
            flash('Filter session not found', 'error')
            return redirect(url_for('advanced_filter'))
        
        # Load original data and filters
        original_data = json.loads(filter_session.original_data)
        df = pd.DataFrame(original_data)
        filters = json.loads(filter_session.filter_state) if filter_session.filter_state else {}
        
        # Apply filters
        if filters:
            mask = pd.Series([True] * len(df))
            for column, selected_values in filters.items():
                if selected_values and len(selected_values) > 0:
                    column_mask = df[column].astype(str).isin([str(v) for v in selected_values])
                    mask = mask & column_mask
            
            filtered_df = df[mask]
        else:
            filtered_df = df
        
        # Create Excel file in memory
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            filtered_df.to_excel(writer, index=False, sheet_name='Filtered Data')
        
        output.seek(0)
        
        # Generate filename
        base_filename = os.path.splitext(filter_session.filename)[0]
        export_filename = f"{base_filename}_filtered_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=export_filename
        )
        
    except Exception as e:
        logger.error(f"Error exporting filtered data: {str(e)}", exc_info=True)
        flash(f'Error exporting data: {str(e)}', 'error')
        return redirect(url_for('advanced_filter'))


# Helper function to check allowed file extensions
def allowed_file(filename):
    """Check if file extension is allowed"""
    ALLOWED_EXTENSIONS = {'xlsx', 'xls'}
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route('/api/search/suggest')
def search_suggest():
    """API endpoint for search suggestions"""
    query = request.args.get('q', '')
    field = request.args.get('field', 'variety_release_name')
    
    if not query or len(query) < 2:
        return jsonify([])
    
    search_query = f"%{query}%"
    
    if field == 'variety_release_name':
        suggestions = Variety.query.filter(
            Variety.variety_release_name.ilike(search_query)
        ).with_entities(Variety.variety_release_name).distinct().limit(10).all()
    elif field == 'crop':
        suggestions = Variety.query.filter(
            Variety.crop.ilike(search_query)
        ).with_entities(Variety.crop).distinct().limit(10).all()
    elif field == 'country':
        suggestions = Variety.query.filter(
            Variety.country.ilike(search_query)
        ).with_entities(Variety.country).distinct().limit(10).all()
    else:
        suggestions = []
    
    return jsonify([s[0] for s in suggestions if s[0]])

@app.route('/api/search/mode', methods=['GET', 'POST'])
def search_mode_api():
    """API endpoint for search mode management"""
    if request.method == 'POST':
        data = request.get_json()
        mode = data.get('mode', 'cascading')
        
        if search_engine.set_search_mode(mode):
            return jsonify({'message': f'Search mode set to {mode}', 'mode': mode})
        else:
            return jsonify({'error': 'Invalid search mode'}), 400
    
    return jsonify({'current_mode': search_engine.current_mode})

@app.route('/advanced_matching', methods=['POST'])
def advanced_matching():
    """Advanced matching endpoint"""
    if session_data.cleaned_country_df is None:
        return jsonify({'error': 'No data available for matching'}), 400
    
    threshold = float(request.form.get('threshold', 1.0))
    matching_field = request.form.get('matching_field', 'variety_release_name')
    
    # Prepare matching engine
    variety_names = [v.variety_release_name for v in Variety.query.all() if v.variety_release_name]
    matching_engine.fit(variety_names)
    
    # Perform matching
    queries = session_data.cleaned_country_df[matching_field].fillna('').tolist()
    matching_results = []
    
    for query in queries:
        matched_name, score = matching_engine.advanced_match(query, threshold)
        matching_results.append({
            'query': query,
            'matched_name': matched_name,
            'confidence_score': score,
            'is_match': score >= threshold
        })
    
    session_data.matching_results = matching_results
    
    # Statistics
    matches_found = sum(1 for r in matching_results if r['is_match'])
    avg_confidence = np.mean([r['confidence_score'] for r in matching_results if r['is_match']]) if matches_found > 0 else 0
    
    return jsonify({
        'total_queries': len(queries),
        'matches_found': matches_found,
        'match_rate': (matches_found / len(queries)) * 100,
        'average_confidence': avg_confidence,
        'results': matching_results[:10]
    })

# Debug and Testing Routes
@app.route('/debug/matching/<variety_name>')
def debug_matching(variety_name):
    """Debug matching for a specific variety name"""
    master_varieties = Variety.query.all()
    variety_names = [v.variety_release_name for v in master_varieties if v.variety_release_name]
    
    if variety_names:
        matching_engine.fit(variety_names)
        matched_name, score = matching_engine.advanced_match(variety_name, 0.7)
        
        return jsonify({
            'input_name': variety_name,
            'cleaned_input': DataCleaningEngine.clean_text(variety_name),
            'matched_name': matched_name,
            'confidence_score': score,
            'is_match': score >= 0.7,
            'total_master_varieties': len(variety_names)
        })
    
    return jsonify({'error': 'No master varieties found'})

@app.route('/debug/matching_stats')
def debug_matching_stats():
    """Debug route to see matching statistics"""
    match_stats = session.get('match_stats', {})
    return jsonify(match_stats)

@app.route('/test_matching', methods=['POST'])
def test_matching():
    """Test matching for specific names"""
    data = request.get_json()
    test_names = data.get('names', [])
    
    master_varieties = Variety.query.all()
    variety_names = [v.variety_release_name for v in master_varieties if v.variety_release_name]
    
    matching_engine.fit(variety_names)
    
    results = []
    for name in test_names:
        matched_name, score = matching_engine.advanced_match(name, 0.7)
        results.append({
            'input': name,
            'cleaned_input': DataCleaningEngine.clean_text(name),
            'matched_name': matched_name,
            'confidence_score': score,
            'is_match': score >= 0.7
        })
    
    return jsonify({
        'total_master_varieties': len(variety_names),
        'results': results
    })

@app.route('/compare_files', methods=['GET', 'POST'])
def compare_files():
    """Compare two files using crop+variety concatenated key matching"""
    if request.method == 'POST':
        if 'file1' not in request.files or 'file2' not in request.files:
            flash('Please select both files', 'error')
            return redirect(request.url)
        
        file1 = request.files['file1']
        file2 = request.files['file2']
        
        if file1.filename == '' or file2.filename == '':
            flash('Please select both files', 'error')
            return redirect(request.url)
        
        if file1 and file2 and allowed_file(file1.filename) and allowed_file(file2.filename):
            try:
                # Read both files
                df1 = pd.read_excel(file1)
                df2 = pd.read_excel(file2)
                
                # Clean both dataframes
                df1_clean = DataCleaner.clean_dataframe(df1)
                df2_clean = DataCleaner.clean_dataframe(df2)
                
                # Map headers for both files using your existing function
                mapping1 = map_your_headers(df1_clean)
                mapping2 = map_your_headers(df2_clean)
                
                # FIX: Store as records instead of JSON string
                session['compare_files'] = {
                    'file1': {
                        'filename': file1.filename,
                        'data': df1_clean.to_dict('records'),  # Store as records
                        'mapping': mapping1,
                        'columns': df1_clean.columns.tolist(),
                    },
                    'file2': {
                        'filename': file2.filename,
                        'data': df2_clean.to_dict('records'),  # Store as records
                        'mapping': mapping2,
                        'columns': df2_clean.columns.tolist(),
                    }
                }
                
                flash(f'Files uploaded successfully! File 1: {len(df1_clean)} records, File 2: {len(df2_clean)} records', 'success')
                return redirect(url_for('process_comparison'))
                
            except Exception as e:
                logger.error(f"Error processing comparison files: {str(e)}")
                flash(f'Error processing files: {str(e)}', 'error')
                return redirect(request.url)
    
    return render_template('compare_files.html', 
                         variety_count=Variety.query.count())

import signal
import time

@app.route('/compare_files/process')
def process_comparison():
    """Process comparison with timeout handling"""
    if 'compare_files' not in session:
        flash('No files uploaded for comparison', 'error')
        return redirect(url_for('compare_files'))
    
    try:
        # Set a timeout for large files (30 seconds)
        start_time = time.time()
        
        compare_data = session['compare_files']
        
        # Reconstruct dataframes
        df1 = pd.DataFrame(compare_data['file1']['data'])
        df2 = pd.DataFrame(compare_data['file2']['data'])
        
        # Check if files are too large
        if len(df1) > 5000 or len(df2) > 5000:
            flash('Large files detected. Processing may take longer...', 'info')
        
        # Process comparison
        comparison_results = perform_crop_variety_comparison(df1, df2, compare_data)
        
        processing_time = time.time() - start_time
        logger.info(f"Comparison processing completed in {processing_time:.2f} seconds")
        
        # Store results in session for export
        session['comparison_results'] = comparison_results
        
        return render_template('compare_results.html',
                             results=comparison_results,
                             file1_name=compare_data['file1']['filename'],
                             file2_name=compare_data['file2']['filename'])
        
    except Exception as e:
        logger.error(f"Error in comparison processing: {str(e)}")
        flash(f'Error processing comparison: {str(e)}. Files may be too large.', 'error')
        return redirect(url_for('compare_files'))

def perform_crop_variety_comparison(df1, df2, compare_data):
    """
    Compare two files using CONCATENATED crop+variety keys
    100% EXACT MATCHING ONLY - OPTIMIZED VERSION
    """
    # Extract crop and variety columns using your mapping function
    mapping1 = compare_data['file1']['mapping']
    mapping2 = compare_data['file2']['mapping']
    
    # Find crop and variety columns for both files
    crop_col_1 = next((col for col, mapped in mapping1.items() if mapped == 'crop'), None)
    variety_col_1 = next((col for col, mapped in mapping1.items() if mapped == 'variety_release_name'), None)
    
    crop_col_2 = next((col for col, mapped in mapping2.items() if mapped == 'crop'), None)
    variety_col_2 = next((col for col, mapped in mapping2.items() if mapped == 'variety_release_name'), None)
    
    logger.info(f"File1 - Crop: {crop_col_1}, Variety: {variety_col_1}")
    logger.info(f"File2 - Crop: {crop_col_2}, Variety: {variety_col_2}")
    
    # OPTIMIZATION: Pre-compute all keys for file2
    file2_keys_set = set()
    file2_lookup = {}
    
    for idx, row2 in df2.iterrows():
        crop_value = row2[crop_col_2] if crop_col_2 and crop_col_2 in row2.index else None
        variety_value = row2[variety_col_2] if variety_col_2 and variety_col_2 in row2.index else None
        
        if variety_value and pd.notna(variety_value) and str(variety_value).strip():
            # Create concatenated key
            key = create_crop_variety_key(crop_value, variety_value)
            file2_keys_set.add(key)
            if key not in file2_lookup:
                file2_lookup[key] = []
            file2_lookup[key].append((idx, row2, crop_value, variety_value))
    
    logger.info(f"Built lookup with {len(file2_lookup)} unique crop+variety keys from File2")
    
    # OPTIMIZATION: Use vectorized operations where possible
    matches = []
    unique_to_file1 = []
    
    # Convert file2 records to list for removal tracking
    file2_records_list = [rec for rec_list in file2_lookup.values() for rec in rec_list]
    file2_matched_indices = set()
    
    for idx, row1 in df1.iterrows():
        crop_value_1 = row1[crop_col_1] if crop_col_1 and crop_col_1 in row1.index else None
        variety_value_1 = row1[variety_col_1] if variety_col_1 and variety_col_1 in row1.index else None
        
        if not variety_value_1 or pd.isna(variety_value_1) or not str(variety_value_1).strip():
            unique_to_file1.append(row1.to_dict())
            continue
        
        # Create concatenated key for file1 record
        key1 = create_crop_variety_key(crop_value_1, variety_value_1)
        
        # 100% EXACT MATCH - only match if keys are identical
        if key1 in file2_lookup:
            matching_file2_records = file2_lookup[key1]
            
            for file2_idx, row2, crop2, variety2 in matching_file2_records:
                match_record = {
                    'file1_data': row1.to_dict(),
                    'file2_data': row2.to_dict(),
                    'confidence': 1.0,
                    'match_type': 'exact_crop_variety',
                    'matched_key': key1,
                    'file1_crop': crop_value_1,
                    'file1_variety': variety_value_1,
                    'file2_crop': crop2,
                    'file2_variety': variety2
                }
                matches.append(match_record)
                file2_matched_indices.add(file2_idx)
        else:
            unique_to_file1.append(row1.to_dict())
    
    # Get unique file2 records (those not matched)
    unique_to_file2 = [rec[1].to_dict() for i, rec in enumerate(file2_records_list) if i not in file2_matched_indices]
    
    # Prepare results
    comparison_stats = {
        'total_file1': len(df1),
        'total_file2': len(df2),
        'matches_found': len(matches),
        'unique_to_file1': len(unique_to_file1),
        'unique_to_file2': len(unique_to_file2),
        'match_rate_file1': (len(matches) / len(df1)) * 100 if len(df1) > 0 else 0,
        'match_rate_file2': (len(matches) / len(df2)) * 100 if len(df2) > 0 else 0,
        'matching_method': '100%_exact_crop_variety',
        'exact_matches': len(matches),
        'fuzzy_matches': 0,
        'name_only_matches': 0,
        'crop_col_1': crop_col_1,
        'variety_col_1': variety_col_1,
        'crop_col_2': crop_col_2,
        'variety_col_2': variety_col_2
    }
    
    logger.info(f"100% EXACT matching completed: {len(matches)} exact matches found")
    
    return {
    'matches': matches[:50],  # Show fewer records for faster loading
    'unique_to_file1': unique_to_file1[:50],
    'unique_to_file2': unique_to_file2[:50],
    'stats': comparison_stats,
    'all_matches': matches,
    'all_unique_file1': unique_to_file1,
    'all_unique_file2': unique_to_file2,
    'total_matches': len(matches),  # ADD THIS
    'total_unique_file1': len(unique_to_file1),  # ADD THIS
    'total_unique_file2': len(unique_to_file2)   # ADD THIS
}

@app.route('/favicon.ico')
def favicon():
    return send_from_directory(os.path.join(app.root_path, 'static'),
                             'favicon.ico', mimetype='image/vnd.microsoft.icon')

@app.route('/compare_files/export_matches')
def export_comparison_matches():
    """Export matched records from comparison"""
    if 'comparison_results' not in session:
        flash('No comparison results to export', 'error')
        return redirect(url_for('compare_files'))
    
    try:
        results = session['comparison_results']
        compare_data = session['compare_files']
        
        data = results['all_matches']
        filename = f"crop_variety_matches_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        
        # Format matches for export
        export_data = []
        for match in data:
            record = {}
            # Add file1 data
            for k, v in match['file1_data'].items():
                record[f"File1_{k}"] = v
            # Add file2 data  
            for k, v in match['file2_data'].items():
                record[f"File2_{k}"] = v
            # Add match info
            record['Match_Confidence'] = match['confidence']
            record['Match_Type'] = match['match_type']
            record['Matched_Key'] = match['matched_key']
            export_data.append(record)
            
        df = pd.DataFrame(export_data)
        
        # Create Excel file in memory
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Matched Records')
        
        output.seek(0)
        
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=filename
        )
        
    except Exception as e:
        logger.error(f"Error exporting comparison matches: {str(e)}")
        flash(f'Error exporting matches: {str(e)}', 'error')
        return redirect(url_for('compare_files'))

@app.route('/compare_files/export_unique_file1')
def export_comparison_unique1():
    """Export unique records from file 1"""
    if 'comparison_results' not in session:
        flash('No comparison results to export', 'error')
        return redirect(url_for('compare_files'))
    
    try:
        results = session['comparison_results']
        compare_data = session['compare_files']
        
        data = results['all_unique_file1']
        filename = f"unique_to_{compare_data['file1']['filename']}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        df = pd.DataFrame(data)
        
        # Create Excel file in memory
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Unique Records')
        
        output.seek(0)
        
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=filename
        )
        
    except Exception as e:
        logger.error(f"Error exporting unique file1: {str(e)}")
        flash(f'Error exporting unique records: {str(e)}', 'error')
        return redirect(url_for('compare_files'))

@app.route('/compare_files/export_unique_file2')
def export_comparison_unique2():
    """Export unique records from file 2"""
    if 'comparison_results' not in session:
        flash('No comparison results to export', 'error')
        return redirect(url_for('compare_files'))
    
    try:
        results = session['comparison_results']
        compare_data = session['compare_files']
        
        data = results['all_unique_file2']
        filename = f"unique_to_{compare_data['file2']['filename']}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        df = pd.DataFrame(data)
        
        # Create Excel file in memory
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Unique Records')
        
        output.seek(0)
        
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=filename
        )
        
    except Exception as e:
        logger.error(f"Error exporting unique file2: {str(e)}")
        flash(f'Error exporting unique records: {str(e)}', 'error')
        return redirect(url_for('compare_files'))
    
@app.route('/extract_attributes', methods=['GET', 'POST'])
def extract_attributes():
    """Dedicated route for extracting attributes from special_attributes column"""
    
    if request.method == 'POST':
        # Check if a file was uploaded
        if 'file' not in request.files:
            flash('No file selected', 'error')
            return redirect(request.url)
        
        file = request.files['file']
        if file.filename == '':
            flash('No file selected', 'error')
            return redirect(request.url)
        
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            filepath = os.path.join(app.config['UPLOAD_FOLDER'], 'attributes', filename)
            
            # Create attributes directory if it doesn't exist
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            
            try:
                file.save(filepath)
                
                # Read the uploaded file
                df = pd.read_excel(filepath)
                logger.info(f"Loaded dataframe with {len(df)} rows and columns: {list(df.columns)}")
                
                # Basic validation
                if df.empty:
                    flash('The uploaded file is empty', 'error')
                    return redirect(request.url)
                
                # Use map_your_headers to automatically detect columns
                column_mapping = map_your_headers(df)
                logger.info(f"Column mapping: {column_mapping}")
                
                # Find the best column for special attributes
                special_attributes_column = request.form.get('special_attributes_column', '').strip()
                confidence_threshold = float(request.form.get('confidence_threshold', 0.3))
                
                # If no column specified, try to auto-detect
                if not special_attributes_column:
                    # Look for mapped special_attributes column
                    for uploaded_col, db_col in column_mapping.items():
                        if db_col == 'special_attributes':
                            special_attributes_column = uploaded_col
                            logger.info(f"Auto-detected special attributes column: {special_attributes_column}")
                            break
                    
                    # If still not found, look for columns with relevant keywords
                    if not special_attributes_column:
                        attribute_keywords = ['special', 'attribute', 'characteristic', 'trait', 'description', 'note']
                        for col in df.columns:
                            col_str = str(col)
                            col_lower = col_str.lower()
                            if any(keyword in col_lower for keyword in attribute_keywords):
                                special_attributes_column = col_str
                                logger.info(f"Keyword-detected special attributes column: {special_attributes_column}")
                                break
                
                # Validate that the specified column exists
                if special_attributes_column and special_attributes_column not in df.columns:
                    available_columns = [str(col) for col in df.columns]
                    # Try to find a close match
                    best_match, score, _ = process.extractOne(str(special_attributes_column), available_columns, scorer=fuzz.partial_ratio)
                    if score > 70:
                        special_attributes_column = best_match
                        flash(f"Column '{special_attributes_column}' not found. Using closest match: '{best_match}'", 'warning')
                    else:
                        flash(f"Column '{special_attributes_column}' not found. Available columns: {', '.join(available_columns)}", 'error')
                        return redirect(request.url)
                
                # If still no column found, show mapping interface
                if not special_attributes_column:
                    uploaded_columns = [str(col) for col in df.columns]
                    return render_template('map_attributes_columns.html',
                                         filename=filename,
                                         uploaded_columns=uploaded_columns,
                                         column_mapping=column_mapping,
                                         auto_suggestions=auto_detect_columns(uploaded_columns))
                
                logger.info(f"Using special attributes column: {special_attributes_column}")
                
                # Extract attributes
                result_df = EnhancedDataCleaner.extract_attributes_from_dataframe(
                    df, 
                    special_attributes_column
                )
                
                # Generate extraction report
                extraction_report = EnhancedDataCleaner.get_attribute_extraction_report(
                    result_df, 
                    special_attributes_column
                )
                
                # Convert to JSON-serializable format
                serializable_report = convert_to_serializable(extraction_report)
                
                # Save the processed file
                output_filename = f"attributes_extracted_{filename}"
                output_path = os.path.join(app.config['UPLOAD_FOLDER'], 'attributes', output_filename)
                result_df.to_excel(output_path, index=False)
                
                # Store in session
                session['extracted_attributes_file'] = output_path
                session['extraction_report'] = serializable_report
                session['original_row_count'] = int(len(df))
                session['processed_row_count'] = int(len(result_df))
                session['special_attributes_column'] = str(special_attributes_column)
                session['confidence_threshold'] = float(confidence_threshold)
                
                # Prepare preview data
                preview_data = []
                for record in result_df.head(10).to_dict('records'):
                    preview_data.append(convert_to_serializable(record))
                
                columns = [str(col) for col in result_df.columns.tolist()]
                
                flash('Attribute extraction completed successfully!', 'success')
                return render_template('attributes_results.html',
                                     preview_data=preview_data,
                                     columns=columns,
                                     total_rows=int(len(result_df)),
                                     extraction_report=serializable_report,
                                     output_filename=output_filename,
                                     confidence_threshold=confidence_threshold,
                                     special_attributes_column=special_attributes_column)
                
            except Exception as e:
                logger.error(f"Error processing attributes file: {str(e)}", exc_info=True)
                flash(f'Error processing file: {str(e)}', 'error')
                if os.path.exists(filepath):
                    os.remove(filepath)
                return redirect(request.url)
    
    return render_template('extract_attributes.html',variety_count=Variety.query.count())

def convert_to_serializable(obj):
    """
    Convert pandas/numpy types to native Python types for JSON serialization
    """
    if isinstance(obj, (pd.Timestamp, pd.Timedelta)):
        return str(obj)
    elif isinstance(obj, pd.Series):
        return obj.tolist()
    elif isinstance(obj, pd.DataFrame):
        return obj.to_dict('records')
    elif hasattr(obj, 'dtype'):  # numpy types
        if pd.api.types.is_integer_dtype(obj):
            return int(obj)
        elif pd.api.types.is_float_dtype(obj):
            return float(obj)
        elif pd.api.types.is_bool_dtype(obj):
            return bool(obj)
        else:
            return str(obj)
    elif isinstance(obj, (np.integer, np.int64, np.int32, np.int16, np.int8)):
        return int(obj)
    elif isinstance(obj, (np.floating, np.float64, np.float32, np.float16)):
        return float(obj)
    elif isinstance(obj, np.bool_):
        return bool(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, dict):
        return {key: convert_to_serializable(value) for key, value in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return [convert_to_serializable(item) for item in obj]
    elif pd.isna(obj) or obj is None:
        return None
    else:
        return obj
    
@app.route('/combine_files', methods=['GET', 'POST'])
def combine_files():
    """Combine multiple Excel files into a single sheet, skipping headers after the first file and only including rows with crop and variety names"""
    if request.method == 'POST':
        if 'files' not in request.files:
            flash('No files selected', 'error')
            return redirect(request.url)
        
        files = request.files.getlist('files')
        
        # Filter out empty file selections
        valid_files = [f for f in files if f.filename != '']
        
        if not valid_files:
            flash('No valid files selected', 'error')
            return redirect(request.url)
        
        if len(valid_files) > 25:
            flash('Maximum 25 files allowed. Please select fewer files.', 'error')
            return redirect(request.url)
        
        # Get form parameters
        enable_data_cleaning = request.form.get('enable_data_cleaning') == 'on'
        group_columns_input = request.form.get('group_columns', 'crop,variety_release_name')
        group_columns = [col.strip() for col in group_columns_input.split(',')]
        
        try:
            combined_data = []
            file_stats = []
            skipped_rows_details = []
            all_columns = set()
            master_mapping = None
            total_valid_records = 0
            total_skipped_records = 0
            
            for i, file in enumerate(valid_files):
                if file and allowed_file(file.filename):
                    # Read Excel file
                    df = pd.read_excel(file)
                    
                    # Use map_your_headers for consistent column mapping
                    file_mapping = map_your_headers(df)
                    
                    # Use the first file's mapping as master for all files
                    if master_mapping is None:
                        master_mapping = file_mapping
                    
                    # Find crop and variety columns in this file
                    crop_column = None
                    variety_column = None
                    
                    # Look for mapped crop and variety columns
                    for file_col, db_col in file_mapping.items():
                        if db_col == 'crop':
                            crop_column = file_col
                        elif db_col == 'variety_release_name':
                            variety_column = file_col
                    
                    # If not found by mapping, try to find by common names
                    if not crop_column:
                        for col in df.columns:
                            col_lower = str(col).lower()
                            if any(keyword in col_lower for keyword in ['crop', 'crop_type', 'species']):
                                crop_column = col
                                break
                    
                    if not variety_column:
                        for col in df.columns:
                            col_lower = str(col).lower()
                            if any(keyword in col_lower for keyword in ['variety', 'release_name', 'variety_name', 'name', 'cultivar']):
                                variety_column = col
                                break
                    
                    # Track file statistics with crop/variety info
                    file_stat = {
                        'filename': file.filename,
                        'original_columns': list(df.columns),
                        'mapped_columns': file_mapping,
                        'record_count': len(df),
                        'columns_count': len(df.columns),
                        'crop_column': crop_column,
                        'variety_column': variety_column,
                        'valid_records': 0,
                        'skipped_records': 0,
                        'skipped_details': []
                    }
                    
                    # Create a reverse mapping from database columns to file columns
                    reverse_mapping = {}
                    for file_col, db_col in file_mapping.items():
                        reverse_mapping[db_col] = file_col
                    
                    # Process each row and only include those with both crop and variety
                    standardized_data = []
                    file_valid_records = 0
                    file_skipped_records = 0
                    
                    for row_idx, row in df.iterrows():
                        # Check if both crop and variety are present and not empty
                        has_crop = False
                        has_variety = False
                        crop_value = None
                        variety_value = None
                        
                        # Check crop
                        if crop_column and crop_column in row:
                            crop_value = row[crop_column]
                            if crop_value is not None and pd.notna(crop_value) and str(crop_value).strip() != '':
                                has_crop = True
                        
                        # Check variety
                        if variety_column and variety_column in row:
                            variety_value = row[variety_column]
                            if variety_value is not None and pd.notna(variety_value) and str(variety_value).strip() != '':
                                has_variety = True
                        
                        # Only include row if both crop and variety are present
                        if has_crop and has_variety:
                            record = {}
                            # Map each database column back to the file's column name if it exists
                            for db_col in master_mapping.values():
                                if db_col in reverse_mapping:
                                    file_col = reverse_mapping[db_col]
                                    if file_col in row:
                                        record[db_col] = row[file_col]
                                    else:
                                        record[db_col] = None
                                else:
                                    record[db_col] = None
                            
                            # Add source file information
                            record['source_file'] = file.filename
                            record['source_index'] = i + 1
                            
                            standardized_data.append(record)
                            file_valid_records += 1
                            total_valid_records += 1
                        else:
                            file_skipped_records += 1
                            total_skipped_records += 1
                            
                            skip_reason = determine_skip_reason(has_crop, has_variety, crop_value, variety_value)
                            
                            skipped_row_detail = {
                                'file_name': file.filename,
                                'file_index': i + 1,
                                'row_number': row_idx + 2,
                                'crop_value': crop_value,
                                'variety_value': variety_value,
                                'skip_reason': skip_reason,
                                'original_data': row.to_dict()
                            }
                            
                            skipped_rows_details.append(skipped_row_detail)
                            file_stat['skipped_details'].append(skipped_row_detail)
                    
                    file_stat['valid_records'] = file_valid_records
                    file_stat['skipped_records'] = file_skipped_records
                    file_stats.append(file_stat)
                    
                    if standardized_data:
                        file_df = pd.DataFrame(standardized_data)
                        combined_data.append(file_df)
                        all_columns.update(file_df.columns.tolist())
                    else:
                        logger.warning(f"No valid records found in file: {file.filename}")
            
            if not combined_data:
                flash('No valid records found in any uploaded files. Please ensure files have both crop name and variety release name filled.', 'error')
                return redirect(request.url)
            
            # Combine all dataframes
            final_df = pd.concat(combined_data, ignore_index=True)
            
            # NEW: Apply text cleaning to the entire dataset to ensure proper sorting
            logger.info("Applying comprehensive text cleaning to combined data")
            text_columns_to_clean = ['crop', 'variety_release_name', 'botanical_name', 'country', 'seed_type', 
                                   'releasing_entity', 'maintainer', 'agroecological_zones', 'special_attributes']
            final_df = clean_text_data(final_df, text_columns_to_clean)
            
            # Reorder columns to put source info at the end
            column_order = [col for col in final_df.columns if col not in ['source_file', 'source_index']]
            column_order.extend(['source_file', 'source_index'])
            final_df = final_df[column_order]
            
            # NEW: Apply data cleaning if enabled
            duplicate_stats = None
            if enable_data_cleaning:
                logger.info("Applying data cleaning with duplicate removal")
                logger.info(f"Dataframe shape before cleaning: {final_df.shape}")
                logger.info(f"Dataframe columns: {list(final_df.columns)}")
                
                original_count = len(final_df)
                
                try:
                    # Define sorting columns - crop first, then variety
                    sort_columns = ['crop', 'variety_release_name']
                    final_df_cleaned = clean_combined_data(final_df, group_columns, sort_columns)
                    
                    duplicate_stats = get_duplicate_stats(final_df, final_df_cleaned, group_columns)
                    final_df = final_df_cleaned
                    logger.info(f"Data cleaning completed: {duplicate_stats['duplicates_removed']} duplicates removed")
                    
                except Exception as e:
                    logger.error(f"Error during data cleaning: {str(e)}", exc_info=True)
                    # Fallback: continue without cleaning
                    flash(f'Data cleaning failed: {str(e)}. Continuing without cleaning.', 'warning')
                    duplicate_stats = {
                        'original_count': len(final_df),
                        'cleaned_count': len(final_df),
                        'duplicates_removed': 0,
                        'duplicate_groups': 0,
                        'completeness_improvement': 0
                    }
            
            # Store in session for download
            output_dir = os.path.join(app.config['UPLOAD_FOLDER'], 'combined')
            os.makedirs(output_dir, exist_ok=True)
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_filename = f"combined_files_{timestamp}.xlsx"
            output_path = os.path.join(output_dir, output_filename)

            # Pre-process data to prevent Excel auto-conversion
            final_df_processed = preprocess_ranges_for_excel(final_df)

            # Save combined file with proper formatting
            try:
                with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                    final_df_processed.to_excel(writer, index=False, sheet_name='Combined Data')
                    
                    workbook = writer.book
                    worksheet = writer.sheets['Combined Data']
                    
                    from openpyxl.styles import NamedStyle
                    text_style = NamedStyle(name="text_style")
                    text_style.number_format = '@'
                    
                    if 'text_style' not in workbook.named_styles:
                        workbook.add_named_style(text_style)
                    
                    text_columns = []
                    for col_idx, col_name in enumerate(final_df_processed.columns, 1):
                        col_lower = str(col_name).lower()
                        if any(keyword in col_lower for keyword in 
                              ['maturity', 'altitude', 'yield', 'year', 'range', 'days', 'release']):
                            text_columns.append(col_idx)
                    
                    for col_idx in text_columns:
                        col_letter = openpyxl.utils.get_column_letter(col_idx)
                        for row in range(2, len(final_df_processed) + 2):
                            cell = worksheet[f'{col_letter}{row}']
                            cell.number_format = '@'
                            
            except Exception as e:
                logger.error(f"Error saving Excel file with formatting: {str(e)}")
                final_df_processed.to_excel(output_path, index=False)

            # Store in session
            session['combined_file_path'] = output_path
            session['combined_download_name'] = output_filename
            session['skipped_rows_details'] = skipped_rows_details
            session['combine_stats'] = {
                'total_files': len(valid_files),
                'total_records': len(final_df),
                'total_valid_records': total_valid_records,
                'total_skipped_records': total_skipped_records,
                'file_stats': file_stats,
                'master_mapping': master_mapping,
                'final_columns': list(final_df.columns),
                'excel_protection_applied': True,
                'skipped_rows_count': len(skipped_rows_details),
                'data_cleaning_enabled': enable_data_cleaning,
                'duplicate_stats': duplicate_stats,
                'group_columns_used': group_columns,
                'text_cleaning_applied': True  # NEW: Flag to indicate text cleaning was applied
            }
            
            # Prepare preview data
            preview_data = final_df.head(20).to_dict('records')
            skipped_preview = skipped_rows_details[:20]
            
            success_message = f'Successfully combined {len(valid_files)} files into {len(final_df)} valid records! {total_skipped_records} records were skipped.'
            if enable_data_cleaning and duplicate_stats and duplicate_stats['duplicates_removed'] > 0:
                success_message += f" {duplicate_stats['duplicates_removed']} duplicates were removed."
            if enable_data_cleaning:
                success_message += " Text data cleaned and sorted alphabetically."
            
            flash(success_message, 'success')
            return render_template('combine_results.html',
                                 preview_data=preview_data,
                                 skipped_preview=skipped_preview,
                                 total_records=len(final_df),
                                 total_files=len(valid_files),
                                 total_skipped_records=total_skipped_records,
                                 columns=list(final_df.columns),
                                 file_stats=file_stats,
                                 skipped_rows_details=skipped_rows_details,
                                 enable_data_cleaning=enable_data_cleaning,
                                 duplicate_stats=duplicate_stats,
                                 group_columns_used=group_columns)
            
        except Exception as e:
            logger.error(f"Error combining files: {str(e)}", exc_info=True)
            flash(f'Error combining files: {str(e)}', 'error')
            return redirect(request.url)
    
    return render_template('combine_files.html', variety_count=Variety.query.count())
    
@app.route('/download_skipped_rows_excel')
def download_skipped_rows_excel():
    """Download the skipped rows as Excel file"""
    skipped_rows_details = session.get('skipped_rows_details', [])
    
    if not skipped_rows_details:
        flash('No skipped rows data found.', 'error')
        return redirect(url_for('combine_files'))
    
    try:
        # Convert skipped rows to DataFrame
        skipped_data = []
        for detail in skipped_rows_details:
            row_data = {
                'file_name': detail['file_name'],
                'row_number': detail['row_number'],
                'skip_reason': detail['skip_reason'],
                'crop_value': detail['crop_value'],
                'variety_value': detail['variety_value']
            }
            # Add original data columns
            for key, value in detail['original_data'].items():
                row_data[f'original_{key}'] = value
            skipped_data.append(row_data)
        
        df = pd.DataFrame(skipped_data)
        
        # Create Excel file in memory
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Skipped Rows')
        
        output.seek(0)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"skipped_rows_{timestamp}.xlsx"
        
        return send_file(
            output,
            as_attachment=True,
            download_name=filename,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
        
    except Exception as e:
        logger.error(f"Error exporting skipped rows: {str(e)}")
        flash(f'Error exporting skipped rows: {str(e)}', 'error')
        return redirect(url_for('combine_files'))

@app.route('/download_skipped_rows_csv')
def download_skipped_rows_csv():
    """Download the skipped rows as CSV file"""
    skipped_rows_details = session.get('skipped_rows_details', [])
    
    if not skipped_rows_details:
        flash('No skipped rows data found.', 'error')
        return redirect(url_for('combine_files'))
    
    try:
        # Convert skipped rows to DataFrame
        skipped_data = []
        for detail in skipped_rows_details:
            row_data = {
                'file_name': detail['file_name'],
                'row_number': detail['row_number'],
                'skip_reason': detail['skip_reason'],
                'crop_value': detail['crop_value'],
                'variety_value': detail['variety_value']
            }
            # Add original data columns
            for key, value in detail['original_data'].items():
                row_data[f'original_{key}'] = value
            skipped_data.append(row_data)
        
        df = pd.DataFrame(skipped_data)
        
        # Create CSV in memory
        output = io.StringIO()
        df.to_csv(output, index=False, encoding='utf-8')
        output.seek(0)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"skipped_rows_{timestamp}.csv"
        
        return send_file(
            io.BytesIO(output.getvalue().encode('utf-8')),
            as_attachment=True,
            download_name=filename,
            mimetype='text/csv'
        )
        
    except Exception as e:
        logger.error(f"Error exporting skipped rows: {str(e)}")
        flash(f'Error exporting skipped rows: {str(e)}', 'error')
        return redirect(url_for('combine_files'))

@app.route('/download_combined_files')
def download_combined_files():
    """Download the combined files"""
    file_path = session.get('combined_file_path')
    download_name = session.get('combined_download_name', 'combined_files.xlsx')
    
    if not file_path or not os.path.exists(file_path):
        flash('No combined file found. Please combine files first.', 'error')
        return redirect(url_for('combine_files'))
    
    try:
        return send_file(
            file_path,
            as_attachment=True,
            download_name=download_name,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
    except Exception as e:
        flash(f'Error downloading file: {str(e)}', 'error')
        return redirect(url_for('combine_files'))

def preprocess_ranges_for_excel(df):
    """
    Pre-process data to prevent Excel from auto-converting ranges to dates
    """
    df_processed = df.copy()
    
    # Columns that might contain ranges or numbers that Excel might convert
    range_columns = [
        'maturity_days', 'production_altitude', 'min_altitude', 'max_altitude',
        'yield_mt_ha', 'year_of_release', 'maturity', 'altitude', 'yield'
    ]
    
    for col in df_processed.columns:
        # Check if column name contains any of our target keywords
        col_lower = str(col).lower()
        is_target_column = any(keyword in col_lower for keyword in 
                              ['maturity', 'altitude', 'yield', 'year', 'range', 'days'])
        
        if is_target_column or col in range_columns:
            # Convert to string and protect problematic patterns
            df_processed[col] = df_processed[col].apply(lambda x: format_cell_for_excel(x))
    
    return df_processed

def clean_text_data(df, text_columns=None):
    """
    Clean text data by removing extra spaces, standardizing case, and handling whitespace
    """
    if df.empty:
        return df
    
    df_clean = df.copy()
    
    # If no specific columns provided, clean all object/string columns
    if text_columns is None:
        text_columns = df_clean.select_dtypes(include=['object']).columns.tolist()
    
    cleaned_count = 0
    for col in text_columns:
        if col in df_clean.columns:
            try:
                # Store original sample for logging
                original_sample = df_clean[col].head(3).tolist()
                
                # Convert to string, handle NaN values
                df_clean[col] = df_clean[col].astype(str)
                
                # Apply comprehensive cleaning
                df_clean[col] = df_clean[col].apply(lambda x: clean_single_text_value(x))
                
                # Log changes
                cleaned_sample = df_clean[col].head(3).tolist()
                if original_sample != cleaned_sample:
                    logger.info(f"Cleaned column '{col}': {original_sample} -> {cleaned_sample}")
                
                cleaned_count += 1
                
            except Exception as e:
                logger.warning(f"Error cleaning column {col}: {str(e)}")
    
    logger.info(f"Successfully cleaned {cleaned_count} text columns")
    return df_clean

def clean_single_text_value(value):
    """
    Enhanced text cleaning with comprehensive space removal and standardization
    """
    if value is None or pd.isna(value) or value in ['None', 'nan', 'NaN', '']:
        return ''
    
    # Convert to string if not already
    value_str = str(value)
    
    # Remove all types of whitespace (spaces, tabs, newlines) from start and end
    value_clean = value_str.strip()
    
    # Replace multiple spaces/tabs/newlines with single space
    value_clean = re.sub(r'\s+', ' ', value_clean)
    
    # Remove spaces that might be at the beginning after cleaning
    value_clean = value_clean.strip()
    
    return value_clean

def clean_combined_data(df, group_columns=None, sort_columns=None):
    """
    Most robust version - separates duplicate removal and sorting completely
    """
    if df.empty:
        return df
    
    if group_columns is None:
        group_columns = ['crop', 'variety_release_name']
    
    if sort_columns is None:
        sort_columns = ['crop', 'variety_release_name']
    
    working_df = df.copy()
    
    # STEP 1: Handle duplicates
    available_group_columns = [col for col in group_columns if col in working_df.columns]
    if available_group_columns:
        try:
            # Add completeness score
            working_df['_completeness'] = working_df.apply(
                lambda row: sum(1 for value in row if value is not None and pd.notna(value) and str(value).strip() not in ['', 'nan']) / len(row) * 100, 
                axis=1
            )
            
            # Sort by completeness (descending) and then drop duplicates
            working_df = working_df.sort_values(by=['_completeness'], ascending=False)
            working_df = working_df.drop_duplicates(subset=available_group_columns, keep='first')
            
            # Remove completeness column
            working_df = working_df.drop('_completeness', axis=1)
            
            logger.info(f"Successfully removed duplicates using: {available_group_columns}")
            
        except Exception as e:
            logger.error(f"Error in duplicate removal: {str(e)}")
            # Continue without duplicate removal if it fails
    
    # STEP 2: Apply sorting (completely separate from duplicate removal)
    available_sort_columns = [col for col in sort_columns if col in working_df.columns]
    if available_sort_columns:
        try:
            # Handle NaN values for each sort column
            for col in available_sort_columns:
                if col in working_df.columns:
                    working_df[col] = working_df[col].fillna('')
            
            # Sort alphabetically
            working_df = working_df.sort_values(by=available_sort_columns, ascending=True)
            logger.info(f"Successfully sorted by: {available_sort_columns}")
            
        except Exception as e:
            logger.error(f"Error in sorting: {str(e)}")
            # Continue without sorting if it fails
    
    return working_df.reset_index(drop=True)
    
    # Calculate completeness
    def calculate_completeness(row):
        total_cells = len(row)
        filled_cells = sum(1 for value in row if value is not None and pd.notna(value) and str(value).strip() not in ['', 'nan'])
        return (filled_cells / total_cells) * 100 if total_cells > 0 else 0
    
    working_df['_completeness'] = working_df.apply(calculate_completeness, axis=1)
    
    # Handle duplicates
    available_group_columns = [col for col in group_columns if col in working_df.columns]
    if available_group_columns:
        # Sort by completeness first (descending) to get best records first
        working_df = working_df.sort_values(by=['_completeness'] + available_group_columns, ascending=[False, True])
        
        # Keep first occurrence (highest completeness) for each group
        working_df = working_df.drop_duplicates(subset=available_group_columns, keep='first')
    
    # Apply sorting on cleaned data
    available_sort_columns = [col for col in sort_columns if col in working_df.columns]
    if available_sort_columns:
        # Fill NaN values with empty string for stable sorting
        for col in available_sort_columns:
            working_df[col] = working_df[col].fillna('')
        
        working_df = working_df.sort_values(by=available_sort_columns, ascending=True)
        logger.info(f"Data sorted alphabetically by: {available_sort_columns}")
    
    # Clean up
    if '_completeness' in working_df.columns:
        working_df = working_df.drop('_completeness', axis=1)
    
    return working_df.reset_index(drop=True)

def get_duplicate_stats(original_df, cleaned_df, group_columns=None):
    """
    Generate statistics about duplicate removal with error handling
    """
    if group_columns is None:
        group_columns = ['crop', 'variety_release_name']
    
    stats = {
        'original_count': len(original_df),
        'cleaned_count': len(cleaned_df),
        'duplicates_removed': len(original_df) - len(cleaned_df),
        'duplicate_groups': 0,
        'completeness_improvement': 0
    }
    
    try:
        available_group_columns = [col for col in group_columns if col in original_df.columns]
        
        if available_group_columns and not original_df.empty:
            # Count duplicate groups
            group_sizes = original_df.groupby(available_group_columns).size()
            duplicate_groups = group_sizes[group_sizes > 1]
            stats['duplicate_groups'] = len(duplicate_groups)
    except Exception as e:
        logger.error(f"Error calculating duplicate stats: {str(e)}")
    
    return stats

def determine_skip_reason(has_crop, has_variety, crop_value, variety_value):
    """Determine the specific reason why a row was skipped"""
    if not has_crop and not has_variety:
        return "Both crop and variety are missing or empty"
    elif not has_crop:
        if crop_value is None or pd.isna(crop_value):
            return "Crop is missing (null/NaN)"
        elif str(crop_value).strip() == '':
            return "Crop is empty"
        else:
            return "Crop value is invalid"
    elif not has_variety:
        if variety_value is None or pd.isna(variety_value):
            return "Variety is missing (null/NaN)"
        elif str(variety_value).strip() == '':
            return "Variety is empty"
        else:
            return "Variety value is invalid"
    else:
        return "Unknown reason"
    

def format_cell_for_excel(value):
    """
    Format individual cell values to prevent Excel auto-conversion
    """
    if pd.isna(value) or value is None or value == '':
        return value
    
    # Convert to string and clean
    value_str = str(value).strip()
    
    # If empty after stripping, return as is
    if not value_str:
        return value_str
    
    # Remove any existing single quotes that might have been added previously
    if value_str.startswith("'") and len(value_str) > 1:
        value_str = value_str[1:]
    
    # Check for range patterns (like "4-6", "12-24", etc.)
    if '-' in value_str:
        parts = value_str.split('-')
        if len(parts) == 2:
            part1, part2 = parts[0].strip(), parts[1].strip()
            
            # Check if this could be mistaken for a date
            if (part1.isdigit() and part2.isdigit()):
                num1, num2 = int(part1), int(part2)
                
                # Pattern 1: Month-Day (4-6, 12-24, etc.)
                is_likely_date = (
                    (1 <= num1 <= 12 and 1 <= num2 <= 31) or
                    (1 <= num1 <= 31 and 1 <= num2 <= 12)  # Also check reverse
                )
                
                # Pattern 2: Year ranges (2020-2023)
                is_year_range = (
                    len(part1) == 4 and len(part2) == 4 and
                    1900 <= num1 <= 2100 and 1900 <= num2 <= 2100
                )
                
                # Pattern 3: Small number ranges that Excel might convert
                is_small_number_range = (num1 < 32 and num2 < 32)
                
                if is_likely_date or is_year_range or is_small_number_range:
                    return "'" + value_str  # Single quote forces Excel to treat as text
    
    # Check for single numbers that Excel might auto-format
    if value_str.replace('.', '').replace(',', '').isdigit():
        clean_num = value_str.replace(',', '')
        
        # Protect years (4-digit numbers)
        if len(clean_num) == 4 and clean_num.isdigit():
            year_num = int(clean_num)
            if 1900 <= year_num <= 2100:
                return "'" + value_str
        
        # Protect small integers that could be dates (1-31)
        elif clean_num.isdigit() and 1 <= int(clean_num) <= 31:
            return "'" + value_str
        
        # Protect numbers with leading zeros
        elif value_str.startswith('0') and len(value_str) > 1:
            return "'" + value_str
    
    # Check for fraction patterns (like "1/2", "3/4" that Excel might convert to dates)
    if '/' in value_str:
        parts = value_str.split('/')
        if len(parts) == 2:
            part1, part2 = parts[0].strip(), parts[1].strip()
            if (part1.isdigit() and part2.isdigit() and
                1 <= int(part1) <= 12 and 1 <= int(part2) <= 31):
                return "'" + value_str
    
    # Check for time-like patterns (like "12:30" but without colon context)
    if ':' in value_str and len(value_str) <= 5:
        parts = value_str.split(':')
        if len(parts) == 2:
            if parts[0].isdigit() and parts[1].isdigit():
                return "'" + value_str
    
    return value_str

@app.route('/download_combined_files_csv')
def download_combined_files_csv():
    """Download the combined files as CSV to avoid Excel auto-conversion"""
    file_path = session.get('combined_file_path')
    
    if not file_path or not os.path.exists(file_path):
        flash('No combined file found. Please combine files first.', 'error')
        return redirect(url_for('combine_files'))
    
    try:
        # Read the Excel file and save as CSV
        df = pd.read_excel(file_path)
        
        # Create CSV in memory
        output = io.StringIO()
        df.to_csv(output, index=False, encoding='utf-8')
        output.seek(0)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        csv_filename = f"combined_files_{timestamp}.csv"
        
        return send_file(
            io.BytesIO(output.getvalue().encode('utf-8')),
            as_attachment=True,
            download_name=csv_filename,
            mimetype='text/csv'
        )
    except Exception as e:
        logger.error(f"Error downloading CSV file: {str(e)}")
        flash(f'Error downloading CSV file: {str(e)}', 'error')
        return redirect(url_for('combine_files'))
    
@app.route('/map_attributes_columns', methods=['POST'])
def map_attributes_columns():
    """Handle column mapping for attribute extraction"""
    filename = request.form.get('filename')
    filepath = os.path.join(app.config['UPLOAD_FOLDER'], 'attributes', filename)
    
    try:
        df = pd.read_excel(filepath)
        special_attributes_column = request.form.get('special_attributes_column')
        confidence_threshold = float(request.form.get('confidence_threshold', 0.3))
        
        if not special_attributes_column:
            flash('Please select a column for special attributes', 'error')
            return redirect(url_for('extract_attributes'))
        
        # Extract attributes
        result_df = EnhancedDataCleaner.extract_attributes_from_dataframe(
            df, 
            special_attributes_column
        )
        
        # Generate extraction report
        extraction_report = EnhancedDataCleaner.get_attribute_extraction_report(
            result_df, 
            special_attributes_column
        )
        
        # Convert to JSON-serializable format
        serializable_report = convert_to_serializable(extraction_report)
        
        # Save the processed file
        output_filename = f"attributes_extracted_{filename}"
        output_path = os.path.join(app.config['UPLOAD_FOLDER'], 'attributes', output_filename)
        result_df.to_excel(output_path, index=False)
        
        # Store in session for download - ensure all values are JSON serializable
        session['extracted_attributes_file'] = output_path
        session['extraction_report'] = serializable_report
        session['original_row_count'] = int(len(df))
        session['processed_row_count'] = int(len(result_df))
        session['special_attributes_column'] = special_attributes_column
        session['confidence_threshold'] = float(confidence_threshold)
        
        # Show preview of results - convert to native Python types
        preview_data = []
        for record in result_df.head(10).to_dict('records'):
            preview_data.append(convert_to_serializable(record))
        
        columns = result_df.columns.tolist()
        
        flash('Attribute extraction completed successfully!', 'success')
        return render_template('attributes_results.html',
                             preview_data=preview_data,
                             columns=columns,
                             total_rows=int(len(result_df)),
                             extraction_report=serializable_report,
                             output_filename=output_filename,
                             confidence_threshold=confidence_threshold,
                             special_attributes_column=special_attributes_column)
        
    except Exception as e:
        logger.error(f"Error in map_attributes_columns: {str(e)}")
        flash(f'Error processing file: {str(e)}', 'error')
        return redirect(url_for('extract_attributes'))

@app.route('/download_extracted_attributes')
def download_extracted_attributes():
    """Download the extracted attributes file"""
    file_path = session.get('extracted_attributes_file')
    
    if not file_path or not os.path.exists(file_path):
        flash('No extracted attributes file found. Please run extraction first.', 'error')
        return redirect(url_for('extract_attributes'))
    
    try:
        return send_file(
            file_path,
            as_attachment=True,
            download_name=f"extracted_attributes_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
    except Exception as e:
        flash(f'Error downloading file: {str(e)}', 'error')
        return redirect(url_for('extract_attributes'))

@app.route('/api/extract_attributes/batch', methods=['POST'])
def api_batch_extract_attributes():
    """API endpoint for batch attribute extraction"""
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400
    
    if file and allowed_file(file.filename):
        try:
            # Read the file
            df = pd.read_excel(file)
            
            # Get parameters
            special_attributes_column = request.form.get('special_attributes_column', 'special_attributes')
            confidence_threshold = float(request.form.get('confidence_threshold', 0.3))
            
            # Validate column exists
            if special_attributes_column not in df.columns:
                return jsonify({
                    'error': f'Column {special_attributes_column} not found',
                    'available_columns': df.columns.tolist()
                }), 400
            
            # Extract attributes
            result_df = EnhancedDataCleaner.extract_attributes_from_dataframe(
                df, 
                special_attributes_column
            )
            
            # Generate report
            extraction_report = EnhancedDataCleaner.get_attribute_extraction_report(
                result_df, 
                special_attributes_column
            )
            
            # Convert to JSON for response
            result_data = result_df.to_dict('records')
            
            return jsonify({
                'success': True,
                'records_processed': len(result_df),
                'extraction_report': extraction_report,
                'sample_data': result_data[:5],  # First 5 records as sample
                'confidence_threshold_used': confidence_threshold
            })
            
        except Exception as e:
            logger.error(f"Error in batch attribute extraction API: {str(e)}")
            return jsonify({'error': str(e)}), 500
    
    return jsonify({'error': 'Invalid file type'}), 400

# Add template for the attribute extraction form

# Error handlers
@app.errorhandler(404)
def not_found_error(error):
    return render_template('404.html'), 404

@app.errorhandler(500)
def internal_error(error):
    db.session.rollback()
    return render_template('500.html'), 500

# Add a debug route to check database content
@app.route('/debug/varieties')
def debug_varieties():
    """Debug route to check what's in the database"""
    varieties = Variety.query.limit(10).all()
    result = []
    for v in varieties:
        result.append({
            'id': v.id,
            'variety_release_name': v.variety_release_name,
            'crop': v.crop,
            'country': v.country,
            'year_of_release': v.year_of_release
        })
    return jsonify({
        'total_varieties': Variety.query.count(),
        'sample_data': result
    })

# Master Import Functions
def map_master_columns(df):
    """Map uploaded file columns to database columns for master catalog"""
    column_mapping = {}
    
    # Define expected columns and their possible aliases
    expected_columns = {
        'variety_release_name': ['variety', 'release_name', 'variety_name', 'name', 'cultivar'],
        'crop': ['crop', 'crop_type', 'species'],
        'country': ['country', 'nation', 'origin'],
        'botanical_name': ['botanical_name', 'scientific_name', 'species_name'],
        'seed_type': ['seed_type', 'seed', 'type'],
        'year_of_release': ['year_of_release', 'release_year', 'year', 'introduction_year'],
        'releasing_entity': ['releasing_entity', 'releaser', 'organization', 'institution'],
        'maintainer': ['maintainer', 'maintaining_entity', 'curator'],
        'production_altitude': ['production_altitude', 'altitude', 'elevation'],
        'min_altitude': ['min_altitude', 'minimum_altitude', 'min_elevation'],
        'max_altitude': ['max_altitude', 'maximum_altitude', 'max_elevation'],
        'agroecological_zones': ['agroecological_zones', 'zones', 'ecological_zones'],
        'maturity_days': ['maturity_days', 'days_to_maturity', 'maturity_period'],
        'yield_mt_ha': ['yield_mt_ha', 'yield', 'yield_per_ha', 'productivity'],
        'presence_in_regional_catalogue': ['presence_in_regional_catalogue', 'regional_catalogue', 'catalogue_presence'],
        'other_countries_of_release': ['other_countries_of_release', 'other_countries', 'additional_countries'],
        'commercialising_companies': ['commercialising_companies', 'companies', 'commercial_companies'],
        'commercialising_names': ['commercialising_names', 'commercial_names', 'trade_names'],
        'special_attributes': ['special_attributes', 'attributes', 'characteristics', 'traits'],
        'licence_type': ['licence_type', 'license_type', 'licensing'],
        'maintenance_status': ['maintenance_status', 'status', 'maintenance'],
        'commercialising_level': ['commercialising_level', 'commercial_level', 'commercialization'],
        'disease_tolerant': ['disease_tolerant', 'disease_resistance', 'disease_tolerance'],
        'field_pest_resistant': ['field_pest_resistant', 'pest_resistance', 'field_resistance'],
        'drought_tolerant': ['drought_tolerant', 'drought_resistance'],
        'storage_pest_resistant': ['storage_pest_resistant', 'storage_resistance'],
        'consumer_preference': ['consumer_preference', 'preference', 'consumer_acceptance']
    }
    
    # Auto-detect column mappings
    uploaded_columns = df.columns.tolist()
    
    for db_column, possible_aliases in expected_columns.items():
        found = False
        
        # First try exact match (case insensitive)
        for uploaded_col in uploaded_columns:
            if uploaded_col.lower() == db_column.lower():
                column_mapping[uploaded_col] = db_column
                found = True
                break
        
        # Then try partial matches with aliases
        if not found:
            for uploaded_col in uploaded_columns:
                uploaded_lower = uploaded_col.lower()
                for alias in possible_aliases:
                    if alias in uploaded_lower or uploaded_lower in alias:
                        column_mapping[uploaded_col] = db_column
                        found = True
                        break
                if found:
                    break
        
        # Finally try fuzzy matching
        if not found:
            best_match, score, _ = process.extractOne(db_column, uploaded_columns, scorer=fuzz.partial_ratio)
            if score > 60:  # Lower threshold for master data
                column_mapping[best_match] = db_column
    
    return column_mapping

@app.route('/process_master_import', methods=['POST'])
def process_master_import_route():
    """Process master catalog import with confirmed column mapping"""
    try:
        filename = request.form.get('filename')
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], 'master', filename)
        df = pd.read_excel(filepath)
        
        # Get column mapping from form
        column_mapping = {}
        for key, value in request.form.items():
            if key.startswith('map_'):
                col_name = key[4:]
                if value:  # Only add if mapping is provided
                    column_mapping[col_name] = value
        
        return process_master_import(df, column_mapping, filename)
        
    except Exception as e:
        logger.error(f"Error in master import: {str(e)}")
        flash(f'Error importing data: {str(e)}', 'error')
        return redirect(url_for('upload_master'))

def process_master_import(df, column_mapping, filename):
    """Process the actual master data import"""
    try:
        # Clear existing data
        Variety.query.delete()
        
        # Import data with column mapping
        records_imported = 0
        skipped_records = 0
        
        for idx, row in df.iterrows():
            # Skip empty rows
            if row.isnull().all():
                skipped_records += 1
                continue
                
            variety_data = {}
            
            # Map data using column mapping
            for uploaded_col, db_col in column_mapping.items():
                if uploaded_col in df.columns:
                    value = row[uploaded_col]
                    # Convert pandas NaN to None
                    if pd.isna(value):
                        value = None
                    variety_data[db_col] = value
            
            # Skip if essential data is missing
            if not variety_data.get('variety_release_name') and not variety_data.get('crop'):
                skipped_records += 1
                continue
            
            # Create variety instance
            variety = Variety(**variety_data)
            db.session.add(variety)
            records_imported += 1
            
            # Commit in batches to avoid memory issues
            if records_imported % 100 == 0:
                db.session.commit()
        
        # Final commit
        db.session.commit()
        
        # Log upload
        upload_log = UploadLog(
            filename=filename,
            file_type='master',
            record_count=records_imported
        )
        db.session.add(upload_log)
        db.session.commit()
        
        # Update stats
        stats = {
            'total_varieties': Variety.query.count(),
            'recent_uploads': UploadLog.query.filter_by(file_type='master').order_by(UploadLog.uploaded_at.desc()).limit(5).all()
        }
        
        flash(f'Successfully imported {records_imported} varieties! {skipped_records} records were skipped.', 'success')
        return render_template('upload_master.html', stats=stats)
        
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error in process_master_import: {str(e)}")
        flash(f'Error importing data: {str(e)}', 'error')
        return redirect(url_for('upload_master'))

if __name__ == '__main__':
    with app.app_context():
        db.create_all()
    app.run(debug=True, host='0.0.0.0', port=5000)