# 🌱 CropIntel - Crop Variety Intelligence System

![CropIntel Banner](https://via.placeholder.com/1200x400/3b82f6/ffffff?text=CropIntel+-+Advanced+Agricultural+Data+Management)

An intelligent, web-based platform for managing, matching, and analyzing crop variety data across multiple catalogs and countries. Built specifically for agricultural researchers, crop breeders, and agricultural organizations.

## 🚀 Quick Start

### Prerequisites
- Python 3.8+
- pip (Python package manager)

### Installation

1. **Clone the repository**
```bash
git clone https://github.com/yourusername/cropintel.git
cd cropintel
```

2. **Create virtual environment**
```bash
python -m venv venv
# On Windows
venv\Scripts\activate
# On macOS/Linux
source venv/bin/activate
```

3. **Install dependencies**
```bash
pip install -r requirements.txt
```

4. **Initialize database**
```bash
python app.py
# The database will be created automatically
```

5. **Run the application**
```bash
python app.py
```
Visit `http://localhost:5000` in your browser

## 📁 Project Structure

```
cropintel/
├── app.py                    # Main Flask application
├── requirements.txt          # Python dependencies
├── uploads/                  # Uploaded files storage
│   ├── master/              # Master catalog files
│   ├── country/            # Country-specific files
│   ├── combined/           # Combined file outputs
│   └── attributes/         # Attribute extraction files
├── templates/              # HTML templates
│   ├── base.html          # Base template
│   ├── index.html         # Dashboard
│   ├── combine_files.html # File combination interface
│   ├── compare_files.html # File comparison interface
│   └── ... (other templates)
└── crop_variety.db        # SQLite database (auto-generated)
```

## 🔧 Core Features

### 📊 Data Management
- **Master Catalog Management**: Upload and maintain reference crop variety databases
- **Multi-file Support**: Process up to 25 Excel files simultaneously
- **Intelligent Column Mapping**: Auto-detect and map columns using pattern recognition

### 🔍 Advanced Matching
- **100% Accurate Matching**: Uses concatenated crop+variety keys
- **Multi-algorithm Approach**: TF-IDF cosine similarity + fuzzy matching
- **Cross-catalog Matching**: Match country data against master catalogs

### 🧹 Data Processing
- **File Combination**: Merge multiple Excel files into unified datasets
- **Smart Cleaning**: Remove duplicates, extra spaces, and standardize formatting
- **Excel Protection**: Prevent auto-formatting issues (e.g., "4-6" to dates)

### 🔎 Search & Analysis
- **Dual Search Modes**: Cascading and global search
- **Advanced Filtering**: Excel-like interactive filters
- **Real-time Analytics**: Data quality dashboards

### 🧬 Attribute Intelligence
- **Automatic Trait Extraction**: NLP-based extraction from descriptions
- **Trait Classification**: Disease tolerance, pest resistance, drought tolerance
- **Confidence Scoring**: Intelligent attribute classification

### 📈 Comparison Tools
- **File Comparison**: Compare two datasets using exact matching
- **Difference Analysis**: Identify unique records
- **Export Options**: Multiple format downloads

## 🎯 User Workflows

### 1. Master Catalog Setup
```
1. Upload Master File → 2. Column Mapping → 3. Database Import → 4. Trait Extraction
```

### 2. Country Data Processing
```
1. Upload Country Files → 2. Column Mapping → 3. Crop+Variety Matching → 4. Export
```

### 3. File Combination
```
1. Upload Multiple Files → 2. Text Cleaning → 3. Duplicate Removal → 4. Export
```

### 4. Data Comparison
```
1. Upload Two Files → 2. Exact Matching → 3. Identify Matches/Uniques → 4. Export
```

## 🗃️ Database Schema

### Core Tables
- **Variety**: Main crop variety entity (25+ attributes)
- **UploadLog**: File upload history and statistics
- **MatchingLog**: Matching operation records
- **FilterSession**: User filtering sessions
- **FilteredData**: Filter criteria and results

## 📋 API Endpoints

### Data Management
- `GET /` - Dashboard
- `POST /upload_master` - Upload master catalog
- `POST /upload_country` - Upload country data
- `GET /export` - Export matched data

### Search & Analysis
- `GET /search` - Advanced search interface
- `GET /api/search` - Search API endpoint
- `GET /data-quality` - Data quality dashboard

### File Operations
- `GET/POST /combine_files` - Combine multiple files
- `GET/POST /compare_files` - Compare two files
- `GET/POST /split_data` - Split files by column
- `GET/POST /extract_attributes` - Extract traits from text

### Download Endpoints
- `GET /download_combined_files` - Download combined data
- `GET /download_combined_files_csv` - Download as CSV
- `GET /download_skipped_rows_excel` - Download skipped rows
- `GET /download_skipped_rows_csv` - Download skipped rows as CSV

## 🛠️ Configuration

### Environment Variables
Create a `.env` file:
```env
FLASK_SECRET_KEY=your-secret-key-here
FLASK_ENV=development
MAX_CONTENT_LENGTH=16777216  # 16MB file limit
UPLOAD_FOLDER=uploads
```

### Database Configuration
Default SQLite configuration (easy to switch to PostgreSQL):
```python
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///crop_variety.db'
# For PostgreSQL: 'postgresql://user:password@localhost/cropintel'
```

## 🧪 Testing

### Run Basic Tests
```bash
# Test the application starts
python -c "import app; print('Import successful')"

# Test database connection
python -c "from app import db; db.create_all(); print('Database initialized')"
```

### Test File Uploads
Sample Excel files are available in the `samples/` directory for testing.

## 📈 Performance Optimizations

### Large File Handling
- Batch processing for datasets >10,000 records
- Server-side session storage for large files
- Progress tracking with loading indicators
- Memory-efficient pandas operations

### Caching Strategy
- LRU caching for frequent operations
- Session-based data persistence
- Intelligent query optimization

## 🔒 Security Features

- Secure file upload validation
- Session-based data isolation
- Input sanitization and validation
- Protected download endpoints
- XSS and CSRF protection

## 🐛 Troubleshooting

### Common Issues

1. **"File too large" error**
   - Increase `MAX_CONTENT_LENGTH` in app.py
   - Split large files using the Split Data feature

2. **Database connection issues**
   - Ensure write permissions in project directory
   - Check disk space availability

3. **Excel formatting issues**
   - Use the CSV export option for perfect data integrity
   - Enable "Excel Protection" in combination features

4. **Import errors**
   - Ensure Excel files are .xlsx or .xls format
   - Check column headers match expected format

### Logs
Check application logs in the terminal or set up file logging:
```python
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='cropintel.log'
)
```

## 📚 Documentation

### For Users
- [User Guide](docs/USER_GUIDE.md) - Complete user documentation
- [FAQs](docs/FAQ.md) - Frequently asked questions
- [Sample Data](samples/) - Example files for testing

### For Developers
- [API Documentation](docs/API.md) - Complete API reference
- [Development Guide](docs/DEVELOPMENT.md) - Contributing guidelines
- [Architecture](docs/ARCHITECTURE.md) - System architecture overview

## 🤝 Contributing

We welcome contributions! Please see our [Contributing Guidelines](CONTRIBUTING.md) for details.

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Agricultural researchers worldwide for their data contributions
- Open source libraries that make this project possible
- Contributors and testers who help improve the system

## 📞 Support

- **Issues**: [GitHub Issues](https://github.com/Adundo-Maseno/cropintel/issues)
- **Documentation**: [Project Wiki](https://github.com/Adundo-Maseno/Crop_Intel/wiki)
- **Email**: support@cropintel.org

## 🌟 Features in Development

- [ ] Geospatial analysis with variety distribution maps
- [ ] Temporal analysis for year-over-year performance
- [ ] API integration with external agricultural databases
- [ ] Machine learning for variety suitability prediction
- [ ] Mobile application for field data collection

---

**CropIntel** - Transforming agricultural data into actionable intelligence for better crop breeding decisions.
