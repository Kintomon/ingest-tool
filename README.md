# YouTube Video Ingestion Tool

Ingests YouTube videos and comments from `list.txt` into InCast.

## 🚀 Usage

**Just edit variables in `ingest.py` and run:**

```bash
python3 ingest.py
```

**No arguments needed!**

## ⚙️ Configuration

Edit these variables in `ingest.py`:

```python
JWT_TOKEN = "YOUR_JWT_TOKEN_HERE"  # Your authentication token
BACKEND_URL = "https://api-dev.incast.ai"  # Backend URL
PUBLISH_URL = "https://api-dev.incast.ai/publish-comment"
LIST_FILE = "list.txt"  # Input file
MAX_COMMENTS = 100  # Max comments per video
DRY_RUN = True  # Set False to actually upload
```

## 📋 Input File

Create `list.txt`:
```
https://youtube.com/watch?v=abc123,Finance
https://youtube.com/watch?v=xyz789,Tech
```

Format: `youtube_url,category`

## 🔐 How to Get JWT Token

1. Login to InCast app in browser
2. Press **F12** → **DevTools** → **Console** tab
3. Run:
```javascript
document.cookie.split('; ').find(row => row.startsWith('JWT=')).split('=')[1]
```
4. Copy the token and paste in `ingest.py` as `JWT_TOKEN`

## 🧪 Testing with Dry Run

**Default mode:** `DRY_RUN = True`

- Extracts all data from YouTube
- Shows what WOULD be uploaded
- **No actual uploads or comments**
- Perfect for testing workflow!

**To actually ingest:** Set `DRY_RUN = False`

## 🔒 User Anonymization

**Automatic privacy:**
- YouTube users anonymized
- "Fred23" → "Fred_47" (random)
- Same user gets same random name across comments
- Profile pictures removed
- No real YouTube info exposed

## 📦 Installation & Setup

### Option 1: Automated Setup (Recommended)

```bash
cd /home/mdev/InCast/Ingest-tool
./setup.sh
```

This will:
- Create virtual environment (`venv`)
- Activate it
- Install all dependencies

### Option 2: Manual Setup

```bash
cd /home/mdev/InCast/Ingest-tool

# Create virtual environment
python3 -m venv venv

# Activate venv
source venv/bin/activate

# Install dependencies
pip install --upgrade pip
pip install -r requirements.txt
```

### Running the Script

After setup:

```bash
# Activate venv (if not already active)
source venv/bin/activate

# Run the script
python3 ingest.py
```

**Note:** Activate venv every time you open a new terminal!

## ⚙️ What It Extracts

From each YouTube video:
- **Title** - Video title
- **Description** - Full description
- **Keywords** - Video tags
- **Comments** - Up to 100 comments

## 🔄 Process Flow

1. Read `list.txt`
2. For each video:
   - Extract title, description, keywords, comments
   - Anonymize user names
   - Create asset via GraphQL (streams URL → GCS)
   - Import comments via Cloud Function
   - NLP processes comments

## 📊 Output

Console shows:
- Progress per video
- Asset IDs created
- Comments imported count
- User anonymization applied
- Final summary

## 🐛 Troubleshooting

**"JWT not found"**: Login to InCast app first

**"Asset creation failed"**: Check JWT token is valid

**"No comments"**: Video may have disabled comments

## 📁 Structure

```
modules/
├── youtube_processor.py  # Extract title, description, keywords, comments
├── asset_creator.py      # GraphQL CreateAssetFromUrl
├── comment_importer.py   # Cloud Function import
├── user_randomizer.py    # Anonymize YouTube users
└── batch_processor.py    # Orchestrate workflow

ingest.py                 # Main script (edit config here)
```

## 🎯 Example Workflow

1. **Edit config:**
   - Set JWT_TOKEN in ingest.py
   - Keep DRY_RUN = True

2. **Test:**
   ```bash
   python3 ingest.py
   ```
   Review output, check it looks good

3. **Actually ingest:**
   - Set DRY_RUN = False
   - python3 ingest.py
   - Done!
