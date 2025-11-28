# YouTube Video Ingestion Tool

Automated tool to ingest YouTube videos and comments into InCast platform.

## 🚀 Quick Start

```bash
# 1. Setup (first time only)
./setup.sh

# 2. Configure
cp config.yaml.example config.yaml
nano config.yaml  # Edit your settings

# 3. Add videos
nano list.txt  # Add YouTube URLs

# 4. Run
source venv/bin/activate
python3 ingest.py
```

## 📋 Prerequisites

- Python 3.8+
- Firebase API Key
- InCast credential

## ⚙️ Configuration

### 1. Copy and Edit `config.yaml`

```bash
cp config.yaml.example config.yaml
nano config.yaml
```

### 2. Required Settings

```yaml
firebase:
  api_key: "YOUR_FIREBASE_API_KEY"  # Get from Firebase console

api:
  backend_url: "https://api-dev.incast.ai"
  publish_url: "https://api-dev.incast.ai/publish-comment"

modes:
  dry_run: true  # Set false to actually upload
  video_only: false
  comments_only: false
  asset_id: ""  # Required if comments_only=true

processing:
  max_items_limit: "all"  # or number like "10"
  skip_live_chat: false
```

## 🔐 Authentication

The tool will **prompt for email/password** when you run it:

```bash
python3 ingest.py
# Email: your@email.com
# Password: ********
```

Credentials are authenticated via:
1. **Firebase** (email/password)
2. **Backend** (exchanges Firebase token for JWT)

**No tokens stored in config files!** 🔒

## 📋 Input File Format

Create `list.txt` with YouTube URLs:

```
https://www.youtube.com/watch?v=dQw4w9WgXcQ,Music
https://www.youtube.com/watch?v=jNQXAC9IVRw,Education
```

**Format:** `youtube_url,category`

## 🎯 Operating Modes

### Normal Mode (Default)
```yaml
dry_run: false
video_only: false
comments_only: false
```
- Downloads video
- Uploads to GCS and create asset
- Imports comments & live chat

### Video Only Mode
```yaml
video_only: true
```
- Only uploads video, skips comments

### Comments Only Mode
```yaml
comments_only: true
asset_id: "existing-asset-uuid-here"
```
- Only imports comments to existing video
- Requires asset_id in config

### Dry Run Mode
```yaml
dry_run: true
```
- Tests everything without uploads
- Shows what would be imported
- Perfect for testing! 🧪

## 🎬 What Gets Extracted

### Video Metadata
- Title
- Description
- Keywords/Tags
- Category

### Comments (with timestamps)
- Top-level comments
- Replies (parent-child relationships)
- Only comments with video timestamps (e.g., "at 1:30")
- Threaded reply structure preserved

### Live Chat (optional)
- Live chat replay messages
- Timestamps synced to video
- Can be disabled: `skip_live_chat: true`

## 🔒 Privacy & Anonymization

All YouTube users are automatically anonymized:
- `"John_Smith123"` → `"John_847"`
- Consistent names (same user = same random name)
- Random avatars generated (DiceBear API)
- UUIDs assigned to each unique user
- No real YouTube data exposed ✅

## 📦 Installation

### Automated Setup (Recommended)

```bash
cd /home/mdev/InCast/Ingest-tool
./setup.sh
```

This creates a virtual environment and installs all dependencies.

### Manual Setup

```bash
# Create virtual environment
python3 -m venv venv

# Activate it
source venv/bin/activate

# Install dependencies
pip install --upgrade pip
pip install -r requirements.txt
```

## 📊 Output & Logs

Console shows:
```
🎬 Processing: https://youtube.com/watch?v=...
📹 Title: Video Name
✅ Video uploaded successfully!
📊 Processing 328 comments with timestamps...
✅ Comments processed: 315/328
✅ Live chats processed: 156/156
```

Final summary:
```
📊 TIMESTAMP DETECTION SUMMARY
   ✓ Comments with timestamp: 315
   ✗ Comments without timestamp: 1977
   💬 Live chat messages published: 156
   
📊 BATCH PROCESSING SUMMARY
Total videos: 3
✅ Successful: 3
💬 Total comments imported: 789
```

## 🐛 Troubleshooting

### Authentication Failed
- Check Firebase API key in `config.yaml`
- Verify email/password are correct
- Ensure backend URL is accessible

### Video Download Fails
- Some videos may require cookies
- Create `cookies.txt` (Netscape format)
- Tool will auto-detect and use it

### No Comments Found
- Video may have comments disabled
- Or no comments have timestamps
- Check YouTube page directly

### HTTP 400/500 Errors
- Token may be expired (should auto-refresh)
- Check backend is running
- Verify API URLs in config

## 📝 Example Workflow

### For Interns/New Users

1. **Initial Setup**
   ```bash
   cd /home/mdev/InCast/Ingest-tool
   ./setup.sh
   cp config.yaml.example config.yaml
   ```

2. **Configure**
   ```bash
   nano config.yaml
   # Add Firebase API key
   # Set dry_run: true
   ```

3. **Prepare Videos**
   ```bash
   nano list.txt
   # Add YouTube URLs, one per line with category
   ```

4. **Test Run (Dry Mode)**
   ```bash
   source venv/bin/activate
   python3 ingest.py
   # Enter email/password when prompted
   # Review output - no actual uploads
   ```

5. **Actual Run**
   ```bash
   nano config.yaml
   # Set dry_run: false
   
   python3 ingest.py
   # Enter credentials again
   # Watch the magic happen! ✨
   ```

## 🆘 Getting Help

1. Check this README
2. Review console output/logs
3. Look at `config.yaml.example` for examples
5. Ask Yehor :)!

## 📄 License

Internal tool for InCast platform.