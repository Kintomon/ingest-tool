"""
Modular YouTube Video Ingestion Tool

Structure:
    - youtube_processor.py: Extract video info/description/keywords/comments
    - gcs_uploader.py: Upload video to GCS bucket
    - asset_creator.py: Create asset via GraphQL mutation
    - comment_importer.py: Import comments via Cloud Function
    - batch_processor.py: Process list of URLs
"""

