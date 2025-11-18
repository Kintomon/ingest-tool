"""User Name Randomizer - Anonymize YouTube users"""

import random
import string
import uuid
from typing import Dict


class UserRandomizer:
    """Anonymize YouTube user information"""
    
    def __init__(self):
        # Map original YouTube names to randomized names
        self.name_map: Dict[str, str] = {}
        # Map original YouTube names to random avatars
        self.avatar_map: Dict[str, str] = {}
        # Map original YouTube names to consistent UUIDs
        self.user_id_map: Dict[str, str] = {}
        self.counter = 1
    
    def get_randomized_name(self, original_name: str) -> str:
        """
        Get consistent randomized name for YouTube user
        
        Args:
            original_name: YouTube username (e.g., "Fred23")
            
        Returns:
            Randomized name (e.g., "Fred_47")
        """
        # If we've seen this user before, return same random name
        if original_name in self.name_map:
            return self.name_map[original_name]
        
        # Generate new random name
        # Keep base part if recognizable (first word), add random number
        parts = original_name.split()
        if parts and len(parts[0]) > 2:
            base = parts[0]  # "Fred23" -> "Fred"
            # Clean base from numbers/special chars
            base = ''.join(c for c in base if c.isalpha())
            if not base:
                base = "User"
        else:
            base = "User"
        
        random_num = random.randint(1, 999)
        randomized = f"{base}_{random_num}"  # "Fred_47"
        
        # Store mapping
        self.name_map[original_name] = randomized
        
        return randomized
    
    def get_random_avatar(self, original_name: str) -> str:
        """
        Get consistent random avatar URL for YouTube user
        
        Args:
            original_name: YouTube username
            
        Returns:
            Random avatar URL (human-like image)
        """
        # If we've seen this user before, return same random avatar
        if original_name in self.avatar_map:
            return self.avatar_map[original_name]
        
        # Generate consistent seed based on name hash
        name_hash = hash(original_name)
        random.seed(name_hash)
        
        # Use DiceBear Avatars with "personas" style for human-like images
        # This provides diverse, human-like avatars
        seed = abs(name_hash) % 1000000  # Use hash as seed for consistency
        avatar_url = f"https://api.dicebear.com/7.x/personas/svg?seed={seed}&backgroundColor=b6e3f4,c0aede,d1d4f9,ffd5dc,ffdfbf"
        
        random.seed()  # Reset seed
        
        # Store mapping
        self.avatar_map[original_name] = avatar_url
        
        return avatar_url
    
    def get_user_id(self, original_name: str) -> str:
        """
        Get consistent UUID for YouTube user
        
        Args:
            original_name: YouTube username
            
        Returns:
            UUID string (consistent for same user)
        """
        # If we've seen this user before, return same UUID
        if original_name in self.user_id_map:
            return self.user_id_map[original_name]
        
        # Generate new UUID for this user
        user_uuid = str(uuid.uuid4())
        
        # Store mapping
        self.user_id_map[original_name] = user_uuid
        
        return user_uuid
    
    def anonymize_comment(self, comment: Dict) -> Dict:
        """
        Anonymize comment user info
        
        Args:
            comment: Comment dict with user_name, profile_picture, created_by_id
            
        Returns:
            Comment dict with anonymized user info
        """
        original_name = comment.get('user_name', 'Unknown')
        
        if 'user_name' in comment:
            comment['user_name'] = self.get_randomized_name(original_name)
        
        # Generate random avatar for this user
        comment['profile_picture'] = self.get_random_avatar(original_name)
        
        # Generate consistent UUID for this user (required by backend)
        comment['created_by_id'] = self.get_user_id(original_name)
        
        return comment
    
    def anonymize_comments(self, comments: list) -> list:
        """
        Anonymize all comments
        
        Args:
            comments: List of comment dicts
            
        Returns:
            Anonymized comments
        """
        for comment in comments:
            self.anonymize_comment(comment)
            
            # Also anonymize replies
            if 'replies' in comment and comment['replies']:
                for reply in comment['replies']:
                    self.anonymize_comment(reply)
        
        return comments

