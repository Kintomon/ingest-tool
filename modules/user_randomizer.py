"""User Name Randomizer"""

import random
import string
import uuid
from typing import Dict


class UserRandomizer:
    def __init__(self):
        self.name_map: Dict[str, str] = {}
        self.avatar_map: Dict[str, str] = {}
        self.user_id_map: Dict[str, str] = {}
        self.counter = 1
    
    def get_randomized_name(self, original_name: str) -> str:
        if original_name in self.name_map:
            return self.name_map[original_name]
        
        parts = original_name.split()
        if parts and len(parts[0]) > 2:
            base = parts[0]
            base = ''.join(c for c in base if c.isalpha())
            if not base:
                base = "User"
        else:
            base = "User"
        
        random_num = random.randint(1, 999)
        randomized = f"{base}_{random_num}"
        
        self.name_map[original_name] = randomized
        
        return randomized
    
    def get_random_avatar(self, original_name: str) -> str:
        if original_name in self.avatar_map:
            return self.avatar_map[original_name]
        
        name_hash = hash(original_name)
        random.seed(name_hash)
        
        seed = abs(name_hash) % 1000000
        avatar_url = f"https://api.dicebear.com/7.x/personas/svg?seed={seed}&backgroundColor=b6e3f4,c0aede,d1d4f9,ffd5dc,ffdfbf"
        
        random.seed()
        
        self.avatar_map[original_name] = avatar_url
        
        return avatar_url
    
    def get_user_id(self, original_name: str) -> str:
        if original_name in self.user_id_map:
            return self.user_id_map[original_name]
        
        user_uuid = str(uuid.uuid4())
        
        self.user_id_map[original_name] = user_uuid
        
        return user_uuid
    
    def anonymize_comment(self, comment: Dict) -> Dict:
        original_name = comment.get('user_name', 'Unknown')
        
        if 'user_name' in comment:
            comment['user_name'] = self.get_randomized_name(original_name)
        
        comment['profile_picture'] = self.get_random_avatar(original_name)
        
        comment['created_by_id'] = self.get_user_id(original_name)
        
        return comment
    
    def anonymize_comments(self, comments: list) -> list:
        for comment in comments:
            self.anonymize_comment(comment)
            
            if 'replies' in comment and comment['replies']:
                for reply in comment['replies']:
                    self.anonymize_comment(reply)
        
        return comments

