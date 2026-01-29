from fastapi import FastAPI, HTTPException, Depends, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, EmailStr
from typing import Optional, List
from datetime import datetime, timedelta
from pymongo import MongoClient
import bcrypt
import hashlib
import jwt
from bson import ObjectId
import os

# Configuration
SECRET_KEY = os.getenv("JWT_SECRET_KEY", "your-secret-key-change-this")  # Change in production!
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 1440  # 24 hours
MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017/")
DATABASE_NAME = os.getenv("DATABASE_NAME", "bookclub_db")

# Initialize FastAPI
app = FastAPI(
    title="Book Club API",
    description="Admin portal API for book club management",
    version="1.0.0"
)

# CORS middleware - configure based on your needs
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:8501"],  # Streamlit default port
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# MongoDB connection
client = MongoClient(MONGODB_URI)
db = client[DATABASE_NAME]

# Security
security = HTTPBearer()

# Pydantic models
class LoginRequest(BaseModel):
    username_or_email: str
    password: str

class TokenResponse(BaseModel):
    access_token: str
    token_type: str
    user: dict

class UserCreate(BaseModel):
    username: str
    email: EmailStr
    password: str
    role: str = "member"

class UserUpdate(BaseModel):
    role: Optional[str] = None

class BookCreate(BaseModel):
    title: str
    author: str
    description: Optional[str] = None
    isbn: Optional[str] = None
    genre: Optional[str] = None

class ClubCreate(BaseModel):
    name: str
    description: Optional[str] = None

class DiscussionCreate(BaseModel):
    title: str
    book_id: Optional[str] = None
    club_id: Optional[str] = None

# Helper functions
def hash_email(email: str) -> str:
    """Hash email address with SHA-256"""
    return hashlib.sha256(email.lower().encode('utf-8')).hexdigest()

def verify_password(plain_password: str, hashed_password: bytes) -> bool:
    """Verify password against bcrypt hash"""
    if isinstance(hashed_password, str):
        hashed_password = hashed_password.encode('utf-8')
    return bcrypt.checkpw(plain_password.encode('utf-8'), hashed_password)

def hash_password(password: str) -> bytes:
    """Hash password with bcrypt"""
    return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())

def create_access_token(data: dict) -> str:
    """Create JWT access token"""
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

def serialize_doc(doc):
    """Convert MongoDB document to JSON-serializable dict"""
    if doc is None:
        return None
    doc["_id"] = str(doc["_id"])
    # Convert datetime objects
    for key, value in doc.items():
        if isinstance(value, datetime):
            doc[key] = value.isoformat()
        elif isinstance(value, ObjectId):
            doc[key] = str(value)
    return doc

def serialize_docs(docs):
    """Convert list of MongoDB documents"""
    return [serialize_doc(doc) for doc in docs]

async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Verify JWT token and return current user"""
    try:
        token = credentials.credentials
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user_id = payload.get("sub")
        if user_id is None:
            raise HTTPException(status_code=401, detail="Invalid authentication credentials")
        
        user = db["users"].find_one({"_id": ObjectId(user_id)})
        if user is None:
            raise HTTPException(status_code=401, detail="User not found")
        
        return serialize_doc(user)
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token has expired")
    except jwt.JWTError:
        raise HTTPException(status_code=401, detail="Could not validate credentials")

async def require_admin(current_user: dict = Depends(get_current_user)):
    """Require user to be admin"""
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin privileges required")
    return current_user

# Authentication endpoints
@app.post("/api/auth/login", response_model=TokenResponse)
async def login(request: LoginRequest):
    """Authenticate user and return JWT token"""
    users_collection = db["users"]
    
    # Try username first
    user = users_collection.find_one({"username": request.username_or_email})
    
    # If not found, try hashed email
    if not user:
        hashed_email = hash_email(request.username_or_email)
        user = users_collection.find_one({"email": hashed_email})
    
    if not user or not verify_password(request.password, user["password"]):
        raise HTTPException(status_code=401, detail="Invalid credentials")
    
    # Check admin status
    if user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin privileges required")
    
    # Create access token
    access_token = create_access_token({"sub": str(user["_id"])})
    
    # Remove password from response
    user_data = serialize_doc(user)
    user_data.pop("password", None)
    
    return {
        "access_token": access_token,
        "token_type": "bearer",
        "user": user_data
    }

@app.get("/api/auth/me")
async def get_me(current_user: dict = Depends(get_current_user)):
    """Get current user info"""
    current_user.pop("password", None)
    return current_user

# Statistics endpoint
@app.get("/api/stats")
async def get_stats(current_user: dict = Depends(require_admin)):
    """Get dashboard statistics"""
    return {
        "users_count": db["users"].count_documents({}),
        "admin_count": db["users"].count_documents({"role": "admin"}),
        "books_count": db["books"].count_documents({}),
        "clubs_count": db["clubs"].count_documents({}),
        "discussions_count": db["discussions"].count_documents({})
    }

# User management endpoints
@app.get("/api/users")
async def list_users(
    search: Optional[str] = None,
    role: Optional[str] = None,
    limit: int = 100,
    current_user: dict = Depends(require_admin)
):
    """List users with optional filters"""
    query = {}
    if search:
        query["username"] = {"$regex": search, "$options": "i"}
    if role and role != "All":
        if role == "Admin":
            query["role"] = "admin"
        elif role == "Member":
            query["role"] = {"$ne": "admin"}
    
    users = list(db["users"].find(query).sort("username", 1).limit(limit))
    # Remove passwords
    for user in users:
        user.pop("password", None)
    
    return serialize_docs(users)

@app.post("/api/users", status_code=status.HTTP_201_CREATED)
async def create_user(user: UserCreate, current_user: dict = Depends(require_admin)):
    """Create a new user"""
    users_collection = db["users"]
    
    # Check if username exists
    if users_collection.find_one({"username": user.username}):
        raise HTTPException(status_code=400, detail="Username already exists")
    
    # Create user
    new_user = {
        "username": user.username,
        "email": hash_email(user.email),
        "password": hash_password(user.password),
        "role": user.role,
        "created_at": datetime.now()
    }
    
    result = users_collection.insert_one(new_user)
    new_user["_id"] = result.inserted_id
    new_user.pop("password")
    
    return serialize_doc(new_user)

@app.patch("/api/users/{user_id}")
async def update_user(
    user_id: str,
    update: UserUpdate,
    current_user: dict = Depends(require_admin)
):
    """Update user (currently just role)"""
    users_collection = db["users"]
    
    update_data = {}
    if update.role:
        update_data["role"] = update.role
    
    if not update_data:
        raise HTTPException(status_code=400, detail="No fields to update")
    
    result = users_collection.update_one(
        {"_id": ObjectId(user_id)},
        {"$set": update_data}
    )
    
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="User not found")
    
    return {"message": "User updated successfully"}

@app.delete("/api/users/{user_id}")
async def delete_user(user_id: str, current_user: dict = Depends(require_admin)):
    """Delete a user"""
    # Prevent self-deletion
    if current_user["_id"] == user_id:
        raise HTTPException(status_code=400, detail="Cannot delete your own account")
    
    result = db["users"].delete_one({"_id": ObjectId(user_id)})
    
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="User not found")
    
    return {"message": "User deleted successfully"}

# Book management endpoints
@app.get("/api/books")
async def list_books(
    search: Optional[str] = None,
    limit: int = 100,
    current_user: dict = Depends(require_admin)
):
    """List books with optional search"""
    query = {}
    if search:
        query = {"$or": [
            {"title": {"$regex": search, "$options": "i"}},
            {"author": {"$regex": search, "$options": "i"}}
        ]}
    
    books = list(db["books"].find(query).sort("title", 1).limit(limit))
    return serialize_docs(books)

@app.post("/api/books", status_code=status.HTTP_201_CREATED)
async def create_book(book: BookCreate, current_user: dict = Depends(require_admin)):
    """Create a new book"""
    new_book = {
        "title": book.title,
        "author": book.author,
        "description": book.description,
        "isbn": book.isbn,
        "genre": book.genre,
        "added_at": datetime.now(),
        "added_by": current_user["username"]
    }
    
    result = db["books"].insert_one(new_book)
    new_book["_id"] = result.inserted_id
    
    return serialize_doc(new_book)

@app.delete("/api/books/{book_id}")
async def delete_book(book_id: str, current_user: dict = Depends(require_admin)):
    """Delete a book"""
    result = db["books"].delete_one({"_id": ObjectId(book_id)})
    
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Book not found")
    
    return {"message": "Book deleted successfully"}

# Club management endpoints
@app.get("/api/clubs")
async def list_clubs(current_user: dict = Depends(require_admin)):
    """List all clubs"""
    clubs = list(db["clubs"].find().sort("name", 1))
    return serialize_docs(clubs)

@app.post("/api/clubs", status_code=status.HTTP_201_CREATED)
async def create_club(club: ClubCreate, current_user: dict = Depends(require_admin)):
    """Create a new club"""
    new_club = {
        "name": club.name,
        "description": club.description,
        "members": [],
        "created_at": datetime.now(),
        "created_by": current_user["username"]
    }
    
    result = db["clubs"].insert_one(new_club)
    new_club["_id"] = result.inserted_id
    
    return serialize_doc(new_club)

@app.delete("/api/clubs/{club_id}")
async def delete_club(club_id: str, current_user: dict = Depends(require_admin)):
    """Delete a club"""
    result = db["clubs"].delete_one({"_id": ObjectId(club_id)})
    
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Club not found")
    
    return {"message": "Club deleted successfully"}

# Discussion management endpoints
@app.get("/api/discussions")
async def list_discussions(current_user: dict = Depends(require_admin)):
    """List all discussions"""
    discussions = list(db["discussions"].find().sort("_id", -1).limit(100))
    return serialize_docs(discussions)

@app.post("/api/discussions", status_code=status.HTTP_201_CREATED)
async def create_discussion(
    discussion: DiscussionCreate,
    current_user: dict = Depends(require_admin)
):
    """Create a new discussion"""
    new_discussion = {
        "title": discussion.title,
        "messages": [],
        "created_at": datetime.now(),
        "created_by": current_user["username"]
    }
    
    if discussion.book_id:
        new_discussion["book_id"] = ObjectId(discussion.book_id)
    if discussion.club_id:
        new_discussion["club_id"] = ObjectId(discussion.club_id)
    
    result = db["discussions"].insert_one(new_discussion)
    new_discussion["_id"] = result.inserted_id
    
    return serialize_doc(new_discussion)

@app.delete("/api/discussions/{discussion_id}")
async def delete_discussion(discussion_id: str, current_user: dict = Depends(require_admin)):
    """Delete a discussion"""
    result = db["discussions"].delete_one({"_id": ObjectId(discussion_id)})
    
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Discussion not found")
    
    return {"message": "Discussion deleted successfully"}

# Health check
@app.get("/")
async def root():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "Book Club API",
        "version": "1.0.0"
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
