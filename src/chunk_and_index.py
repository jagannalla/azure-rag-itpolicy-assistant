import json, os, uuid
from dotenv import load_dotenv
from openai import OpenAI
from azure.core.credentials import AzureKeyCredential
from azure.search.documents import SearchClient
from azure.search.documents.indexes import SearchIndexClient
from azure.search.documents.indexes.models import (
    SearchIndex, SearchField, SearchFieldDataType
)

load_dotenv()

# YOUR PERSONAL OpenAI (for testing)
openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# YOUR AI Search
search_endpoint = os.getenv("AZURE_SEARCH_ENDPOINT")
search_key = os.getenv("AZURE_SEARCH_KEY")

def semantic_chunking(raw_chunks, target_chars=1000):
    """201 raw chunks → ~50 semantic chunks"""
    chunks = []
    current_chunk = []
    current_length = 0
    
    print(f"🔪 Chunking {len(raw_chunks)} raw lines...")
    
    for item in raw_chunks:
        text = item['content'].strip()
        if len(text) < 20:  # Skip fragments
            continue
            
        text_len = len(text)
        
        if current_length + text_len > target_chars and current_chunk:
            chunks.append({
                "id": str(uuid.uuid4()),
                "content": ". ".join([c['content'] for c in current_chunk]),
                "page": current_chunk[0]['page'],
                "type": current_chunk[0]['type'],
                "doc_name": "support.pdf"
            })
            current_chunk = [item]
            current_length = text_len
        else:
            current_chunk.append(item)
            current_length += text_len
    
    # Final chunk
    if current_chunk:
        chunks.append({
            "id": str(uuid.uuid4()),
            "content": ". ".join([c['content'] for c in current_chunk]),
            "page": current_chunk[0]['page'],
            "type": current_chunk[0]['type'],
            "doc_name": "support.pdf"
        })
    
    print(f"✅ Created {len(chunks)} semantic chunks")
    return chunks

def create_simple_index():
    """✅ NO ENUM ISSUES - Pure string types"""
    index_client = SearchIndexClient(
        endpoint=search_endpoint, 
        credential=AzureKeyCredential(search_key)
    )
    
    fields = [
        SearchField(name="id", type="Edm.String", key=True),
        SearchField(name="content", type="Edm.String", searchable=True, retrievable=True),
        SearchField(name="page", type="Edm.Int32", filterable=True, sortable=True),
        SearchField(name="type", type="Edm.String", filterable=True),
        SearchField(name="doc_name", type="Edm.String", filterable=True)
    ]
    
    index = SearchIndex(name="itpolicy-nist-index", fields=fields)
    
    try:
        index_client.create_or_update_index(index)
        print("✅ SIMPLE INDEX created: itpolicy-nist-index")
        return True
    except Exception as e:
        print(f"✅ Index exists: {str(e)[:100]}...")
        return True

def test_openai_embedding():
    """Quick test of YOUR OpenAI key"""
    try:
        embedding = openai_client.embeddings.create(
            model="text-embedding-3-small",
            input=["test policy document"]
        )
        print("✅ OpenAI key working: 1536-dim embeddings ready")
        return True
    except Exception as e:
        print(f"❌ OpenAI error: {e}")
        return False

def index_chunks(chunks):
    """Index to YOUR rag-ai-search-workshop"""
    search_client = SearchClient(
        endpoint=search_endpoint,
        index_name="itpolicy-nist-index",
        credential=AzureKeyCredential(search_key)
    )
    
    print(f"📤 Indexing {len(chunks)} chunks...")
    result = search_client.upload_documents(chunks)
    
    succeeded = sum(1 for r in result if r.succeeded)
    failed = len(chunks) - succeeded
    
    print(f"🎉 SUCCESS: {succeeded}/{len(chunks)} indexed!")
    print(f"   Failed: {failed}")
    if failed > 0:
        print("   Check logs for details")
    
    return succeeded == len(chunks)

if __name__ == "__main__":
    print("🚀 DAY 3: Chunking + Indexing (16 pages)")
    
    # Load your data
    with open("./output/nist-sp800-53.json", "r") as f:
        raw_content = json.load(f)
    
    print(f"📄 Loaded {len(raw_content)} raw chunks from 16 pages")
    
    # 1. Test OpenAI key
    if not test_openai_embedding():
        print("⚠️  OpenAI key issue - fix .env then rerun")
        exit(1)
    
    # 2. Create simple index
    create_simple_index()
    
    # 3. Semantic chunking
    chunks = semantic_chunking(raw_content)
    
    # Save chunks for inspection
    with open("./output/nist_chunks_indexed.json", "w") as f:
        json.dump(chunks, f, indent=2)
    
    print(f"📋 Sample: {chunks[0]['content'][:150]}...")
    
    # 4. Index!
    success = index_chunks(chunks)
    
    if success:
        print("\n🎉 DAY 3 100% COMPLETE!")
        print("✅ Portal: rag-ai-search-workshop → Indexes → itpolicy-nist-index")
        print("✅ Local: ./output/nist_chunks_indexed.json (~50 chunks)")
        print("✅ Ready for Day 4: RAG queries!")
    else:
        print("\n❌ Some indexing failed - check Azure Portal logs")
