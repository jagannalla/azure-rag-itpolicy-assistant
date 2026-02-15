import os, json
from azure.ai.documentintelligence import DocumentIntelligenceClient
from azure.core.credentials import AzureKeyCredential
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

load_dotenv()

# YOUR resources
blob_conn = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
di_endpoint = os.getenv("AZURE_DOCUMENT_INTELLIGENCE_ENDPOINT")
di_key = os.getenv("AZURE_DOCUMENT_INTELLIGENCE_KEY")

blob_client = BlobServiceClient.from_connection_string(blob_conn)
di_client = DocumentIntelligenceClient(endpoint=di_endpoint, credential=AzureKeyCredential(di_key))

def analyze_document(file_path):
    """✅ ROBUST: Handles ALL Document Intelligence response formats"""
    print("🔍 Analyzing with Document Intelligence...")
    
    with open(file_path, "rb") as f:
        poller = di_client.begin_analyze_document("prebuilt-layout", body=f)
        result = poller.result()
    
    content = []
    
    # Parse PAGES (primary structure)
    for page in result.pages or []:
        page_num = getattr(page, 'page_number', 'unknown')
        
        # METHOD 1: Try paragraphs (newer models)
        if hasattr(page, 'paragraphs') and page.paragraphs:
            for para in page.paragraphs:
                content.append({
                    "type": "paragraph",
                    "content": para.content or "",
                    "page": page_num
                })
        
        # METHOD 2: Fallback to LINES (most reliable)
        elif hasattr(page, 'lines') and page.lines:
            for line in page.lines:
                content.append({
                    "type": "line", 
                    "content": line.content or "",
                    "page": page_num,
                    "confidence": getattr(line, 'confidence', 0.0)
                })
        
        # METHOD 3: TABLES (if present)
        if hasattr(page, 'tables') and page.tables:
            for table in page.tables:
                table_content = []
                for cell in table.cells or []:
                    cell_text = f"R{cell.row_index}C{cell.column_index}: {getattr(cell, 'content', '')}"
                    table_content.append(cell_text)
                
                content.append({
                    "type": "table",
                    "content": "\n".join(table_content),
                    "page": page_num,
                    "rows": getattr(table, 'row_count', 0),
                    "cols": getattr(table, 'column_count', 0)
                })
        
        # METHOD 4: WORDS fallback (last resort)
        elif hasattr(page, 'words') and page.words:
            page_text = " ".join([word.content for word in page.words])
            content.append({
                "type": "page_text",
                "content": page_text,
                "page": page_num
            })
    
    print(f"✅ Extracted {len(content)} content elements")
    return content

def ingest_file(local_path):
    """✅ Complete pipeline for YOUR resources"""
    container = blob_client.get_container_client("documents")
    blob_name = os.path.basename(local_path)
    
    # 1. Upload to YOUR jagann1storage1/documents/
    print(f"📤 Uploading {blob_name}...")
    with open(local_path, "rb") as f:
        container.upload_blob(blob_name, f, overwrite=True)
    print("✅ Blob upload complete")
    
    # 2. Parse with YOUR rag-ai-servies-workshop
    parsed = analyze_document(local_path)
    
    # 3. Save JSON output
    os.makedirs("./output", exist_ok=True)
    output_path = f"./output/{blob_name.replace('.pdf', '')}.json"
    with open(output_path, "w") as f:
        json.dump(parsed[:500], f, indent=2)  # First 500 chunks for Day 2
    
    # 4. Preview first few items
    print("\n📋 SAMPLE OUTPUT:")
    for item in parsed[:3]:
        print(f"  {item['type']}: {item['content'][:100]}...")
    
    print(f"\n🎉 DAY 2 SUCCESS: {len(parsed)} elements → {output_path}")
    return parsed

if __name__ == "__main__":
    # WORKS WITH ANY PDF IN YOUR docs/ folder
    ingest_file("./docs/nist-sp800-53.pdf")  # or nist-sp800-53.pdf or support.pdf
