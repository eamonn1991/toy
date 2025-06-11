import csv
from datetime import datetime
from typing import Dict
import os
from models import Repository, get_db

def dump_to_csv(output_file: str = None) -> str:
    """Dump all repository data to a CSV file"""
    if output_file is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"github_crawler_data_{timestamp}.csv"
    
    db = next(get_db())
    try:
        repos = db.query(Repository).all()
        
        # Define CSV headers
        fieldnames = ['id', 'name', 'star_count', 'updated_at', 'last_crawled_at']
        
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            for repo in repos:
                writer.writerow({
                    'id': repo.id,
                    'name': repo.name,
                    'star_count': repo.star_count,
                    'updated_at': repo.updated_at.isoformat(),
                    'last_crawled_at': repo.last_crawled_at.isoformat()
                })
        
        print(f"Successfully dumped {len(repos)} repositories to {output_file}")
        return output_file
    finally:
        db.close()

def upload_from_csv(input_file: str) -> Dict[str, int]:
    """Upload repository data from a CSV file"""
    if not os.path.exists(input_file):
        raise FileNotFoundError(f"Input file not found: {input_file}")
    
    db = next(get_db())
    try:
        stats = {"total": 0, "processed": 0, "failed": 0}
        
        # Read and process CSV file
        with open(input_file, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                stats["total"] += 1
                try:
                    # Create repository object
                    repo = Repository(
                        id=row["id"],
                        name=row["name"],
                        star_count=int(row["star_count"]),
                        updated_at=datetime.fromisoformat(row["updated_at"]),
                        last_crawled_at=datetime.fromisoformat(row["last_crawled_at"])
                    )
                    
                    # Merge will handle both insert and update
                    db.merge(repo)
                    stats["processed"] += 1
                    
                except Exception as e:
                    print(f"Error processing repository {row.get('id', 'unknown')}: {str(e)}")
                    stats["failed"] += 1
        
        # Commit all changes
        db.commit()
        
        print("\nUpload Summary:")
        print("-" * 50)
        print(f"Total repositories in file: {stats['total']}")
        print(f"Successfully processed: {stats['processed']}")
        print(f"Failed to process: {stats['failed']}")
        print("-" * 50)
        
        return stats
    
    finally:
        db.close()

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Database CSV dump and upload tool for github crawler')
    parser.add_argument('action', choices=['dump', 'upload'], help='Action to perform')
    parser.add_argument('--file', help='Input/output file path')
    
    args = parser.parse_args()
    
    if args.action == 'dump':
        dump_to_csv(args.file)
    else:  # upload
        if not args.file:
            print("Error: --file is required for upload action")
        else:
            upload_from_csv(args.file) 