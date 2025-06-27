import requests
import time
from datetime import datetime
import pytz
import argparse
import calendar
import logging
from typing import Dict, Any, List
from queue import Queue
from concurrent.futures import ThreadPoolExecutor
from threading import Lock

from src.models import Repository, get_db
from src.config import settings

# Disable logging from other libraries
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('sqlalchemy').setLevel(logging.WARNING)

# Configure root logger to only show messages without timestamp
logging.basicConfig(
    level=logging.INFO,
    format='%(message)s',  # Only show the message without timestamp or level
    force=True  # Override any existing configuration
)

class TokenManager:
    def __init__(self, token):
        """Initialize with a single token or a list of tokens"""
        self.tokens = [token] if isinstance(token, str) else token
        self.current_index = 0
        self.lock = Lock()
        
    def get_token(self):
        """Get the next token in a thread-safe manner"""
        with self.lock:
            token = self.tokens[self.current_index]
            self.current_index = (self.current_index + 1) % len(self.tokens)
            return token

class ThreadSafeCounter:
    def __init__(self, initial=0):
        self.value = initial
        self.lock = Lock()
        
    def increment(self, amount=1):
        with self.lock:
            self.value += amount
            return self.value
            
    def get(self):
        with self.lock:
            return self.value
            
    def set(self, value):
        with self.lock:
            self.value = value

class ThreadSafeDateRangeQueue:
    """
    Thread-safe queue for distributing date ranges to crawler threads.
    Prevents multiple threads from working on the same time period.
    """
    def __init__(self, start_year, start_month):
        self.current_year = start_year
        self.current_month = start_month
        self.lock = Lock()
        self.assigned_ranges = set()  # Track assigned ranges for debugging
        
    def get_next_date_range(self):
        """Get the next available date range in a thread-safe manner"""
        with self.lock:
            year = self.current_year
            month = self.current_month
            
            # Create range identifier for tracking
            range_id = f"{year}-{month:02d}"
            self.assigned_ranges.add(range_id)
            
            # Move to next date range for future requests
            if month == 1:
                self.current_year = year - 1
                self.current_month = 12
            else:
                self.current_month = month - 1
                
            return year, month
    
    def get_assigned_count(self):
        """Get number of assigned date ranges (for debugging)"""
        with self.lock:
            return len(self.assigned_ranges)

# Constants from Config
GITHUB_API_URL = settings.github_graphql_url
BATCH_SIZE = settings.batch_size

# Initialize token manager with the GitHub token
token_manager = TokenManager(settings.github_token)

def check_total_repos(shared_counters, target_total):
    """Helper function to check if we've reached the target total"""
    return shared_counters['total'].get() >= target_total

def send_crawl_request(query, variables=None):
    """
    Creates a GraphQL request with proper headers and authentication
    """
    token = token_manager.get_token()
    
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json',
    }
    
    json_data = {
        'query': query,
        'variables': variables or {}
    }
    
    
    return requests.post(GITHUB_API_URL, json=json_data, headers=headers)

def build_search_query(
    min_stars=0,
    language=None,
    created_after=None,  # Format: YYYY-MM-DD
    created_before=None, # Format: YYYY-MM-DD
    keywords=None,      # Keywords to search in code/description
    sort_by=None
):
    """
    Builds a GitHub search query with various filters
    
    Parameters:
    - min_stars: Minimum number of stars
    - language: Programming language (e.g., "python", "javascript")
    - created_after: Created after date (YYYY-MM-DD)
    - created_before: Created before date (YYYY-MM-DD)
    - keywords: List of keywords to search in code/description
    - sort_by: How to sort results ("stars", "updated", "created", "forks")
    """
    # Start with base query
    query_parts = []
    
    # Add keywords if provided
    if keywords:
        query_parts.extend(keywords)
    
    # Add language filter
    if language:
        query_parts.append(f"language:{language}")
    
    # Add stars filter
    if min_stars:
        query_parts.append(f"stars:>={min_stars}")
    
    # Add creation date filter
    if created_after and created_before:
        query_parts.append(f"created:{created_after}..{created_before}")
    elif created_after:
        query_parts.append(f"created:>{created_after}")
    elif created_before:
        query_parts.append(f"created:<{created_before}")
    
    # Add sort
    sort_mapping = {
        "stars": "stars",
        "updated": "updated",
        "created": "created",
        "forks": "forks"
    }
    if sort_by and sort_by.lower() != 'none':
        sort_term = sort_mapping.get(sort_by, "stars")
        query_parts.append(f"sort:{sort_term}")
    
    return " ".join(query_parts)

def fetch_repositories(
    batch_size=5,
    min_stars=1,
    language=None,
    created_after=None,
    created_before=None,
    keywords=None,
    sort_by=None,
    after_cursor=None  # For pagination
):
    """
    Fetches repositories using GitHub's GraphQL API with enhanced search options
    """
    search_query = build_search_query(
        min_stars=min_stars,
        language=language,
        created_after=created_after,
        created_before=created_before,
        keywords=keywords,
        sort_by=sort_by
    )
    
    query = """
    query($batch_size: Int!, $searchQuery: String!, $afterCursor: String) {
        search(query: $searchQuery, type: REPOSITORY, first: $batch_size, after: $afterCursor) {
            repositoryCount
            pageInfo {
                hasNextPage
                endCursor
            }
            nodes {
                ... on Repository {
                    id
                    stargazerCount
                }
            }
        }
    }
    """
    
    variables = {
        'batch_size': batch_size,
        'searchQuery': search_query,
        'afterCursor': after_cursor
    }
    
    response = send_crawl_request(query, variables)
    
    if response.status_code == 200:
        data = response.json()
        if 'errors' in data:
            print("GraphQL Errors:", data['errors'])
            return
            
        search_data = data['data']['search']
        print(f"\nSearch Query: {search_query}")
        print(f"Total number of found repo: {search_data['repositoryCount']}")
        
        # Pagination information
        page_info = search_data['pageInfo']
        has_next_page = page_info['hasNextPage']
        end_cursor = page_info['endCursor']
        
        print(f"Showing {batch_size} repositories:")
        if has_next_page:
            print(f"More results available. Use cursor: {end_cursor}")
        return {
            'repositories': search_data['nodes'],
            'has_next_page': has_next_page,
            'end_cursor': end_cursor
        }
    else:
        print(f"Error: {response.status_code}")
        print(response.text)
        return None

def db_write_batch(repo_data_list: List[Dict[Any, Any]], max_retries: int = 1) -> bool:
    """
    Write a batch of repository data to the database with retry mechanism.
    Only updates repositories if their star count has changed.
    
    Expected format for each dictionary:
    {
        "id": "ID",
        "stargazerCount": 100
    }
    """
    if not repo_data_list:
        return True

    for retry_count in range(max_retries):
        db = next(get_db())
        try:
            # Get all existing repositories with their current star counts
            existing_repos = {
                r.id: r for r in db.query(Repository).filter(
                    Repository.id.in_([r["id"] for r in repo_data_list])
                ).all()
            }
            
            # Prepare updates and inserts
            to_update = []
            to_insert = []
            
            for repo_data in repo_data_list:
                repo = Repository(
                    id=repo_data["id"],
                    star_count=repo_data["stargazerCount"]
                )
                
                if repo.id in existing_repos:
                    # Only update if star count has changed
                    existing_repo = existing_repos[repo.id]
                    if existing_repo.star_count != repo.star_count:
                        to_update.append(repo)
                else:
                    to_insert.append(repo)
            
            # Bulk insert new repositories
            if to_insert:
                db.bulk_save_objects(to_insert)
            
            # Bulk update repositories with changed star counts
            if to_update:
                for repo in to_update:
                    db.merge(repo)
            
            db.commit()
            return True
            
        except Exception as e:
            print(f"Error in db_write_batch: {str(e)}")
            db.rollback()
            if retry_count == max_retries - 1:
                return False
        finally:
            db.close()
    
    return False

def wait_for_rate_limit_reset(reset_at):
    """
    Waits until the rate limit resets
    """
    # Convert reset_at string to datetime
    reset_time = datetime.strptime(reset_at, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=pytz.UTC)
    now = datetime.now(pytz.UTC)
    
    # Calculate wait time
    wait_seconds = (reset_time - now).total_seconds()
    if wait_seconds > 0:
        print(f"Rate limit reached. Waiting for {wait_seconds/60:.2f} minutes until {reset_at}")
        time.sleep(wait_seconds + 1)  # Add 1 second buffer

def get_month_date_range(year, month):
    """
    Returns the start and end date for a given year and month.
    Uses calendar to get the correct number of days in the month.
    """
    _, last_day = calendar.monthrange(year, month)
    start_date = f"{year}-{month:02d}-01"
    end_date = f"{year}-{month:02d}-{last_day:02d}"
    return start_date, end_date

def get_next_date_range(year, month):
    """Helper function to get the next date range"""
    if month == 1:
        return year - 1, 12
    else:
        return year, month - 1

def crawl_worker(args, date_range_queue, shared_counters, thread_key, max_retries=None):
    """Worker function for threaded crawling with centralized date range management"""
    try:
        if max_retries is None:
            max_retries = settings.max_retries

        target_total = args.total_num_repo if args.total_num_repo else settings.total_num_repo
        
        while not check_total_repos(shared_counters, target_total):
            # Get next date range from centralized queue (thread-safe)
            year, month = date_range_queue.get_next_date_range()
            
            # Set date range once for this iteration (outside pagination loop)
            created_after, created_before = get_month_date_range(year, month)
            
            count_current_partition = 0
            after_cursor = None
            flag_no_more_page = False
            
            with shared_counters['print_lock']:
                print(f"Thread {thread_key} assigned new date range: {year}-{month:02d} ({created_after} to {created_before})")
                print(f"Total date ranges assigned: {date_range_queue.get_assigned_count()}")
            
            while not flag_no_more_page and count_current_partition < args.partition_threshold:
                if check_total_repos(shared_counters, target_total):
                    return
                    
                # Date range is already set above, now just paginate through it
                
                retry_count = 0
                while retry_count < max_retries:
                    try:
                        with shared_counters['print_lock']:
                            print(f"Thread {thread_key} ({year}-{month:02d}) fetching from {created_after} to {created_before}")
                            print(f"Progress: {shared_counters['total'].get()}/{target_total} repositories")
                        
                        crawl_start_time = time.time()
                        
                        crawl_result = fetch_repositories(
                            batch_size=args.batch_size,
                            min_stars=args.min_stars,
                            language=args.language,
                            keywords=[args.keywords] if args.keywords else None,
                            sort_by=args.sort_by,
                            created_after=created_after,
                            created_before=created_before,
                            after_cursor=after_cursor
                        )
                        
                        crawl_time = time.time() - crawl_start_time
                        shared_counters['crawl_time'].increment(crawl_time)
                        shared_counters['crawl_ops'].increment()
                        
                        if not crawl_result:
                            raise Exception("Failed to fetch repositories")
                        
                        list_repo_data = crawl_result['repositories']
                        
                        write_start_time = time.time()
                        
                        if not db_write_batch(list_repo_data, max_retries=max_retries):
                            with shared_counters['print_lock']:
                                print("Failed to write batch to database, skipping this batch...")
                            continue
                        
                        write_time = time.time() - write_start_time
                        shared_counters['write_time'].increment(write_time)
                        shared_counters['write_ops'].increment()
                        
                        num_fetched = len(list_repo_data)
                        
                        # Update both counters
                        shared_counters['thread_counts'][thread_key].increment(num_fetched)
                        shared_counters['total'].increment(num_fetched)
                        
                        with shared_counters['print_lock']:
                            print(f"Thread {thread_key} ({year}-{month:02d}) fetched and saved {num_fetched} repositories")
                            print(f"Thread total: {shared_counters['thread_counts'][thread_key].get()}")
                            print(f"Crawl time: {crawl_time:.2f}s, Write time: {write_time:.2f}s")
                            
                            if shared_counters['crawl_ops'].get() > 0 and shared_counters['write_ops'].get() > 0:
                                avg_crawl = shared_counters['crawl_time'].get() / shared_counters['crawl_ops'].get()
                                avg_write = shared_counters['write_time'].get() / shared_counters['write_ops'].get()
                                print(f"Average times - Crawl: {avg_crawl:.2f}s, Write: {avg_write:.2f}s")
                        
                        count_current_partition += num_fetched
                        
                        if crawl_result['has_next_page']:
                            after_cursor = crawl_result['end_cursor']
                        else:
                            with shared_counters['print_lock']:
                                print(f"No more repositories for {year}-{month:02d}")
                            flag_no_more_page = True
                            break
                            
                    except Exception as e:
                        error_msg = str(e)
                        with shared_counters['print_lock']:
                            print(f"API Error occurred in thread {thread_key} ({year}-{month:02d}): {error_msg}")
                        
                        if "Rate limit nearly exceeded" in error_msg:
                            reset_at = error_msg.split("Resets at ")[-1]
                            wait_for_rate_limit_reset(reset_at)
                            continue
                        
                        retry_count += 1
                        if retry_count >= max_retries:
                            with shared_counters['print_lock']:
                                print(f"Max retries reached for thread {thread_key} ({year}-{month:02d}). Moving to next date range...")
                            flag_no_more_page = True
                            break
                        with shared_counters['print_lock']:
                            print(f"Retrying in 2 seconds... (Attempt {retry_count + 1}/{max_retries})")
                        time.sleep(2)
            
            # This date range is complete, thread will get next range from queue in next iteration
            with shared_counters['print_lock']:
                print(f"Thread {thread_key} completed date range {year}-{month:02d}")
                    
    except Exception as e:
        with shared_counters['print_lock']:
            print(f"Error in crawl_worker for thread {thread_key}: {e}")

def crawl_pipeline(args, max_retries=None):
    try:
        if max_retries is None:
            max_retries = settings.max_retries
            
        # Set number of threads for parallel processing
        num_threads = args.num_threads
        target_total = args.total_num_repo if args.total_num_repo else settings.total_num_repo
        print(f"Starting multi-threaded crawl with {num_threads} threads (using GitHub token)")
        print(f"Target total repositories: {target_total}")

        # Create centralized date range queue to prevent thread collisions
        date_range_queue = ThreadSafeDateRangeQueue(args.start_year, args.start_month)

        # Initialize shared counters
        shared_counters = {
            'total': ThreadSafeCounter(0),
            'crawl_time': ThreadSafeCounter(0),
            'write_time': ThreadSafeCounter(0),
            'crawl_ops': ThreadSafeCounter(0),
            'write_ops': ThreadSafeCounter(0),
            'print_lock': Lock(),
            'thread_counts': {}  # Track per-thread counts
        }

        print("*"*80 + "\nGITHUB REPO Crawling...\n" + "*"*80)
        print(f"Starting with date range: {args.start_year}-{args.start_month:02d}")
        print(f"Using centralized date range queue to prevent thread collisions")
        print(f"Target total repositories: {target_total}\n")
        
        # Record start time for wall clock timing
        total_start_time = time.time()
        
        # Create thread pool and start crawling
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = []
            for i in range(num_threads):
                thread_key = f"thread_{i}"
                shared_counters['thread_counts'][thread_key] = ThreadSafeCounter(0)
                futures.append(
                    executor.submit(
                        crawl_worker,
                        args,
                        date_range_queue,
                        shared_counters,
                        thread_key,
                        max_retries
                    )
                )
            
            # Wait for all threads to complete
            for future in futures:
                future.result()
        
        # Calculate total wall clock time
        total_wall_time = time.time() - total_start_time
        
        print("\n" + "*"*80 + "\nFinal Performance Statistics\n" + "*"*80)
        
        # Verify total count
        total_from_threads = sum(counter.get() for counter in shared_counters['thread_counts'].values())
        total_reported = shared_counters['total'].get()
        
        print("\nRepository Count Verification:")
        print(f"Total from thread counters: {total_from_threads}")
        print(f"Total from shared counter: {total_reported}")
        
        if total_from_threads != total_reported:
            print(f"WARNING: Count mismatch detected! Difference: {abs(total_from_threads - total_reported)}")
            print("\nPer-thread counts:")
            for thread_key, counter in shared_counters['thread_counts'].items():
                print(f"  {thread_key}: {counter.get()}")
        
        print(f"\nTotal repositories fetched: {total_from_threads}")
        
        if shared_counters['crawl_ops'].get() > 0 and shared_counters['write_ops'].get() > 0:
            total_crawl_time = shared_counters['crawl_time'].get()
            total_write_time = shared_counters['write_time'].get()
            total_crawl_ops = shared_counters['crawl_ops'].get()
            total_write_ops = shared_counters['write_ops'].get()
            
            print(f"\nOperation Statistics:")
            print(f"Total operations - Crawl: {total_crawl_ops}, Write: {total_write_ops}")
            print(f"Average time per operation:")
            print(f"  - Crawl: {(total_crawl_time/total_crawl_ops):.2f}s")
            print(f"  - Write: {(total_write_time/total_write_ops):.2f}s")
            print(f"\nParallel execution statistics ({num_threads} threads):")
            print(f"  - Total wall clock time: {total_wall_time:.2f}s")
            print(f"  - Cumulative crawl time: {total_crawl_time:.2f}s")
            print(f"  - Cumulative write time: {total_write_time:.2f}s")
            print(f"  - Cumulative processing time: {(total_crawl_time + total_write_time):.2f}s")
            print(f"  - Effective parallel speedup: {((total_crawl_time + total_write_time)/total_wall_time):.2f}x")
            print(f"  - Average processing rate: {(total_from_threads/total_wall_time):.2f} repos/second")
            
    except Exception as e:
        print(f"Error in crawl_pipeline: {e}")

def main():
    parser = argparse.ArgumentParser(description='GitHub Repository Crawler')
    parser.add_argument('--mode', choices=['pipeline', 'single'], default='pipeline',
                      help='Run mode: pipeline (full crawl) or single (one fetch)')
    parser.add_argument('--min-stars', type=int, default=settings.default_min_stars,
                      help='Minimum stars')
    parser.add_argument('--language', type=str, help='Programming language')
    parser.add_argument('--batch-size', type=int, default=settings.batch_size,
                      help='Batch size (max 100)')
    parser.add_argument('--keywords', type=str, help='Search keywords')
    parser.add_argument('--sort-by', choices=['stars', 'updated', 'created', 'forks', 'None'],
                      help='Sort results by')
    parser.add_argument('--created-after', type=str, help='Created after date (YYYY-MM-DD)')
    parser.add_argument('--created-before', type=str, help='Created before date (YYYY-MM-DD)')
    parser.add_argument('--start-year', type=int, default=settings.default_start_year,
                      help='Starting year for pipeline crawl')
    parser.add_argument('--start-month', type=int, default=settings.default_start_month,
                      help='Starting month for pipeline crawl')
    parser.add_argument('--partition-threshold', type=int, default=settings.default_partition_threshold,
                      help='Number of repos to fetch before changing date range (max 1000)')
    parser.add_argument('--total-num-repo', type=int, help='Override total number of repositories to fetch')
    parser.add_argument('--num-threads', type=int, default=settings.default_number_threads,
                      help='Number of threads to use for crawling (default: 4)')
    parser.add_argument('--repeat-count', type=int, help='Number of times to repeat the single fetch operation')

    args = parser.parse_args()
    
    if args.mode == 'single':
        print("\nRunning multi-threaded single fetch_repositories() call...")
        
        # Add repeat count argument
        repeat_count = args.repeat_count if hasattr(args, 'repeat_count') else 1
        num_threads = args.num_threads
        
        # Initialize shared counters
        shared_counters = {
            'total_fetch_time': ThreadSafeCounter(0),
            'total_repos': ThreadSafeCounter(0),
            'print_lock': Lock()
        }
        
        # Record total start time
        total_start_time = time.time()
        
        def single_worker(thread_id):
            thread_fetch_time = 0
            thread_repos = 0
            
            for i in range(repeat_count):
                if i > 0:  # Don't sleep before the first iteration
                    time.sleep(0.02)  # Sleep for 1 seconds between repetitions
                
                with shared_counters['print_lock']:
                    print(f"\nThread {thread_id} - Iteration {i+1}/{repeat_count}")
                
                # Start timing the fetch operation
                fetch_start_time = time.time()
                result = fetch_repositories(
                    batch_size=args.batch_size,
                    min_stars=args.min_stars,
                    language=args.language,
                    keywords=[args.keywords] if args.keywords else None,
                    sort_by=args.sort_by,
                    created_after=args.created_after,
                    created_before=args.created_before
                )
                fetch_time = time.time() - fetch_start_time
                thread_fetch_time += fetch_time
                
                if result:
                    num_repos = len(result['repositories'])
                    thread_repos += num_repos
                    
                    with shared_counters['print_lock']:
                        print(f"\nThread {thread_id} - Fetch completed successfully!")
                        print(f"Fetched {num_repos} repositories")
                        print(f"Has next page: {result['has_next_page']}")
                        if result['has_next_page']:
                            print(f"Next cursor: {result['end_cursor']}")
                        
                        print("\nPerformance Statistics:")
                        print(f"Fetch time: {fetch_time:.2f}s")
                        print(f"Processing rate: {(num_repos/fetch_time):.2f} repos/second")
                    
                    # Write to database if we have results
                    if result['repositories']:
                        success = db_write_batch(result['repositories'])
                        if not success:
                            with shared_counters['print_lock']:
                                print(f"Thread {thread_id} - Warning: Failed to write repositories to database")
            
            # Update shared counters
            shared_counters['total_fetch_time'].increment(thread_fetch_time)
            shared_counters['total_repos'].increment(thread_repos)
            
            with shared_counters['print_lock']:
                print(f"\nThread {thread_id} Summary:")
                print(f"Total fetch time: {thread_fetch_time:.2f}s")
                print(f"Total repositories: {thread_repos}")
                print(f"Average fetch time: {(thread_fetch_time/repeat_count):.2f}s")
                print(f"Average processing rate: {(thread_repos/thread_fetch_time):.2f} repos/second")
        
        # Create thread pool and start crawling
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = []
            for i in range(num_threads):
                futures.append(executor.submit(single_worker, i))
                time.sleep(0.005)  # Sleep for 0.1 seconds between thread starts
            
            # Wait for all threads to complete
            for future in futures:
                future.result()
        
        # Calculate total wall clock time
        total_wall_time = time.time() - total_start_time
        
        # Print final summary
        total_fetch_time = shared_counters['total_fetch_time'].get()
        total_repos = shared_counters['total_repos'].get()
        total_iterations = repeat_count * num_threads
        
        print("\n" + "="*50)
        print("Final Performance Statistics:")
        print(f"Total threads: {num_threads}")
        print(f"Total iterations: {total_iterations}")
        print(f"Total repositories fetched: {total_repos}")
        print(f"Total wall clock time: {total_wall_time:.2f}s")
        print(f"Total fetch time (excluding sleeps): {total_fetch_time:.2f}s")
        print(f"Average fetch time per iteration: {(total_fetch_time/total_iterations):.2f}s")
        print(f"Raw processing rate: {(total_repos/total_fetch_time):.2f} repos/second")
        print(f"Effective processing rate: {(total_repos/total_wall_time):.2f} repos/second")
        
        # Calculate estimated time for 100k repos
        effective_rate = total_repos/total_wall_time
        time_for_100k = 100000/effective_rate
        hours = int(time_for_100k // 3600)
        minutes = int((time_for_100k % 3600) // 60)
        seconds = int(time_for_100k % 60)
        print(f"Estimated time for 100k repos: {hours}h {minutes}m {seconds}s")
        print("="*50)
    elif args.mode == 'pipeline':
        crawl_pipeline(args=args)

if __name__ == "__main__":
    main() 