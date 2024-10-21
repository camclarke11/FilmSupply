import requests
import time
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
import logging
from logging.handlers import RotatingFileHandler  # Import RotatingFileHandler directly
from tqdm import tqdm  # Importing tqdm for progress bars
import re  # Import the 're' module for regular expressions

def setup_logging():
    """
    Sets up logging to file and console with rotation to prevent large log files.
    """
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # Rotating file handler
    handler = RotatingFileHandler('title_votes_scraping.log', maxBytes=5*1024*1024, backupCount=2)  # 5MB per file, 2 backups
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    # Console handler
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(formatter)
    logger.addHandler(console)

def fetch_page(page_number, headers):
    """
    Fetches a single submission page from the API.

    Args:
        page_number (int): The page number to fetch.
        headers (dict): Headers to include in the request.

    Returns:
        list: A list of submission dictionaries.
    """
    API_URL_TEMPLATE = 'https://editfest.filmsupply.com/api/submissions?page={page}'
    api_url = API_URL_TEMPLATE.format(page=page_number)
    try:
        response = requests.get(api_url, headers=headers)
        if response.status_code != 200:
            logging.error(f"Failed to retrieve page {page_number}: Status code {response.status_code}")
            return []
        data = response.json()
        submissions = data.get('data', [])
        logging.info(f"Page {page_number}: Retrieved {len(submissions)} submissions.")
        return submissions
    except Exception as e:
        logging.error(f"Exception while fetching page {page_number}: {e}")
        return []

def fetch_all_submissions(total_pages, headers, max_threads=5):
    """
    Fetches all submissions across multiple pages concurrently.

    Args:
        total_pages (int): The total number of pages to fetch.
        headers (dict): Headers to include in the requests.
        max_threads (int): Maximum number of concurrent threads.

    Returns:
        list: A combined list of all submissions.
    """
    all_submissions = []
    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        # Submit all page fetch tasks
        futures = {executor.submit(fetch_page, page, headers): page for page in range(1, total_pages + 1)}
        
        # Initialize tqdm progress bar for fetching submissions
        for future in tqdm(as_completed(futures), total=len(futures), desc="Fetching Submissions", unit="page"):
            page = futures[future]
            try:
                submissions = future.result()
                all_submissions.extend(submissions)
            except Exception as e:
                logging.error(f"Error processing page {page}: {e}")
    return all_submissions

def extract_submission_details(submission, options):
    """
    Extracts the title and votes from a given submission using Selenium.

    Args:
        submission (dict): A dictionary containing submission details.
        options (Options): Selenium Chrome options.

    Returns:
        dict: The updated submission dictionary with 'title' and 'votes' keys added.
    """
    title = None
    votes = None
    submission_hash = submission.get('hash')
    submission_url = f"https://editfest.filmsupply.com/submissions/{submission_hash}"

    logging.info(f"Processing submission: {submission_url}")

    try:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        # Redefine navigator.webdriver to undefined to avoid detection
        driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
            'source': '''
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                })
            '''
        })
    except Exception as e:
        logging.error(f"Error initializing WebDriver for {submission_url}: {e}")
        submission['title'] = None
        submission['votes'] = None
        return submission

    try:
        driver.get(submission_url)
        logging.info(f" - Navigated to {submission_url}")
        
        # Wait for the page to load
        time.sleep(5)  # Adjust based on page load speed or replace with explicit waits

        # Extract Title
        try:
            title_element = driver.find_element(By.CSS_SELECTOR, 'div.css-1w984ju')
            title_text = title_element.text.strip()
            logging.info(f" - Title Text Found: '{title_text}'")
            if title_text:
                title = title_text
                logging.info(f"   - Extracted Title: {title}")
            else:
                logging.warning(f"   - Title text is empty for {submission_url}")
        except Exception as e_title:
            logging.error(f"   - Error extracting title for {submission_url}: {e_title}")
            title = None

        # Extract Votes
        try:
            votes_elements = driver.find_elements(By.CSS_SELECTOR, 'div.css-tumkbo')
            votes_texts = [elem.text.strip() for elem in votes_elements]
            logging.info(f" - Votes Texts Found: {votes_texts}")
            for idx, votes_text in enumerate(votes_texts, start=1):
                logging.info(f"   - Element {idx}: '{votes_text}'")
                # Extract number from text using regex
                match = re.search(r'(\d+)', votes_text)
                if match:
                    votes = int(match.group(1))
                    logging.info(f"     - Extracted Votes: {votes}")
                    break  # Assuming the first numerical value is the desired vote count
                else:
                    logging.warning("     - No numerical votes found in this element.")
            if votes is None:
                logging.warning(f"No numerical votes found for '{submission_url}'.")
        except Exception as e_votes:
            logging.error(f"   - Error extracting votes for {submission_url}: {e_votes}")
            votes = None

    except Exception as e:
        logging.error(f" - Error processing submission {submission_url}: {e}")
        # Optionally, save the page source for debugging
        try:
            with open(f'page_source_error_{submission_hash}.html', 'w', encoding='utf-8') as f:
                f.write(driver.page_source)
            logging.info(f"   - Page source saved to 'page_source_error_{submission_hash}.html' for debugging.")
        except Exception as save_error:
            logging.error(f"   - Failed to save page source for {submission_url}: {save_error}")

    finally:
        try:
            driver.quit()
            logging.info(f" - Closed WebDriver for {submission_url}.")
        except Exception as e_quit:
            logging.error(f" - Error closing WebDriver for {submission_url}: {e_quit}")

    submission['title'] = title
    submission['votes'] = votes
    submission['url'] = submission_url  # Ensure the URL is included
    return submission

def extract_all_submission_details(submissions, max_threads=3):
    """
    Extracts titles and votes for all submissions concurrently.

    Args:
        submissions (list): A list of submission dictionaries.
        max_threads (int): Maximum number of concurrent threads.

    Returns:
        list: The updated list of submissions with 'title' and 'votes' added.
    """
    updated_submissions = []
    
    # Set up Selenium Chrome options once
    options = Options()
    options.headless = True  # Set to False for debugging
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    # Anti-detection options
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument("--disable-blink-features=AutomationControlled")
    
    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        # Submit all title and votes extraction tasks
        futures = {executor.submit(extract_submission_details, submission, options): submission for submission in submissions}
        
        # Initialize tqdm progress bar for title and votes extraction
        for future in tqdm(as_completed(futures), total=len(futures), desc="Extracting Titles & Votes", unit="submission"):
            submission = futures[future]
            try:
                updated_submission = future.result()
                updated_submissions.append(updated_submission)
            except Exception as e:
                logging.error(f"Exception occurred while extracting details for submission {submission.get('hash', 'No Hash')}: {e}")
                submission['title'] = None
                submission['votes'] = None
                updated_submissions.append(submission)
    return updated_submissions

def main():
    setup_logging()
    
    # Headers with randomized User-Agent can be implemented here if needed
    headers = {
        'User-Agent': 'Mozilla/5.0',
    }
    
    # Step 1: Fetch the first page to determine total pages
    API_URL_TEMPLATE = 'https://editfest.filmsupply.com/api/submissions?page={page}'
    first_page_url = API_URL_TEMPLATE.format(page=1)
    try:
        response = requests.get(first_page_url, headers=headers)
        if response.status_code != 200:
            logging.error(f"Failed to retrieve the first page: Status code {response.status_code}")
            return
        data = response.json()
        meta = data.get('meta', {})
        last_page = meta.get('last_page', 1)
        logging.info(f"Total pages to fetch: {last_page}")
    except Exception as e:
        logging.error(f"Exception while fetching the first page: {e}")
        return

    # Step 2: Fetch all submissions concurrently with progress bar
    logging.info("Fetching all submissions...")
    all_submissions = fetch_all_submissions(last_page, headers, max_threads=5)
    logging.info(f"Total submissions collected: {len(all_submissions)}")

    # Step 3: Filter submissions by category "Title Sequence" and prepare submissions data
    submissions_data = []
    for submission in all_submissions:
        submission_hash = submission.get('hash')
        category = submission.get('category', '').strip()
        if submission_hash and category.lower() == "title sequence":
            submissions_data.append(submission)
            logging.info(f" - Added submission: {submission.get('title', 'No Title')} by {submission.get('name', 'No Name')} [Category: {category}]")
        else:
            if not submission_hash:
                logging.warning(f"Submission without hash found: {submission}")
            elif category.lower() != "title sequence":
                logging.info(f" - Skipping submission: {submission.get('title', 'No Title')} by {submission.get('name', 'No Name')} [Category: {category}]")

    logging.info(f"\nTotal submissions prepared for title and votes extraction: {len(submissions_data)}")

    # Step 4: Extract titles and votes concurrently with progress bar
    logging.info("\nStarting to extract titles and votes using threading...")
    updated_submissions = extract_all_submission_details(submissions_data, max_threads=3)
    logging.info("\nAll titles and votes have been extracted.")

    # Step 5: Save submission data to CSV file
    logging.info("\nSaving submission data to 'titles_votes.csv'...")
    keys = ['url', 'title', 'votes', 'name', 'category', 'hash']
    try:
        with open('titles_votes.csv', 'w', newline='', encoding='utf-8') as output_file:
            dict_writer = csv.DictWriter(output_file, fieldnames=keys)
            dict_writer.writeheader()
            for submission in updated_submissions:
                # Create a new dict with only the desired keys
                row = {key: submission.get(key, '') for key in keys}
                dict_writer.writerow(row)
        logging.info("Submission data has been saved to 'titles_votes.csv'.")
    except Exception as e:
        logging.error(f"Error writing to CSV: {e}")

    # Step 6: Save submission URLs to a text file
    logging.info("\nSaving submission URLs to 'submission_urls.txt'...")
    try:
        with open('submission_urls.txt', 'w') as f:
            for submission in updated_submissions:
                f.write(f"{submission.get('url', 'No URL')}\n")
        logging.info("Submission URLs have been saved to 'submission_urls.txt'.")
    except Exception as e:
        logging.error(f"Error writing submission URLs to text file: {e}")

    logging.info("\nScraping completed successfully!")

if __name__ == "__main__":
    main()
