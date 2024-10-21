import requests
import time
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import re
import logging

# Configuration
API_URL_TEMPLATE = 'https://editfest.filmsupply.com/api/submissions?page={page}'
SUBMISSION_URL_TEMPLATE = 'https://editfest.filmsupply.com/submissions/{submission_hash}'
MAX_FETCH_THREADS = 5    # Number of concurrent threads for fetching pages
MAX_VOTES_THREADS = 3    # Number of concurrent threads for votes extraction

# Initialize lists
submission_urls = []
submissions_data = []

# Setup Logging
logging.basicConfig(
    filename='scraping.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(levelname)s - %(message)s')
console.setFormatter(formatter)
logging.getLogger().addHandler(console)

# Headers to mimic a browser request
headers = {
    'User-Agent': 'Mozilla/5.0',
}

logging.info("Starting to scrape submission URLs...")

def fetch_page(page_number):
    """
    Fetches a single submission page from the API.

    Args:
        page_number (int): The page number to fetch.

    Returns:
        list: A list of submission dictionaries.
    """
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

def fetch_all_submissions(total_pages):
    """
    Fetches all submissions across multiple pages concurrently.

    Args:
        total_pages (int): The total number of pages to fetch.

    Returns:
        list: A combined list of all submissions.
    """
    all_submissions = []
    with ThreadPoolExecutor(max_workers=MAX_FETCH_THREADS) as executor:
        future_to_page = {executor.submit(fetch_page, page): page for page in range(1, total_pages + 1)}
        for future in as_completed(future_to_page):
            page = future_to_page[future]
            try:
                submissions = future.result()
                all_submissions.extend(submissions)
            except Exception as e:
                logging.error(f"Error processing page {page}: {e}")
    return all_submissions

def extract_votes(submission):
    """
    Extracts the number of votes for a given submission using Selenium.

    Args:
        submission (dict): A dictionary containing submission details.

    Returns:
        dict: The updated submission dictionary with 'votes' key added.
    """
    title = submission.get('title', 'No Title')
    name = submission.get('name', 'No Name')
    url = submission.get('url')
    votes = None

    logging.info(f"Fetching votes for '{title}' by {name}...")

    # Initialize Selenium WebDriver for each thread
    options = Options()
    options.headless = True  # Set to False for debugging
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    # Anti-detection options
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument("--disable-blink-features=AutomationControlled")
    
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
        logging.error(f"Error initializing WebDriver for '{title}': {e}")
        submission['votes'] = votes
        return submission

    try:
        driver.get(url)
        logging.info(f" - Navigated to {url}")

        # Wait for a few seconds to let the page load fully
        time.sleep(5)

        # Use JavaScript to get all 'div.css-tumkbo' texts
        votes_texts = driver.execute_script('return Array.from(document.querySelectorAll("div.css-tumkbo")).map(el => el.textContent.trim());')
        logging.info(f"Votes Texts via JavaScript: {votes_texts}")

        for idx, votes_text in enumerate(votes_texts, start=1):
            logging.info(f" - Element {idx}: '{votes_text}'")
            # Extract number from text
            match = re.search(r'(\d+)', votes_text)
            if match:
                votes = int(match.group(1))
                logging.info(f"   - Extracted Votes: {votes}")
                break  # Assuming the first match is the desired one
            else:
                logging.warning("   - No numerical votes found in this element.")
        
        if votes is None:
            logging.warning(f"No numerical votes found for '{title}'.")
    
    except Exception as e:
        logging.error(f" - Could not fetch votes for '{title}': {e}")
        # Optionally, save the page source for debugging
        try:
            with open('page_source_error.html', 'w', encoding='utf-8') as f:
                f.write(driver.page_source)
            logging.info("   - Page source saved to 'page_source_error.html' for debugging.")
        except Exception as save_error:
            logging.error(f"   - Failed to save page source: {save_error}")
    
    finally:
        try:
            driver.quit()
            logging.info(f" - Closed WebDriver for '{title}'.")
        except Exception as e_quit:
            logging.error(f" - Error closing WebDriver for '{title}': {e_quit}")

    submission['votes'] = votes
    return submission

def extract_all_votes(submissions):
    """
    Extracts votes for all submissions concurrently.

    Args:
        submissions (list): A list of submission dictionaries.

    Returns:
        list: The updated list of submissions with 'votes' added.
    """
    updated_submissions = []
    with ThreadPoolExecutor(max_workers=MAX_VOTES_THREADS) as executor:
        future_to_submission = {executor.submit(extract_votes, submission): submission for submission in submissions}
        for future in as_completed(future_to_submission):
            submission = future_to_submission[future]
            try:
                updated_submission = future.result()
                updated_submissions.append(updated_submission)
            except Exception as e:
                logging.error(f"Exception occurred while fetching votes for '{submission.get('title', 'No Title')}': {e}")
                submission['votes'] = None
                updated_submissions.append(submission)
    return updated_submissions

def main():
    # Step 1: Fetch the first page to determine total pages
    try:
        response = requests.get(API_URL_TEMPLATE.format(page=1), headers=headers)
        if response.status_code != 200:
            logging.error(f"Failed to retrieve page 1: Status code {response.status_code}")
            return
        data = response.json()
        meta = data.get('meta', {})
        last_page = meta.get('last_page', 1)
        logging.info(f"Total pages to fetch: {last_page}")
    except Exception as e:
        logging.error(f"Exception while fetching the first page: {e}")
        return

    # Step 2: Fetch all submissions concurrently
    logging.info("Fetching all submissions...")
    all_submissions = fetch_all_submissions(last_page)
    logging.info(f"Total submissions collected: {len(all_submissions)}")

    # Step 3: Prepare submissions data
    for submission in all_submissions:
        submission_hash = submission.get('hash')
        title = submission.get('title', 'No Title')
        name = submission.get('name', 'No Name')
        category = submission.get('category', 'No Category')
        if submission_hash:
            submission_url = SUBMISSION_URL_TEMPLATE.format(submission_hash=submission_hash)
            submissions_data.append({
                'url': submission_url,
                'title': title,
                'name': name,
                'category': category,
                'hash': submission_hash
            })
            submission_urls.append(submission_url)
            logging.info(f" - Added submission: {title} by {name}")
        else:
            logging.warning(f"Submission without hash found: {submission}")

    logging.info(f"\nTotal submissions prepared for votes extraction: {len(submissions_data)}")

    # Step 4: Extract votes concurrently
    logging.info("\nStarting to fetch votes using threading...")
    updated_submissions = extract_all_votes(submissions_data)
    logging.info("\nAll votes have been fetched.")

    # Step 5: Save submission data to CSV file
    logging.info("\nSaving submission data to 'submissions.csv'...")
    keys = ['url', 'title', 'name', 'category', 'votes']
    try:
        with open('submissions.csv', 'w', newline='', encoding='utf-8') as output_file:
            dict_writer = csv.DictWriter(output_file, keys)
            dict_writer.writeheader()
            dict_writer.writerows(updated_submissions)
        logging.info("Submission data has been saved to 'submissions.csv'.")
    except Exception as e:
        logging.error(f"Error writing to CSV: {e}")

    # Step 6: Save submission URLs to a text file
    logging.info("\nSaving submission URLs to 'submission_urls.txt'...")
    try:
        with open('submission_urls.txt', 'w') as f:
            for url in submission_urls:
                f.write(url + '\n')
        logging.info("Submission URLs have been saved to 'submission_urls.txt'.")
    except Exception as e:
        logging.error(f"Error writing submission URLs to text file: {e}")

    logging.info("\nScraping completed successfully!")

if __name__ == "__main__":
    main()
