import re
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def extract_votes(submission_url):
    """
    Extracts the number of votes from a given submission URL.

    Args:
        submission_url (str): The URL of the submission page.

    Returns:
        int or None: The number of votes if found, else None.
    """
    # Set up Selenium WebDriver in non-headless mode for debugging
    options = Options()
    # Uncomment the next line to run in headless mode after debugging
    # options.headless = True
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    # Anti-detection options
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument("--disable-blink-features=AutomationControlled")
    
    # Initialize WebDriver
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
        print(f"Error initializing WebDriver: {e}")
        return None
    
    votes = None
    
    try:
        # Navigate to the submission URL
        driver.get(submission_url)
        print(f"Navigated to {submission_url}")
        
        # Wait for a few seconds to let the page load fully
        time.sleep(5)
        
        # Take a screenshot for debugging (optional)
        driver.save_screenshot('page_screenshot.png')
        print("Page screenshot saved to 'page_screenshot.png'.")
        
        # Use explicit wait to ensure the votes elements are loaded
        wait = WebDriverWait(driver, 15)  # 15 seconds timeout
        votes_present = wait.until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'div.css-tumkbo'))
        )
        
        # Use JavaScript to get all 'div.css-tumkbo' texts
        votes_texts = driver.execute_script('return Array.from(document.querySelectorAll("div.css-tumkbo")).map(el => el.textContent.trim());')
        print(f"Votes Texts via JavaScript: {votes_texts}")
        
        for idx, votes_text in enumerate(votes_texts, start=1):
            print(f" - Element {idx}: '{votes_text}'")
            # Extract number from text
            match = re.search(r'(\d+)', votes_text)
            if match:
                votes = int(match.group(1))
                print(f"   - Extracted Votes: {votes}")
                break  # Assuming the first match is the desired one
            else:
                print("   - No numerical votes found in this element.")
        
        if votes is None:
            print("No numerical votes found. Saving page source for debugging.")
            with open('page_source.html', 'w', encoding='utf-8') as f:
                f.write(driver.page_source)
            print("Page source saved to 'page_source.html'.")
    
    except Exception as e:
        print(f"Error extracting votes: {e}")
        # Optionally, save the page source for debugging
        with open('page_source_error.html', 'w', encoding='utf-8') as f:
            f.write(driver.page_source)
        print("Page source saved to 'page_source_error.html' for debugging.")
    
    finally:
        driver.quit()
    
    return votes

if __name__ == "__main__":
    # Example submission URL (replace with an actual URL you want to test)
    test_submission_url = 'https://editfest.filmsupply.com/submissions/JneNzh'  # Replace with a valid URL
    
    print(f"Testing vote extraction for: {test_submission_url}\n")
    votes = extract_votes(test_submission_url)
    
    if votes is not None:
        print(f"\nNumber of Votes: {votes}")
    else:
        print("\nFailed to extract votes.")
