from selenium import webdriver
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.by import By
from selenium.webdriver.edge.service import Service
from selenium.webdriver.edge.options import Options
import os
import time
import glob

def wait_for_download(download_dir, timeout=30):
    """Wait for download to complete"""
    start_time = time.time()
    while time.time() - start_time < timeout:
        # Check for .crdownload files (Chrome/Edge temp files)
        if not glob.glob(os.path.join(download_dir, "*.crdownload")):
            time.sleep(1)  # Give it a moment to finish
            return True
        time.sleep(1)
    return False

# Set up Edge options for automatic downloads
edge_options = Options()
download_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "downloads")
os.makedirs(download_dir, exist_ok=True)

print(f"Download directory: {download_dir}")

# Configure Edge to download files automatically
prefs = {
    "download.default_directory": download_dir,
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True
}
edge_options.add_experimental_option("prefs", prefs)

# Try to create Edge driver without webdriver-manager first
try:
    # This will use the built-in EdgeDriver that comes with Edge browser
    driver = webdriver.Edge(options=edge_options)
    print("Using built-in EdgeDriver")
except Exception as e:
    print(f"Built-in EdgeDriver failed: {e}")
    # Fallback: try to find EdgeDriver manually
    try:
        # Common EdgeDriver locations
        edge_driver_paths = [
            r"C:\Program Files (x86)\Microsoft\Edge\Application\msedgedriver.exe",
            r"C:\Program Files\Microsoft\Edge\Application\msedgedriver.exe",
            r"msedgedriver.exe"  # If in PATH
        ]
        
        driver_path = None
        for path in edge_driver_paths:
            if os.path.exists(path):
                driver_path = path
                break
        
        if driver_path:
            driver = webdriver.Edge(service=Service(driver_path), options=edge_options)
            print(f"Using EdgeDriver from: {driver_path}")
        else:
            print("EdgeDriver not found. Please install Edge browser or download EdgeDriver manually.")
            exit(1)
    except Exception as e2:
        print(f"Manual EdgeDriver setup failed: {e2}")
        exit(1)

try:
    # Open the website
    url = 'http://ssrn.aviyaan.com/traffic_controller'
    driver.get(url)

    # Find the <select> element
    location_select = Select(driver.find_element(By.CSS_SELECTOR, 'select[name="location"]'))

    # Iterate through each option starting from the second one (index 1)
    for index in range(1, len(location_select.options)):
        # Re-find the <select> element inside the loop
        location_select = Select(driver.find_element(By.CSS_SELECTOR, 'select[name="location"]'))

        # Select the option
        selected_option = location_select.options[index].text
        location_select.select_by_index(index)

        # Find the form and submit it
        form = driver.find_element(By.CSS_SELECTOR, 'form')
        form.submit()

        # Wait for page to load
        time.sleep(2)

        try:
            # Find the table with class 'link-table'
            table = driver.find_element(By.CSS_SELECTOR, 'table.link-table.column.span-24')

            # Iterate through each row (excluding the header row)
            for row in table.find_elements(By.XPATH, './/tbody/tr[position()>1]'):
                first_param = None
                second_param = None
                try:
                    # Extract parameters from the row
                    first_param = row.find_element(By.XPATH, './/td[8]').text
                    second_param = row.find_element(By.XPATH, './/td[2]').text

                    # Find the link in the last column
                    link = row.find_element(By.XPATH, './/td[last()]/a')

                    # Get the URL from the link
                    link_url = link.get_attribute('href')

                    # Open the link in a new tab
                    driver.execute_script("window.open('');")
                    driver.switch_to.window(driver.window_handles[-1])
                    driver.get(link_url)
                    
                    # Wait for page to load
                    time.sleep(2)
                    
                    # Click the download button using JavaScript with dynamic parameters
                    script = f"exportToExcel('{first_param}', '{second_param}');"
                    print(f'Getting {selected_option} {first_param}...')
                    driver.execute_script(script)
                    
                    # Wait for download to complete
                    if wait_for_download(download_dir):
                        print(f'\t✓ Downloaded {selected_option} {first_param}')
                    else:
                        print(f'\t✗ Download timeout for {selected_option} {first_param}')

                except Exception as e:
                    print(f'\t\tCannot get {selected_option} {first_param if first_param else "unknown"}: {str(e)}')

                finally:
                    # Close the new tab if it exists
                    if len(driver.window_handles) > 1:
                        driver.close()
                        # Switch back to the original tab
                        driver.switch_to.window(driver.window_handles[0])

        except Exception as e:
            print(f"Error processing location {selected_option}: {str(e)}")

except Exception as e:
    print(f"Script error: {str(e)}")

finally:
    # Close the browser window
    driver.quit()

print(f"Script completed. Check {download_dir} for downloaded files.")