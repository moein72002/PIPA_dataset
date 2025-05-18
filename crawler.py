#!/usr/bin/env python3
"""
PIPA Dataset Crawler - Highest Resolution Version

This script downloads images from Flickr using image IDs from the PIPA dataset
without requiring a Flickr API key. It uses direct URL access to public photos
and attempts to get the highest resolution available through multiple methods.
"""

import os
import sys
import time
import re
import json
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import concurrent.futures
from PIL import Image

class PIPACrawler:
    def __init__(self, data_file='all_data.txt', output_dir='pipa_images_highest_res', max_retries=3, delay=1):
        """
        Initialize the PIPA crawler.
        
        Args:
            data_file: Path to the all_data.txt file containing image IDs
            output_dir: Directory to save downloaded images
            max_retries: Maximum number of retries for failed downloads
            delay: Delay between requests to avoid rate limiting
        """
        self.data_file = data_file
        self.output_dir = output_dir
        self.max_retries = max_retries
        self.delay = delay
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # User agent to mimic a browser
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
    
    def _ensure_url_scheme(self, url):
        """
        Ensure URL has a proper scheme (http/https).
        
        Args:
            url: URL that might be protocol-relative
            
        Returns:
            URL with proper scheme
        """
        if url.startswith('//'):
            return f"https:{url}"
        elif not url.startswith(('http://', 'https://')):
            return f"https://{url}"
        return url
    
    def parse_image_ids(self, limit=None):
        """
        Parse image IDs from the data file.
        
        Args:
            limit: Optional limit on the number of IDs to parse
            
        Returns:
            List of (index, image_id) tuples
        """
        image_ids = []
        with open(self.data_file, 'r') as f:
            for i, line in enumerate(f):
                if limit and i >= limit:
                    break
                parts = line.strip().split()
                if len(parts) >= 2:
                    image_ids.append((i, parts[1]))
        return image_ids
    
    def get_image_urls(self, photo_id):
        """
        Get all available image URLs for a given photo ID by scraping the Flickr page.
        
        Args:
            photo_id: Flickr photo ID
            
        Returns:
            Dictionary of available sizes with their URLs, or None if not found or private
        """
        flickr_url = f"https://www.flickr.com/photo.gne?id={photo_id}"
        
        for attempt in range(self.max_retries):
            try:
                # First check if the photo exists and is public
                response = requests.get(flickr_url, headers=self.headers, timeout=10)
                
                # Check if the page was found
                if response.status_code == 200:
                    if "This photo is private" in response.text:
                        print(f"  Photo {photo_id} is private")
                        return None
                    
                    soup = BeautifulSoup(response.text, 'html.parser')
                    available_sizes = {}
                    
                    # Method 1: Try to extract image URLs directly from the page
                    # Look for the largest available size in the page
                    for img in soup.select('img[src*="staticflickr"]'):
                        if 'src' in img.attrs:
                            src = img['src']
                            # Try to get the base URL without size suffix
                            base_url = src
                            for suffix in ['_m', '_n', '_s', '_t', '_q', '_sq']:
                                if suffix in base_url:
                                    base_url = base_url.rsplit(suffix, 1)[0]
                                    break
                            
                            # If we found a base URL, try to construct URLs for different sizes
                            if base_url:
                                # Remove file extension if present
                                if '.' in base_url:
                                    base_url = base_url.rsplit('.', 1)[0]
                                
                                # Try different size suffixes
                                suffixes = {
                                    "_b.jpg": "Large",
                                    "_c.jpg": "Medium 800",
                                    "_z.jpg": "Medium 640",
                                    ".jpg": "Medium",
                                    "_n.jpg": "Small 320",
                                    "_m.jpg": "Small"
                                }
                                
                                for suffix, name in suffixes.items():
                                    size_url = f"{base_url}{suffix}"
                                    available_sizes[name] = self._ensure_url_scheme(size_url)
                    
                    # Method 2: Try to find the "View all sizes" link and follow it
                    if not available_sizes:
                        sizes_links = soup.select('a[href*="/sizes/"]')
                        if sizes_links:
                            sizes_url = urljoin("https://www.flickr.com", sizes_links[0]['href'])
                            sizes_response = requests.get(sizes_url, headers=self.headers, timeout=10)
                            
                            if sizes_response.status_code == 200:
                                sizes_soup = BeautifulSoup(sizes_response.text, 'html.parser')
                                
                                # Look for all available size links
                                size_options = sizes_soup.select('ol.sizes-list li a')
                                for size_link in size_options:
                                    size_name = size_link.text.strip()
                                    size_href = size_link['href']
                                    
                                    # Follow the link to get the actual image URL
                                    size_page_url = urljoin("https://www.flickr.com", size_href)
                                    size_page_response = requests.get(size_page_url, headers=self.headers, timeout=10)
                                    
                                    if size_page_response.status_code == 200:
                                        size_page_soup = BeautifulSoup(size_page_response.text, 'html.parser')
                                        img = size_page_soup.select_one('img#allsizes-photo')
                                        if img and 'src' in img.attrs:
                                            available_sizes[size_name] = self._ensure_url_scheme(img['src'])
                    
                    # Method 3: Try to extract from OpenGraph or Twitter card meta tags
                    if not available_sizes:
                        # Look for image in OpenGraph meta tags
                        og_image = soup.select_one('meta[property="og:image"]')
                        if og_image and 'content' in og_image.attrs:
                            url = og_image['content']
                            available_sizes["OpenGraph"] = self._ensure_url_scheme(url)
                        
                        # Look for image in Twitter card
                        twitter_image = soup.select_one('meta[name="twitter:image"]')
                        if twitter_image and 'content' in twitter_image.attrs:
                            url = twitter_image['content']
                            available_sizes["TwitterCard"] = self._ensure_url_scheme(url)
                    
                    # Method 4: Try to use the example URLs provided by the user
                    # Extract server and secret from any available URL
                    server = None
                    secret = None
                    
                    for url in available_sizes.values():
                        parsed_url = urlparse(url)
                        path_parts = parsed_url.path.split('/')
                        if len(path_parts) >= 3:
                            server = path_parts[1]
                            filename = path_parts[-1]
                            parts = filename.split('_')
                            if len(parts) >= 2:
                                secret = parts[1].split('.')[0]
                                break
                    
                    # If we found server and secret, try to construct high-res URLs
                    if server and secret:
                        # Try the URL patterns from the user's example
                        large_url = f"https://live.staticflickr.com/{server}/{photo_id}_{secret}_h_d.jpg"
                        available_sizes["Large HD"] = large_url
                        
                        # Don't add original URL as it requires authentication
                        # original_url = f"https://live.staticflickr.com/{server}/{photo_id}_{secret}_o_d.jpg"
                        # available_sizes["Original"] = original_url
                    
                    if available_sizes:
                        return available_sizes
                    else:
                        print(f"  Could not find any image URLs for photo {photo_id}")
                        return None
                
                elif response.status_code == 404:
                    print(f"  Photo {photo_id} not found (404)")
                    return None
                
                else:
                    print(f"  Unexpected status code {response.status_code} for photo {photo_id}, retrying...")
                    time.sleep(self.delay * (attempt + 1))
            
            except Exception as e:
                print(f"  Error accessing photo {photo_id}: {str(e)}, retrying...")
                time.sleep(self.delay * (attempt + 1))
        
        print(f"  Failed to access photo {photo_id} after {self.max_retries} attempts")
        return None
    
    def download_image(self, idx, photo_id):
        """
        Download an image given its photo ID.
        
        Args:
            idx: Index of the image in the dataset
            photo_id: Flickr photo ID
            
        Returns:
            Tuple of (success, filename or error message, resolution)
        """
        filename = os.path.join(self.output_dir, f'{idx:05d}.jpg')
        
        # Skip if already downloaded
        if os.path.exists(filename):
            try:
                with Image.open(filename) as img:
                    width, height = img.size
                    print(f"  Image {filename} already exists, size: {width}x{height}")
                    return True, filename, (width, height)
            except Exception:
                print(f"  Image {filename} exists but could not be opened, will redownload")
        
        print(f"Processing photo ID: {photo_id}")
        
        # Try to get all available image URLs
        available_urls = self.get_image_urls(photo_id)
        
        if not available_urls:
            return False, "Could not find any image URLs", (0, 0)
        
        # Priority order for sizes
        size_priority = [
            "Large HD", "Large", "Medium 800", "Medium 640", 
            "Medium", "OpenGraph", "TwitterCard", "Small 320", "Small"
        ]
        
        # Try downloading in priority order
        for size_name in size_priority:
            if size_name in available_urls:
                url = available_urls[size_name]
                print(f"  Trying URL ({size_name}): {url}")
                
                for attempt in range(self.max_retries):
                    try:
                        response = requests.get(url, headers=self.headers, timeout=10, stream=True)
                        
                        if response.status_code == 200:
                            with open(filename, 'wb') as f:
                                for chunk in response.iter_content(chunk_size=8192):
                                    f.write(chunk)
                            
                            # Verify the image was downloaded correctly
                            if os.path.getsize(filename) > 0:
                                try:
                                    with Image.open(filename) as img:
                                        width, height = img.size
                                        print(f"  Successfully downloaded {filename}, size: {width}x{height}")
                                        return True, filename, (width, height)
                                except Exception as e:
                                    os.remove(filename)
                                    print(f"  Downloaded file is not a valid image: {str(e)}")
                            else:
                                os.remove(filename)
                                print(f"  Downloaded empty file")
                        
                        elif response.status_code == 410:
                            # 410 Gone - This URL is no longer available, try next size
                            print(f"  URL returned 410 Gone, trying next size")
                            break
                        
                        else:
                            print(f"  Failed to download image, status code: {response.status_code}, retrying...")
                        
                        time.sleep(self.delay * (attempt + 1))
                    
                    except Exception as e:
                        print(f"  Error downloading image: {str(e)}, retrying...")
                        time.sleep(self.delay * (attempt + 1))
        
        # If we've tried all sizes and none worked, try any remaining URLs
        for name, url in available_urls.items():
            if name not in size_priority:
                print(f"  Trying URL ({name}): {url}")
                
                for attempt in range(self.max_retries):
                    try:
                        response = requests.get(url, headers=self.headers, timeout=10, stream=True)
                        
                        if response.status_code == 200:
                            with open(filename, 'wb') as f:
                                for chunk in response.iter_content(chunk_size=8192):
                                    f.write(chunk)
                            
                            # Verify the image was downloaded correctly
                            if os.path.getsize(filename) > 0:
                                try:
                                    with Image.open(filename) as img:
                                        width, height = img.size
                                        print(f"  Successfully downloaded {filename}, size: {width}x{height}")
                                        return True, filename, (width, height)
                                except Exception as e:
                                    os.remove(filename)
                                    print(f"  Downloaded file is not a valid image: {str(e)}")
                            else:
                                os.remove(filename)
                                print(f"  Downloaded empty file")
                        
                        elif response.status_code == 410:
                            # 410 Gone - This URL is no longer available, try next size
                            print(f"  URL returned 410 Gone, trying next size")
                            break
                        
                        else:
                            print(f"  Failed to download image, status code: {response.status_code}, retrying...")
                        
                        time.sleep(self.delay * (attempt + 1))
                    
                    except Exception as e:
                        print(f"  Error downloading image: {str(e)}, retrying...")
                        time.sleep(self.delay * (attempt + 1))
        
        return False, f"Failed to download after trying all available URLs", (0, 0)
    
    def crawl(self, limit=None, num_workers=1):
        """
        Crawl and download images from the dataset.
        
        Args:
            limit: Optional limit on the number of images to download
            num_workers: Number of parallel workers for downloading
            
        Returns:
            Dictionary with statistics about the crawl
        """
        image_ids = self.parse_image_ids(limit)
        total = len(image_ids)
        
        print(f"Found {total} image IDs to process")
        
        results = {
            'total': total,
            'success': 0,
            'failed': 0,
            'private': 0,
            'not_found': 0,
            'resolutions': []
        }
        
        if num_workers > 1:
            # Parallel download
            with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
                future_to_id = {executor.submit(self.download_image, idx, photo_id): (idx, photo_id) 
                               for idx, photo_id in image_ids}
                
                for future in concurrent.futures.as_completed(future_to_id):
                    idx, photo_id = future_to_id[future]
                    try:
                        success, result, resolution = future.result()
                        if success:
                            results['success'] += 1
                            results['resolutions'].append((idx, photo_id, resolution))
                        else:
                            results['failed'] += 1
                            if "private" in result.lower():
                                results['private'] += 1
                            elif "not found" in result.lower():
                                results['not_found'] += 1
                    except Exception as e:
                        print(f"  Error processing {photo_id}: {str(e)}")
                        results['failed'] += 1
        else:
            # Sequential download
            for idx, photo_id in image_ids:
                try:
                    success, result, resolution = self.download_image(idx, photo_id)
                    if success:
                        results['success'] += 1
                        results['resolutions'].append((idx, photo_id, resolution))
                    else:
                        results['failed'] += 1
                        if "private" in str(result).lower():
                            results['private'] += 1
                        elif "not found" in str(result).lower():
                            results['not_found'] += 1
                except Exception as e:
                    print(f"  Error processing {photo_id}: {str(e)}")
                    results['failed'] += 1
                
                # Add delay between requests
                time.sleep(self.delay)
        
        print("\nCrawl Summary:")
        print(f"Total images processed: {results['total']}")
        print(f"Successfully downloaded: {results['success']}")
        print(f"Failed: {results['failed']}")
        print(f"  - Private images: {results['private']}")
        print(f"  - Not found: {results['not_found']}")
        
        if results['resolutions']:
            print("\nImage Resolutions:")
            for idx, photo_id, (width, height) in results['resolutions']:
                print(f"  {idx:05d}.jpg (ID: {photo_id}): {width}x{height}")
        
        return results


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Download PIPA dataset images in highest available resolution without Flickr API key')
    parser.add_argument('--data-file', type=str, default='all_data.txt',
                        help='Path to all_data.txt file (default: all_data.txt)')
    parser.add_argument('--output-dir', type=str, default='pipa_images_highest_res',
                        help='Directory to save downloaded images (default: pipa_images_highest_res)')
    parser.add_argument('--limit', type=int, default=None,
                        help='Limit the number of images to download (default: download all)')
    parser.add_argument('--workers', type=int, default=1,
                        help='Number of parallel download workers (default: 1)')
    parser.add_argument('--delay', type=float, default=1.0,
                        help='Delay between requests in seconds (default: 1.0)')
    parser.add_argument('--retries', type=int, default=3,
                        help='Maximum number of retries for failed downloads (default: 3)')
    
    args = parser.parse_args()
    
    crawler = PIPACrawler(
        data_file=args.data_file,
        output_dir=args.output_dir,
        max_retries=args.retries,
        delay=args.delay
    )
    
    crawler.crawl(limit=args.limit, num_workers=args.workers)


if __name__ == "__main__":
    main()
