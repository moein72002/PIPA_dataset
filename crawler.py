#!/usr/bin/env python3
"""
PIPA Dataset Crawler

This script downloads images from Flickr using image IDs from the PIPA dataset
without requiring a Flickr API key. It uses direct URL access to public photos.
"""

import os
import sys
import time
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import concurrent.futures

class PIPACrawler:
    def __init__(self, data_file='all_data.txt', output_dir='pipa_images', max_retries=3, delay=1):
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
    
    def get_image_url(self, photo_id):
        """
        Get the image URL for a given photo ID by scraping the Flickr page.
        
        Args:
            photo_id: Flickr photo ID
            
        Returns:
            Image URL or None if not found or private
        """
        flickr_url = f"https://www.flickr.com/photo.gne?id={photo_id}"
        
        for attempt in range(self.max_retries):
            try:
                response = requests.get(flickr_url, headers=self.headers, timeout=10)
                
                # Check if the page was found
                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    
                    # Check if the image is private
                    if "This photo is private" in response.text:
                        print(f"  Photo {photo_id} is private")
                        return None
                    
                    # Try to find the image in different ways
                    
                    # Method 1: Look for the main image
                    img = soup.select_one('img.main-photo')
                    if img and 'src' in img.attrs:
                        url = img['src']
                        return self._ensure_url_scheme(url)
                    
                    # Method 2: Look for image in OpenGraph meta tags
                    og_image = soup.select_one('meta[property="og:image"]')
                    if og_image and 'content' in og_image.attrs:
                        url = og_image['content']
                        return self._ensure_url_scheme(url)
                    
                    # Method 3: Look for image in Twitter card
                    twitter_image = soup.select_one('meta[name="twitter:image"]')
                    if twitter_image and 'content' in twitter_image.attrs:
                        url = twitter_image['content']
                        return self._ensure_url_scheme(url)
                    
                    # Method 4: Look for any large image on the page
                    for img in soup.select('img[src*="live.staticflickr.com"]'):
                        if 'src' in img.attrs:
                            # Try to get the largest version by modifying URL
                            src = img['src']
                            # Replace size indicators like _m, _n, etc. with _b (large) or _o (original)
                            for size in ['_m', '_n', '_s', '_t', '_q', '_sq']:
                                if size in src:
                                    url = src.replace(size, '_b')
                                    return self._ensure_url_scheme(url)
                            return self._ensure_url_scheme(src)
                    
                    print(f"  Could not find image URL for photo {photo_id}")
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
            Tuple of (success, filename or error message)
        """
        filename = os.path.join(self.output_dir, f'{idx:05d}.jpg')
        
        # Skip if already downloaded
        if os.path.exists(filename):
            print(f"  Image {filename} already exists, skipping")
            return True, filename
        
        print(f"Processing photo ID: {photo_id}")
        image_url = self.get_image_url(photo_id)
        
        if not image_url:
            return False, "Could not find image URL"
        
        print(f"  URL: {image_url}")
        
        # Download the image
        for attempt in range(self.max_retries):
            try:
                response = requests.get(image_url, headers=self.headers, timeout=10, stream=True)
                
                if response.status_code == 200:
                    with open(filename, 'wb') as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            f.write(chunk)
                    
                    # Verify the image was downloaded correctly
                    if os.path.getsize(filename) > 0:
                        print(f"  Successfully downloaded {filename}")
                        return True, filename
                    else:
                        os.remove(filename)
                        print(f"  Downloaded empty file for {photo_id}, retrying...")
                
                else:
                    print(f"  Failed to download image, status code: {response.status_code}, retrying...")
                
                time.sleep(self.delay * (attempt + 1))
            
            except Exception as e:
                print(f"  Error downloading image: {str(e)}, retrying...")
                time.sleep(self.delay * (attempt + 1))
        
        return False, f"Failed to download after {self.max_retries} attempts"
    
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
            'not_found': 0
        }
        
        if num_workers > 1:
            # Parallel download
            with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
                future_to_id = {executor.submit(self.download_image, idx, photo_id): (idx, photo_id) 
                               for idx, photo_id in image_ids}
                
                for future in concurrent.futures.as_completed(future_to_id):
                    idx, photo_id = future_to_id[future]
                    try:
                        success, result = future.result()
                        if success:
                            results['success'] += 1
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
                    success, result = self.download_image(idx, photo_id)
                    if success:
                        results['success'] += 1
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
        
        return results


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Download PIPA dataset images without Flickr API key')
    parser.add_argument('--data-file', type=str, default='all_data.txt',
                        help='Path to all_data.txt file (default: all_data.txt)')
    parser.add_argument('--output-dir', type=str, default='pipa_images',
                        help='Directory to save downloaded images (default: pipa_images)')
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
