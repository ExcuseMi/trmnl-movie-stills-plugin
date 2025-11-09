import requests
import json
import os
import time
from pathlib import Path
from typing import List, Dict
import sys
from PIL import Image
from dotenv import load_dotenv
import re


class TMDBDatasetBuilder:
    def __init__(self, api_key: str, output_dir: str = "movie_dataset"):
        self.api_key = api_key
        self.base_url = "https://api.themoviedb.org/3"
        self.image_base_url = "https://image.tmdb.org/t/p/original"
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.genres = {}

    def generate_slug(self, title: str) -> str:
        """Generate a URL-friendly slug from movie title"""
        slug = title.lower()
        # Replace spaces with hyphens
        slug = slug.replace(' ', '-')
        # Remove all non-alphanumeric characters except hyphens
        slug = re.sub(r'[^a-z0-9\-]', '', slug)
        # Replace multiple consecutive hyphens with single hyphen
        slug = re.sub(r'-+', '-', slug)
        # Remove leading/trailing hyphens
        slug = slug.strip('-')
        return slug

    def fetch_genres(self):
        """Fetch genre mapping from TMDB"""
        try:
            url = f"{self.base_url}/genre/movie/list"
            params = {
                "api_key": self.api_key,
                "language": "en-US"
            }

            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()

            # Create id -> name mapping
            self.genres = {g['id']: g['name'] for g in data['genres']}
            print(f"✅ Loaded {len(self.genres)} genres")

        except Exception as e:
            print(f"Error fetching genres: {e}")
            sys.exit(1)

    def verify_and_clean_images(self, movie_dir: Path, image_list: List[str]) -> List[str]:
        """Verify that image files exist and return only valid ones"""
        valid_images = []
        removed_count = 0

        for img_filename in image_list:
            img_path = movie_dir / img_filename
            if img_path.exists():
                valid_images.append(img_filename)
            else:
                removed_count += 1
                print(f"  ⚠️  Image missing: {img_filename}")

        if removed_count > 0:
            print(f"  🧹 Cleaned {removed_count} missing image(s)")

        return valid_images

    def get_top_movies(self, total_movies: int = 500) -> List[Dict]:
        """Fetch top rated movies from TMDB"""
        movies = []
        pages_needed = (total_movies // 20) + 1

        print(f"Fetching top {total_movies} movies...")

        for page in range(1, pages_needed + 1):
            try:
                url = f"{self.base_url}/movie/top_rated"
                params = {
                    "api_key": self.api_key,
                    "page": page,
                    "language": "en-US"
                }

                response = requests.get(url, params=params)
                response.raise_for_status()
                data = response.json()

                movies.extend(data['results'])
                print(f"  Fetched page {page}/{pages_needed} ({len(movies)} movies so far)")

                time.sleep(0.25)  # Rate limiting

                if len(movies) >= total_movies:
                    break

            except Exception as e:
                print(f"Error fetching page {page}: {e}")
                continue

        return movies[:total_movies]

    def get_movie_images(self, movie_id: int) -> List[str]:
        """Get actual movie stills/screenshots from the movie"""
        try:
            url = f"{self.base_url}/movie/{movie_id}/images"
            params = {
                "api_key": self.api_key,
                "include_image_language": "en,null"
            }

            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()

            # Backdrops are typically scene stills from the movie
            backdrops = data.get('backdrops', [])

            # Filter for higher resolution
            stills = [
                img for img in backdrops
                if img.get('width', 0) >= 1920  # High resolution
            ]

            # Sort by vote average to get best quality stills
            sorted_stills = sorted(
                stills if stills else backdrops,
                key=lambda x: -x.get('vote_average', 0)
            )

            return [img['file_path'] for img in sorted_stills[:3]]

        except Exception as e:
            print(f"  Error fetching images for movie {movie_id}: {e}")
            return []

    def download_and_convert_image(self, image_path: str, save_path: Path) -> bool:
        """Download an image from TMDB and convert to WebP"""
        try:
            url = f"{self.image_base_url}{image_path}"
            response = requests.get(url, stream=True)
            response.raise_for_status()

            # Save temporarily as JPG
            temp_path = save_path.with_suffix('.jpg')
            with open(temp_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            # Convert to WebP
            img = Image.open(temp_path)
            img.save(save_path, 'WEBP', quality=85)

            # Remove temp file
            temp_path.unlink()

            return True

        except Exception as e:
            print(f"  Error downloading/converting image: {e}")
            return False

    def update_existing_slugs(self):
        """Update all existing movie.json files to add slug field"""
        movies_file = self.output_dir / "movies.json"
        if not movies_file.exists():
            print("No movies.json found!")
            return

        print("\n🔄 Updating existing entries with slugs...")

        with open(movies_file, 'r', encoding='utf-8') as f:
            dataset = json.load(f)

        updated_count = 0

        for movie_data in dataset:
            movie_id = movie_data['id']
            movie_dir = self.output_dir / str(movie_id)
            movie_json_path = movie_dir / "movie.json"

            if not movie_json_path.exists():
                continue

            # Check if slug already exists
            if 'slug' not in movie_data:
                slug = self.generate_slug(movie_data['title'])
                movie_data['slug'] = slug

                # Update individual movie.json
                with open(movie_json_path, 'w', encoding='utf-8') as f:
                    json.dump(movie_data, f, indent=2, ensure_ascii=False)

                updated_count += 1
                print(f"  ✅ Added slug to: {movie_data['title']} -> {slug}")

        # Save updated dataset
        if updated_count > 0:
            self.save_dataset(dataset)
            print(f"\n✅ Updated {updated_count} movies with slugs")
        else:
            print("\n✅ All movies already have slugs")

    def build_dataset(self, total_movies: int = 500):
        """Build the complete dataset"""
        # First fetch genres
        self.fetch_genres()
        """Save dataset to JSON file"""
        output_file = self.output_dir / "genres.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(self.genres, f, indent=2, ensure_ascii=False)
        movies = self.get_top_movies(total_movies)
        dataset = []

        print(f"\nProcessing {len(movies)} movies and downloading images...")

        for idx, movie in enumerate(movies, 1):
            movie_id = movie['id']
            title = movie['title']
            original_title = movie.get('original_title')

            print(f"\n[{idx}/{len(movies)}] Processing: {title}")

            # Create movie folder using movie ID
            movie_dir = self.output_dir / str(movie_id)
            folder_existed: bool = movie_dir.exists()
            movie_dir.mkdir(exist_ok=True)
            movie_json_path = movie_dir / "movie.json"

            if not folder_existed or not movie_json_path.exists():
                # Get images for this movie
                image_paths = self.get_movie_images(movie_id)

                if not image_paths:
                    print(f"  No images found, skipping...")
                    continue

                # Download and convert images
                downloaded_images = []
                for img_idx, img_path in enumerate(image_paths):
                    filename = f"still_{img_idx}.webp"
                    save_path = movie_dir / filename

                    if save_path.exists():
                        print(f"  Image {img_idx + 1} already exists")
                        downloaded_images.append(filename)
                    else:
                        print(f"  Downloading image {img_idx + 1}/{len(image_paths)}...")
                        if self.download_and_convert_image(img_path, save_path):
                            downloaded_images.append(filename)
                            time.sleep(0.1)  # Rate limiting

                if not downloaded_images:
                    print(f"  No images downloaded, skipping...")
                    continue

                # Convert genre IDs to names
                genre_names = [self.genres.get(gid, f"Unknown_{gid}") for gid in movie.get('genre_ids', [])]

                # Generate slug
                slug = self.generate_slug(title)

                # Store metadata
                movie_data = {
                    "id": movie_id,
                    "title": title,
                    "slug": slug,
                    "year": movie.get('release_date', '')[:4],
                    "overview": movie.get('overview'),
                    "rating": movie.get('vote_average'),
                    "genres": genre_names,
                    "images": downloaded_images
                }

                # Only include original_title if different from title
                if original_title and original_title != title:
                    movie_data["original_title"] = original_title

                with open(movie_json_path, 'w', encoding='utf-8') as f:
                    json.dump(movie_data, f, indent=2, ensure_ascii=False)
            else:
                # Load existing data
                with open(movie_json_path, 'r', encoding='utf-8') as f:
                    movie_data = json.load(f)

                # Add slug if missing
                if 'slug' not in movie_data:
                    movie_data['slug'] = self.generate_slug(movie_data['title'])
                    # Save updated movie.json
                    with open(movie_json_path, 'w', encoding='utf-8') as f:
                        json.dump(movie_data, f, indent=2, ensure_ascii=False)
                    print(f"  Added slug: {movie_data['slug']}")

                # Verify images still exist and clean up if needed
                original_image_count = len(movie_data.get('images', []))
                valid_images = self.verify_and_clean_images(movie_dir, movie_data.get('images', []))

                # Update if images were removed
                if len(valid_images) != original_image_count:
                    movie_data['images'] = valid_images
                    # Save updated movie.json
                    with open(movie_json_path, 'w', encoding='utf-8') as f:
                        json.dump(movie_data, f, indent=2, ensure_ascii=False)

                # Skip movies with no valid images
                if not valid_images:
                    print(f"  No valid images, skipping...")
                    continue

            # Add to dataset
            dataset.append(movie_data)

            # Progress update and save periodically
            if idx % 50 == 0:
                self.save_dataset(dataset)
                print(f"\n  Progress saved! ({len(dataset)} movies with images)")

        # Final save
        self.save_dataset(dataset)
        print(f"\n✅ Dataset complete! {len(dataset)} movies with images saved.")
        print(f"📁 Location: {self.output_dir.absolute()}")

        return len(dataset)

    def save_dataset(self, dataset: List[Dict]):
        """Save dataset to JSON file"""
        output_file = self.output_dir / "movies.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(dataset, f, indent=2, ensure_ascii=False)

    def cleanup_missing_images(self):
        """Scan all movie folders and clean up missing image references"""
        print("\n🧹 Scanning for missing images...")

        movies_file = self.output_dir / "movies.json"
        if not movies_file.exists():
            print("No movies.json found!")
            return

        total_removed = 0
        movies_cleaned = 0
        movies_removed = 0

        # Load current dataset
        with open(movies_file, 'r', encoding='utf-8') as f:
            dataset = json.load(f)

        updated_dataset = []

        for movie_data in dataset:
            movie_id = movie_data['id']
            movie_dir = self.output_dir / str(movie_id)
            movie_json_path = movie_dir / "movie.json"

            if not movie_dir.exists():
                print(f"  ⚠️  Movie folder missing: {movie_data['title']} (ID: {movie_id})")
                movies_removed += 1
                continue

            # Add slug if missing
            if 'slug' not in movie_data:
                movie_data['slug'] = self.generate_slug(movie_data['title'])
                print(f"  Added slug to: {movie_data['title']}")

            # Verify images
            original_images = movie_data.get('images', [])
            valid_images = self.verify_and_clean_images(movie_dir, original_images)

            if len(valid_images) != len(original_images):
                removed = len(original_images) - len(valid_images)
                total_removed += removed
                movies_cleaned += 1
                print(f"  Cleaned {movie_data['title']}: {removed} missing image(s)")

                # Update movie data
                movie_data['images'] = valid_images

                # Update individual movie.json
                if movie_json_path.exists():
                    with open(movie_json_path, 'w', encoding='utf-8') as f:
                        json.dump(movie_data, f, indent=2, ensure_ascii=False)

            # Only keep movies with at least one image
            if valid_images:
                updated_dataset.append(movie_data)
            else:
                print(f"  ⚠️  No valid images for: {movie_data['title']} (ID: {movie_id})")
                movies_removed += 1

        # Save cleaned dataset
        if total_removed > 0 or movies_removed > 0:
            self.save_dataset(updated_dataset)
            print(f"\n✅ Cleanup complete!")
            print(f"  Movies cleaned: {movies_cleaned}")
            print(f"  Movies removed: {movies_removed}")
            print(f"  Total images removed from metadata: {total_removed}")
        else:
            print("\n✅ No cleanup needed - all images valid!")

    def process_delete_list(self, delete_list_path: str):
        """Process a delete list JSON file from the image reviewer"""
        delete_file = Path(delete_list_path)
        if not delete_file.exists():
            print(f"❌ Delete list file not found: {delete_list_path}")
            return

        print(f"📋 Loading delete list from: {delete_file.name}")

        with open(delete_file, 'r', encoding='utf-8') as f:
            delete_list = json.load(f)

        print(f"Found {len(delete_list)} images marked for deletion")
        print("\nFiles to be deleted:")
        for item in delete_list:
            print(f"  - {item['movieTitle']}: {item['filename']}")

        response = input(f"\n⚠️  Delete {len(delete_list)} images? (yes/no): ").strip().lower()
        if response != 'yes':
            print("❌ Deletion cancelled")
            return

        deleted_count = 0
        failed_count = 0

        for item in delete_list:
            # Reconstruct the full path
            movie_id = item['movieId']
            filename = item['filename']
            file_path = self.output_dir / movie_id / filename

            try:
                if file_path.exists():
                    file_path.unlink()
                    deleted_count += 1
                    print(f"  ✅ Deleted: {item['movieTitle']} - {filename}")
                else:
                    print(f"  ⚠️  File not found: {filename}")
                    failed_count += 1
            except Exception as e:
                print(f"  ❌ Error deleting {filename}: {e}")
                failed_count += 1

        print(f"\n✅ Deletion complete!")
        print(f"  Deleted: {deleted_count}")
        print(f"  Failed/Not found: {failed_count}")

        # Now clean up the metadata
        print("\n🧹 Cleaning up metadata...")
        self.cleanup_missing_images()

    def print_stats(self):
        """Print dataset statistics"""
        movies_file = self.output_dir / "movies.json"
        if not movies_file.exists():
            print("No dataset found!")
            return

        with open(movies_file, 'r', encoding='utf-8') as f:
            dataset = json.load(f)

        total_images = sum(len(m['images']) for m in dataset)
        movies_with_slugs = sum(1 for m in dataset if 'slug' in m)

        print("\n📊 Dataset Statistics:")
        print(f"  Total movies: {len(dataset)}")
        print(f"  Movies with slugs: {movies_with_slugs}/{len(dataset)}")
        print(f"  Total images: {total_images}")
        print(f"  Avg images per movie: {total_images / len(dataset):.1f}")
        print(f"  Storage location: {self.output_dir.absolute()}")


load_dotenv()

if __name__ == "__main__":
    # Check for PIL/Pillow
    try:
        from PIL import Image
    except ImportError:
        print("❌ Pillow is required for WebP conversion")
        print("Install with: pip install Pillow")
        sys.exit(1)

    # Get API key from environment or command line
    api_key = os.environ.get('TMDB_API_KEY')

    if not api_key:
        print("Please provide your TMDB API key:")
        print("1. Set environment variable: export TMDB_API_KEY='your_key_here'")
        print("2. Or edit this script and add it below")
        api_key = input("\nEnter API key: ").strip()

    if not api_key:
        print("❌ No API key provided!")
        sys.exit(1)

    # Build dataset
    builder = TMDBDatasetBuilder(api_key)

    # Uncomment the action you want to perform:

    # Build new dataset (automatically cleans during build)
    # builder.build_dataset(total_movies=1000)

    # Update existing entries with slugs (doesn't re-download images)
    builder.update_existing_slugs()

    # Or just run cleanup on existing dataset
    # builder.cleanup_missing_images()

    # Or process a delete list from the image reviewer
    # builder.process_delete_list("delete_list_1762669507505.json")

    builder.print_stats()