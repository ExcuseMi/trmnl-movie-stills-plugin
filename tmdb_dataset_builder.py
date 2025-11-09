import requests
import json
import os
import time
from pathlib import Path
from typing import List, Dict
import sys
from PIL import Image
from dotenv import load_dotenv


class TMDBDatasetBuilder:
    def __init__(self, api_key: str, output_dir: str = "movie_dataset"):
        self.api_key = api_key
        self.base_url = "https://api.themoviedb.org/3"
        self.image_base_url = "https://image.tmdb.org/t/p/original"
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.genres = {}

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

    def build_dataset(self, total_movies: int = 500):
        """Build the complete dataset"""
        # First fetch genres
        self.fetch_genres()

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
            movie_dir.mkdir(exist_ok=True)

            # Get images for this movie
            image_paths = self.get_movie_images(movie_id)

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

            # Store metadata
            movie_data = {
                "id": movie_id,
                "title": title,
                "year": movie.get('release_date', '')[:4],
                "overview": movie.get('overview'),
                "rating": movie.get('vote_average'),
                "genres": genre_names,
                "images": downloaded_images
            }

            # Only include original_title if different from title
            if original_title and original_title != title:
                movie_data["original_title"] = original_title

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

    def print_stats(self):
        """Print dataset statistics"""
        movies_file = self.output_dir / "movies.json"
        if not movies_file.exists():
            print("No dataset found!")
            return

        with open(movies_file, 'r', encoding='utf-8') as f:
            dataset = json.load(f)

        total_images = sum(len(m['images']) for m in dataset)

        print("\n📊 Dataset Statistics:")
        print(f"  Total movies: {len(dataset)}")
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
    builder.build_dataset(total_movies=1000)
    builder.print_stats()