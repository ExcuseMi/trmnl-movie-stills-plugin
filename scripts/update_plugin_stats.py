#!/usr/bin/env python3
import requests
import re
from datetime import datetime


def load_plugin_config():
    """Load plugin IDs from plugins.env file"""
    config = {
        'plugin_ids': [],
        'section_title': '🚀 Plugin Statistics'
    }

    try:
        with open('plugins.env', 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    if '=' in line:
                        key, value = line.split('=', 1)
                        key = key.strip()
                        value = value.strip()

                        if key == 'PLUGIN_IDS':
                            # Split by comma and remove empty strings
                            config['plugin_ids'] = [pid.strip() for pid in value.split(',') if pid.strip()]
                        elif key == 'SECTION_TITLE':
                            config['section_title'] = value
    except FileNotFoundError:
        print("⚠️  plugins.env file not found. Using default configuration.")

    return config


def fetch_plugin_data(plugin_id: str):
    url = f"https://usetrmnl.com/recipes/{plugin_id}.json"

    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching plugin data for {plugin_id}: {e}")
        return None


def generate_plugin_section(data, plugin_id: str):
    if not data:
        return f"<!-- Plugin data unavailable for {plugin_id} -->"

    plugin = data['data']
    stats = plugin['stats']

    markdown = f"""
## <img src="{plugin['icon_url']}" alt="Plugin icon" width="32"/> [{plugin['name']}](https://usetrmnl.com/recipes/{plugin_id})

![Plugin screenshot]({plugin['screenshot_url']})
### Description
{plugin['author_bio']['description']}

### 📊 Statistics

| Metric | Value |
|--------|-------|
| Installs | {stats['installs']} |
| Forks | {stats['forks']} |

---
"""
    return markdown


def update_readme(plugin_sections: str, section_title: str):
    # Read existing README.md
    try:
        with open('README.md', 'r') as f:
            content = f.read()
    except FileNotFoundError:
        content = "# Project README\n\n"

    # Define markers for the plugin sections
    start_marker = "<!-- PLUGIN_STATS_START -->"
    end_marker = "<!-- PLUGIN_STATS_END -->"

    # Create the new plugin sections content
    timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')
    new_content = f"{start_marker}\n## {section_title}\n\n*Last updated: {timestamp}*\n\n{plugin_sections}\n{end_marker}"

    # Check if markers exist in the README
    if start_marker in content and end_marker in content:
        # Replace existing content between markers
        pattern = f"{re.escape(start_marker)}.*?{re.escape(end_marker)}"
        updated_content = re.sub(pattern, new_content, content, flags=re.DOTALL)
    else:
        # Append to the end of the README
        updated_content = content + "\n\n" + new_content + "\n"

    # Write updated content back to README.md
    with open('README.md', 'w') as f:
        f.write(updated_content)


def main():
    # Load configuration from plugins.env
    config = load_plugin_config()
    plugin_ids = config['plugin_ids']
    section_title = config['section_title']

    if not plugin_ids:
        print("❌ No plugin IDs found in plugins.env")
        return

    print(f"📋 Tracking {len(plugin_ids)} plugins: {', '.join(plugin_ids)}")

    plugin_sections = []
    for plugin_id in plugin_ids:
        print(f"🔍 Fetching data for plugin {plugin_id}...")
        data = fetch_plugin_data(plugin_id)
        section = generate_plugin_section(data, plugin_id)
        plugin_sections.append(section)

    all_sections = "\n".join(plugin_sections)
    update_readme(all_sections, section_title)

    print("✅ README.md updated successfully with plugin statistics!")


if __name__ == "__main__":
    main()