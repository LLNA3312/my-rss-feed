#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
merge_feeds.py
================

This script reads configuration from ``sources.yml`` and produces
``feed.xml``, an aggregated podcast feed that includes items from
external podcast RSS feeds and the latest videos from specified
YouTube channels converted to audio. When run as part of a GitHub
Actions workflow, it will also download any new YouTube videos as
MP3 files into the repository so they can be served via
``raw.githubusercontent.com``.

Usage:
    python merge_feeds.py

Dependencies (install via pip):
    feedparser, feedgen, pyyaml, pytz, yt_dlp

The script is idempotent: it keeps track of downloaded YouTube
videos using per-channel download archive files under the
``.cache`` directory, so reruns will skip already downloaded
content.
"""

import os
import sys
import time
import email.utils
import hashlib
import datetime
import re
import yaml
import feedparser
from feedgen.feed import FeedGenerator
import pytz

try:
    from yt_dlp import YoutubeDL
except ImportError:
    # yt_dlp is optional at import time; the workflow will install it
    YoutubeDL = None


def is_audio_enclosure(link: dict) -> bool:
    """Return True if a link dict from feedparser appears to be an audio enclosure."""
    if not link:
        return False
    href = (link.get('href') or '').lower()
    ltype = (link.get('type') or '').lower()
    return (
        ltype.startswith('audio/')
        or href.endswith('.mp3')
        or href.endswith('.m4a')
        or href.endswith('.aac')
        or href.endswith('.ogg')
    )


def pick_audio_enclosure(links) -> dict:
    """Return the first audio enclosure from a list of links, or None."""
    if not links:
        return None
    for link in links:
        if is_audio_enclosure(link):
            return link
    return None


def slugify(text: str) -> str:
    """Convert arbitrary text into a filesystem- and URL-friendly slug."""
    text = text.strip().lower()
    # Replace non-alphanumeric characters with hyphens
    text = re.sub(r'[^0-9a-z]+', '-', text)
    return text.strip('-') or 'channel'


def format_pubdate(dt: datetime.datetime) -> str:
    """Format a datetime for RSS (RFC-2822)."""
    return email.utils.format_datetime(dt)


def main() -> int:
    # Load configuration
    with open('sources.yml', 'r', encoding='utf-8') as f:
        cfg = yaml.safe_load(f)

    tzname = cfg.get('timezone', 'UTC')
    timezone = pytz.timezone(tzname)

    # Prepare feed generator
    fg = FeedGenerator()
    fg.load_extension('podcast')
    fg.title(cfg.get('title', 'Aggregated Feed'))
    fg.link(href=cfg.get('link', ''), rel='alternate')
    fg.description(cfg.get('description', ''))
    fg.language('zh-CN')

    items = []  # collect all feed items here

    max_items_per_source = int(cfg.get('max_items_per_source', 10))
    max_total_items = int(cfg.get('max_total_items', 100))

    # Process external podcast RSS feeds
    podcasts = cfg.get('podcasts') or []
    for feed_url in podcasts:
        try:
            parsed = feedparser.parse(feed_url)
        except Exception as exc:
            print(f"[warn] Failed to parse podcast feed {feed_url}: {exc}", file=sys.stderr)
            continue
        count = 0
        for entry in parsed.entries:
            enc = pick_audio_enclosure(getattr(entry, 'links', []))
            if not enc:
                continue
            # Determine publication date
            if getattr(entry, 'published_parsed', None):
                dt = datetime.datetime.fromtimestamp(time.mktime(entry.published_parsed), timezone)
            elif getattr(entry, 'updated_parsed', None):
                dt = datetime.datetime.fromtimestamp(time.mktime(entry.updated_parsed), timezone)
            else:
                dt = datetime.datetime.now(timezone)
            guid_source = getattr(entry, 'id', '') or getattr(entry, 'link', '') or enc.get('href', '')
            guid = hashlib.sha1(f"podcast-{guid_source}".encode('utf-8')).hexdigest()
            items.append({
                'title': getattr(entry, 'title', 'Untitled'),
                'link': getattr(entry, 'link', feed_url),
                'enclosure_url': enc.get('href'),
                'enclosure_type': enc.get('type') or 'audio/mpeg',
                'enclosure_length': enc.get('length') or '0',
                'pubdate': dt,
                'guid': guid,
            })
            count += 1
            if count >= max_items_per_source:
                break

    # Process YouTube channels if yt_dlp is available
    youtube_channels = cfg.get('youtube_channels') or []
    global_max_videos = int(cfg.get('max_videos_per_channel', 3))
    if youtube_channels and YoutubeDL is None:
        print("[warn] yt_dlp is not installed; skipping YouTube channels")
    for channel_cfg in youtube_channels:
        if YoutubeDL is None:
            break
        channel_url = channel_cfg.get('url')
        if not channel_url:
            continue
        max_videos = int(channel_cfg.get('max_videos', global_max_videos))
        # Derive slug for directory and archive file
        slug = slugify(channel_url.split('/')[-1])
        audio_dir = os.path.join('youtube_audio', slug)
        os.makedirs(audio_dir, exist_ok=True)
        os.makedirs('.cache', exist_ok=True)
        archive_path = os.path.join('.cache', f'{slug}_archive.txt')
        # Extract list of recent videos (metadata only)
        ydl_opts_info = {
            'ignoreerrors': True,
            'quiet': True,
            'extract_flat': True,
            'dump_single_json': False,
        }
        try:
            with YoutubeDL(ydl_opts_info) as ydl:
                info = ydl.extract_info(channel_url, download=False)
        except Exception as exc:
            print(f"[warn] Failed to extract YouTube channel {channel_url}: {exc}", file=sys.stderr)
            continue
        entries = info.get('entries', []) if info else []
        # Some channels return nested playlists; flatten if necessary
        video_entries = []
        for entry in entries:
            if entry is None:
                continue
            if 'entries' in entry:
                # playlist within channel
                video_entries.extend([e for e in entry['entries'] if e])
            else:
                video_entries.append(entry)
        # Take only the most recent max_videos entries
        video_entries = video_entries[:max_videos]
        # Prepare a counter for items per channel
        channel_count = 0
        for entry in video_entries:
            video_id = entry.get('id')
            if not video_id:
                continue
            title = entry.get('title') or f"YouTube Video {video_id}"
            upload_date = entry.get('upload_date')  # e.g. '20250101'
            # Convert upload_date string to datetime
            if upload_date and len(upload_date) == 8:
                try:
                    dt = datetime.datetime.strptime(upload_date, '%Y%m%d')
                    dt = timezone.localize(dt)
                except Exception:
                    dt = datetime.datetime.now(timezone)
            else:
                dt = datetime.datetime.now(timezone)
            # Determine audio file path and raw URL
            outfile = os.path.join(audio_dir, f'{video_id}.mp3')
            raw_url = f"https://raw.githubusercontent.com/LLNA3312/my-rss-feed/main/{outfile}"
            # Download the audio if not already present or recorded in archive
            need_download = not os.path.exists(outfile)
            if os.path.exists(archive_path):
                try:
                    with open(archive_path, 'r', encoding='utf-8') as ap:
                        if video_id in ap.read().splitlines():
                            need_download = False
                except Exception:
                    pass
            if need_download:
                print(f"[info] Downloading {channel_url} video {video_id}")
                ydl_opts_download = {
                    'format': 'bestaudio/best',
                    'outtmpl': os.path.join(audio_dir, f'{video_id}.%(ext)s'),
                    'postprocessors': [
                        {'key': 'FFmpegExtractAudio', 'preferredcodec': 'mp3', 'preferredquality': '192'},
                        {'key': 'FFmpegMetadata'},
                    ],
                    'quiet': True,
                    'ignoreerrors': True,
                    'download_archive': archive_path,
                }
                try:
                    with YoutubeDL(ydl_opts_download) as ydl:
                        ydl.download([f'https://www.youtube.com/watch?v={video_id}'])
                except Exception as exc:
                    print(f"[warn] Error downloading video {video_id}: {exc}", file=sys.stderr)
            # Compute file length if exists
            length = '0'
            if os.path.exists(outfile):
                try:
                    length = str(os.path.getsize(outfile))
                except Exception:
                    length = '0'
            # Build unique guid for this video
            guid = hashlib.sha1(f'youtube-{video_id}'.encode('utf-8')).hexdigest()
            items.append({
                'title': title,
                'link': f'https://www.youtube.com/watch?v={video_id}',
                'enclosure_url': raw_url,
                'enclosure_type': 'audio/mpeg',
                'enclosure_length': length,
                'pubdate': dt,
                'guid': guid,
            })
            channel_count += 1
            if channel_count >= max_items_per_source:
                break

    # Sort all items by publication date (descending)
    items.sort(key=lambda x: x['pubdate'], reverse=True)
    # Trim to the maximum total items
    items = items[:max_total_items]

    # Emit RSS feed
    for it in items:
        fe = fg.add_entry()
        fe.title(it['title'])
        fe.link(href=it['link'])
        fe.guid(it['guid'])
        fe.pubDate(format_pubdate(it['pubdate']))
        fe.enclosure(it['enclosure_url'], it['enclosure_length'], it['enclosure_type'])

    rss_bytes = fg.rss_str(pretty=True)
    with open('feed.xml', 'wb') as out:
        out.write(rss_bytes)
    print(f"Generated feed.xml with {len(items)} items")
    return 0


if __name__ == '__main__':
    sys.exit(main())