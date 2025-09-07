#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
生成聚合 feed.xml，并把 YouTube 频道的视频转为 mp3 保存到 youtube_audio/ 目录。
- 读取配置：sources.yml
- 聚合：podcasts（必须有音频 enclosure） + YouTube（下载音频）
- 只在“文件真实存在”时写入 <item>，避免空链接
"""

import os
import re
import sys
import time
import yaml
import pytz
import hashlib
import feedparser
from datetime import datetime
from pathlib import Path

try:
    from yt_dlp import YoutubeDL
except ImportError:
    YoutubeDL = None

from feedgen.feed import FeedGenerator
import email.utils

# -----------------------------
# Helpers
# -----------------------------

REPO = os.getenv("GITHUB_REPOSITORY", "LLNA3312/my-rss-feed")
AUDIO_ROOT = Path("youtube_audio")
CACHE_DIR = Path(".cache")
CACHE_DIR.mkdir(exist_ok=True, parents=True)

def fmt_rfc2822(dt: datetime) -> str:
    return email.utils.format_datetime(dt)

def make_guid(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

def is_audio_link(link) -> bool:
    if not link:
        return False
    t = (link.get("type") or "").lower()
    href = (link.get("href") or "").lower()
    return (t.startswith("audio/") or href.endswith(".mp3") or href.endswith(".m4a") or href.endswith(".aac") or href.endswith(".ogg"))

def pick_audio_enclosure(links):
    if not links:
        return None
    for l in links:
        if is_audio_link(l):
            return l
    return None

def slug_from_channel_url(url: str) -> str:
    """
    生成一个目录名（slug），优先用 handle 或 channel id，兜底哈希。
    """
    url = url.strip()
    m = re.search(r"youtube\.com/(?:@[^/?#]+|channel/[^/?#]+|c/[^/?#]+)", url)
    if m:
        slug = m.group(0).split("/", 1)[-1].replace("@", "")
        slug = re.sub(r"[^a-zA-Z0-9_-]+", "", slug)
        if slug:
            return slug.lower()
    return "ch_" + make_guid(url)[:10]

def list_recent_videos(channel_url: str, limit: int):
    """
    用 yt-dlp 列出频道最近的若干条视频（只取 id/title/upload_date）
    """
    if not YoutubeDL:
        print("[ERROR] yt-dlp is not installed", file=sys.stderr)
        return []

    # 使用 extract_flat 避免真正下载；playlistend 控制数量
    ydl_opts = {
        "quiet": True,
        "noprogress": True,
        "extract_flat": "in_playlist",
        "playlistend": max(1, int(limit)),
        "skip_download": True,
        "ignoreerrors": True,
        "retries": 3,
    }

    url = channel_url
    # 直接给频道 URL 即可，yt-dlp 会解析主页 feed；
    # 如不稳定，可改为 f"{channel_url}/videos"
    try:
        with YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)
    except Exception as e:
        print(f"[WARN] list videos failed: {e}")
        return []

    entries = []
    if not info:
        return entries

    # info 可能是 playlist（有 entries）或者单条
    raw = info.get("entries") or []
    for e in raw:
        if not e:
            continue
        vid = e.get("id")
        title = e.get("title") or "(no title)"
        # upload_date 可能是 "20250907"
        up = e.get("upload_date")
        dt = None
        if up and len(up) == 8:
            try:
                dt = datetime.strptime(up, "%Y%m%d")
            except Exception:
                dt = None
        if not dt:
            dt = datetime.utcnow()
        entries.append({"id": vid, "title": title, "dt": dt})
        if len(entries) >= limit:
            break
    return entries

def download_youtube_audio(video_id: str, channel_slug: str) -> Path | None:
    """
    下载指定视频的音频为 mp3；返回文件路径（存在）或 None
    """
    if not YoutubeDL:
        print("[ERROR] yt-dlp is not installed", file=sys.stderr)
        return None

    outdir = AUDIO_ROOT / channel_slug
    outdir.mkdir(parents=True, exist_ok=True)

    archive_path = CACHE_DIR / f"{channel_slug}.downloaded.txt"
    outtmpl = str(outdir / f"{video_id}.%(ext)s")

    ydl_opts = {
        "format": "bestaudio/best",
        "outtmpl": outtmpl,
        "postprocessors": [
            {"key": "FFmpegExtractAudio", "preferredcodec": "mp3", "preferredquality": "192"},
            {"key": "FFmpegMetadata"},
        ],
        "quiet": True,
        "noprogress": True,
        "ignoreerrors": True,
        "continuedl": True,
        "retries": 3,
        "download_archive": str(archive_path),
    }

    try:
        with YoutubeDL(ydl_opts) as ydl:
            ydl.download([f"https://www.youtube.com/watch?v={video_id}"])
    except Exception as e:
        print(f"[WARN] download failed for {video_id}: {e}")

    mp3_path = outdir / f"{video_id}.mp3"
    return mp3_path if mp3_path.exists() else None

# -----------------------------
# Build feed.xml
# -----------------------------

def main():
    with open("sources.yml", "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}

    title = cfg.get("title", "My Aggregated Feed")
    link = cfg.get("link", f"https://{REPO.split('/')[0]}.github.io/{REPO.split('/')[1]}/")
    description = cfg.get("description", "")
    tz = pytz.timezone(cfg.get("timezone", "UTC"))
    max_total = int(cfg.get("max_total_items", 100))
    max_each = int(cfg.get("max_items_per_source", 10))
    global_max_videos = int(cfg.get("max_videos_per_channel", 3))

    fg = FeedGenerator()
    fg.load_extension("podcast")

    fg.title(title)
    fg.link(href=link, rel="alternate")
    fg.description(description)
    fg.language("zh-CN")

    items = []

    # ---- podcasts
    for u in cfg.get("podcasts", []) or []:
        print(f"[info] fetch podcast: {u}")
        d = feedparser.parse(u)
        cnt = 0
        for e in d.entries:
            enc = pick_audio_enclosure(getattr(e, "links", []))
            if not enc:
                continue
            # 时间
            if getattr(e, "published_parsed", None):
                dt = datetime.fromtimestamp(time.mktime(e.published_parsed), tz)
            elif getattr(e, "updated_parsed", None):
                dt = datetime.fromtimestamp(time.mktime(e.updated_parsed), tz)
            else:
                dt = datetime.now(tz)
            guid = make_guid((getattr(e, "id", "") or getattr(e, "link", "") or enc.get("href", "")) + "pod")
            items.append({
                "title": getattr(e, "title", "Untitled"),
                "link": getattr(e, "link", u),
                "enclosure_url": enc.get("href"),
                "enclosure_type": enc.get("type") or "audio/mpeg",
                "enclosure_length": enc.get("length") or "0",
                "pubdate": dt,
                "guid": guid,
            })
            cnt += 1
            if cnt >= max_each:
                break

    # ---- youtube
    for entry in cfg.get("youtube_channels", []) or []:
        channel_url = (entry.get("url") if isinstance(entry, dict) else entry).strip()
        max_videos = int(entry.get("max_videos", global_max_videos)) if isinstance(entry, dict) else global_max_videos
        slug = slug_from_channel_url(channel_url)
        print(f"[info] youtube channel: {channel_url} -> slug={slug} (max={max_videos})")

        videos = list_recent_videos(channel_url, max_videos)
        for v in videos:
            vid = v.get("id")
            if not vid:
                continue
            title = v.get("title") or "(no title)"
            dt = v.get("dt") or datetime.utcnow()

            mp3_path = download_youtube_audio(vid, slug)
            if not mp3_path:
                print(f"[skip] no audio for video {vid}")
                continue

            try:
                length = str(mp3_path.stat().st_size)
            except Exception:
                length = "0"

            raw_url = f"https://raw.githubusercontent.com/{REPO}/main/{mp3_path.as_posix()}"
            guid = make_guid(f"youtube-{vid}-{slug}")

            items.append({
                "title": title,
                "link": f"https://www.youtube.com/watch?v={vid}",
                "enclosure_url": raw_url,
                "enclosure_type": "audio/mpeg",
                "enclosure_length": length,
                "pubdate": dt,
                "guid": guid,
            })

            if len(items) >= max_total:
                break

    # 排序截断
    items.sort(key=lambda x: x["pubdate"], reverse=True)
    items = items[:max_total]

    # 写入 feed
    for it in items:
        fe = fg.add_entry()
        fe.title(it["title"])
        fe.link(href=it["link"])
        fe.guid(it["guid"])
        # 统一转为 tz 的时间
        if not isinstance(it["pubdate"], datetime):
            dt = datetime.utcnow()
        else:
            dt = it["pubdate"]
        fe.pubDate(fmt_rfc2822(dt))
        fe.enclosure(it["enclosure_url"], it["enclosure_length"], it["enclosure_type"])

    out = fg.rss_str(pretty=True)
    with open("feed.xml", "wb") as f:
        f.write(out)

    print(f"[done] feed.xml generated. items={len(items)}")

if __name__ == "__main__":
    main()
