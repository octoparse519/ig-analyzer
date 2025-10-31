import asyncio, json, re, sys, csv, pathlib, random, time
from dataclasses import dataclass, asdict
from typing import List, Dict, Any, Optional
from bs4 import BeautifulSoup
import pandas as pd
from playwright.async_api import async_playwright

UA_LIST = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_2) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.3 Safari/605.1.15",
]

def safe_float(v):
    try: return float(v)
    except: return None

def safe_pct(v):
    try: return f"{float(v)*100:.2f}%"
    except: return "n/a"

def extract_numbers(s):
    if not s: return None
    m = re.findall(r"([\d,.]+)", s.replace(",", ""))
    if not m: return None
    return int(float(m[0]))

async def scrape_profile(url: str, limit_posts: int = 12):

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=[
            "--disable-blink-features=AutomationControlled",
            "--no-sandbox",
            "--disable-setuid-sandbox"
        ])

        context = await browser.new_context(
            user_agent=random.choice(UA_LIST),
            viewport={"width": 1440, "height": 900},
            locale="en-US"
        )

        page = await context.new_page()
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_timeout(3000)

        # try clicking cookie banners
        try:
            await page.locator("button", has_text=re.compile("Accept|Allow|Essential")).click(timeout=2000)
        except: pass

        html = await page.content()
        soup = BeautifulSoup(html, "lxml")

        # Profile stats by scraping visible page text
        followers = extract_numbers(soup.text.split("followers")[0].split()[-1]) if "followers" in soup.text else None
        following = extract_numbers(soup.text.split("following")[0].split()[-1]) if "following" in soup.text else None
        posts = extract_numbers(soup.text.split("posts")[0].split()[-1]) if "posts" in soup.text else None

        # Fallback: look in meta tags
        for meta in soup.find_all("meta"):
            c = str(meta.get("content", ""))
            if "Followers" in c:
                try:
                    parts = c.split("Followers")[0].strip().split()[-1]
                    followers = extract_numbers(parts)
                except: pass

        post_links = []
        for a in soup.find_all("a", href=True):
            if a["href"].startswith("/p/"):
                post_links.append("https://www.instagram.com" + a["href"])
        post_links = list(dict.fromkeys(post_links))[:limit_posts]

        posts_data = []
        for link in post_links:
            try:
                await page.goto(link, wait_until="domcontentloaded", timeout=60000)
                await page.wait_for_timeout(2500)
                post_html = await page.content()
                post_soup = BeautifulSoup(post_html, "lxml")

                likes = comments = None

                # extract "Liked by" text
                like_el = post_soup.find("span", string=re.compile("likes|Likes|liked"))
                if like_el:
                    likes = extract_numbers(like_el.text)

                comment_count = len(post_soup.find_all("ul", {"class": re.compile("Mr508|XQXOT")}))
                if comment_count > 0:
                    comments = comment_count

                posts_data.append({
                    "url": link,
                    "likes": likes,
                    "comments": comments,
                    "engagement": safe_float((likes or 0 + comments or 0) / followers) if followers else None
                })

            except Exception:
                posts_data.append({"url": link, "likes": None, "comments": None, "engagement": None})

        avg_likes = safe_float(sum((p.get("likes") or 0) for p in posts_data) / len(posts_data)) if posts_data else None
        avg_comments = safe_float(sum((p.get("comments") or 0) for p in posts_data) / len(posts_data)) if posts_data else None

        avg_eng = None
        eng_vals = [p.get("engagement") for p in posts_data if p.get("engagement") is not None]
        if eng_vals:
            avg_eng = sum(eng_vals) / len(eng_vals)

        return {
            "profile_url": url,
            "followers": followers,
            "following": following,
            "posts_total": posts,
            "posts_sampled": len(posts_data),
            "avg_like_estimate": avg_likes,
            "avg_comment_estimate": avg_comments,
            "avg_engagement_estimate": avg_eng,
            "posts": posts_data
        }

def save_reports(result: Dict[str, Any], outdir: str = "reports"):
    pathlib.Path(outdir).mkdir(parents=True, exist_ok=True)
    json_path = pathlib.Path(outdir) / "summary.json"
    csv_summary = pathlib.Path(outdir) / "summary.csv"
    csv_posts = pathlib.Path(outdir) / "posts.csv"

    json_path.write_text(json.dumps(result, indent=2))

    with open(csv_summary, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["profile_url","followers","following","posts_total","posts_sampled","avg_like_estimate","avg_comment_estimate","avg_engagement_estimate"])
        w.writerow([
            result.get("profile_url"),
            result.get("followers"),
            result.get("following"),
            result.get("posts_total"),
            result.get("posts_sampled"),
            result.get("avg_like_estimate"),
            result.get("avg_comment_estimate"),
            result.get("avg_engagement_estimate")
        ])

    pd.DataFrame(result["posts"]).to_csv(csv_posts, index=False)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python ig_analyze.py <profile_url> <limit_posts>")
        sys.exit(0)
    url = sys.argv[1]
    limit = int(sys.argv[2]) if len(sys.argv) > 2 else 12
    res = asyncio.run(scrape_profile(url, limit_posts=limit))
    save_reports(res)
    print(f"\nProfile: {res.get('profile_url')}")
    print(f"Followers: {res.get('followers')} | Following: {res.get('following')} | Posts: {res.get('posts_total')}")
    print(f"Avg likes: {res.get('avg_like_estimate')} | Avg comments: {res.get('avg_comment_estimate')}")
    print(f"Engagement: {safe_pct(res.get('avg_engagement_estimate'))}")
