import asyncio, json, re, sys, csv, pathlib
from dataclasses import dataclass, asdict
from typing import List, Dict, Any, Optional
from bs4 import BeautifulSoup
import pandas as pd
from playwright.async_api import async_playwright

JSON_PATTERNS = [
    r"window\._sharedData\s*=\s*({.*?});</script>",
    r"\"deployment_stage\":.*?({.*?\"__typename\":\"GraphProfile.*?})",
    r"<script type=\"application/json\"[^>]*>(\{.*?\})</script>",
]

def find_json_blobs(html: str) -> List[Dict[str, Any]]:
    blobs = []
    for pat in JSON_PATTERNS:
        for m in re.finditer(pat, html, re.S | re.I):
            try:
                text = m.group(1)
                if text.endswith("</script>"):
                    text = text[:-9]
                blobs.append(json.loads(text))
            except Exception:
                pass
    return blobs

def extract_profile_counts(blobs: List[Dict[str, Any]]) -> Dict[str, Optional[int]]:
    followers = following = posts = None
    for b in blobs:
        try:
            user = (
                b.get("entry_data", {})
                 .get("ProfilePage", [{}])[0]
                 .get("graphql", {})
                 .get("user", {})
            )
            if user:
                followers = followers or user.get("edge_followed_by", {}).get("count")
                following = following or user.get("edge_follow", {}).get("count")
                posts = posts or user.get("edge_owner_to_timeline_media", {}).get("count")
        except Exception:
            pass
    return {"followers": followers, "following": following, "posts": posts}

def extract_recent_post_urls(html: str, limit: int = 12) -> List[str]:
    soup = BeautifulSoup(html, "lxml")
    urls = []
    for a in soup.select("a[href^='/p/']"):
        u = a.get("href")
        if u and u.startswith("/p/"):
            urls.append("https://www.instagram.com" + u)
    seen, out = set(), []
    for u in urls:
        if u not in seen:
            out.append(u); seen.add(u)
    return out[:limit]

def extract_like_comment_from_post_html(html: str) -> Dict[str, Optional[int]]:
    likes = comments = None
    for b in find_json_blobs(html):
        try:
            media = (
                b.get("entry_data", {})
                 .get("PostPage", [{}])[0]
                 .get("graphql", {})
                 .get("shortcode_media", {})
            )
            if media:
                likes = likes or media.get("edge_media_preview_like", {}).get("count")
                comments = comments or media.get("edge_media_to_parent_comment", {}).get("count")
        except Exception:
            pass
    if likes is None:
        m = re.search(r"\"edge_media_preview_like\":\s*\{\"count\":\s*(\d+)\}", html)
        if m: likes = int(m.group(1))
    if comments is None:
        m = re.search(r"\"edge_media_to_parent_comment\":\s*\{\"count\":\s*(\d+)\}", html)
        if m: comments = int(m.group(1))
    return {"likes": likes, "comments": comments}

@dataclass
class PostStat:
    url: str
    likes: Optional[int]
    comments: Optional[int]
    engagement: Optional[float]

async def scrape_profile(url: str, limit_posts: int = 12, timeout: int = 30000) -> Dict[str, Any]:
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        ctx = await browser.new_context(user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"))
        page = await ctx.new_page()
        await page.goto(url, wait_until="domcontentloaded", timeout=timeout)
        try:
            await page.get_by_role("button", name=re.compile("Only allow essential|Accept|Allow", re.I)).click(timeout=3000)
        except Exception:
            pass
        await page.wait_for_timeout(2500)
        html = await page.content()
        blobs = find_json_blobs(html)
        counts = extract_profile_counts(blobs)

        post_urls = extract_recent_post_urls(html, limit_posts)
        posts: List[PostStat] = []
        followers = counts.get("followers") or 0

        for i, pu in enumerate(post_urls):
            try:
                await page.wait_for_timeout(1200 + (i % 3)*250)
                await page.goto(pu, wait_until="domcontentloaded", timeout=timeout)
                await page.wait_for_timeout(2000)
                phtml = await page.content()
                lc = extract_like_comment_from_post_html(phtml)
                likes, comments = lc["likes"], lc["comments"]
                engagement = None
                if followers and (likes is not None or comments is not None):
                    engagement = ((likes or 0) + (comments or 0)) / followers
                posts.append(PostStat(url=pu, likes=likes, comments=comments, engagement=engagement))
            except Exception:
                posts.append(PostStat(url=pu, likes=None, comments=None, engagement=None))

        await ctx.close(); await browser.close()

    valid = [p for p in posts if p.engagement is not None]
    avg_eng = float(sum(p.engagement for p in valid)/len(valid)) if valid else None
    avg_likes = float(sum((p.likes or 0) for p in posts)/len(posts)) if posts else None
    avg_comments = float(sum((p.comments or 0) for p in posts)/len(posts)) if posts else None

    return {
        "profile_url": url,
        "followers": counts.get("followers"),
        "following": counts.get("following"),
        "posts_total": counts.get("posts"),
        "posts_sampled": len(posts),
        "avg_like_estimate": avg_likes,
        "avg_comment_estimate": avg_comments,
        "avg_engagement_estimate": avg_eng,
        "posts": [asdict(p) for p in posts],
    }

def save_reports(result: Dict[str, Any], outdir: str = "reports"):
    pathlib.Path(outdir).mkdir(parents=True, exist_ok=True)
    (pathlib.Path(outdir) / "summary.json").write_text(json.dumps(result, indent=2))
    with open(pathlib.Path(outdir)/"summary.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["profile_url","followers","following","posts_total","posts_sampled","avg_like_estimate","avg_comment_estimate","avg_engagement_estimate"])
        w.writerow([
            result.get("profile_url"), result.get("followers"), result.get("following"),
            result.get("posts_total"), result.get("posts_sampled"),
            result.get("avg_like_estimate"), result.get("avg_comment_estimate"),
            result.get("avg_engagement_estimate")
        ])
    pd.DataFrame(result["posts"]).to_csv(pathlib.Path(outdir)/"posts.csv", index=False)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python scripts/ig_analyze.py <profile_url> [limit_posts]")
        sys.exit(2)
    url = sys.argv[1].strip()
    limit = int(sys.argv[2]) if len(sys.argv) > 2 else 12
    res = asyncio.run(scrape_profile(url, limit_posts=limit))
    save_reports(res)
    ae = res.get("avg_engagement_estimate")
    pct = f"{ae*100:.2f}%" if isinstance(ae, (int, float)) else "n/a"
    print(f"\nProfile: {res.get('profile_url')}")
    print(f"Followers: {res.get('followers')} | Following: {res.get('following')} | Posts: {res.get('posts_total')}")
    print(f"Sampled: ${res.get('posts_sampled')} | Avg likes: {res.get('avg_like_estimate'):.2f} | Avg comments: {res.get('avg_comment_estimate'):.2f}")
    print(f"Estimated avg engagement (sample): {pct}\n")
