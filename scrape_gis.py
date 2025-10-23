import os, re, time, random, hashlib, tempfile, urllib.parse
from typing import Optional, List, Dict, Any, Tuple
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import (
    NoSuchElementException, TimeoutException, WebDriverException, SessionNotCreatedException
)
from webdriver_manager.chrome import ChromeDriverManager
EXCEL_PATH  = "Ташкент_рестораны.xlsx"
CITY_SLUG   = "tashkent"
BASE_DOMAIN = "https://2gis.uz"
OUT_PROGRESS = "2gis_reviews_progress.csv"
OUT_DIR      = "out"
CHUNK_SIZE   = 20
REQUESTS_PER_MIN       = 8
MAX_LOAD_STEPS         = 150
STAGNATION_ROUNDS      = 3
PER_CARD_HARD_TIMEOUT  = 200
WAIT_SMALL             = 0.5
HEADLESS               = False
PROFILE_DIR            = os.path.abspath("./chrome-profile-2gis")
PAGELOAD_STRATEGY      = "eager"
VERBOSE                = True
def log(msg: str):
    if VERBOSE:
        print(msg, flush=True)
def norm(s: Optional[str]) -> Optional[str]:
    if s is None: return None
    return re.sub(r"\s+", " ", str(s)).strip()
def only_digits(s: str) -> str:
    return re.sub(r"\D+", "", s or "")
def split_phones(raw: str) -> List[str]:
    if not raw: return []
    parts = re.split(r"[;,/|]+|\s{2,}", str(raw))
    return [p.strip() for p in parts if p.strip()]
def phone_variants(raw: str) -> List[str]:
    d = only_digits(raw)
    vs: List[str] = []
    if d.startswith("998") and len(d) >= 12:
        vs += [f"+{d}", d, d[-9:]]
    elif len(d) == 9:
        vs += [f"+998{d}", f"998{d}", d]
    else:
        vs.append(d)
        if len(d) >= 7: vs.append(d[-7:])
    out, seen = [], set()
    for v in vs:
        v = v.strip()
        if v and v not in seen:
            seen.add(v); out.append(v)
    return out
def pick_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    for c in candidates:
        if c in df.columns: return c
    low = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in low: return low[cand.lower()]
    return None
def id_prefix_from_full(full_id: Optional[str]) -> Optional[str]:
    if not full_id: return None
    s = str(full_id).strip()
    return s.split("_", 1)[0] if "_" in s else s
def firm_url(fid: str)  -> str: return f"{BASE_DOMAIN}/{CITY_SLUG}/firm/{fid}"
def branch_url(fid: str)-> str: return f"{BASE_DOMAIN}/{CITY_SLUG}/branch/{fid}"
def search_url(q: str, lon: Optional[float]=None, lat: Optional[float]=None) -> str:
    base = f"{BASE_DOMAIN}/{CITY_SLUG}/search/{urllib.parse.quote(q)}"
    if lon is not None and lat is not None:
        return f"{base}?m={lon}%2C{lat}%2F12"
    return base
def build_options(user_data_dir: Optional[str]) -> Options:
    opts = Options()
    if HEADLESS:
        opts.add_argument("--headless=new")
    opts.add_argument("--window-size=1280,1000")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--lang=ru-RU")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--no-first-run")
    opts.add_argument("--no-default-browser-check")
    opts.add_argument("--remote-debugging-port=0")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    opts.page_load_strategy = PAGELOAD_STRATEGY
    if user_data_dir:
        os.makedirs(user_data_dir, exist_ok=True)
        opts.add_argument(f"--user-data-dir={user_data_dir}")
    return opts
def start_chrome_with_fallback() -> webdriver.Chrome:
    try:
        service = Service(ChromeDriverManager().install())
        opts = build_options(PROFILE_DIR)
        drv = webdriver.Chrome(service=service, options=opts)
        return drv
    except (SessionNotCreatedException, WebDriverException) as e:
        log(f"!! Chrome с постоянным профилем не стартанул: {e}")
    tmp_dir = tempfile.mkdtemp(prefix="chrome-2gis-")
    log(f"Повторный запуск с временным профилем: {tmp_dir}")
    service = Service(ChromeDriverManager().install())
    opts = build_options(tmp_dir)
    drv = webdriver.Chrome(service=service, options=opts)
    return drv
def stable_get(driver, url: str, retries: int = 3, base_sleep: float = 0.6) -> bool:
    for attempt in range(1, retries + 1):
        try:
            driver.get(url)
            time.sleep(base_sleep + random.uniform(0.1, 0.2))
            return True
        except (TimeoutException, WebDriverException) as e:
            log(f"    • Ошибка открытия [{attempt}/{retries}]: {e}")
            time.sleep(base_sleep * (1.5 ** (attempt - 1)) + random.uniform(0.1, 0.3))
    return False
def safe_find(ctx, by, value):
    try: return ctx.find_element(by, value)
    except NoSuchElementException: return None
def safe_finds(ctx, by, value):
    try: return ctx.find_elements(by, value)
    except NoSuchElementException: return []
def search_page_collect_cards(driver) -> List[str]:
    links = safe_finds(driver, By.XPATH, "//a[contains(@href,'/firm/') or contains(@href,'/branch/')]")
    out, seen = [], set()
    for a in links[:120]:
        href = (a.get_attribute("href") or "").strip()
        if not href: continue
        m = re.search(r"(/(?:firm|branch)/\d+)", href)
        if not m: continue
        card = f"{BASE_DOMAIN}/{CITY_SLUG}{m.group(1)}"
        if card not in seen:
            seen.add(card); out.append(card)
    return out
def open_candidates_by_phone(driver, phones_raw: str, lon: Optional[float], lat: Optional[float]) -> List[str]:
    urls: List[str] = []
    for raw in split_phones(phones_raw):
        for q in phone_variants(raw):
            u = search_url(q, lon, lat)
            log(f"    → Поиск по телефону: {q} → {u}")
            if not stable_get(driver, u, retries=2): continue
            found = search_page_collect_cards(driver)
            log(f"      найдено карточек: {len(found)}")
            urls.extend(found)
    uniq, seen = [], set()
    for u in urls:
        if u not in seen: seen.add(u); uniq.append(u)
    return uniq
def open_candidates_by_id(fid_prefix: str) -> List[str]:
    return [firm_url(fid_prefix), branch_url(fid_prefix)]
def open_candidates_by_name(driver, name: str, lon: Optional[float], lat: Optional[float]) -> List[str]:
    u = search_url(name, lon, lat)
    log(f"    → Поиск по имени: {name} → {u}")
    if not stable_get(driver, u, retries=2):
        return []
    found = search_page_collect_cards(driver)
    log(f"      найдено карточек: {len(found)}")
    return found
def filter_by_prefix(urls: List[str], want_prefix: Optional[str]) -> List[str]:
    if not want_prefix: return urls
    def url_id_prefix(u: str) -> Optional[str]:
        m = re.search(r"/(?:firm|branch)/(\d+)", u)
        return m.group(1) if m else None
    filtered = [u for u in urls if url_id_prefix(u) == want_prefix]
    return filtered or urls
JS_FIND_REVIEWS_CONTAINER = r"""
return (function(){
  let container = null;
  try {
    const items = document.querySelectorAll('a._1msln3t');
    if (items.length) {
      let p = items[0];
      for (let i = 0; i < 12 && p; i++) {
        p = p.parentElement;
        if (!p) break;
        const rect = p.getBoundingClientRect();
        if (rect && rect.height > 150 && rect.width > 300) {
          const st = getComputedStyle(p);
          if ((st.overflowY === 'auto' || st.overflowY === 'scroll') && p.scrollHeight - p.clientHeight > 60) {
            container = p;
            break;
          }
        }
      }
    }
  } catch(e) {}
  if (!container) {
    const roots = [document];
    const tw = document.createTreeWalker(document, NodeFilter.SHOW_ELEMENT);
    let n = tw.currentNode;
    while(n) { if(n.shadowRoot) roots.push(n.shadowRoot); n = tw.nextNode(); }
    for (const r of roots) {
      const scrollables = r.querySelectorAll('[style*="overflow"],[class*="scroll"],[class*="review"]');
      for (const el of scrollables) {
        const st = getComputedStyle(el);
        if ((st.overflowY === 'auto' || st.overflowY === 'scroll') && el.scrollHeight - el.clientHeight > 60) {
          container = el;
          break;
        }
      }
      if (container) break;
    }
  }
  if (!container) {
    container = document.scrollingElement || document.body;
  }
  try { container.focus(); } catch(e) {}
  return container;
})();
"""
JS_EXTRACT_VISIBLE = r"""
return (function(){
  function ratingFromWidth(w){
    if(!w) return null;
    let m=(''+w).match(/([\d.]+)\s*px/i);
    let px = m ? parseFloat(m[1]) : parseFloat(w);
    if(!isFinite(px)) return null;
    let val = px/10;
    return (Math.round(val*10)/10).toString();
  }
  function extractReviewCount(text){
    if(!text) return null;
    let m = (''+text).match(/(\d+)/);
    return m ? parseInt(m[1]) : null;
  }
  const items = [];
  const roots = [document];
  const tw = document.createTreeWalker(document, NodeFilter.SHOW_ELEMENT);
  let n = tw.currentNode;
  while(n) { if(n.shadowRoot) roots.push(n.shadowRoot); n = tw.nextNode(); }
  for (const r of roots) {
    const texts = Array.from(r.querySelectorAll('a._1msln3t'));
    for (const el of texts) {
      let box = el, dateEl = null, rateEl = null, nameEl = null, reviewCountEl = null;
      for (let step = 0; step < 12 && box; step++) {
        if (!dateEl) dateEl = box.querySelector('div._a5f6uz,span._a5f6uz,time._a5f6uz,[class*="_a5f6uz"],[class*="date"],time');
        if (!rateEl) rateEl = box.querySelector('div._1fkin5c,[class*="rating"],[class*="stars"]');
        if (!nameEl) nameEl = box.querySelector('span._16s5yj36');
        if (!reviewCountEl) reviewCountEl = box.querySelector('span._89b5km');
        if (dateEl && rateEl && nameEl && reviewCountEl) break;
        box = box.parentElement;
      }
      const text = (el.textContent || '').trim();
      const date = dateEl ? (dateEl.textContent || '').trim() : null;
      const name = nameEl ? (nameEl.getAttribute('title') || nameEl.textContent || '').trim() : null;
      const reviewCount = reviewCountEl ? extractReviewCount(reviewCountEl.textContent) : null;
      let w = null;
      if (rateEl) {
        try {
          const cs = getComputedStyle(rateEl);
          w = cs && cs.width ? cs.width : (rateEl.getAttribute('style') || '');
        } catch(e) { w = rateEl.getAttribute('style') || ''; }
      }
      const rating = ratingFromWidth(w);
      if (text) items.push({text, date, rating, name, reviewCount});
    }
  }
  return items;
})();
"""
JS_SCROLL_TO_BOTTOM = r"""
return (function(container){
  if (!container) container = document.scrollingElement || document.body;
  let moved = false;
  try {
    const beforeHeight = container.scrollHeight;
    container.scrollTop = container.scrollHeight;
    container.dispatchEvent(new Event('scroll', {bubbles: true, cancelable: true}));
    container.dispatchEvent(new WheelEvent('wheel', {deltaY: container.clientHeight, bubbles: true, cancelable: true}));
    moved = container.scrollTop > 0 || container.scrollHeight > beforeHeight;
  } catch(e) {}
  return moved;
})(arguments[0]);
"""
JS_CLICK_SHOW_MORE = r"""
return (function(container){
  function inBottomHalf(el){
    try{
      const r = el.getBoundingClientRect();
      const h = (container && container.getBoundingClientRect ? container.getBoundingClientRect().height : window.innerHeight) || 0;
      return r.top > h*0.45;
    }catch(e){ return true; }
  }
  const rx = /(показать|ещ[её]|more|show|load|дал[её]е|больше)/i;
  let clicked = false;
  for (let attempt = 0; attempt < 5; attempt++) {
    const roots = [document];
    const tw = document.createTreeWalker(document, NodeFilter.SHOW_ELEMENT);
    let n = tw.currentNode;
    while(n) { if(n.shadowRoot) roots.push(n.shadowRoot); n = tw.nextNode(); }
    for (const r of roots) {
      const nodes = r.querySelectorAll('button, a, div[role="button"]');
      for (const b of nodes) {
        try {
          const t = (b.textContent || '').trim().toLowerCase();
          const ar = (b.getAttribute('aria-label') || '').toLowerCase();
          if (!(rx.test(t) || rx.test(ar))) continue;
          if (!inBottomHalf(b)) continue;
          b.click();
          clicked = true;
          (function(ms) {
            var start = Date.now();
            while(Date.now() - start < ms) {}
          })(1200);
        } catch(e) {}
      }
    }
    if (clicked) break;
  }
  return clicked;
})(arguments[0]);
"""
def extract_total_hint(driver) -> Optional[int]:
    for loc in [
        (By.XPATH, "//a[contains(@href,'reviews')]"),
        (By.XPATH, "//*[contains(text(),'Отзывы')]"),
        (By.XPATH, "//*[contains(@class,'reviews') or contains(@class,'count')]"),
    ]:
        el = safe_find(driver, *loc)
        if el:
            t = norm(el.text) or ""
            m = re.search(r"(\d[\d\s]*)", t)
            if m:
                try: return int(m.group(1).replace(" ",""))
                except: pass
    return None
def crawl_reviews_incremental(driver, total_hint: Optional[int]) -> List[Dict[str, Optional[str]]]:
    start = time.time()
    seen_keys: set[str] = set()
    collected: List[Dict[str, Optional[str]]] = []
    try:
        container = driver.execute_script(JS_FIND_REVIEWS_CONTAINER)
    except Exception:
        container = None
    try:
        body = driver.find_element(By.TAG_NAME, "body")
        body.send_keys(Keys.HOME); time.sleep(0.15)
        ActionChains(driver).move_by_offset(random.randint(100, 300), random.randint(100, 300)).perform()
    except Exception:
        pass
    try:
        review_elements = driver.execute_script("return document.querySelectorAll('a._1msln3t').length;")
        if not review_elements:
            log("      • Отзывы отсутствуют, пропускаем скроллинг")
            return []
    except Exception:
        log("      • Не удалось проверить наличие отзывов, продолжаем")
    try:
        batch = driver.execute_script(JS_EXTRACT_VISIBLE) or []
        for it in batch:
            txt = norm(it.get("text"))
            dt  = norm(it.get("date"))
            if not txt: continue
            key = hashlib.md5((txt + "|" + (dt or "")).encode("utf-8")).hexdigest()
            if key in seen_keys: continue
            seen_keys.add(key)
            collected.append({
                "review_id": key,
                "review_text": txt,
                "review_date": dt,
                "review_rating": norm(it.get("rating")),
                "reviewer_name": norm(it.get("name")),
                "reviewer_total_reviews": it.get("reviewCount"),
                "review_link": None,
                "reviewer_profile_url": None,
                "likes_count": None,
                "photos_count": None,
                "photos_urls": None,
                "owner_reply_text": None,
                "owner_reply_date": None,
            })
        log(f"      • Начальная загрузка: собрано {len(collected)}")
    except Exception:
        pass
    stable_iters = 0
    last_count = len(collected)
    for step in range(1, MAX_LOAD_STEPS + 1):
        if time.time() - start > PER_CARD_HARD_TIMEOUT:
            log("      • Таймаут по карточке — стоп")
            break
        try:
            batch = driver.execute_script(JS_EXTRACT_VISIBLE) or []
        except Exception:
            batch = []
        added = 0
        for it in batch:
            txt = norm(it.get("text"))
            dt  = norm(it.get("date"))
            if not txt: continue
            key = hashlib.md5((txt + "|" + (dt or "")).encode("utf-8")).hexdigest()
            if key in seen_keys: continue
            seen_keys.add(key)
            collected.append({
                "review_id": key,
                "review_text": txt,
                "review_date": dt,
                "review_rating": norm(it.get("rating")),
                "reviewer_name": norm(it.get("name")),
                "reviewer_total_reviews": it.get("reviewCount"),
                "review_link": None,
                "reviewer_profile_url": None,
                "likes_count": None,
                "photos_count": None,
                "photos_urls": None,
                "owner_reply_text": None,
                "owner_reply_date": None,
            })
            added += 1
        log(f"      • Шаг {step}: в DOM видно {len(batch)}, всего собрано {len(collected)} (+{added})")
        if total_hint and len(collected) >= total_hint:
            log("      • Достигнуто ожидаемое количество отзывов")
            break
        clicked = False
        try:
            clicked = bool(driver.execute_script(JS_CLICK_SHOW_MORE, container))
            if clicked:
                log("      • Кликнули «Ещё»")
                time.sleep(1.2 + random.uniform(0.1, 0.2))
        except Exception:
            pass
        try:
            moved = bool(driver.execute_script(JS_SCROLL_TO_BOTTOM, container))
            ActionChains(driver).move_by_offset(random.randint(-30, 30), random.randint(-30, 30)).perform()
            if random.random() < 0.2:
                driver.execute_script("window.scrollBy(0, -100);")
                time.sleep(0.1)
                driver.execute_script("window.scrollBy(0, 100);")
        except Exception:
            moved = False
        if not moved and not clicked:
            try:
                body = driver.find_element(By.TAG_NAME, "body")
                body.send_keys(Keys.PAGE_DOWN); time.sleep(0.1)
                body.send_keys(Keys.END); time.sleep(0.15)
            except Exception:
                pass
        wait_time = 1.5 + random.uniform(0.1, 0.2) if clicked else WAIT_SMALL + random.uniform(0.1, 0.2) + (0.2 if len(collected) > 50 else 0.0)
        time.sleep(wait_time)
        if len(collected) == last_count:
            stable_iters += 1
        else:
            stable_iters = 0
            last_count = len(collected)
        if stable_iters >= STAGNATION_ROUNDS:
            log(f"      • Стагнация ({stable_iters}/{STAGNATION_ROUNDS})")
            break
    try:
        for _ in range(3):
            clicked = bool(driver.execute_script(JS_CLICK_SHOW_MORE, container))
            if clicked:
                log("      • Финальный клик «Ещё»")
                time.sleep(1.2 + random.uniform(0.1, 0.2))
        for _ in range(3):
            driver.execute_script(JS_SCROLL_TO_BOTTOM, container)
            time.sleep(1.5 + random.uniform(0.1, 0.2))
        batch2 = driver.execute_script(JS_EXTRACT_VISIBLE) or []
        for it in batch2:
            txt = norm(it.get("text"))
            dt  = norm(it.get("date"))
            if not txt: continue
            key = hashlib.md5((txt + "|" + (dt or "")).encode("utf-8")).hexdigest()
            if key in seen_keys: continue
            seen_keys.add(key)
            collected.append({
                "review_id": key,
                "review_text": txt,
                "review_date": dt,
                "review_rating": norm(it.get("rating")),
                "reviewer_name": norm(it.get("name")),
                "reviewer_total_reviews": it.get("reviewCount"),
                "review_link": None,
                "reviewer_profile_url": None,
                "likes_count": None,
                "photos_count": None,
                "photos_urls": None,
                "owner_reply_text": None,
                "owner_reply_date": None,
            })
        log(f"      • Финальный добор: всего {len(collected)}")
    except Exception:
        pass
    return collected
ID_COLS   = ["id", "firm_id", "2gis_id"]
NAME_COLS = ["name", "Название", "title"]
PHONE_COLS= ["phones", "phone", "телефон", "номер", "phone_number", "contacts"]
LAT_COLS  = ["lat", "latitude"]
LON_COLS  = ["lon", "longitude", "lng"]
def save_progress(rows: List[Dict[str, Any]], chunk_idx: int):
    if not os.path.isdir(OUT_DIR):
        os.makedirs(OUT_DIR, exist_ok=True)
    pd.DataFrame(rows).to_csv(OUT_PROGRESS, index=False, encoding="utf-8-sig")
    if rows:
        chunk_path = os.path.join(OUT_DIR, f"2gis_reviews_chunk_{chunk_idx:03}.csv")
        pd.DataFrame(rows).to_csv(chunk_path, index=False, encoding="utf-8-sig")
        log(f"💾 Сохранено: {OUT_PROGRESS} и {chunk_path}")
def main():
    print("Файл загружен, вызываю main() ...", flush=True)
    if not os.path.exists(EXCEL_PATH):
        print(f"ОШИБКА: файл не найден: {EXCEL_PATH}", flush=True)
        return
    df = pd.read_excel(EXCEL_PATH)
    print("=== Старт парсера 2ГИС (отзывы) ===")
    print(f"Excel загружен: {EXCEL_PATH}, строк: {len(df)}")
    print(f"Колонки: {list(df.columns)}")
    id_col   = pick_col(df, ID_COLS)
    name_col = pick_col(df, NAME_COLS)
    phone_col= pick_col(df, PHONE_COLS)
    lat_col  = pick_col(df, LAT_COLS)
    lon_col  = pick_col(df, LON_COLS)
    print(f"Опознаны колонки → id:{id_col}, name:{name_col}, phone:{phone_col}, lat:{lat_col}, lon:{lon_col}")
    driver = start_chrome_with_fallback()
    driver.set_page_load_timeout(30)
    print("Браузер запущен.\n")
    out_rows: List[Dict[str, Any]] = []
    processed_rows = 0
    chunk_idx = 0
    seen_card_urls: set[str] = set()
    try:
        for i, row in df.iterrows():
            t0 = time.time()
            id_full = str(row[id_col]).strip() if id_col and pd.notna(row[id_col]) else None
            id_pref = id_prefix_from_full(id_full) if id_full else None
            name    = str(row[name_col]).strip() if name_col and pd.notna(row[name_col]) else None
            phones  = str(row[phone_col]).strip() if phone_col and pd.notna(row[phone_col]) else None
            lat     = float(row[lat_col]) if lat_col and pd.notna(row[lat_col]) else None
            lon     = float(row[lon_col]) if lon_col and pd.notna(row[lon_col]) else None
            print(f"[{i+1}/{len(df)}] ► {name or id_pref or phones}")
            candidates: List[str] = []
            if phones:
                found = open_candidates_by_phone(driver, phones, lon, lat)
                found = filter_by_prefix(found, id_pref)
                print(f"  Кандидаты по телефону (после фильтра по prefix): {len(found)}")
                candidates += found
            if id_pref:
                candidates += open_candidates_by_id(id_pref)
                print("  Добавлены кандидаты по id-prefix (firm/branch).")
            if name:
                found = open_candidates_by_name(driver, name, lon, lat)
                found = filter_by_prefix(found, id_pref)
                print(f"  Кандидаты по name (после фильтра по prefix): {len(found)}")
                candidates += found
            uniq, seen = [], set()
            for u in candidates:
                if u.startswith(f"{BASE_DOMAIN}/{CITY_SLUG}/") and u not in seen:
                    seen.add(u); uniq.append(u)
            candidates = uniq
            print(f"  Итого уникальных карточек: {len(candidates)}")
            total_reviews_for_row = 0
            hits = 0
            for base_url in candidates:
                if base_url in seen_card_urls:
                    log(f"    - уже посещали: {base_url}")
                    continue
                started = time.time()
                if not stable_get(driver, base_url, retries=3):
                    log(f"    - не открылось: {base_url}")
                    continue
                final_url = driver.current_url
                if final_url.rstrip("/") == f"{BASE_DOMAIN}/{CITY_SLUG}":
                    log("    - редирект на главную, пропуск")
                    continue
                seen_card_urls.add(final_url)
                log(f"    Открыта карточка: {final_url}")
                jumped = False
                for suf in ("/tab/reviews", "/reviews"):
                    if stable_get(driver, final_url.rstrip("/") + suf, retries=2, base_sleep=0.5):
                        jumped = True
                        log(f"    Перейдено на: {driver.current_url}")
                        break
                if not jumped:
                    el = safe_find(driver, By.XPATH, "//div[@role='tab' and contains(normalize-space(),'Отзывы')]")
                    if el:
                        try:
                            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
                            time.sleep(0.15); el.click(); time.sleep(0.5)
                            ActionChains(driver).move_by_offset(random.randint(-30, 30), random.randint(-30, 30)).perform()
                        except Exception:
                            pass
                try:
                    body = driver.find_element(By.TAG_NAME, "body")
                    body.send_keys(Keys.END); time.sleep(0.2)
                    body.send_keys(Keys.HOME); time.sleep(0.15)
                except Exception:
                    pass
                total_hint = extract_total_hint(driver)
                reviews = crawl_reviews_incremental(driver, total_hint)
                if not reviews and total_hint and total_hint > 0:
                    reviews = crawl_reviews_incremental(driver, total_hint)
                if not reviews:
                    with open(f"debug_{i}_{hits}.html", "w", encoding="utf-8") as f:
                        f.write(driver.page_source)
                    try: driver.save_screenshot(f"debug_{i}_{hits}.png")
                    except Exception: pass
                    log(f"    ⚠ debug сохранён: debug_{i}_{hits}.html/png")
                    out_rows.append({
                        "src_row_index": i, "firm_id": id_pref, "org_name": name,
                        "two_gis_url": final_url,
                        "rating_value": None,
                        "rating_reviews": total_hint,
                        "review_id": None, "review_date": None, "review_rating": None,
                        "reviewer_name": None, "reviewer_total_reviews": None,
                        "reviewer_profile_url": None,
                        "review_text": None, "likes_count": None, "photos_count": None,
                        "photos_urls": None, "owner_reply_text": None, "owner_reply_date": None,
                        "review_link": None, "error": "0 reviews (virtualized)",
                    })
                    log("    Отзывов: 0")
                else:
                    for r in reviews:
                        out_rows.append({
                            "src_row_index": i, "firm_id": id_pref, "org_name": name,
                            "two_gis_url": final_url,
                            "rating_value": None,
                            "rating_reviews": total_hint,
                            **r, "error": None,
                        })
                    total_reviews_for_row += len(reviews)
                    if total_hint and len(reviews) < total_hint:
                        with open(f"debug_{i}_{hits}_incomplete.html", "w", encoding="utf-8") as f:
                            f.write(driver.page_source)
                        try: driver.save_screenshot(f"debug_{i}_{hits}_incomplete.png")
                        except Exception: pass
                        log(f"    ⚠ debug сохранён (неполный сбор): debug_{i}_{hits}_incomplete.html/png")
                    log(f"    Отзывов собрано: {len(reviews)} (ожидалось: {total_hint or '—'})")
                hits += 1
                if time.time() - started > PER_CARD_HARD_TIMEOUT:
                    log("    • пер-карточный таймаут — к след.")
                    break
                time.sleep(random.uniform(0.2, 0.4))
            processed_rows += 1
            if processed_rows % CHUNK_SIZE == 0:
                save_progress(out_rows, processed_rows // CHUNK_SIZE)
            elapsed = time.time() - t0
            min_interval = 60.0 / max(1, REQUESTS_PER_MIN)
            time.sleep(max(0.0, min_interval - elapsed) + random.uniform(0.1, 0.3))
            print(f"  ► ИТОГО по строке: карточек {hits}, отзывов {total_reviews_for_row}")
    finally:
        try: driver.quit()
        except Exception: pass
    if processed_rows % CHUNK_SIZE != 0:
        save_progress(out_rows, processed_rows // CHUNK_SIZE + 1)
    else:
        pd.DataFrame(out_rows).to_csv(OUT_PROGRESS, index=False, encoding="utf-8-sig")
    print(f"\nГотово. Всего строк в прогрессе: {len(out_rows)} → {OUT_PROGRESS}")
    print(f"Частями см. в каталоге: {OUT_DIR}\\2gis_reviews_chunk_*.csv")
if __name__ == "__main__":
    main()