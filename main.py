import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from urllib.parse import urlencode

import requests
from tqdm import tqdm


def log(msg, level="INFO"):
    """带时间戳的日志"""
    ts = datetime.now().strftime("%H:%M:%S")
    prefix = {"INFO": "  ", "OK": "  ✓", "WARN": "  !", "ERR": "  ✗"}.get(level, "  ")
    print(f"[{ts}] {prefix} {msg}")


# 下载用请求头（与 API 一致）
HEADERS = {
    'Accept': 'application/json',
    'Accept-Language': 'en',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'Pragma': 'no-cache',
    'Referer': 'https://bdif.amf-france.org/en?societes=RS00003416_UBISOFT%20ENTERTAINMENT',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
    'sec-ch-ua': '"Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"'
}


def _to_api_date(s, end_of_day=False):
    """简单日期 -> API 格式。支持 2024-12-31 / 2024/12/31 / 20241231"""
    if not s or not s.strip():
        return None
    s = s.strip().replace("/", "-").replace(" ", "")
    if len(s) == 8 and s.isdigit():
        s = f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    try:
        d = datetime.strptime(s[:10], "%Y-%m-%d")
        if end_of_day:
            return d.strftime("%Y-%m-%dT23:59:59.000Z")
        return d.strftime("%Y-%m-%dT00:00:00.000Z")
    except ValueError:
        return None


def _build_informations_params(startNum, pageSize, RechercheTexte=None, DateDebut=None, DateFin=None, Jetons=None):
    """构建 informations 接口的查询参数"""
    params = {"From": startNum, "Size": pageSize}
    if Jetons:
        params["Jetons"] = Jetons
    if RechercheTexte:
        params["RechercheTexte"] = RechercheTexte
    if DateDebut:
        params["DateDebut"] = DateDebut
    if DateFin:
        params["DateFin"] = DateFin
    return params


def get_res_total(startNum, pageSize, RechercheTexte=None, DateDebut=None, DateFin=None, Jetons=None):
    params = _build_informations_params(startNum, pageSize, RechercheTexte, DateDebut, DateFin, Jetons)
    url = "https://bdif.amf-france.org/back/api/v1/informations?" + urlencode(params)
    response = requests.get(url, headers=HEADERS)
    return response.json()['total']


def get_res_pdfs(startNum, pageSize, RechercheTexte=None, DateDebut=None, DateFin=None, Jetons=None):
    params = _build_informations_params(startNum, pageSize, RechercheTexte, DateDebut, DateFin, Jetons)
    url = "https://bdif.amf-france.org/back/api/v1/informations?" + urlencode(params)
    response = requests.get(url, headers=HEADERS)
    return response.json()


def download_pdf(doc, save_dir, session=None, show_progress=False, if_exists="overwrite"):
    """下载单个 PDF。if_exists: 'overwrite' 覆盖已存在, 'skip' 跳过。返回 True=已下载, False=已跳过。"""
    filename = doc['nomFichier']
    filepath = os.path.join(save_dir, filename)
    if if_exists == "skip" and os.path.isfile(filepath):
        return False
    url = "https://bdif.amf-france.org/back/api/v1/documents/" + doc['path']
    req = session or requests
    r = req.get(url, headers=HEADERS, stream=True)
    r.raise_for_status()
    total = int(r.headers.get('content-length', 0))
    with open(filepath, 'wb') as f:
        if show_progress:
            with tqdm(total=total, unit='B', unit_scale=True, unit_divisor=1024,
                      desc=filename[:40], leave=False) as pbar:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        pbar.update(len(chunk))
        else:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
    return True


if __name__ == '__main__':
    # ========== 总设置参数 ==========
    workers = 10  # 下载并发数，建议3~5
    if_exists = "skip"  # 文件已存在时: "skip" 跳过  |  "overwrite" 覆盖
    pageSize = 20
    Jetons = None  # 暂不需要
    RechercheTexte = "Ubisoft"
    DateDebut = "2025-01-01"
    DateFin = "2026-02-06"
    # ================================

    DateDebut = _to_api_date(DateDebut, end_of_day=False) if DateDebut else None
    DateFin = _to_api_date(DateFin, end_of_day=True) if DateFin else None

    total = get_res_total(0, pageSize, RechercheTexte, DateDebut, DateFin, Jetons)
    total_pages = (total + pageSize - 1) // pageSize
    save_dir = f"{date.today()}_{RechercheTexte or 'all'}"
    os.makedirs(save_dir, exist_ok=True)

    log(f"{save_dir}  |  本次查到 {total} 条  |  共 {total_pages} 页  |  {workers} 线程  |  已存在:{if_exists}")

    total_ok, total_skip, total_fail = 0, 0, 0
    start_time = time.perf_counter()
    session = requests.Session() if workers <= 1 else None

    def do_one(doc):
        return download_pdf(doc, save_dir, session, show_progress=(workers <= 1), if_exists=if_exists)

    with tqdm(total=total_pages, desc=f"下载({workers}线程)", unit="页", leave=True) as page_bar:
        for page_idx in range(total_pages):
            start = page_idx * pageSize
            res = get_res_pdfs(start, pageSize, RechercheTexte, DateDebut, DateFin, Jetons)
            pdf_docs = []
            for item in res.get('result', []):
                for doc in item.get('documents', []):
                    if doc.get('nomFichier', '').lower().endswith('.pdf'):
                        pdf_docs.append(doc)
            if not pdf_docs:
                page_bar.update(1)
                continue

            page_bar.set_postfix(本页=f"0/{len(pdf_docs)}")
            if workers <= 1:
                done = 0
                for doc in pdf_docs:
                    try:
                        if do_one(doc):
                            total_ok += 1
                        else:
                            total_skip += 1
                    except Exception:
                        total_fail += 1
                    done += 1
                    page_bar.set_postfix(本页=f"{done}/{len(pdf_docs)}")
                page_bar.update(1)
            else:
                done = 0
                with ThreadPoolExecutor(max_workers=workers) as pool:
                    future_to_doc = {pool.submit(do_one, doc): doc for doc in pdf_docs}
                    for future in as_completed(future_to_doc):
                        try:
                            if future.result():
                                total_ok += 1
                            else:
                                total_skip += 1
                        except Exception:
                            total_fail += 1
                        done += 1
                        page_bar.set_postfix(本页=f"{done}/{len(pdf_docs)}")
                page_bar.update(1)

    elapsed = time.perf_counter() - start_time
    parts = [f"下载 {total_ok}"]
    if total_skip:
        parts.append(f"跳过 {total_skip}")
    if total_fail:
        parts.append(f"失败 {total_fail}")
    log(f"完成: {'  '.join(parts)}  耗时 {elapsed:.1f}s  → {os.path.abspath(save_dir)}", "OK")
