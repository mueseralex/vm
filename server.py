import tls_client
import time
import subprocess
import csv
import os
import random
import uuid
import pandas as pd
import math
import requests
import base64
from datetime import datetime
from rich.console import Console

console = Console()

chains = ["bsc", "sol", "eth", "base"]
current_chain_index = 0
ENABLE_IP_ROTATION = True

locations = [
    "al", "at", "au", "be", "bg", "ca", "ch", "cz", "de", "dk", "ee", "es",
    "fi", "fr", "gr", "hk", "hu", "ie", "il", "it", "jp", "lt", "lu", "lv",
    "md", "mk", "nl", "no", "nz", "pl", "pt", "ro", "rs", "se", "sg", "sk",
    "th", "tr", "uk", "us", "ca-mtr", "ca-van", "de-ber", "de-frk", "dk-cph", 
    "fi-hel", "fr-par", "gb-ldn", "ie-dub", "it-mil", "nl-ams", "no-osl", 
    "pl-waw", "se-got", "se-sto", "sg-sin", "us-atl", "us-dal", "us-lax", 
    "us-mia", "us-nyc", "us-sea", "us-sfo"
]

SESSION_DEVICE_ID = str(uuid.uuid4())
session = tls_client.Session(client_identifier="firefox_114", random_tls_extension_order=True)

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:114.0) Gecko/20100101 Firefox/114.0",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://gmgn.ai/",
    "Origin": "https://gmgn.ai",
    "Connection": "keep-alive",
    "DNT": "1",
    "Upgrade-Insecure-Requests": "1",
}

def log_message(message):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}")

def rotate_ip():
    if not ENABLE_IP_ROTATION:
        return
    try:
        location = random.choice(locations)
        subprocess.run(["mullvad", "relay", "set", "location", location], 
                      capture_output=True, text=True)
        subprocess.run(["mullvad", "disconnect"], capture_output=True, text=True)
        time.sleep(5)
        subprocess.run(["mullvad", "connect"], capture_output=True, text=True)
        time.sleep(5)
        log_message(f"IP rotated to {location}")
    except Exception as e:
        log_message(f"IP rotation failed: {str(e)}")

def get_balance_field(chain):
    balance_fields = {
        "sol": "sol_balance",
        "eth": "eth_balance",
        "base": "eth_balance",
        "bsc": "bnb_balance"
    }
    return balance_fields[chain]

def collect_trending_cas(chain):
    url = f"https://gmgn.ai/defi/quotation/v1/rank/{chain}/swaps/6h?orderby=marketcap&direction=asc"
    
    archive_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ca_archive.csv")
    archived_cas = set()
    
    if os.path.exists(archive_file):
        with open(archive_file, 'r') as file:
            reader = csv.reader(file)
            next(reader, None)
            for row in reader:
                if row and len(row) >= 2 and row[0].strip() and row[1] == chain:
                    archived_cas.add(row[0].strip())
    
    new_addresses = set()
    attempt = 0
    while True:
        attempt += 1
        try:
            response = session.get(url, headers=headers)
            if response.status_code == 200:
                data = response.json()
                for item in data.get("data", {}).get("rank", []):
                    address = item.get("address")
                    if address and address not in archived_cas:
                        new_addresses.add(address)
                log_message(f"Collected {len(new_addresses)} new CAs for {chain}")
                break
            elif response.status_code in [403, 429, 500]:
                log_message(f"Error {response.status_code} collecting CAs for {chain}. Rotating IP and retrying...")
                rotate_ip()
                time.sleep(1)
            else:
                log_message(f"Unexpected status {response.status_code} for {chain} CA collection. Rotating IP and retrying...")
                rotate_ip()
                time.sleep(1)
        except Exception as e:
            log_message(f"Failed to collect CAs for {chain} (attempt {attempt}): {str(e)}")
            log_message(f"Rotating IP and retrying...")
            rotate_ip()
            time.sleep(1)
    
    if new_addresses:
        file_exists = os.path.exists(archive_file)
        with open(archive_file, 'a', newline='') as file:
            writer = csv.writer(file)
            if not file_exists:
                writer.writerow(["contract_address", "chain", "date_added", "processed"])
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            for address in new_addresses:
                writer.writerow([address, chain, current_time, "yes"])
    
    return list(new_addresses)

def get_top_traders_addresses(contract_address, chain):
    url = f"https://gmgn.ai/vas/api/v1/token_traders/{chain}/{contract_address}?limit=100&orderby=profit&direction=desc"
    
    for attempt in range(3):
        try:
            response = session.get(url, headers=headers)
            if response.status_code == 200:
                data = response.json()
                if data["code"] == 0 and "data" in data and "list" in data["data"]:
                    # Extract addresses from the list, max 100
                    addresses = []
                    total_traders = len(data["data"]["list"])
                    console.print(f"[blue]API returned {total_traders} traders for {contract_address}[/blue]")
                    
                    for trader in data["data"]["list"][:100]:
                        if trader.get("address"):
                            addresses.append(trader["address"])
                    
                    console.print(f"[green]Extracted {len(addresses)} valid addresses[/green]")
                    return addresses
            elif response.status_code in [403, 429]:
                rotate_ip()
            else:
                return []
        except Exception as e:
            console.print(f"[red]Error in get_top_traders_addresses: {str(e)}[/red]")
            rotate_ip()
        time.sleep(0.5)
    return []



def collect_wallet_data(wallet, chain):
    url = f"https://gmgn.ai/defi/quotation/v1/smartmoney/{chain}/walletNew/{wallet}"
    
    for attempt in range(3):
        try:
            response = session.get(url, headers=headers)
            if response.status_code == 200:
                data = response.json().get("data", {})
                
                raw_tags = data.get("tags", [])
                tags_str = "/".join(raw_tags) if isinstance(raw_tags, list) and raw_tags else ""
                
                wallet_data = {
                    "wallet": wallet,
                    "unrealized_profit": data.get("unrealized_profit", "None"),
                    "unrealized_pnl": data.get("unrealized_pnl", "None"),
                    "realized_profit_7d": data.get("realized_profit_7d", "None"),
                    "realized_profit_30d": data.get("realized_profit_30d", "None"),
                    "total_profit": data.get("total_profit", "None"),
                    "winrate": data.get("winrate", "None"),
                    "all_pnl": data.get("all_pnl", "None"),
                    "buy_7d": data.get("buy_7d", "None"),
                    "sell_7d": data.get("sell_7d", "None"),
                    "token_sold_avg_profit": data.get("token_sold_avg_profit", "None"),
                    get_balance_field(chain): data.get(get_balance_field(chain), "None"),
                    "pnl_lt_2x_num": data.get("pnl_lt_2x_num", "None"),
                    "pnl_2x_5x_num": data.get("pnl_2x_5x_num", "None"),
                    "pnl_gt_5x_num": data.get("pnl_gt_5x_num", "None"),
                    "tags": tags_str,
                    "avg_holding_peroid": data.get("avg_holding_peroid", "None"),
                    "sub 75k avg entry": "None",
                    "sub 75k entries": "None",
                    "sub 75k avg buy amount": "None",
                    "sub 75k avg buy 30d": "None",
                    "sub 75k avg sell 30d": "None",
                    "sub 75k avg total profit pnl": "None",
                    "75k - 250k avg entry": "None",
                    "75k - 250k entries": "None",
                    "75k - 250k avg buy amount": "None",
                    "75k - 250k avg buy 30d": "None",
                    "75k - 250k avg sell 30d": "None",
                    "75k - 250k avg total profit pnl": "None",
                    "fast_trades_percentage": "None",
                    "date_reviewed": datetime.now().strftime("%m-%d-%Y")
                }
                
                if all(value == "None" for key, value in wallet_data.items() 
                      if key not in ["wallet", "tags"]):
                    raise ValueError("Incomplete data")
                
                return wallet_data
                
            elif response.status_code in {403, 429, 500}:
                rotate_ip()
            else:
                return {}
        except Exception:
            rotate_ip()
        time.sleep(0.5)
    
    return {}

def calculate_fdv_insights(wallet, chain):
    client_id = "gmgn_web_20250723-1443-1ebae8c"
    app_ver = "20250723-1443-1ebae8c"
    fp_did = "b98a942c17b15b010887f415e5684ead"
    
    url = f"https://gmgn.ai/api/v1/wallet_holdings/{chain}/{wallet}?device_id={SESSION_DEVICE_ID}&client_id={client_id}&from_app=gmgn&app_ver={app_ver}&tz_name=America%2FNew_York&tz_offset=-14400&app_lang=en-US&fp_did={fp_did}&os=web&limit=50&orderby=last_active_timestamp&direction=desc&showsmall=true&sellout=true&hide_airdrop=false&tx30d=true"
    
    fdv_headers = {
        **headers,
        "Accept": "application/json, text/plain, */*",
        "Accept-Encoding": "gzip, deflate, br",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "X-Requested-With": "XMLHttpRequest"
    }
    
    for attempt in range(3):
        try:
            response = session.get(url, headers=fdv_headers)
            if response.status_code == 429:
                rotate_ip()
                time.sleep(0.5)
                response = session.get(url, headers=fdv_headers)
                
            if response.status_code != 200:
                return {}
                
            holdings = response.json().get("data", {}).get("holdings", [])
            fdv_insights = {
                "sub_75k_avg_entry": [],
                "sub_75k_entries": 0,
                "sub_75k_avg_buy_amount": [],
                "sub_75k_buy_30d": [],
                "sub_75k_sell_30d": [],
                "sub_75k_total_profit_pnl": [],
                "75k_250k_avg_entry": [],
                "75k_250k_entries": 0,
                "75k_250k_avg_buy_amount": [],
                "75k_250k_buy_30d": [],
                "75k_250k_sell_30d": [],
                "75k_250k_total_profit_pnl": []
            }
            
            fast_trades = 0
            slow_trades = 0
            
            for token in holdings:
                avg_cost = float(token.get("avg_cost", 0))
                total_supply = float(token.get("total_supply", 0))
                history_bought_cost = float(token.get("history_bought_cost", 0))
                buy_30d = token.get("buy_30d", 0)
                sell_30d = token.get("sell_30d", 0)
                total_profit_pnl = float(token.get("total_profit_pnl", 0))

                start_holding_at = token.get("start_holding_at")
                end_holding_at = token.get("end_holding_at")
                
                if start_holding_at is not None and end_holding_at is not None:
                    holding_time = end_holding_at - start_holding_at
                    if holding_time <= 60:
                        fast_trades += 1
                    else:
                        slow_trades += 1

                if avg_cost > 0 and total_supply > 0:
                    fdv_entry = avg_cost * total_supply
                    
                    if fdv_entry <= 75000:
                        fdv_insights["sub_75k_avg_entry"].append(fdv_entry)
                        fdv_insights["sub_75k_entries"] += 1
                        fdv_insights["sub_75k_avg_buy_amount"].append(history_bought_cost)
                        fdv_insights["sub_75k_buy_30d"].append(buy_30d)
                        if sell_30d > 0:
                            fdv_insights["sub_75k_sell_30d"].append(sell_30d)
                        fdv_insights["sub_75k_total_profit_pnl"].append(total_profit_pnl)
                    elif 75000 < fdv_entry <= 250000:
                        fdv_insights["75k_250k_avg_entry"].append(fdv_entry)
                        fdv_insights["75k_250k_entries"] += 1
                        fdv_insights["75k_250k_avg_buy_amount"].append(history_bought_cost)
                        fdv_insights["75k_250k_buy_30d"].append(buy_30d)
                        if sell_30d > 0:
                            fdv_insights["75k_250k_sell_30d"].append(sell_30d)
                        fdv_insights["75k_250k_total_profit_pnl"].append(total_profit_pnl)
            
            total_trades = fast_trades + slow_trades
            fast_trades_percentage = round((fast_trades / total_trades * 100), 2) if total_trades > 0 else 0
            
            def calculate_avg_fdv_entry(fdv_entries):
                if not fdv_entries:
                    return "None"
                return round(sum(fdv_entries) / len(fdv_entries), 2)
            
            return {
                "sub 75k avg entry": calculate_avg_fdv_entry(fdv_insights["sub_75k_avg_entry"]),
                "sub 75k entries": fdv_insights["sub_75k_entries"],
                "sub 75k avg buy amount": round(sum(fdv_insights["sub_75k_avg_buy_amount"]) / fdv_insights["sub_75k_entries"], 2) if fdv_insights["sub_75k_entries"] > 0 else "None",
                "sub 75k avg buy 30d": round(sum(fdv_insights["sub_75k_buy_30d"]) / len(fdv_insights["sub_75k_buy_30d"]), 1) if fdv_insights["sub_75k_buy_30d"] else "None",
                "sub 75k avg sell 30d": round(sum(fdv_insights["sub_75k_sell_30d"]) / len(fdv_insights["sub_75k_sell_30d"]), 1) if fdv_insights["sub_75k_sell_30d"] else "None",
                "sub 75k avg total profit pnl": round(sum(fdv_insights["sub_75k_total_profit_pnl"]) / len(fdv_insights["sub_75k_total_profit_pnl"]), 4) if fdv_insights["sub_75k_total_profit_pnl"] else "None",
                "75k - 250k avg entry": calculate_avg_fdv_entry(fdv_insights["75k_250k_avg_entry"]),
                "75k - 250k entries": fdv_insights["75k_250k_entries"],
                "75k - 250k avg buy amount": round(sum(fdv_insights["75k_250k_avg_buy_amount"]) / fdv_insights["75k_250k_entries"], 2) if fdv_insights["75k_250k_entries"] > 0 else "None",
                "75k - 250k avg buy 30d": round(sum(fdv_insights["75k_250k_buy_30d"]) / len(fdv_insights["75k_250k_buy_30d"]), 1) if fdv_insights["75k_250k_buy_30d"] else "None",
                "75k - 250k avg sell 30d": round(sum(fdv_insights["75k_250k_sell_30d"]) / len(fdv_insights["75k_250k_sell_30d"]), 1) if fdv_insights["75k_250k_sell_30d"] else "None",
                "75k - 250k avg total profit pnl": round(sum(fdv_insights["75k_250k_total_profit_pnl"]) / len(fdv_insights["75k_250k_total_profit_pnl"]), 4) if fdv_insights["75k_250k_total_profit_pnl"] else "None",
                "fast_trades_percentage": fast_trades_percentage
            }
            
        except Exception:
            time.sleep(0.5)
    
    return {}

def save_to_csv(data, chain):
    csv_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), f"{chain}.csv")
    file_is_empty = not os.path.isfile(csv_file_path) or os.path.getsize(csv_file_path) == 0
    
    reordered_data = {key: data[key] for key in data if key != "date_reviewed"}
    reordered_data["date_reviewed"] = data["date_reviewed"]
    
    with open(csv_file_path, mode="a", newline="") as csv_file:
        fieldnames = reordered_data.keys()
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        
        if file_is_empty:
            writer.writeheader()
        
        writer.writerow(reordered_data)

def remove_duplicates(chain):
    csv_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), f"{chain}.csv")
    if not os.path.exists(csv_file_path):
        return
    
    df = pd.read_csv(csv_file_path)
    
    df['date_reviewed'] = pd.to_datetime(df['date_reviewed'])
    df_deduped = df.sort_values('date_reviewed').drop_duplicates(subset='wallet', keep='last')
    df_deduped['date_reviewed'] = df_deduped['date_reviewed'].dt.strftime('%m-%d-%Y')
    
    df_deduped.to_csv(csv_file_path, index=False)
    log_message(f"Removed duplicates from {chain}.csv")

def format_csv(chain):
    csv_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), f"{chain}.csv")
    if not os.path.exists(csv_file_path):
        return
    
    df = pd.read_csv(csv_file_path)
    
    balance_field = get_balance_field(chain)
    
    column_mapping = {
        'wallet': 'address',
        'unrealized_profit': 'total unrealized profit',
        'realized_profit_7d': '7d realized profit',
        'realized_profit_30d': '30d realized profit',
        'total_profit': 'all time profit',
        'winrate': 'winrate',
        'buy_7d': 'buy txns 7d',
        'sell_7d': 'sell txns 7d',
        'token_sold_avg_profit': 'avg profit',
        balance_field: 'balance',
        'pnl_lt_2x_num': '<2x count',
        'pnl_2x_5x_num': '2x-5x count',
        'pnl_gt_5x_num': '>5x count',
        'tags': 'tags',
        'avg_holding_peroid': 'avg hold time',
        'sub 75k avg entry': 'sub 75k avg entry',
        'sub 75k entries': 'sub 75k entries',
        'sub 75k avg buy amount': '< 75k avg buy amount',
        'sub 75k avg total profit pnl': '< 75k avg total profit pnl',
        '75k - 250k avg entry': '75k - 250k avg entry',
        '75k - 250k entries': '75k - 250k entries',
        '75k - 250k avg buy amount': '75k - 250k avg buy amount',
        '75k - 250k avg total profit pnl': '75k - 250k avg total profit pnl',
        'fast_trades_percentage': 'fast trades',
        'date_reviewed': 'date reviewed [m/d/y]'
    }
    
    df_filtered = df[list(column_mapping.keys())].copy()
    df_filtered.rename(columns=column_mapping, inplace=True)
    
    no_round_columns = ['address', 'date reviewed [m/d/y]', 'tags']
    percentage_columns = ['winrate', '< 75k avg total profit pnl', '75k - 250k avg total profit pnl']
    avg_hold_time_column = 'avg hold time'
    
    for column in df_filtered.columns:
        if column not in no_round_columns:
            df_filtered[column] = pd.to_numeric(df_filtered[column], errors='coerce')
            
            if column in percentage_columns:
                df_filtered[column] = df_filtered[column].apply(lambda x: int(math.ceil(x * 100)) if pd.notna(x) else x)
            elif column == avg_hold_time_column:
                df_filtered[column] = df_filtered[column].apply(lambda x: round(x / 3600, 2) if pd.notna(x) else x)
            else:
                df_filtered[column] = df_filtered[column].apply(lambda x: int(math.ceil(x)) if pd.notna(x) else x)
            
            if column != avg_hold_time_column:
                df_filtered[column] = df_filtered[column].astype('object')
    
    formatted_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), f"formatted_{chain}.csv")
    df_filtered.to_csv(formatted_file, index=False)
    
    with open(formatted_file, 'r') as file:
        content = file.read()
    content = content.replace('.0', '')
    with open(formatted_file, 'w') as file:
        file.write(content)
    
    log_message(f"Formatted {chain}.csv")

def disconnect_vpn():
    try:
        subprocess.run(["mullvad", "disconnect"], capture_output=True, text=True)
        time.sleep(3)
        log_message("Disconnected from VPN for GitHub upload")
    except Exception as e:
        log_message(f"Failed to disconnect VPN: {str(e)}")

def reconnect_vpn():
    try:
        subprocess.run(["mullvad", "connect"], capture_output=True, text=True)
        time.sleep(5)
        log_message("Reconnected to VPN")
    except Exception as e:
        log_message(f"Failed to reconnect VPN: {str(e)}")

def upload_to_github(chain, github_token, repo_owner, repo_name):
    formatted_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), f"formatted_{chain}.csv")
    if not os.path.exists(formatted_file):
        return False
    
    disconnect_vpn()
    
    try:
        with open(formatted_file, 'r') as file:
            content = file.read()
        
        encoded_content = base64.b64encode(content.encode()).decode()
        
        url = f"https://api.github.com/repos/{repo_owner}/{repo_name}/contents/{chain}"
        headers = {
            "Authorization": f"token {github_token}",
            "Accept": "application/vnd.github.v3+json"
        }
        
        get_response = requests.get(url, headers=headers)
        sha = None
        if get_response.status_code == 200:
            sha = get_response.json()["sha"]
        
        data = {
            "message": f"Update {chain} wallet data - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "content": encoded_content,
            "branch": "main"
        }
        
        if sha:
            data["sha"] = sha
        
        response = requests.put(url, json=data, headers=headers)
        
        if response.status_code in [200, 201]:
            log_message(f"Successfully uploaded {chain} data to GitHub")
            return True
        else:
            log_message(f"Failed to upload {chain} data: {response.status_code}")
            return False
            
    except Exception as e:
        log_message(f"GitHub upload error for {chain}: {str(e)}")
        return False
    finally:
        reconnect_vpn()

def load_existing_wallets(chain):
    csv_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), f"{chain}.csv")
    existing_wallets = set()
    if os.path.exists(csv_file_path):
        with open(csv_file_path, mode="r") as csv_file:
            reader = csv.DictReader(csv_file)
            for row in reader:
                existing_wallets.add(row["wallet"])
    return existing_wallets

def process_chain(chain, github_token=None, repo_owner=None, repo_name=None):
    log_message(f"Starting {chain.upper()} processing")
    
    cas = collect_trending_cas(chain)
    if not cas:
        log_message(f"No new CAs found for {chain}")
        return
    
    existing_wallets = load_existing_wallets(chain)
    all_wallets_to_analyze = set()
    
    for i, ca in enumerate(cas):
        log_message(f"Processing CA {i+1}/{len(cas)} for {chain}: {ca}")
        
        try:
            top_traders = get_top_traders_addresses(ca, chain)
            
            wallets_to_add = {wallet for wallet in top_traders if wallet not in existing_wallets}
            all_wallets_to_analyze.update(wallets_to_add)
            
            log_message(f"Found {len(wallets_to_add)} new wallets for CA {ca}")
            
        except Exception as e:
            log_message(f"Error processing CA {ca}: {str(e)}")
    
    log_message(f"Total wallets to analyze for {chain}: {len(all_wallets_to_analyze)}")
    
    for i, wallet in enumerate(all_wallets_to_analyze):
        log_message(f"Processing wallet {i+1}/{len(all_wallets_to_analyze)} for {chain}: {wallet}")
        
        primary_data = collect_wallet_data(wallet, chain)
        if not primary_data:
            continue
        
        fdv_insights = calculate_fdv_insights(wallet, chain)
        all_insights = {**primary_data, **fdv_insights}
        all_insights["date_reviewed"] = datetime.now().strftime("%m-%d-%Y")
        
        save_to_csv(all_insights, chain)
        existing_wallets.add(wallet)
    
    remove_duplicates(chain)
    format_csv(chain)
    
    if github_token and repo_owner and repo_name:
        upload_to_github(chain, github_token, repo_owner, repo_name)
    
    log_message(f"Completed {chain.upper()} processing")

def load_env_file():
    env_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    if os.path.exists(env_file):
        with open(env_file, 'r') as f:
            for line in f:
                if '=' in line and not line.strip().startswith('#'):
                    key, value = line.strip().split('=', 1)
                    os.environ[key] = value

def main():
    load_env_file()
    
    github_token = os.environ.get("GITHUB_TOKEN")
    repo_owner = os.environ.get("REPO_OWNER")
    repo_name = os.environ.get("REPO_NAME")
    
    if not all([github_token, repo_owner, repo_name]):
        log_message("GitHub credentials not found in environment variables")
        log_message("Set GITHUB_TOKEN, REPO_OWNER, and REPO_NAME environment variables")
    
    log_message("Starting 24/7 Multi-Chain Scraper Server")
    
    while True:
        try:
            for chain in chains:
                process_chain(chain, github_token, repo_owner, repo_name)
            
            log_message("Completed full cycle of all chains. Waiting 30 minutes before restart...")
            time.sleep(1800)
            
        except KeyboardInterrupt:
            log_message("Server stopped by user")
            break
        except Exception as e:
            log_message(f"Unexpected error: {str(e)}")
            time.sleep(300)

if __name__ == "__main__":
    main()
