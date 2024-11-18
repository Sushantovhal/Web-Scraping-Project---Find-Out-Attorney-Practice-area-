import mysql.connector
import tkinter as tk
import tkinter.ttk as ttk
from tkinter import filedialog, messagebox
import pandas as pd
import requests
from bs4 import BeautifulSoup
from urllib.robotparser import RobotFileParser
from requests.exceptions import RequestException, Timeout
import datetime
import traceback
import inspect
from mysql.connector import Error
import threading
import time
import os
import json
import webbrowser
from tkinter import messagebox
from collections import Counter
import re
from tkcalendar import Calendar
import datetime
from tkinter import messagebox, filedialog, StringVar
import mysql.connector
import pandas as pd
import ttkbootstrap as ttk
from ttkbootstrap.constants import *
import ttkbootstrap as tb

checkbox_state = False
check_box_checked = False
current_page_id = None
webmap_paused = False
webmap_thread = None
paused_page_id = None
paused_site_id = None
paused_url = None
current_page_id = 0
current_timeout = 2
timeout_var = None
webmap_pause_event = threading.Event()
webmap_paused_event = threading.Event()

root = tb.Window(themename="superhero")


class MySQLConnectionWindow:
    def __init__(self, parent, main_window_callback):
        self.parent = parent
        self.main_window_callback = main_window_callback
        self.parent.title("MySQL Connection")
        self.parent.geometry("400x300")

        self.label_username = tk.Label(parent, text="Username:")
        self.label_username.pack(pady=10)
        self.entry_username = tk.Entry(parent)
        self.entry_username.pack(pady=5)

        self.label_password = tk.Label(parent, text="Password:")
        self.label_password.pack(pady=10)
        self.entry_password = tk.Entry(parent, show="*")
        self.entry_password.pack(pady=5)

        self.label_database = tk.Label(parent, text="Database Name")
        self.label_database.pack(pady=10)
        self.entry_database = tk.Entry(parent)
        self.entry_database.pack(pady=5)

        try:
            with open("credentials.json", "r") as json_file:
                data = json.load(json_file)
                saved_username = data.get("Credentials", {}).get("Username", "")
                saved_password = data.get("Credentials", {}).get("Password", "")
                saved_database = data.get("Credentials", {}).get("Database", "")
        except (FileNotFoundError, json.JSONDecodeError):
            saved_username = ""
            saved_password = ""
            saved_database = ""

        if saved_username:
            self.entry_username.insert(0, saved_username)
        if saved_password:
            self.entry_password.insert(0, saved_password)
        if saved_database:
            self.entry_database.insert(0, saved_database)

        self.button_save = tk.Button(parent, text="Save", command=self.save_credentials)
        self.button_save.pack(pady=15)
        self.button_save.config(state=tk.NORMAL, bg='lightgray', fg='black')

    def save_credentials(self):
        username = self.entry_username.get()
        password = self.entry_password.get()
        database = self.entry_database.get()
        try:
            start_time = time.time()
            db = mysql.connector.connect(
                host="localhost",
                user=username,
                password=password,
            )
            cursor = db.cursor()
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database}")
            cursor.close()
            db.close()
            data = {"Credentials": {"Username": username, "Password": password, 'Database': database}}
            with open("credentials.json", "w") as json_file:
                json.dump(data, json_file)
            self.parent.destroy()
            end_time = time.time()
            execution_time = end_time - start_time
            print(' fetch_and_process_sitemaps Execution time:', execution_time)
        except mysql.connector.Error as e:
            messagebox.showerror("Error", f"Failed to connect to MySQL: {e}")


def update_error_log(cursor, db, site_id, page_url, error, error_line):
    try:
        time_date = datetime.datetime.now().date()
        error_data = (site_id, page_url, str(error), error_line, time_date)
        cursor.execute(
            "INSERT INTO error_log (site_id, page_url, error, error_line, time_date) VALUES (%s, %s, %s, %s, %s)",
            error_data)
        db.commit()
    except mysql.connector.errors.OperationalError as e:
        if 'MySQL Connection not available.' in str(e):
            cursor.reconnect()
            time_date = datetime.datetime.now().date()
            error_data = (site_id, page_url, str(error), error_line, time_date)
            cursor.execute(
                "INSERT INTO error_log (site_id, page_url, error, error_line, time_date) VALUES (%s, %s, %s, %s, %s)",
                error_data)
            db.commit()
        else:
            print(f"Error updating error log: {e}")
    except Exception as e:
        print(f"Error updating error log: {e}")


def fetch_and_process_sitemap(sitemap_url, site_id, cursor, db):
    start_time = time.time()
    try:
        print(f"Processing sitemap:{sitemap_url}")
        response = requests.get(sitemap_url, timeout=current_timeout)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'xml')
        # a = soup.find_all(['lastmod', 'image', 'changefreq', 'priority'])
        # a.decompose()
        for tag in soup.find_all(['lastmod', 'image', 'changefreq', 'priority']):
            tag.decompose()
        text_content = soup.get_text(separator=' ', strip=True)
        print("Text content from", sitemap_url, ":\n", text_content)
        urls = [url.text for url in soup.find_all('loc')]
        query = "INSERT INTO page_site (site_id, page_url) VALUES (%s, %s)"
        data = list([(site_id, x) for x in urls])
        cursor.executemany(query, data)
        db.commit()
    except (RequestException, Timeout) as e:
        print(f"Error while fetching or processing sitemap {sitemap_url}: {e}")
        update_error_log(cursor, db, site_id, urls, e, inspect.currentframe().f_lineno)
    end_time = time.time()
    print("excution time fetch and procoess sitemap", end_time - start_time)


def process_sitemap_index(sitemap_index_url, keyword_ddf, site_id, cursor, db):
    start_time = time.time()
    try:
        response = requests.get(sitemap_index_url, timeout=current_timeout)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'xml')
        sitemaps = soup.find_all('sitemap')
        for sitemap in sitemaps:
            if webmap_paused:
                return
            sitemap_url = sitemap.find('loc').text
            fetch_and_process_sitemap(sitemap_url, site_id, cursor, db)
    except (RequestException, Timeout) as e:
        print(f"Error while fetching or processing sitemap index {sitemap_index_url}: {e}")
    end_time = time.time()
    execution_time1 = end_time - start_time
    print("process_sitemap_index execution time", execution_time1)


def fetch_and_process_sitemaps(content, site_id, keyword_ddf, cursor, db):
    global webmap_paused, url
    try:
        start_time = time.time()
        robots_url = f"{content}/robots.txt"
        rp = RobotFileParser(robots_url)
        rp.read()

        sitemap_urls = rp.site_maps()

        if sitemap_urls is None:
            sitemap_url = f"{content}/sitemap.xml"
            process_sitemap_index(sitemap_url, keyword_ddf, site_id, cursor, db)
            fetch_and_process_sitemap(sitemap_url, site_id, cursor, db)
        else:
            print("Sitemap URLs from robots.txt:", sitemap_urls)
            for sitemap_url in sitemap_urls:
                if 'sitemap_index.xml' in sitemap_url or 'post-sitemap.xml' in sitemap_url or \
                        'sitemap.xml' in sitemap_url or 'sitemap-index.xml' in sitemap_url or \
                        'page-sitemap.xml' in sitemap_url or 'practices-sitemap.xml' in sitemap_url or \
                        'attorneys-sitemap.xml' in sitemap_url or 'wp-sitemap.xml' in sitemap_url or \
                        'wp_sitemap-index.xml' in sitemap_url or 'pages-sitemap.xml' in sitemap_url or \
                        'page_sitemap.xml' in sitemap_url or 'sitemap-misc.xml' in sitemap_url:

                    if webmap_paused:
                        return
                    process_sitemap_index(sitemap_url, keyword_ddf, site_id, cursor, db)
                    fetch_and_process_sitemap(sitemap_url, site_id, cursor, db)
                else:
                    if webmap_paused:
                        return
                    process_sitemap_index(sitemap_url, keyword_ddf, cursor, db)
                    fetch_and_process_sitemap(sitemap_url, site_id, cursor, db)
    except Exception as e:
        print(f"Error while processing {content}: {e}")
        update_error_log(cursor, db, site_id, content, e, None)
    end_time = time.time()
    execution_time2 = end_time - start_time
    print("fetch_and_process_sitemaps execution time", execution_time2)


def create_tables(cursor, db):
    start_time = time.time()
    try:
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS keyword_table(
            Practice_area VARCHAR(800),
            keyword VARCHAR(255),
            weightage int)
        """)
        cursor.execute("""
                CREATE TABLE IF NOT EXISTS sites_table (
                    sfid text,
                    url VARCHAR(4000),
                    acc_name text,
                    id INT PRIMARY KEY AUTO_INCREMENT,
                    start_date_time DATETIME,
                    end_date_time DATETIME,
                    progress decimal(5,2),
                    status varchar(255) default 'open',
                    download_status varchar(600) default 'NO')
                """)
        cursor.execute("""
                CREATE TABLE IF NOT EXISTS error_log (
                    site_id VARCHAR(4000),
                    page_url VARCHAR(4000),
                    error VARCHAR(5000),
                    error_line INT,
                    time_date DATETIME
                )
                """)
        cursor.execute("""
                CREATE TABLE IF NOT EXISTS page_site (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    site_id INT,
                    page_url VARCHAR(9000),
                    status VARCHAR(20) default 'open',
                    progress DECIMAL(5,2),
                    FOREIGN KEY (site_id) REFERENCES sites_table(id))
                """)
        cursor.execute("""
                        CREATE TABLE IF NOT EXISTS Law_keywords (
                            keywords varchar(1500),
                            threshold int,
                            priority int)
                        """)
        cursor.execute("""
                        CREATE TABLE IF NOT EXISTS Law_count (
                            site_id int,
                            keywords text,
                            count int,
                            law_firm varchar(1000),
                            FOREIGN KEY (site_id) REFERENCES sites_table(id))
                        """)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS found_keywords_table (
            id INT AUTO_INCREMENT PRIMARY KEY,
            site_id INT,
            page_url VARCHAR(8000),
            keyword VARCHAR(255),
            count int,
            FOREIGN KEY (site_id) REFERENCES sites_table(id)
        )
        """)
        cursor.execute(""" CREATE TABLE IF NOT EXISTS email_table (
                        sfid text,
                      url varchar(4000),
                      acc_name text,
                      all_keywords text,
                      practice_area text,
                      legal_keywords text,
                      law_firm varchar(1000),
                      Date datetime)""")
        try:
            cursor.execute("ALTER TABLE email_table ADD COLUMN Date datetime")
        except Error as e:
            print(f"table:")
    except Error as e:
        print(f"Error creating tables: {e}")
    end_time = time.time()
    execution_time3 = end_time - start_time
    print("Create table", execution_time3)


def update_credentials(username, password, database):
    data = {"Credentials": {"Username": username, "Password": password, "Database": database}}
    with open("credentials.json", "w") as json_file:
        json.dump(data, json_file)
    run_webmap()


id = 0
site_id = 0
url = ""


def run_webmap():
    start_time = time.time()
    try:
        with open("credentials.json", "r") as json_file:
            data = json.load(json_file)
            username = data["Credentials"]["Username"]
            password = data["Credentials"]["Password"]
            database = data["Credentials"]["Database"]
    except (FileNotFoundError, json.JSONDecodeError, KeyError):
        username = ""
        password = ""
        database = ""
        root_mysql = tk.Tk()
        connection_window = MySQLConnectionWindow(root_mysql, update_credentials)
        root_mysql.mainloop()
        run_webmap()
        return
    db = mysql.connector.connect(
        host="localhost",
        user=username,
        password=password,
        database=database
    )
    cursor = db.cursor()
    create_tables(cursor, db)
    # root = tk.Tk()
    root.title("WebMap Tool")
    root.geometry("1700x790")
    end_time = time.time()
    execution_time4 = end_time - start_time
    print("credientials", execution_time4)

    def upload_keyword_csv():
        start_time = time.time()
        global keyword_ddf
        keyword_csv_path = filedialog.askopenfilename(title="Select Keyword CSV File",
                                                      filetypes=[("CSV files", "*.csv")])
        if keyword_csv_path:
            keywords_df = pd.read_csv(keyword_csv_path)
            practice_area = keywords_df['Practice_area'].tolist()
            import_keywords = keywords_df['Keywords'].tolist()
            weightage_column = keywords_df['weightage'].tolist()
            try:
                db = mysql.connector.connect(
                    host="localhost",
                    user=username,
                    password=password,
                    database=database,
                )
                cursor = db.cursor()
                cursor.execute("truncate table keyword_table")

                insert_values = []
                for practice, keywords, weightagei in zip(practice_area, import_keywords, weightage_column):
                    keyword_list = re.split('[;,]', keywords)
                    if isinstance(weightagei, int):
                        weightage_values = [int(weightagei)]
                    else:
                        weightage_values = [int(val) for val in weightagei.split(';')] if ';' in weightagei else [
                            int(weightagei)]

                    if len(keyword_list) == len(weightage_values):
                        for keyword, weightage in zip(keyword_list, weightage_values):
                            insert_values.append((practice, keyword.strip(), weightage))
                    else:
                        print(
                            f"Warning: Mismatched number of keywords and weightages in row - Practice={practice}, Keywords={keyword_list}, Weightages={weightage_values}")
                        messagebox.showwarning("Warning", "Mismatched number of keywords and weightages in a row.")

                insert_query = "INSERT INTO keyword_table (Practice_area, keyword, weightage) VALUES (%s, %s, %s)"
                cursor.executemany(insert_query, insert_values)
                cursor.execute(
                    "UPDATE page_site SET status = 'completed' WHERE status = 'open'")
                cursor.execute("UPDATE sites_table SET download_status = 'YES' WHERE status = 'completed'")
                cursor.execute("UPDATE sites_table SET download_status = 'YES' WHERE status = 'url not found'")
                cursor.execute("UPDATE sites_table SET download_status = 'YES' WHERE status = 'open'")
                cursor.execute("UPDATE sites_table SET status = 'completed' WHERE status = 'open'")

                db.commit()
                cursor.close()
                db.close()
                upload_keyword_button.config(state=tk.DISABLED)
                upload_law_keyword_button.config(state=tk.NORMAL, bg='lightgray', fg='black')
                upload_site_button.config(state=tk.NORMAL, bg='lightgray', fg='black')
                keyword_label = tk.Label(root, text="Keyword CSV file uploaded successfully!")
                keyword_label.pack()
            except mysql.connector.Error as e:
                messagebox.showerror("Error", f"Failed to connect to MySQL: {e}")
            end_time = time.time()
            execution_time = end_time - start_time
            print("Upload keywords csv ", execution_time)

    def upload_site_csv():
        start_time1 = time.time()
        global site_df, progress_bar, Totalurl
        site_csv_path = filedialog.askopenfilename(title="Select Site CSV File", filetypes=[("CSV files", "*.csv")])
        if site_csv_path:
            site_df = pd.read_csv(site_csv_path)
            account_list = site_df['account_id'].tolist()
            url_list = site_df['url'].tolist()
            acc_name_list = site_df['acc_name'].tolist()
            try:
                db = mysql.connector.connect(
                    host="localhost",
                    user=username,
                    password=password,
                    database=database,
                )
                cursor = db.cursor()

                insert_values = []
                for account_id, url, acc_name in zip(account_list, url_list, acc_name_list):
                    if url.startswith(('https:')):
                        url = url
                    else:
                        url = 'https://www.' + url
                    start_time = datetime.datetime.now()
                    insert_values.append((account_id, url, acc_name, start_time, start_time, 'open'))

                insert_query = "INSERT INTO sites_table (sfid,url, acc_name, start_date_time, end_date_time,status) VALUES (%s, %s, %s, %s, %s,%s)"
                cursor.executemany(insert_query, insert_values)
                # cursor.execute("UPDATE page_site SET progress = 0 WHERE status = 'completed'")
                # cursor.execute("UPDATE page_site SET progress = 0 WHERE status = 'url not found'")
                # cursor.execute("UPDATE sites_table SET download_status = 'YES' WHERE status = 'completed'")
                # cursor.execute("UPDATE sites_table SET download_status = 'YES' WHERE status = 'url not found'")
                db.commit()
                cursor.close()
                db.close()
                url_count_label.config(text=f"Total Number of Site URLs to scan: {len(site_df)}")
                upload_site_button.config(state=tk.DISABLED)
                upload_law_keyword_button.config(state=tk.NORMAL, bg='lightgray',fg='black')
                run_webmap_button.config(state=tk.NORMAL, bg='lightgray', fg='black')
                check_button.config(state=tk.NORMAL, bg='lightgray', fg='black')
                sites_csv = tk.Label(root, text='Sites CSV file uploaded successfully! We are ready to process URLs!')
                sites_csv.pack()
            except mysql.connector.Error as e:
                messagebox.showerror("Error", f"Failed to connect to MySQL: {e}")
            end_time1 = time.time()
            execution_time1 = end_time1 - start_time1
            print("upload site csv", execution_time1)

    def open_mysql_connection_window():
        root_mysql = tk.Toplevel()
        connection_window = MySQLConnectionWindow(root_mysql, return_to_main_window)
        db = mysql.connector.connect(
            host="localhost",
            user=username,
            password=password,
            database=database
        )
        cursor = db.cursor()
        create_tables(cursor, db)
        root_mysql.mainloop()

    def return_to_main_window():
        root.destroy()
        run_webmap()

    def update_progress(progress_bar, progress_value):
        progress_bar["value"] = 0
        progress_label.config(text="0.00%")
        progress_bar["value"] = progress_value
        progress_label.config(text=f"{progress_value:.2f}%")
        root.update()

    def update_progress_in_page_site(cursor, db, progress_value):
        try:
            cursor.execute("UPDATE page_site SET  progress = %s ", (progress_value,))
            db.commit()
            update_progress(progress_bar, progress_value)
        except Exception as e:
            print(f"Error updating progress in page_site table: {e}")

    def process_page_site_urls(site_id, cursor, db, keyword_ddf, webmap_paused, url):
        try:
            cursor.execute("SELECT site_id FROM page_site WHERE status='open'")
            site_ids = cursor.fetchall()

            cursor.execute("SELECT count(*) FROM page_site WHERE status='open'")
            total_id = cursor.fetchone()[0]

            for site_id in site_ids:
                site_id = site_id[0]
                # cursor.execute("SELECT COUNT(*) FROM page_site WHERE status='open' AND site_id = %s", (site_id,))
                # total_sites = cursor.fetchone()[0]

                cursor.execute("SELECT site_id, page_url, status FROM page_site WHERE status='open' AND site_id = %s",
                               (site_id,))
                page_urls_and_statuses = cursor.fetchall()

                batch_data = []

                for current_page_id, (site_id, url, status) in enumerate(page_urls_and_statuses, start=1):
                    if webmap_pause_event.is_set():
                        return

                    if status == 'open':
                        try:
                            start_timen = time.time()
                            url_response = requests.get(url, timeout=current_timeout)
                            url_response.raise_for_status()
                            url_soup = BeautifulSoup(url_response.content, 'html.parser')
                            for tag in url_soup(['script', 'style']):
                                tag.decompose()
                            url_text_content = url_soup.get_text(separator=' ', strip=True).lower()
                            print(f"Content from {current_page_id}", url, ":\n", url_text_content)

                            keywords_pattern = '|'.join(map(re.escape, map(str.lower, keyword_ddf)))
                            found_keywords = re.findall(keywords_pattern, url_text_content, re.IGNORECASE)
                            if found_keywords:
                                print(f"Keywords found in URL: {url}")
                                keyword_counts = Counter(found_keywords)
                                for keyword, count in keyword_counts.items():
                                    print(
                                        f"Keyword '{keyword}' found {count} times in URL: {url} and site_id {site_id}")

                                    batch_data.append((site_id, url, keyword, count))

                            cursor.execute(
                                "UPDATE page_site SET status = 'completed' WHERE page_url = %s AND site_id=%s",
                                (url, site_id))
                            db.commit()
                            end_timen = time.time()
                            print("Process Page site urls", end_timen - start_timen)

                            progress_value = current_page_id / total_id * 100
                            update_progress_in_page_site(cursor, db, progress_value)
                            Totalurl.config(text=f"Total Number of Page URLs to scan: {total_id}")
                        except (RequestException, Timeout) as e:
                            print(f"Error while fetching or processing URL {url}: {e}")
                            update_error_log(cursor, db, site_id, url, e, inspect.currentframe().f_lineno)
                            cursor.execute("UPDATE page_site SET status = 'url not found' WHERE page_url = %s", (url,))
                            cursor.execute("UPDATE sites_table SET status = 'url not found' WHERE url = %s", (url,))
                            db.commit()
                            progress_value = current_page_id / total_id * 100
                            update_progress_in_page_site(cursor, db, progress_value)

                        if webmap_pause_event.is_set():
                            return
                if batch_data:
                    cursor.executemany(
                        "INSERT INTO found_keywords_table (site_id, page_url, keyword, count) VALUES (%s, %s, %s, %s)",
                        batch_data)
                    cursor.execute("UPDATE page_site SET status = 'completed' WHERE site_id = %s", (site_id,))
                    db.commit()

                    cursor.execute(
                        "SELECT COUNT(*) FROM page_site WHERE status != 'completed' AND status != 'url not found' AND site_id = %s",
                        (site_id,))
                    remaining_urls_count = cursor.fetchone()[0]

                    if remaining_urls_count == 0:
                        cursor.execute("UPDATE sites_table SET status = 'completed' WHERE id = %s", (site_id,))
                        db.commit()

        except (RequestException, Timeout) as e:
            print(f"Error while fetching or processing URLs: {e}")
            update_error_log(cursor, db, site_id, url, e, inspect.currentframe().f_lineno)
            cursor.execute("UPDATE page_site SET status = 'url not found' WHERE page_url = %s", (url,))
            cursor.execute("UPDATE sites_table SET status = 'url not found' WHERE url = %s", (url,))
            db.commit()
            update_progress_in_page_site(cursor, db, 0)

    def resume_process_open_urls(cursor, db, keyword_ddf, site_id):
        try:
            cursor.execute("SELECT site_id, page_url, progress FROM page_site WHERE status = 'open'")
            open_urls = cursor.fetchall()

            insert_values = []
            update_sites_table_values = []

            for current_page_id, (site_id, url, progress) in enumerate(open_urls, start=1):
                print(f"Resuming processing for URL {url} and site_id {site_id}")
                try:
                    url_response = requests.get(url, timeout=current_timeout)
                    url_response.raise_for_status()
                    url_soup = BeautifulSoup(url_response.content, 'html.parser')
                    for tag in url_soup(['script', 'style']):
                        tag.decompose()
                    url_text_content = url_soup.get_text(separator=' ', strip=True)
                    print(f"Content from URL: {url}:\n", url_text_content)
                    found_keywords = [keyword for keyword in keyword_ddf if keyword.lower() in url_text_content.lower()]
                    if found_keywords:
                        print(f"Keywords found in URL: {url}")
                        for keyword in found_keywords:
                            print(f"Keyword '{keyword}' found in URL: {url} and site_id {site_id}")
                            insert_values.append((site_id, url, keyword))

                    cursor.execute("UPDATE page_site SET status = 'completed' WHERE page_url = %s and site_id=%s",
                                   (url, site_id))
                    update_sites_table_values.append((url, site_id))

                    v = 100 - float(progress)
                    progress_value = float(progress) + (current_page_id / len(open_urls) * v)
                    update_progress_in_page_site(cursor, db, progress_value)

                except (RequestException, Timeout) as e:
                    print(f"Error while fetching or processing URL {url}: {e}")
                    update_error_log(cursor, db, site_id, url, e, inspect.currentframe().f_lineno)
                    update_sites_table_values.append((url,))

                    v = 100 - float(progress)
                    progress_value = float(progress) + (current_page_id / len(open_urls) * v)
                    update_progress_in_page_site(cursor, db, progress_value)

            if insert_values:
                cursor.executemany("INSERT INTO found_keywords_table (site_id, page_url, keyword) VALUES (%s, %s, %s)",
                                   insert_values)

            if update_sites_table_values:
                cursor.executemany("UPDATE sites_table SET status = 'completed' WHERE url = %s AND id = %s",
                                   update_sites_table_values)

            db.commit()

        except (RequestException, Timeout) as e:
            print(f"Error while fetching or processing URLs: {e}")
            update_error_log(cursor, db, site_id, url, e, inspect.currentframe().f_lineno)

    def insert_data_into_email_table():
        start_time = time.time()
        try:
            if checkbox_state:
                email_query = """SELECT
                                    subquery.sfid,
                                    subquery.url,
                                    subquery.acc_name,
                                    GROUP_CONCAT(CONCAT(subquery.keyword , ':-', subquery.count) SEPARATOR '; ') AS all_keywords,
                                    SUBSTRING_INDEX(GROUP_CONCAT(CASE WHEN subquery.keyword_ranks <= 1 THEN keyword_table.practice_area  END ORDER BY keyword_table.weightage ASC SEPARATOR '; '), '; ', 1) AS practice_area
                                FROM (
                                    SELECT
                                        s.sfid,
                                        s.url,
                                        s.acc_name,
                                        f.keyword,
                                        f.count,
                                        k.weightage ,
                                        ROW_NUMBER() OVER (PARTITION BY s.sfid ORDER BY f.count DESC) AS keyword_rank,
                                        DENSE_RANK() OVER (PARTITION BY s.sfid ORDER BY SUM(f.count) DESC) AS keyword_ranks
                                    FROM
                                        sites_table s
                                    JOIN
                                        found_keywords_table f ON s.id = f.site_id
                                    JOIN
                                        keyword_table k ON f.keyword = k.keyword
                                    WHERE
                                        s.download_status = 'NO' 
                                    GROUP BY
                                        s.sfid, s.url, s.acc_name, f.keyword,f.count, k.weightage
                                ) AS subquery
                                LEFT JOIN
                                    keyword_table ON subquery.keyword = keyword_table.keyword
                                GROUP BY
                                    subquery.sfid, subquery.url, subquery.acc_name;"""
                db = mysql.connector.connect(
                    host="localhost",
                    user=username,
                    password=password,
                    database=database
                )
                cursor = db.cursor()
                cursor.execute(email_query)
                email_data = cursor.fetchall()

                insert_values = []
                for email_row in email_data:
                    date = current_date = datetime.datetime.now().date()
                    sfid, url, acc_name, all_keywords, practice_area = email_row
                    insert_values.append((sfid, url, acc_name, all_keywords, practice_area, date))

                if insert_values:
                    cursor.executemany("""
                                                INSERT INTO email_table (sfid, url, acc_name, all_keywords, practice_area, Date)
                                                VALUES (%s, %s, %s, %s, %s, %s)
                                            """, insert_values)
                    db.commit()
            else:
                email_query = """SELECT
                                    subquery.sfid,
                                    subquery.url,
                                    subquery.acc_name,
                                    TRIM(BOTH '; ' FROM GROUP_CONCAT(
                                        CASE 
                                            WHEN subquery.keyword != '' 
                                            THEN CONCAT(subquery.keyword, ':-', subquery.count_sum) 
                                            ELSE '' 
                                        END 
                                        SEPARATOR '; '
                                    )) AS all_keywords,
                                    SUBSTRING_INDEX(
                                        GROUP_CONCAT(
                                            CASE 
                                                WHEN subquery.keyword_ranks <= 1 
                                                THEN keyword_table.practice_area 
                                            END 
                                            ORDER BY keyword_table.weightage ASC 
                                            SEPARATOR '; '
                                        ), 
                                        '; ', 1
                                    ) AS practice_area,
                                    TRIM(BOTH '; ' FROM GROUP_CONCAT(
                                        DISTINCT 
                                        CASE 
                                            WHEN subquery.keywords != '' 
                                            THEN CONCAT(subquery.keywords, ':-', subquery.law_count_keywords) 
                                            ELSE '' 
                                        END 
                                        SEPARATOR '; '
                                    )) AS legal_keywords,
                                    subquery.law_firm
                                FROM (
                                    SELECT
                                        s.sfid,
                                        s.url,
                                        s.acc_name,
                                        COALESCE(f.keyword, '') AS keyword,
                                        COALESCE(SUM(f.count), 0) AS count_sum,
                                        SUM(l.count) AS law_count_keywords,
                                        COALESCE(k.weightage, 0) AS weightage,
                                        ROW_NUMBER() OVER (PARTITION BY s.sfid ORDER BY COALESCE(SUM(f.count), 0) DESC) AS keyword_rank,
                                        DENSE_RANK() OVER (PARTITION BY s.sfid ORDER BY COALESCE(SUM(f.count), 0) DESC) AS keyword_ranks,
                                        l.keywords,
                                        l.law_firm
                                    FROM
                                        sites_table s
                                    LEFT JOIN
                                        found_keywords_table f ON s.id = f.site_id
                                    LEFT JOIN 
                                        law_count l ON s.id = l.site_id
                                    LEFT JOIN
                                        keyword_table k ON f.keyword = k.keyword
                                    LEFT JOIN
                                        error_log e ON s.url = e.page_url  -- Adding this line to join with the error log table
                                    WHERE
                                        s.download_status = 'NO'
                                        AND e.page_url IS NULL  -- Adding this condition to exclude URLs present in the error log table
                                    GROUP BY
                                        s.sfid, s.url, s.acc_name, f.keyword, k.weightage, l.keywords, l.law_firm
                                ) AS subquery
                                LEFT JOIN
                                    keyword_table ON subquery.keyword = keyword_table.keyword
                                GROUP BY
                                    subquery.sfid, subquery.url, subquery.acc_name, subquery.law_firm;"""

                db = mysql.connector.connect(
                    host="localhost",
                    user=username,
                    password=password,
                    database=database
                )
                cursor = db.cursor()
                cursor.execute(email_query)
                email_data = cursor.fetchall()

                insert_values = []
                for email_row in email_data:
                    date = datetime.datetime.now().date()
                    sfid, url, acc_name, all_keywords, practice_area, legal_keywords, law_firm = email_row
                    insert_values.append((sfid, url, acc_name, all_keywords, practice_area, legal_keywords, law_firm, date))

                if insert_values:
                    cursor.executemany("""
                                                 INSERT INTO email_table (sfid, url, acc_name, all_keywords, practice_area,legal_keywords,law_firm,Date)
                                                 VALUES (%s, %s, %s, %s, %s, %s,%s,%s)
                                             """, insert_values)
                    db.commit()

        except Exception as e:
            print("An error occurred:", e)
            traceback.print_exc()  # Print traceback for detailed error analysis
            db.rollback()
        finally:
            if 'cursor' in locals() and cursor is not None:
                cursor.close()
            if 'db' in locals() and db is not None:
                db.close()

        end_time = time.time()
        execution_time = end_time - start_time
        print("Insert data into table execution time:", execution_time)

    def run_webmap_process():
        start_time = time.time()
        global webmap_paused, site_id, url, paused_site_id, paused_url, webmap_pause_event, checkbox_state
        webmap_paused = False
        upload_law_keyword_button.config(state=tk.DISABLED, bg='lightgray', fg='black')
        pause_button.config(state=tk.NORMAL, bg='lightgray', fg='black')
        resume_button.config(state=tk.DISABLED)
        run_webmap_button.config(state=tk.NORMAL, bg='lightgray', fg='black')
        Download_button.config(state=tk.NORMAL, bg='lightgray', fg='black')

        def run_webmap_thread():
            global webmap_paused, cursor, db
            db = mysql.connector.connect(
                host="localhost",
                user=username,
                password=password,
                database=database
            )
            cursor = db.cursor()
            try:
                if checkbox_state:
                    cursor.execute("SELECT distinct(keyword) FROM keyword_table")
                    keywords_data = cursor.fetchall()
                    keyword_ddf = [keyword[0] for keyword in keywords_data]
                    cursor.execute("SELECT id, url, status FROM sites_table")
                    sites = cursor.fetchall()

                    for site in sites:
                        site_id, url, status = site
                        start_time = datetime.datetime.now()
                        if status != 'open':
                            print(f"Skipping Site {site_id} - {url} as it is already marked as complete.")
                            continue
                        try:
                            fetch_and_process_sitemaps(url, site_id, keyword_ddf, cursor, db)
                        except Exception as e:
                            update_error_log(cursor, db, site_id, url, e, None)
                            # cursor.execute("UPDATE sites_table SET status = 'not visited' WHERE id = %s", (site_id,))
                            db.commit()
                        end_date_time = datetime.datetime.now()
                        # cursor.execute("UPDATE sites_table SET status = 'completed' WHERE id = %s", (site_id,))
                        db.commit()
                        cursor.execute("UPDATE sites_table SET end_date_time = %s WHERE id = %s",
                                       (end_date_time, site_id))
                        db.commit()

                    while webmap_paused:
                        time.sleep(1)
                    process_page_site_urls(site_id, cursor, db, keyword_ddf, webmap_paused, url)

                else:
                    cursor.execute("SELECT distinct(keyword) FROM keyword_table")
                    keywords_data = cursor.fetchall()
                    keyword_ddf = [keyword[0] for keyword in keywords_data]
                    cursor.execute("SELECT id, url, status FROM sites_table")
                    sites = cursor.fetchall()

                    insert_page_site_values = []

                    for site in sites:
                        site_id, url, status = site
                        start_time = datetime.datetime.now()
                        if status != 'open':
                            print(f"Skipping Site {site_id} - {url} as it is already marked as complete.")
                            continue
                        try:
                            insert_page_site_values.append((site_id, url))
                        except Exception as e:
                            update_error_log(cursor, db, site_id, url, e, None)
                            # cursor.execute("UPDATE sites_table SET status = 'not visited' WHERE id = %s", (site_id,))
                            db.commit()

                    if insert_page_site_values:
                        cursor.executemany("INSERT INTO page_site (site_id, page_url) VALUES (%s, %s)",
                                           insert_page_site_values)
                        db.commit()

                    while webmap_paused:
                        webmap_pause_event.wait()
                        if webmap_paused:
                            time.sleep(1)
                    search_keyword_on_home_page(site_id, url, cursor, db, keyword_ddf)

            except Exception as e:
                db.rollback()
                messagebox.showerror("Error", f"An error occurred: {e}")
                traceback.print_exc()
            finally:
                cursor.close()
                db.close()
            insert_data_into_email_table()

        webmap_thread = threading.Thread(target=run_webmap_thread)
        webmap_thread.start()
        end_time = time.time()
        execution_time = end_time - start_time
        print("run web map process", execution_time)

    def on_pause_button_click():
        global webmap_paused, webmap_pause_event, webmap_paused_event
        webmap_paused = True
        webmap_pause_event.set()
        webmap_paused_event.set()
        pause_csv = tk.Label(root,
                             text='Wepmap process paused successfully!')
        pause_csv.pack()
        pause_button.config(state=tk.DISABLED)
        resume_button.config(state=tk.NORMAL, bg='lightgray', fg='black')

    def is_law_firm(content, keywords):
        for entry in keywords:
            keyword = entry['keyword']
            threshold = entry['threshold']
            frequency = content.lower().count(keyword.lower())
            if frequency >= threshold:
                print(f"Keyword '{keyword}' found with frequency {frequency} which is above the threshold {threshold}.")
                return keyword, frequency, True
        print("No keyword found with sufficient frequency.")
        return None, 0, False

    def search_keyword_on_home_page(site_id, url, cursor, db, keyword_ddf):
        global webmap_paused_event, webmap_paused
        try:
            cursor.execute("SELECT site_id, page_url, status FROM page_site WHERE status='open'")
            sites = cursor.fetchall()
            total_sites = len(sites)

            update_page_site_values = []
            update_sites_table_values = []
            insert_found_keywords_values = []
            insert_email_values = []

            for index, (site_id, url, status) in enumerate(sites, start=1):
                if webmap_paused_event.is_set():
                    return
                if status == 'open':
                    try:
                        start_time = time.time()
                        url_response = requests.get(url, timeout=current_timeout)
                        url_response.raise_for_status()
                        url_soup = BeautifulSoup(url_response.content, 'html.parser')

                        for tag in url_soup(['script', 'style']):
                            tag.decompose()

                        url_text_content = url_soup.get_text(separator=' ', strip=True)
                        print(f"Content from {index}", url, ":\n", url_text_content)

                        cursor.execute('SELECT keywords, threshold, priority FROM law_keywords ORDER BY priority')
                        law_keywords = cursor.fetchall()

                        is_law_firm = False
                        law_keyword_counts = []

                        for keyword, threshold, priority in law_keywords:
                            count = url_text_content.lower().count(keyword.lower())
                            if count >= threshold:
                                is_law_firm = True
                            law_keyword_counts.append((keyword, count))
                        law_firm_status = 'Yes' if is_law_firm else 'No'
                        for keyword, count in law_keyword_counts:
                            insert_email_values.append(
                                (site_id, keyword, law_firm_status, count)
                            )

                        if is_law_firm:
                            cursor.execute("SELECT keyword FROM keyword_table")
                            keywords = [row[0] for row in cursor.fetchall()]

                            keyword_counts = Counter()
                            for keyword in keywords:
                                count = url_text_content.lower().count(keyword.lower())
                                if count > 0:
                                    keyword_counts[keyword] = count
                            if keyword_counts:
                                print("Keyword occurrences on Home Page:")
                                for keyword, count in keyword_counts.items():
                                    print(f"Keyword '{keyword}' found {count} times")
                                    insert_found_keywords_values.append((site_id, url, keyword, count))
                                    update_page_site_values.append((url, site_id))
                                    update_sites_table_values.append((url, site_id))
                        else:
                            insert_found_keywords_values.append((site_id, url, '', 0))

                        end_time = time.time()
                        print("Search Keyword on Homepage", end_time - start_time)
                        progress_value = index / total_sites * 100
                        update_progress_in_page_site(cursor, db, progress_value)
                        Totalurl.config(text=f"Total Number of Page URLs to scan: {total_sites}")

                    except (requests.RequestException, requests.Timeout) as e:
                        print(f"Error while fetching or processing URL {url}: {e}")
                        update_error_log(cursor, db, site_id, url, e, inspect.currentframe().f_lineno)
                        continue  # Skip the insertion and update for this URL

                if webmap_paused_event.is_set():
                    return

            if insert_found_keywords_values:
                cursor.executemany("""
                    INSERT INTO found_keywords_table (site_id, page_url, keyword, count) 
                    VALUES (%s, %s, %s, %s)
                """, insert_found_keywords_values)

            if insert_email_values:
                cursor.executemany("""
                    INSERT INTO law_count(site_id, keywords, law_firm, count) 
                    VALUES (%s, %s, %s, %s)
                """, insert_email_values)

            if update_page_site_values:
                cursor.executemany("""
                    UPDATE page_site SET status = 'completed' WHERE page_url = %s AND site_id = %s
                """, update_page_site_values)

            if update_sites_table_values:
                cursor.executemany("""
                    UPDATE sites_table SET status = 'completed' WHERE url = %s AND id = %s
                """, update_sites_table_values)

            db.commit()

        except (requests.RequestException, requests.Timeout) as e:
            print(f"Error while fetching or processing URLs: {e}")
            update_error_log(cursor, db, site_id, url, e, inspect.currentframe().f_lineno)
            cursor.execute("UPDATE page_site SET status = 'url not found' WHERE page_url = %s", (url,))
            cursor.execute("UPDATE sites_table SET status = 'url not found' WHERE url = %s", (url,))
            db.commit()

    def toggle_checkbox():
        global checkbox_state, url, webmap_pause_event, webmap_paused_event
        checkbox_window = tk.Toplevel(root)
        checkbox_window.title("Checkbox Window")
        check_var = tk.IntVar()
        checkbox = tk.Checkbutton(checkbox_window, text="Check for Page URLs", variable=check_var)
        checkbox.pack(pady=10)

        def get_checkbox_status():
            global checkbox_state
            checked = check_var.get()
            print("Checkbox Checked:", checked)
            print("Page URL:", url)
            checkbox_state = checked
            checkbox_window.destroy()
            if checkbox_state:
                run_webmap_button.config(command=run_webmap_process)
                webmap_pause_event.clear()
            else:
                run_webmap_button.config(command=search_keyword_on_home_page)
                webmap_paused_event.set()

        submit_button = tk.Button(checkbox_window, text="Submit", command=get_checkbox_status)
        submit_button.pack(pady=10)

    def new_resume_button_click():
        global webmap_paused, webmap_pause_event, webmap_paused_event
        resume_csv = tk.Label(root,
                              text='Wepmap process resumed successfully!')
        Download_button.config(state=tk.NORMAL, bg='lightgray', fg='black')
        resume_csv.pack()
        if checkbox_state:
            resume_webmap_process()
            webmap_pause_event.clear()
            Download_button.config(state=tk.NORMAL, bg='lightgray', fg='black')
            pause_button.config(state=tk.NORMAL,bg='lightgray',fg='black')
        else:
            webmap_paused = False
            resume_webmap_process()
            webmap_paused_event.clear()
            pause_button.config(state=tk.DISABLED)
            resume_button.config(state=tk.DISABLED)
            Download_button.config(state=tk.NORMAL, bg='lightgray', fg='black')
            pause_button.config(state=tk.NORMAL, bg='lightgray', fg='black')

    def resume_webmap_process():
        Download_button.config(state=tk.NORMAL, bg='lightgray', fg='black')
        global webmap_pause_event, site_id, url, webmap_thread, webmap_paused_event
        db = mysql.connector.connect(
            host="localhost",
            user=username,
            password=password,
            database=database,
        )
        cursor = db.cursor()
        cursor.execute("SELECT distinct(keyword) FROM keyword_table")
        keywords_data = cursor.fetchall()
        keyword_ddf = [keyword[0] for keyword in keywords_data]
        webmap_pause_event.clear()
        pause_button.config(state=tk.DISABLED)
        resume_button.config(state=tk.DISABLED)
        run_webmap_button.config(state=tk.NORMAL, bg='lightgray', fg='black')
        Download_button.config(state=tk.NORMAL, bg='lightgray', fg='black')
        resume_process_open_urls(cursor, db, keyword_ddf, site_id)

    # root = tb.Window(themename="superhero")
    def download_with_date_range():
        def download_csv_with_dates(from_date, to_date):
            try:
                download_path = filedialog.askdirectory()
                with mysql.connector.connect(
                        host="localhost",
                        user=username,
                        password=password,
                        database=database,
                ) as db:
                    cursor = db.cursor()

                    if from_date == to_date:
                        cursor.execute(
                            f"SELECT site_id,page_url,error,error_line,time_date FROM error_log WHERE Time_Date = '{from_date}'"
                        )
                    else:
                        cursor.execute(
                            f"SELECT site_id,page_url,error,error_line,time_date FROM error_log WHERE Time_Date BETWEEN '{from_date}' AND '{to_date}'"
                        )
                    error_log_data = cursor.fetchall()
                    error_log_df = pd.DataFrame(
                        error_log_data,
                        columns=["Site_id", "Page_url", "Error", "Error_line", "Time_Date"],
                    )
                    date_today = datetime.datetime.now().strftime("%m.%d.%Y")
                    error_log_filename = f'{download_path}/Error_log{date_today}.csv'
                    error_log_df.to_csv(error_log_filename, index=False)

                    if from_date == to_date:
                        cursor.execute(
                            f"SELECT * FROM email_table WHERE Date = '{from_date}'"
                        )
                    else:
                        cursor.execute(
                            f"SELECT * FROM email_table WHERE Date BETWEEN '{from_date}' AND '{to_date}'"
                        )
                    result = cursor.fetchall()
                    result_df = pd.DataFrame(result,
                                             columns=["SFID", "Website", "Account_name", "Keywords",
                                                      "Practice_area","Legal_Keywords","Law_Firm", "Date"])
                    date_today1 = datetime.datetime.now().strftime("%m.%d.%Y")
                    result_filename = f'{download_path}/WebCrawl_result{date_today1}.csv'
                    result_df.to_csv(result_filename, index=False)

                    messagebox.showinfo("Success", "Data downloaded successfully!")
            except Exception as e:
                messagebox.showerror("Error", f"An error occurred: {e}")

        def open_date_window():
            date_window = tk.Toplevel(root)
            date_window.title("Download Options")
            date_window.geometry("300x200")

            download_option = tk.StringVar()
            download_option.set("Select Download Options")
            download_options = ["Download Result", "Download Practice Area Keyword File",
                                "Download Lawfirm Keyword file"]

            options_dropdown = ttk.Combobox(date_window, textvariable=download_option, values=download_options,
                                            state="readonly",width=40)
            options_dropdown.grid(row=0, column=0, columnspan=2, padx=10, pady=5)

            def show_date_fields():
                from_date_label.grid()
                from_date_entry.grid()
                to_date_label.grid()
                to_date_entry.grid()

            def hide_date_fields():
                from_date_label.grid_remove()
                from_date_entry.grid_remove()
                to_date_label.grid_remove()
                to_date_entry.grid_remove()

            def on_dropdown_change(event):
                selected_option = download_option.get()
                if selected_option == "Download Result":
                    show_date_fields()
                else:
                    hide_date_fields()

            options_dropdown.bind("<<ComboboxSelected>>", on_dropdown_change)

            from_date_label = ttk.Label(date_window, text="From Date :")
            from_date_label.grid(row=1, column=0, padx=10, pady=5)

            from_date_entry = ttk.DateEntry(date_window, bootstyle="primary", dateformat="%m/%d/%Y")
            from_date_entry.grid(row=1, column=1, padx=10, pady=5)

            to_date_label = ttk.Label(date_window, text="To Date :")
            to_date_label.grid(row=2, column=0, padx=10, pady=5)

            to_date_entry = ttk.DateEntry(date_window, bootstyle="primary", dateformat="%m/%d/%Y")
            to_date_entry.grid(row=2, column=1, padx=10, pady=5)

            hide_date_fields()  # Initially hide date fields

            def download_button_click():
                from_date = from_date_entry.entry.get()
                to_date = to_date_entry.entry.get()
                try:
                    from_date = datetime.datetime.strptime(from_date, "%m/%d/%Y")
                    to_date = datetime.datetime.strptime(to_date, "%m/%d/%Y")
                    if from_date > to_date:
                        messagebox.showerror("Error", "From date cannot be after To date.")
                        return
                    download_csv_with_dates(from_date.strftime("%Y-%m-%d"), to_date.strftime("%Y-%m-%d"))
                    date_window.destroy()
                except ValueError:
                    messagebox.showerror("Error", "Invalid date format. Please use MM/DD/YYYY.")

            def download_practicearea_keywords():
                download_path = filedialog.askdirectory()
                cursor.execute("SELECT * FROM keyword_table")
                keywords = cursor.fetchall()
                keyword_df = pd.DataFrame(keywords, columns=["Practice_Area", "Keyword", "Weightage"])
                date_today = datetime.datetime.now().strftime("%m.%d.%Y")
                keyword_filename = f'{download_path}/Practice_Area_Keywords_{date_today}.csv'
                keyword_df.to_csv(keyword_filename, index=False)
                print(f"Practice Area Keywords saved to {keyword_filename}")

            def download_lawfirm_keywords():
                download_path = filedialog.askdirectory()
                cursor.execute("SELECT * FROM law_keywords")
                law_keywords = cursor.fetchall()
                keyword_law = pd.DataFrame(law_keywords, columns=["Keyword", "Threshold", "Priority"])
                date_today = datetime.datetime.now().strftime("%m.%d.%Y")
                keyword_filename_law = f'{download_path}/Lawfirm_Keywords_{date_today}.csv'
                keyword_law.to_csv(keyword_filename_law, index=False)
                print(f"Lawfirm Keywords saved to {keyword_filename_law}")

            def on_download_button_click():
                selected_option = download_option.get()
                if selected_option == 'Download Result':
                    download_button_click()
                elif selected_option == "Download Practice Area Keyword File":
                    download_practicearea_keywords()
                elif selected_option == "Download Lawfirm Keyword file":
                    download_lawfirm_keywords()
                else:
                    print("Please select a valid option from the dropdown menu.")

            download_button = ttk.Button(date_window, text="Download", command=on_download_button_click)
            download_button.grid(row=3, column=0, columnspan=2, padx=10, pady=10)

        open_date_window()

    def check_and_rename_file(filename, max_size_mb=25):
        try:
            file_size_bytes = os.path.getsize(filename)
            file_size_mb = file_size_bytes / (1024 * 1024)
            if file_size_mb > max_size_mb:
                file_name, file_extension = os.path.splitext(filename)
                count = 1
                new_filename = f"{file_name}_{count}{file_extension}"
                while os.path.exists(new_filename):
                    count += 1
                    new_filename = f"{file_name}_{count}{file_extension}"
                os.rename(filename, new_filename)
                print(f"File '{filename}' exceeded size limit. Renamed to '{new_filename}'.")
        except Exception as e:
            print(f"Error checking and renaming file: {e}")
        Download_button.config(state=tk.NORMAL, bg='lightgray', fg='black')

    button_frame = tk.Frame(root)
    button_frame.pack(pady=10)
    settings_options = ["Database Setting", "Timeout Setting"]
    selected_setting = tk.StringVar()
    settings_dropdown = ttk.Combobox(button_frame, textvariable=selected_setting, values=settings_options,
                                     state="readonly")
    settings_dropdown.grid(row=20, column=6, padx=10)
    settings_dropdown.set("Select setting option")

    def add_keywords():
        lawfirm_keyword = filedialog.askopenfilename(title='Select Lawfirm Keywords', filetypes=[("CSV file", "*.csv")])

        if lawfirm_keyword:
            keywords_df = pd.read_csv(lawfirm_keyword)
            keywords = keywords_df['keywords'].tolist()
            thresholds = keywords_df['threshold'].tolist()
            priorities = keywords_df['priority'].tolist()

            try:
                cursor.execute('TRUNCATE TABLE law_keywords')  # Truncate the table first

                insert_values = []
                for keyword, threshold, priority in zip(keywords, thresholds, priorities):
                    keyword_list = re.split('[;,]', keyword)

                    if isinstance(threshold, int):
                        threshold_values = [threshold]
                    else:
                        threshold_values = [int(val) for val in threshold.split(';')] if ';' in threshold else [
                            int(threshold)]

                    if len(keyword_list) == len(threshold_values):
                        for kword, tvalue in zip(keyword_list, threshold_values):
                            insert_values.append((kword.strip(), tvalue, priority))
                    else:
                        print(
                            f'Mismatch in number of keywords and thresholds in row - keyword={keyword}, threshold={threshold}, priority={priority}')
                        messagebox.showwarning("Warning", "Mismatch in number of keywords and thresholds")

                insert_query = "INSERT INTO law_keywords (keywords, threshold, priority) VALUES (%s, %s, %s)"
                cursor.executemany(insert_query, insert_values)
                db.commit()

                keyword_label = tk.Label(root, text="Lawfirm_Keyword CSV file uploaded successfully!")
                keyword_label.pack()
            except mysql.connector.Error as e:
                messagebox.showerror("Error", f"Failed to connect to MySQL: {e}")
            finally:
                cursor.close()
                db.close()

    def open_selected_settings_window(event):
        selected_option = selected_setting.get()
        if selected_option == "Database Setting":
            open_mysql_connection_window()
        elif selected_option == "Timeout Setting":
            open_settings_window()
        elif selected_option == "Add Lawfirm Keywords":
            add_keywords()

    def open_settings_window():
        global timeout_var
        settings_window = tk.Toplevel(root)
        settings_window.title("Settings Window")
        settings_window.geometry('230x150')
        timeout_label = tk.Label(settings_window, text="Enter new timeout value:")
        timeout_label.pack(pady=10)
        timeout_var = tk.StringVar()
        timeout_entry = tk.Entry(settings_window, textvariable=timeout_var)
        timeout_entry.pack(pady=10)
        update_timeout_button = tk.Button(settings_window, text="Update Timeout",
                                          command=lambda: update_timeout(int(timeout_var.get()), settings_window))
        update_timeout_button.pack(pady=10)

    def update_timeout(new_timeout, settings_window):
        global current_timeout, timeout_var
        try:
            if new_timeout > 0:
                current_timeout = new_timeout
                timeout_var.set(new_timeout)
                settings_window.destroy()
                messagebox.showinfo("Timeout Updated", f"Timeout value updated to {new_timeout} seconds.")
            else:
                messagebox.showwarning("Invalid Timeout", "Please enter a positive integer for the timeout.")
        except ValueError:
            messagebox.showwarning("Invalid Timeout", "Please enter a valid integer for the timeout.")

    settings_dropdown.bind("<<ComboboxSelected>>", open_selected_settings_window)

    upload_keyword_button = tk.Button(button_frame, text="Upload Practice Area", state=tk.DISABLED,
                                      command=upload_keyword_csv, fg="black",height=1 ,width= 18)
    upload_keyword_button.grid(row=20, column=0, padx=10)

    label_keyword = tk.Label(button_frame, text="1")
    label_keyword.grid(row=21, column=0, pady=5)

    upload_site_button = tk.Button(button_frame, text="Upload Websites", state=tk.DISABLED, command=upload_site_csv,
                                   fg="black",height=1 ,width= 18)
    upload_site_button.grid(row=20, column=1, padx=23)
    label_keyword = tk.Label(button_frame, text="2")
    label_keyword.grid(row=21, column=1, pady=5)

    upload_law_keyword_button = tk.Button(button_frame, text="Upload Law Keywords", state=tk.DISABLED, command=add_keywords,
                                   fg="black",height=1 ,width= 18)
    upload_law_keyword_button.grid(row=20, column=2, padx=23)
    label_keyword = tk.Label(button_frame, text="3")
    label_keyword.grid(row=21, column=2, pady=5)

    check_button = tk.Button(button_frame, text="Visit Suburls", state=tk.DISABLED, command=toggle_checkbox, fg="black",height=1 ,width= 18)
    check_button.grid(row=20, column=3, padx=23)
    label_keyword = tk.Label(button_frame, text="4")
    label_keyword.grid(row=21, column=3, pady=5)
    run_webmap_button = tk.Button(button_frame, text="Run WebCrawler", state=tk.DISABLED, command=run_webmap_process,
                                  fg="black",height=1 ,width= 18)
    run_webmap_button.grid(row=20, column=4, padx=23)
    label_keyword = tk.Label(button_frame, text="5")
    label_keyword.grid(row=21, column=4, pady=5)
    Download_button = tk.Button(button_frame, text="Download Result", state=tk.DISABLED,
                                command=download_with_date_range, fg="black",height=1 ,width= 18)
    Download_button.grid(row=20, column=5, padx=23)
    label_keyword = tk.Label(button_frame, text="6")
    label_keyword.grid(row=21, column=5, pady=5)
    url_count_label = tk.Label(root, text="Total Number of Site URLs to scan: 0")
    url_count_label.place(relx=0.5, rely=0.120, anchor="center")
    Totalurl = tk.Label(root, text="Total Number of Page URLs to scan: 0")
    Totalurl.place(relx=0.5, rely=0.160, anchor="center")
    progress_bar = ttk.Progressbar(root, orient="horizontal", length=400, mode="determinate")
    progress_bar.place(relx=0.5, rely=0.20, anchor="center")
    progress_label = tk.Label(root, text="0%")
    progress_label.place(relx=0.5, rely=0.250, anchor="center")
    pause_button = tk.Button(button_frame, text="Pause", state=tk.DISABLED, command=on_pause_button_click, fg="black",height=1 ,width= 18)
    pause_button.grid(row=23, column=6, padx=15, pady=12)
    resume_button = tk.Button(button_frame, text="Resume", state=tk.DISABLED, command=new_resume_button_click,
                              fg="black",height=1 ,width= 18)
    resume_button.grid(row=26, column=6, padx=13, pady=12)
    restart_button = tk.Button(button_frame, text="Resume on Halt", state=tk.DISABLED, command=new_resume_button_click,
                               fg="black",height=1 ,width= 18)
    restart_button.grid(row=29, column=6, padx=13, pady=12)
    upload_keyword_button.config(state=tk.NORMAL, bg='lightgray', fg='black')
    restart_button.config(state=tk.NORMAL, bg='lightgray', fg='black')
    Download_button.config(state=tk.NORMAL, bg='lightgray', fg='black')
    #upload_law_keyword_button.config(state=tk.NORMAL ,bg='lightgray',fg='black')
    root.mainloop()
    cursor.close()
    db.close()

if __name__ == "__main__":
    run_webmap()