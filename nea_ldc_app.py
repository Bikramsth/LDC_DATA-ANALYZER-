import os
import re
import time
import sqlite3
import threading
import io
import datetime
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# ==========================================
# 1. CONFIGURATION & DATABASE SETUP
# ==========================================
FOLDER_TO_WATCH = "./LDC_Data"
DATABASE_NAME = "NEA_LDC_Database.db"


def init_db():
    conn = sqlite3.connect(DATABASE_NAME, timeout=30.0)
    cursor = conn.cursor()
    cursor.execute('PRAGMA journal_mode=WAL;')

    # 1. Main Data Table
    cursor.execute('''
                   CREATE TABLE IF NOT EXISTS system_log_data
                   (
                       nepali_year
                       INTEGER,
                       nepali_month
                       INTEGER,
                       nepali_day
                       INTEGER,
                       time_interval
                       TEXT,
                       parameter_name
                       TEXT,
                       value
                       REAL,
                       last_updated
                       DATETIME
                       DEFAULT
                       CURRENT_TIMESTAMP,
                       UNIQUE
                   (
                       nepali_year,
                       nepali_month,
                       nepali_day,
                       time_interval,
                       parameter_name
                   )
                       )
                   ''')

    # 2. SMART TRACKING TABLE: Remembers which files we've already scanned
    cursor.execute('''
                   CREATE TABLE IF NOT EXISTS processed_files
                   (
                       filename
                       TEXT
                       UNIQUE,
                       mtime
                       REAL,
                       processed_date
                       DATETIME
                       DEFAULT
                       CURRENT_TIMESTAMP
                   )
                   ''')

    conn.commit()
    conn.close()


def run_query(query, params=()):
    conn = sqlite3.connect(DATABASE_NAME, timeout=30.0)
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    return df


# ==========================================
# 2. DATA EXTRACTION ENGINE (CSV & Excel)
# ==========================================
def parse_filename(filename):
    year, month = None, None
    year_match = re.search(r'(20[7-9]\d)', filename)
    if year_match: year = int(year_match.group(1))
    month_match = re.search(r'[-_](1[0-2]|[1-9])[-_]|\b(1[0-2]|[1-9])[-_](?:20[7-9]\d)', filename)
    if month_match:
        month = int(month_match.group(1) or month_match.group(2))
    return year, month


def extract_data(df, year, month, day, cursor):
    rows_inserted = 0
    header_row_index = -1
    time_columns = {}
    current_block = "IPP_ZONE"

    for idx, row in df.iterrows():
        current_time_cols = {}
        for col_idx in range(1, len(row)):
            val = row[col_idx]
            if pd.isna(val): continue

            str_val = str(val).strip()
            match = re.search(r'\b(\d{1,2}):(\d{2})', str_val)
            if match:
                hr = int(match.group(1));
                mnt = int(match.group(2))
                current_time_cols[col_idx] = f"{hr:02d}:{mnt:02d}:00"
            else:
                try:
                    num = float(val)
                    if 0 < num <= 24:
                        hr = int(num);
                        mnt = int(round((num - hr) * 60))
                        current_time_cols[col_idx] = f"{hr:02d}:{mnt:02d}:00"
                except:
                    pass

        if len(current_time_cols) >= 20:
            time_columns = current_time_cols
            header_row_index = idx
            break

    if header_row_index == -1: return 0

    for row_idx in range(header_row_index + 1, len(df)):
        row = df.iloc[row_idx]
        if pd.isna(row[0]) or str(row[0]).strip() == "": continue
        raw_name = str(row[0]).strip()

        if "Total IPP" in raw_name:
            db_param_name = raw_name
            current_block = "NEA_SUB_ZONE"
        elif "Total NEA SUBSIDIARIES" in raw_name:
            db_param_name = raw_name
            current_block = "ROR_ZONE"
        elif "Total ROR" in raw_name:
            db_param_name = raw_name
            current_block = "STORAGE_ZONE"
        elif "Total STORGE" in raw_name or "Total Storage" in raw_name:
            db_param_name = raw_name
            current_block = "IMPORT_ZONE"
        elif "Total IMPORT" in raw_name:
            db_param_name = "SUMMARY_TOTAL_IMPORT"
            current_block = "AFTER_IMPORT"
        elif "TOTAL NATIONAL LOAD" in raw_name or "NATIONAL LOAD" in raw_name:
            current_block = "EXPORT_ZONE"
            db_param_name = raw_name
        elif "Total EXPORT" in raw_name:
            db_param_name = "SUMMARY_TOTAL_EXPORT"
            current_block = "FINAL_TOTALS"
        else:
            if current_block == "IPP_ZONE":
                db_param_name = f"ZONE_IPP_{raw_name}"
            elif current_block == "NEA_SUB_ZONE":
                db_param_name = f"ZONE_NEASUB_{raw_name}"
            elif current_block == "ROR_ZONE":
                db_param_name = f"ZONE_ROR_{raw_name}"
            elif current_block == "STORAGE_ZONE":
                db_param_name = f"ZONE_STORAGE_{raw_name}"
            elif current_block == "IMPORT_ZONE":
                db_param_name = f"ZONE_IMPORT_{raw_name}"
            elif current_block == "EXPORT_ZONE":
                db_param_name = f"ZONE_EXPORT_{raw_name}"
            else:
                db_param_name = raw_name

        for col_idx, time_interval in time_columns.items():
            cell_value = row[col_idx] if col_idx < len(row) else None
            try:
                clean_val = str(cell_value).replace(',', '').strip()
                if clean_val in ['', '-']:
                    final_value = 0.0
                else:
                    final_value = float(clean_val)
                    if current_block == "EXPORT_ZONE" or db_param_name == "SUMMARY_TOTAL_EXPORT":
                        final_value = -abs(final_value)
            except:
                final_value = 0.0

            cursor.execute('''
                           INSERT INTO system_log_data
                           (nepali_year, nepali_month, nepali_day, time_interval, parameter_name, value)
                           VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(nepali_year, nepali_month, nepali_day, time_interval, parameter_name) 
                DO
                           UPDATE SET value = excluded.value
                           ''', (year, month, day, time_interval, db_param_name, final_value))
            rows_inserted += 1

    return rows_inserted


def process_file(file_path):
    time.sleep(2)
    filename = os.path.basename(file_path)

    if not filename.lower().endswith(('.xlsx', '.xls', '.csv')) or filename.startswith('~'):
        return "ERROR", f"Skipped `{filename}`: Unsupported or temporary file."

    year, month = parse_filename(filename)
    if year is None or month is None:
        return "ERROR", f"⚠️ OVERWRITE PREVENTED: `{filename}` does not contain a clear Year and Month (e.g., '10-2082')."

    try:
        # Get the exact Last Modified timestamp of the file
        mtime = os.path.getmtime(file_path)

        conn = sqlite3.connect(DATABASE_NAME, timeout=30.0)
        cursor = conn.cursor()

        # SMART CHECK: Have we scanned this exact version of the file before?
        cursor.execute("SELECT mtime FROM processed_files WHERE filename = ?", (filename,))
        row = cursor.fetchone()
        if row and row[0] == mtime:
            conn.close()
            return "SKIPPED", f"Skipped `{filename}` (Already recorded in database)."

        # If it's new or has been edited, extract the data
        rows_inserted = 0
        if filename.lower().endswith('.csv'):
            match_day = re.search(r'-\s*(\d+)\.csv$', filename.lower())
            day = int(match_day.group(1)) if match_day else 1
            df = pd.read_csv(file_path, header=None, encoding='utf-8', on_bad_lines='skip')
            rows_inserted += extract_data(df, year, month, day, cursor)
        else:
            xl = pd.ExcelFile(file_path, engine='openpyxl' if filename.endswith('.xlsx') else None)
            for sheet_name in xl.sheet_names:
                if sheet_name.strip().isdigit():
                    day = int(sheet_name.strip())
                    df = xl.parse(sheet_name, header=None)
                    rows_inserted += extract_data(df, year, month, day, cursor)

        if rows_inserted > 0:
            # Save the file to our memory so we don't scan this version again
            cursor.execute('''
                           INSERT INTO processed_files (filename, mtime)
                           VALUES (?, ?) ON CONFLICT(filename) DO
                           UPDATE
                           SET mtime=excluded.mtime
                           ''', (filename, mtime))
            conn.commit()
            conn.close()
            return "SUCCESS", f"✅ Added `{filename}` (Year {year}, Month {month})."
        else:
            conn.commit()
            conn.close()
            return "ERROR", f"No matching grid data found inside `{filename}`."

    except Exception as e:
        return "ERROR", f"Error reading `{filename}`: {str(e)}"


# ==========================================
# 3. BACKGROUND FOLDER MONITOR
# ==========================================
class FileWatcher(FileSystemEventHandler):
    def on_modified(self, event):
        if not event.is_directory and event.src_path.lower().endswith(('.xlsx', '.xls', '.csv')):
            time.sleep(1)
            process_file(event.src_path)

    def on_created(self, event):
        if not event.is_directory and event.src_path.lower().endswith(('.xlsx', '.xls', '.csv')):
            time.sleep(1)
            process_file(event.src_path)


@st.cache_resource
def start_background_monitor():
    if not os.path.exists(FOLDER_TO_WATCH):
        os.makedirs(FOLDER_TO_WATCH)
    event_handler = FileWatcher()
    observer = Observer()
    observer.schedule(event_handler, FOLDER_TO_WATCH, recursive=False)
    monitor_thread = threading.Thread(target=observer.start, daemon=True)
    monitor_thread.start()
    return observer


init_db()
observer = start_background_monitor()

# ==========================================
# 4. STREAMLIT DASHBOARD UI
# ==========================================
st.set_page_config(page_title="NEA LDC Dashboard", layout="wide", initial_sidebar_state="expanded")
st.markdown("<style>.stApp { background-color: #FFFFFF; color: #000000; }</style>", unsafe_allow_html=True)
st.title("⚡ Nepal Electricity Authority - LDC Dashboard")

years_df = run_query("SELECT DISTINCT nepali_year FROM system_log_data ORDER BY nepali_year DESC")

# --- INITIAL EMPTY STATE SCREEN ---
if years_df.empty:
    st.warning("Database is currently empty.")
    st.info(
        f"**Step 1:** Ensure your files have the Month and Year in the name (e.g. `Log-10-2082.xlsx`).\n\n**Step 2:** Place them into the `{FOLDER_TO_WATCH}` folder.\n\n**Step 3:** Click Force Scan.")

    if st.button("🚀 Force Manual Scan (Scan All Files)"):
        new_files, skipped_files, error_files = 0, 0, 0
        if os.path.exists(FOLDER_TO_WATCH):
            for filename in os.listdir(FOLDER_TO_WATCH):
                if filename.lower().endswith(('.xlsx', '.xls', '.csv')) and not filename.startswith('~'):
                    file_path = os.path.join(FOLDER_TO_WATCH, filename)
                    st.write(f"Checking `{filename}`...")
                    status, message = process_file(file_path)

                    if status == "SKIPPED":
                        skipped_files += 1
                    elif status == "SUCCESS":
                        new_files += 1
                        st.success(message)
                    else:
                        error_files += 1
                        st.error(message)

        st.info(
            f"📊 **Scan Complete:** {new_files} New Files Added | {skipped_files} Previously Recorded Skipped | {error_files} Errors")
        if new_files > 0:
            time.sleep(3)
            st.rerun()
    st.stop()

# --- SIDEBAR UI ---
st.sidebar.header("Filter Data")

if st.sidebar.button("🚀 Force Manual Scan"):
    new_files, skipped_files, error_files = 0, 0, 0
    if os.path.exists(FOLDER_TO_WATCH):
        with st.sidebar.status("Scanning Folder...", expanded=True) as status_box:
            for filename in os.listdir(FOLDER_TO_WATCH):
                if filename.lower().endswith(('.xlsx', '.xls', '.csv')) and not filename.startswith('~'):
                    file_path = os.path.join(FOLDER_TO_WATCH, filename)
                    status, message = process_file(file_path)

                    if status == "SKIPPED":
                        skipped_files += 1
                    elif status == "SUCCESS":
                        new_files += 1
                        st.write(f"✅ {filename}")
                    else:
                        error_files += 1
                        st.write(f"❌ Error in {filename}")

            status_box.update(label="Scan Complete!", state="complete", expanded=False)

        # Show the nice summarized output to the user
        st.sidebar.success(f"**{new_files}** New files processed.")
        st.sidebar.info(f"**{skipped_files}** Previous files skipped.")
        if error_files > 0: st.sidebar.error(f"**{error_files}** Files failed.")

        if new_files > 0:
            time.sleep(2)
            st.rerun()
    else:
        st.sidebar.error("LDC_Data folder not found.")

st.sidebar.divider()

years_df = run_query("SELECT DISTINCT nepali_year FROM system_log_data ORDER BY nepali_year DESC")
if not years_df.empty:
    selected_year = st.sidebar.selectbox("Select Nepali Year", years_df['nepali_year'].tolist())
    months_df = run_query(
        "SELECT DISTINCT nepali_month FROM system_log_data WHERE nepali_year = ? ORDER BY nepali_month",
        (selected_year,))
    selected_month = st.sidebar.selectbox("Select Nepali Month", months_df['nepali_month'].tolist())
    days_df = run_query(
        "SELECT DISTINCT nepali_day FROM system_log_data WHERE nepali_year = ? AND nepali_month = ? ORDER BY nepali_day",
        (selected_year, selected_month))
    selected_day = st.sidebar.selectbox("Select Nepali Day", days_df['nepali_day'].tolist())

# --- UI TABS ---
tab1, tab2, tab3 = st.tabs(["Daily Operations", "Monthly Peak Load Graph", "Historical Monthly Peak Load"])

with tab1:
    sub_tab_main, sub_tab_import, sub_tab_subs, sub_tab_ipp, sub_tab_ror, sub_tab_storage = st.tabs([
        "Main System", "Total IMPORT", "Total NEA SUBSIDIARIES", "Total IPP", "Total ROR", "Total STORAGE"
    ])

    query_all = '''
                SELECT time_interval as Time, parameter_name, value as MW
                FROM system_log_data
                WHERE nepali_year = ? AND nepali_month = ? AND nepali_day = ?
                ORDER BY time_interval ASC \
                '''
    df_all = run_query(query_all, (selected_year, selected_month, selected_day))

    if not df_all.empty:
        # --- SUB-TAB: MAIN SYSTEM ---
        with sub_tab_main:
            all_db_params = df_all['parameter_name'].unique()
            raw_supply_components = [
                p for p in all_db_params
                if (
                               'TOTAL' in p.upper() or 'INTERRUPTION' in p.upper() or 'ROR' in p.upper() or 'STORGE' in p.upper() or 'STORAGE' in p.upper())
                   and 'IMPORT' not in p.upper()
                   and 'EXPORT' not in p.upper()
                   and 'LOAD' not in p.upper()
                   and 'OTHER IPP' not in p.upper()
                   and 'ZONE_' not in p.upper()
            ]

            import_tag = 'SUMMARY_TOTAL_IMPORT'
            export_tag = 'SUMMARY_TOTAL_EXPORT'

            required_params = raw_supply_components.copy()
            if import_tag in all_db_params: required_params.append(import_tag)
            if export_tag in all_db_params: required_params.append(export_tag)

            df_summary = df_all[df_all['parameter_name'].isin(required_params)].copy()

            if not df_summary.empty:
                df_pivot = df_summary.pivot(index='Time', columns='parameter_name', values='MW').fillna(0.0)
                df_pivot = df_pivot.sort_index()

                valid_supply_components = []
                for col in raw_supply_components:
                    if col in df_pivot.columns:
                        if df_pivot[col].abs().sum() > 0 or 'STORGE' in col.upper() or 'STORAGE' in col.upper():
                            valid_supply_components.append(col)

                if import_tag in df_pivot.columns and import_tag not in valid_supply_components:
                    valid_supply_components.append(import_tag)

                if export_tag not in df_pivot.columns: df_pivot[export_tag] = 0.0
                if import_tag not in df_pivot.columns: df_pivot[import_tag] = 0.0

                df_pivot[import_tag] = df_pivot[import_tag] - df_pivot[export_tag].abs()
                df_pivot['DYNAMIC_NATIONAL_LOAD'] = df_pivot[valid_supply_components].sum(axis=1)
                df_pivot['DYNAMIC_SYSTEM_LOAD'] = df_pivot['DYNAMIC_NATIONAL_LOAD'] + df_pivot[export_tag].abs()

                sys_peak_hour = df_pivot['DYNAMIC_SYSTEM_LOAD'].idxmax()
                sys_peak_val = df_pivot.loc[sys_peak_hour, 'DYNAMIC_SYSTEM_LOAD']
                nat_peak_hour = df_pivot['DYNAMIC_NATIONAL_LOAD'].idxmax()
                nat_peak_val = df_pivot.loc[nat_peak_hour, 'DYNAMIC_NATIONAL_LOAD']

                m1, m2 = st.columns(2)
                m1.metric(f"Max System Load (at {sys_peak_hour})", f"{sys_peak_val:.2f} MW")
                m2.metric(f"Max National Peak (at {nat_peak_hour})", f"{nat_peak_val:.2f} MW")

                fig_main = go.Figure()

                for col in valid_supply_components:
                    legend_name = col.replace('SUMMARY_TOTAL_', 'Total ').replace('STORGE', 'Storage').replace('Storge',
                                                                                                               'Storage')
                    if col == import_tag: legend_name = "Total IMPORT (Net)"

                    fig_main.add_trace(go.Scatter(
                        x=df_pivot.index, y=df_pivot[col], name=legend_name,
                        stackgroup='supply_stack', line=dict(width=0.5),
                        hovertemplate='<b>%{fullData.name}</b>: %{y:.2f} MW<extra></extra>'
                    ))

                if df_pivot[export_tag].abs().sum() > 0:
                    fig_main.add_trace(go.Scatter(
                        x=df_pivot.index, y=-df_pivot[export_tag].abs(),
                        name='Total EXPORT', stackgroup='export_stack',
                        line=dict(width=0.5),
                        hovertemplate='<b>%{fullData.name}</b>: %{y:.2f} MW<extra></extra>'
                    ))

                fig_main.add_trace(go.Scatter(
                    x=df_pivot.index, y=df_pivot['DYNAMIC_SYSTEM_LOAD'],
                    name='SYSTEM LOAD', line=dict(color='red', width=2, dash='dot')
                ))

                fig_main.add_trace(go.Scatter(
                    x=df_pivot.index, y=df_pivot['DYNAMIC_NATIONAL_LOAD'],
                    name='NATIONAL LOAD', line=dict(color='black', width=3)
                ))

                fig_main.update_layout(
                    title="System Operation & Load Curve",
                    hovermode="x unified", template="plotly_white",
                    xaxis=dict(title="Time", type="category"),
                    yaxis_title="Power (MW)",
                    legend=dict(orientation="v", y=1, x=1.02, bgcolor="rgba(255,255,255,0.8)"),
                    margin=dict(l=10, r=150, t=40, b=10)
                )

                fig_main.add_hline(y=0, line_width=2, line_color="black")
                st.plotly_chart(fig_main, use_container_width=True)

                with st.expander("🔍 View Filtered Supply Components & Loads"):
                    display_cols = ['DYNAMIC_SYSTEM_LOAD', 'DYNAMIC_NATIONAL_LOAD'] + valid_supply_components + [
                        export_tag]
                    st.dataframe(df_pivot[display_cols].style.format("{:.2f}"))
            else:
                st.info("No system summary data found.")

        # --- SUB-TAB: TOTAL IMPORT ---
        with sub_tab_import:
            df_imp = df_all[df_all['parameter_name'].str.startswith('ZONE_IMPORT_', na=False)].copy()

            if not df_imp.empty:
                df_imp['Display_Name'] = df_imp['parameter_name'].str.replace('ZONE_IMPORT_', '')
                df_pivot = df_imp.pivot(index='Time', columns='Display_Name', values='MW').fillna(0)
                df_pivot = df_pivot.sort_index()
                df_pivot['CALCULATED_TOTAL'] = df_pivot.sum(axis=1)

                fig = go.Figure()
                for col in df_pivot.columns:
                    if col == 'CALCULATED_TOTAL': continue
                    fig.add_trace(go.Scatter(
                        x=df_pivot.index, y=df_pivot[col], name=col,
                        stackgroup='import_flow',
                        line=dict(width=1),
                        hovertemplate='<b>%{fullData.name}</b>: %{y:.2f} MW<extra></extra>'
                    ))

                fig.add_trace(go.Scatter(
                    x=df_pivot.index, y=df_pivot['CALCULATED_TOTAL'],
                    name='TOTAL IMPORT',
                    line=dict(color='black', width=3),
                    hovertemplate='<b>TOTAL</b>: %{y:.2f} MW<extra></extra>'
                ))

                fig.update_layout(
                    title="Dynamic Breakdown: Import & Export",
                    hovermode="x unified", template="plotly_white",
                    xaxis=dict(title="Time", type="category"),
                    yaxis_title="MW (+Import / -Export)",
                    legend=dict(orientation="v", y=1, x=1.02, bgcolor="rgba(255,255,255,0.8)"),
                    margin=dict(l=10, r=150, t=40, b=10)
                )
                fig.add_hline(y=0, line_width=2, line_color="black")
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No Import zone data found.")

        # --- SUB-TAB: TOTAL NEA SUBSIDIARIES ---
        with sub_tab_subs:
            zone_prefix = 'ZONE_NEASUB_'
            df_subs = df_all[df_all['parameter_name'].str.startswith(zone_prefix, na=False)].copy()

            if not df_subs.empty:
                df_subs['Display_Name'] = df_subs['parameter_name'].str.replace(zone_prefix, '')
                df_pivot = df_subs.pivot(index='Time', columns='Display_Name', values='MW').fillna(0.0)
                df_pivot = df_pivot.sort_index()

                valid_cols = [c for c in df_pivot.columns if df_pivot[c].abs().sum() > 0]
                df_pivot['CALCULATED_TOTAL'] = df_pivot[valid_cols].sum(axis=1)

                fig_subs = go.Figure()
                for col in valid_cols:
                    fig_subs.add_trace(go.Scatter(
                        x=df_pivot.index, y=df_pivot[col], name=col,
                        stackgroup='subs_stack', line=dict(width=0.5),
                        hovertemplate='<b>%{fullData.name}</b>: %{y:.2f} MW<extra></extra>'
                    ))

                fig_subs.add_trace(go.Scatter(
                    x=df_pivot.index, y=df_pivot['CALCULATED_TOTAL'],
                    name='CALCULATED TOTAL', line=dict(color='black', width=3),
                    hovertemplate='<b>TOTAL SUBSIDIARIES</b>: %{y:.2f} MW<extra></extra>'
                ))

                fig_subs.update_layout(
                    title="Dynamic Breakdown: NEA Subsidiaries",
                    hovermode="x unified", template="plotly_white",
                    xaxis=dict(title="Time", type="category"),
                    yaxis_title="Power (MW)",
                    legend=dict(orientation="v", y=1, x=1.02, bgcolor="rgba(255,255,255,0.8)"),
                    margin=dict(l=10, r=150, t=40, b=10)
                )
                st.plotly_chart(fig_subs, use_container_width=True)

                with st.expander("🔍 View Component Breakdown & Verification"):
                    st.dataframe(df_pivot[valid_cols + ['CALCULATED_TOTAL']].style.format("{:.2f}"))
            else:
                st.info("No rows found in the NEA Subsidiaries zone.")

        # --- SUB-TAB: TOTAL IPP ---
        with sub_tab_ipp:
            zone_prefix = 'ZONE_IPP_'
            df_ipp = df_all[df_all['parameter_name'].str.startswith(zone_prefix, na=False)].copy()

            if not df_ipp.empty:
                df_ipp['Display_Name'] = df_ipp['parameter_name'].str.replace(zone_prefix, '')
                df_pivot = df_ipp.pivot(index='Time', columns='Display_Name', values='MW').fillna(0.0)
                df_pivot = df_pivot.sort_index()

                valid_cols = [c for c in df_pivot.columns if df_pivot[c].abs().sum() > 0]
                df_pivot['CALCULATED_TOTAL'] = df_pivot[valid_cols].sum(axis=1)

                fig_ipp = go.Figure()
                for col in valid_cols:
                    fig_ipp.add_trace(go.Scatter(
                        x=df_pivot.index, y=df_pivot[col], name=col,
                        stackgroup='ipp_stack', line=dict(width=0.5),
                        hovertemplate='<b>%{fullData.name}</b>: %{y:.2f} MW<extra></extra>'
                    ))

                fig_ipp.add_trace(go.Scatter(
                    x=df_pivot.index, y=df_pivot['CALCULATED_TOTAL'],
                    name='CALCULATED TOTAL', line=dict(color='black', width=3),
                    hovertemplate='<b>TOTAL IPP</b>: %{y:.2f} MW<extra></extra>'
                ))

                fig_ipp.update_layout(
                    title="Dynamic Breakdown: Independent Power Producers (IPP)",
                    hovermode="x unified", template="plotly_white",
                    xaxis=dict(title="Time", type="category"),
                    yaxis_title="Power (MW)",
                    legend=dict(orientation="v", y=1, x=1.02, bgcolor="rgba(255,255,255,0.8)"),
                    margin=dict(l=10, r=150, t=40, b=10)
                )
                st.plotly_chart(fig_ipp, use_container_width=True)

                with st.expander("🔍 View Component Breakdown & Verification"):
                    st.dataframe(df_pivot[valid_cols + ['CALCULATED_TOTAL']].style.format("{:.2f}"))
            else:
                st.info("No rows found in the IPP zone.")

        # --- SUB-TAB: TOTAL ROR ---
        with sub_tab_ror:
            zone_prefix = 'ZONE_ROR_'
            df_ror = df_all[df_all['parameter_name'].str.startswith(zone_prefix, na=False)].copy()

            if not df_ror.empty:
                df_ror['Display_Name'] = df_ror['parameter_name'].str.replace(zone_prefix, '')
                df_pivot = df_ror.pivot(index='Time', columns='Display_Name', values='MW').fillna(0.0)
                df_pivot = df_pivot.sort_index()

                valid_cols = [c for c in df_pivot.columns if df_pivot[c].abs().sum() > 0]
                df_pivot['CALCULATED_TOTAL'] = df_pivot[valid_cols].sum(axis=1)

                fig_ror = go.Figure()
                for col in valid_cols:
                    fig_ror.add_trace(go.Scatter(
                        x=df_pivot.index, y=df_pivot[col], name=col,
                        stackgroup='ror_stack', line=dict(width=0.5),
                        hovertemplate='<b>%{fullData.name}</b>: %{y:.2f} MW<extra></extra>'
                    ))

                fig_ror.add_trace(go.Scatter(
                    x=df_pivot.index, y=df_pivot['CALCULATED_TOTAL'],
                    name='CALCULATED TOTAL', line=dict(color='black', width=3),
                    hovertemplate='<b>TOTAL ROR</b>: %{y:.2f} MW<extra></extra>'
                ))

                fig_ror.update_layout(
                    title="Dynamic Breakdown: Run of River (ROR) Plants",
                    hovermode="x unified", template="plotly_white",
                    xaxis=dict(title="Time", type="category"),
                    yaxis_title="Power (MW)",
                    legend=dict(orientation="v", y=1, x=1.02, bgcolor="rgba(255,255,255,0.8)"),
                    margin=dict(l=10, r=150, t=40, b=10)
                )
                st.plotly_chart(fig_ror, use_container_width=True)

                with st.expander("🔍 View Component Breakdown & Verification"):
                    st.dataframe(df_pivot[valid_cols + ['CALCULATED_TOTAL']].style.format("{:.2f}"))
            else:
                st.info("No rows found in the ROR zone.")

        # --- SUB-TAB: TOTAL STORAGE ---
        with sub_tab_storage:
            zone_prefix = 'ZONE_STORAGE_'
            df_storage = df_all[df_all['parameter_name'].str.startswith(zone_prefix, na=False)].copy()

            if not df_storage.empty:
                df_storage['Display_Name'] = df_storage['parameter_name'].str.replace(zone_prefix, '')
                df_pivot = df_storage.pivot(index='Time', columns='Display_Name', values='MW').fillna(0.0)
                df_pivot = df_pivot.sort_index()

                valid_cols = list(df_pivot.columns)
                df_pivot['CALCULATED_TOTAL'] = df_pivot[valid_cols].sum(axis=1)

                fig_storage = go.Figure()
                for col in valid_cols:
                    fig_storage.add_trace(go.Scatter(
                        x=df_pivot.index, y=df_pivot[col], name=col,
                        stackgroup='storage_stack', line=dict(width=0.5),
                        hovertemplate='<b>%{fullData.name}</b>: %{y:.2f} MW<extra></extra>'
                    ))

                fig_storage.add_trace(go.Scatter(
                    x=df_pivot.index, y=df_pivot['CALCULATED_TOTAL'],
                    name='CALCULATED TOTAL', line=dict(color='black', width=3),
                    hovertemplate='<b>TOTAL STORAGE</b>: %{y:.2f} MW<extra></extra>'
                ))

                fig_storage.update_layout(
                    title="Storage Plants",
                    hovermode="x unified", template="plotly_white",
                    xaxis=dict(title="Time", type="category"),
                    yaxis_title="Power (MW)",
                    legend=dict(orientation="v", y=1, x=1.02, bgcolor="rgba(255,255,255,0.8)"),
                    margin=dict(l=10, r=150, t=40, b=10)
                )
                st.plotly_chart(fig_storage, use_container_width=True)

                with st.expander("🔍 View Component Breakdown & Verification"):
                    st.dataframe(df_pivot[valid_cols + ['CALCULATED_TOTAL']].style.format("{:.2f}"))
            else:
                st.info("No rows found in the Storage zone.")

    else:
        st.info("No data found for the selected date.")

with tab2:
    st.subheader(f"Daily Peak Loads for Month {selected_month}, {selected_year}")

    query_monthly = '''
                    SELECT nepali_day, time_interval, parameter_name, value
                    FROM system_log_data
                    WHERE nepali_year = ? \
                      AND nepali_month = ?
                      AND parameter_name IN ('TOTAL SYSTEM LOAD (ACTUAL)', 'SUMMARY_TOTAL_EXPORT') \
                    '''
    df_raw = run_query(query_monthly, (selected_year, selected_month))

    if not df_raw.empty:
        df_pivot = df_raw.pivot_table(
            index=['nepali_day', 'time_interval'],
            columns='parameter_name',
            values='value'
        ).fillna(0).reset_index()

        if 'TOTAL SYSTEM LOAD (ACTUAL)' not in df_pivot.columns: df_pivot['TOTAL SYSTEM LOAD (ACTUAL)'] = 0.0
        if 'SUMMARY_TOTAL_EXPORT' not in df_pivot.columns: df_pivot['SUMMARY_TOTAL_EXPORT'] = 0.0

        df_pivot['System Peak Load'] = df_pivot['TOTAL SYSTEM LOAD (ACTUAL)']
        df_pivot['National Peak Load'] = df_pivot['System Peak Load'] - df_pivot['SUMMARY_TOTAL_EXPORT'].abs()

        idx_sys = df_pivot.groupby('nepali_day')['System Peak Load'].idxmax()
        df_sys = df_pivot.loc[idx_sys, ['nepali_day', 'time_interval', 'System Peak Load']]
        df_sys.rename(columns={'System Peak Load': 'Peak_MW'}, inplace=True)
        df_sys['Load Type'] = 'System Peak Load'

        idx_nat = df_pivot.groupby('nepali_day')['National Peak Load'].idxmax()
        df_nat = df_pivot.loc[idx_nat, ['nepali_day', 'time_interval', 'National Peak Load']]
        df_nat.rename(columns={'National Peak Load': 'Peak_MW'}, inplace=True)
        df_nat['Load Type'] = 'National Peak Load'

        df_combined = pd.concat([df_sys, df_nat]).sort_values('nepali_day')

        fig_monthly = px.line(
            df_combined,
            x='nepali_day',
            y='Peak_MW',
            color='Load Type',
            custom_data=['time_interval'],
            markers=True,
            title="System vs National Peak Load Each Day"
        )

        fig_monthly.update_traces(
            hovertemplate='<b>%{fullData.name}</b><br>Day %{x}<br>Peak Load: %{y:.2f} MW<br>Time of Peak: %{customdata[0]}<extra></extra>'
        )

        fig_monthly.update_layout(
            xaxis=dict(title="Day of Month", type="category"),
            yaxis_title="Peak Load (MW)",
            template="plotly_white",
            hovermode="x unified",
            legend=dict(title="", orientation="h", y=1.1, x=0)
        )

        st.plotly_chart(fig_monthly, use_container_width=True)

        buffer_monthly = io.BytesIO()
        df_export = df_combined[['nepali_day', 'Load Type', 'Peak_MW', 'time_interval']]
        with pd.ExcelWriter(buffer_monthly, engine='openpyxl') as writer:
            df_export.to_excel(writer, index=False, sheet_name=f'Peaks_{selected_year}_{selected_month}')

        st.download_button(
            label=f"📥 Download {selected_year}/{selected_month} Peaks (Excel)",
            data=buffer_monthly.getvalue(),
            file_name=f"NEA_Monthly_Peaks_{selected_year}_{selected_month}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

with tab3:
    st.subheader("Historical Trend: Maximum Load per Month")

    query_historical = '''
                       SELECT nepali_year, nepali_month, nepali_day, time_interval, parameter_name, value
                       FROM system_log_data
                       WHERE parameter_name IN ('TOTAL SYSTEM LOAD (ACTUAL)', 'SUMMARY_TOTAL_EXPORT') \
                       '''
    df_raw = run_query(query_historical)

    if not df_raw.empty:
        df_pivot = df_raw.pivot_table(
            index=['nepali_year', 'nepali_month', 'nepali_day', 'time_interval'],
            columns='parameter_name',
            values='value'
        ).fillna(0).reset_index()

        if 'TOTAL SYSTEM LOAD (ACTUAL)' not in df_pivot.columns: df_pivot['TOTAL SYSTEM LOAD (ACTUAL)'] = 0.0
        if 'SUMMARY_TOTAL_EXPORT' not in df_pivot.columns: df_pivot['SUMMARY_TOTAL_EXPORT'] = 0.0

        df_pivot['System Peak Load'] = df_pivot['TOTAL SYSTEM LOAD (ACTUAL)']
        df_pivot['National Peak Load'] = df_pivot['System Peak Load'] - df_pivot['SUMMARY_TOTAL_EXPORT'].abs()

        idx_sys = df_pivot.groupby(['nepali_year', 'nepali_month'])['System Peak Load'].idxmax()
        df_sys = df_pivot.loc[
            idx_sys, ['nepali_year', 'nepali_month', 'nepali_day', 'time_interval', 'System Peak Load']]
        df_sys.rename(columns={'System Peak Load': 'Peak_MW'}, inplace=True)
        df_sys['Load Type'] = 'System Peak Load'

        idx_nat = df_pivot.groupby(['nepali_year', 'nepali_month'])['National Peak Load'].idxmax()
        df_nat = df_pivot.loc[
            idx_nat, ['nepali_year', 'nepali_month', 'nepali_day', 'time_interval', 'National Peak Load']]
        df_nat.rename(columns={'National Peak Load': 'Peak_MW'}, inplace=True)
        df_nat['Load Type'] = 'National Peak Load'

        df_combined = pd.concat([df_sys, df_nat])
        df_combined['Year/Month'] = df_combined['nepali_year'].astype(str) + "/" + df_combined['nepali_month'].astype(
            str).str.zfill(2)
        df_combined.sort_values(['nepali_year', 'nepali_month'], inplace=True)

        fig_hist = px.line(
            df_combined,
            x='Year/Month',
            y='Peak_MW',
            color='Load Type',
            custom_data=['nepali_day', 'time_interval'],
            markers=True,
            title="System vs National Growth: Monthly Peaks Over Time"
        )

        fig_hist.update_traces(
            hovertemplate='<b>%{fullData.name}</b><br>Month: %{x}<br>Peak Load: %{y:.2f} MW<br>Day of Peak: %{customdata[0]}<br>Time of Peak: %{customdata[1]}<extra></extra>'
        )

        fig_hist.update_layout(
            xaxis=dict(title="Nepali Year/Month", type="category"),
            yaxis_title="Highest Peak Load (MW)",
            template="plotly_white",
            hovermode="x unified",
            legend=dict(title="", orientation="h", y=1.1, x=0)
        )

        st.plotly_chart(fig_hist, use_container_width=True)