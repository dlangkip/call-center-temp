import streamlit as st
import pandas as pd
import gspread
import plotly.express as px
import datetime
import time
from google.oauth2.service_account import Credentials

# --- CONFIGURATION (Must be first) ---
st.set_page_config(
    page_title="M-AJIRA", 
    layout="wide", 
    page_icon="🇰🇪",
    initial_sidebar_state="collapsed"
)

# --- 1. SETUP AUTHENTICATION ---
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

@st.cache_resource
def get_client():
    creds_dict = st.secrets["gcp_service_account"]
    creds = Credentials.from_service_account_info(
        creds_dict, scopes=SCOPES
    )
    return gspread.authorize(creds)

# ==========================================
# ⚙️ SHEET CONFIGURATION
# To switch to TEST mode, change this to: 'M-ajira_Logs_Test'
# ==========================================
SHEET_NAME = 'M-ajira_Logs_Test' 
ADMIN_PASSWORD = st.secrets["general"]["admin_password"]

# --- CUSTOM CSS (NUCLEAR STEALTH MODE) ---
st.markdown("""
<style>
    button[data-testid="manage-app-button"] {display: none !important;}
    button[class^="_terminalButton"] {display: none !important;}
    header {visibility: hidden !important;}
    footer {visibility: hidden !important;}
    div[data-testid="stToolbar"] {display: none !important;}
    div[data-testid="stDecoration"] {display: none !important;}
    div[data-testid="stStatusWidget"] {display: none !important;}
    .block-container {padding-top: 1rem !important; padding-bottom: 0rem !important;}
    div[data-testid="metric-container"] {
        background-color: #262730; border: 1px solid #3d3f4e; padding: 15px; border-radius: 8px;
    }
</style>
""", unsafe_allow_html=True)

# --- DATA ENGINE ---
def load_data():
    try:
        gc = get_client()
        sh = gc.open(SHEET_NAME).sheet1
        rows = sh.get_all_values()
        
        if len(rows) < 2:
            return pd.DataFrame()

        headers = [h.strip() for h in rows[0]]
        df = pd.DataFrame(rows[1:], columns=headers)
        
        # --- FIX 1: DATE PARSING ---
        # We use dayfirst=True because data arrives as DD/MM/YYYY (e.g. 20/12/2025)
        # We NO LONGER add +11 hours because the UserScript sends EAT time directly.
        df['Timestamp'] = pd.to_datetime(df['Timestamp'], dayfirst=True, errors='coerce')
        df['Hour'] = df['Timestamp'].dt.hour

        # --- FIX 2: HANDLE MISSING COLUMNS (Backward Compatibility) ---
        if 'Call Status' not in df.columns: df['Call Status'] = 'Answered'
        if 'Category' not in df.columns: df['Category'] = df.get('Disposition', 'General Inquiry')
        if 'Specific Reason' not in df.columns: df['Specific Reason'] = 'N/A'
        
        # --- FIX 3: THE NEW 'LEAD STATUS' FIELD ---
        if 'Lead Status' not in df.columns: 
            df['Lead Status'] = 'Inquiry Only' # Default for old rows
        
        return df
    except Exception as e:
        st.error(f"Connection Error: {str(e)}")
        return pd.DataFrame()

# --- MAIN DASHBOARD ---
c_head, c_status = st.columns([3, 1])
c_head.title("🇰🇪 M-AJIRA")

# Live Clock (EAT)
utc_now = datetime.datetime.utcnow()
eat_now = utc_now + datetime.timedelta(hours=3)
c_status.caption(f"Live Feed • {eat_now.strftime('%H:%M')} EAT")

df = load_data()

if df.empty:
    st.info("⏳ Waiting for data stream...")
    time.sleep(10)
    st.rerun()

# --- KPI CALCULATION ---
total_calls = len(df)

# --- NEW KPI LOGIC (Based on Lead Status) ---
# 'Interested' = Hot Lead
# 'Registered' = Paid Activation (Satisfies Manager)
interested_mask = df['Lead Status'].isin(['Interested', 'Registered', 'Registered (Paid Activation)'])
interested_count = len(df[interested_mask])

top_source = df['Source'].mode()[0] if not df.empty else "N/A"
top_skill = df['Skill'].mode()[0] if not df.empty else "N/A"

# KPI ROW
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Inquiries", total_calls)
col2.metric("Interested Candidates", interested_count, delta=f"{round(interested_count/total_calls*100, 1)}%" if total_calls > 0 else "0%")
col3.metric("Top Traffic Source", top_source)
col4.metric("Top Requested Skill", top_skill)

st.markdown("---")

# TABS
tab_market, tab_talent, tab_ops = st.tabs(["📻 Marketing & ROI", "🛠️ Talent & Geography", "🔒 Operations & Logs"])

with tab_market:
    c1, c2 = st.columns(2)
    
    # 1. Source Quality
    if not df.empty:
        fig_roi = px.bar(df, x='Source', color='Category', title="Source Quality (By Category)", barmode='group', template="plotly_dark", color_discrete_sequence=px.colors.qualitative.Safe)
        c1.plotly_chart(fig_roi, use_container_width=True)
    
    # 2. Lead Status Breakdown (REPLACES OLD STATUS CHART)
    if 'Lead Status' in df.columns and not df.empty:
        lead_counts = df['Lead Status'].value_counts().reset_index()
        lead_counts.columns = ['Status', 'Count']
        
        # Color Map: Green=Interested, Orange=Paid, Blue=Inquiry, Red=Not Interested
        color_map = {
            'Interested': '#00cc96', 
            'Registered': '#FFA15A', 
            'Registered (Paid Activation)': '#FFA15A',
            'Inquiry Only': '#636efa', 
            'Not Interested': '#EF553B'
        }
        
        fig_status = px.pie(lead_counts, names='Status', values='Count', title="Lead Conversion (Interest Level)", hole=0.5, template="plotly_dark", color='Status', color_discrete_map=color_map)
        c2.plotly_chart(fig_status, use_container_width=True)
    
    c3, c4 = st.columns(2)

    # 3. Deep Dive (Sunburst)
    df_clean = df[df['Category'] != '']
    if not df_clean.empty:
        fig_sun = px.sunburst(df_clean, path=['Category', 'Specific Reason'], title="Inquiry Breakdown", template="plotly_dark")
        c3.plotly_chart(fig_sun, use_container_width=True)

    # 4. Hourly Volume
    if not df.empty:
        traffic_counts = df['Hour'].value_counts().sort_index().reset_index()
        traffic_counts.columns = ['Hour', 'Calls']
        fig_time = px.line(traffic_counts, x='Hour', y='Calls', title="Hourly Volume", markers=True, template="plotly_dark", color_discrete_sequence=['#3366ff'])
        c4.plotly_chart(fig_time, use_container_width=True)

with tab_talent:
    c1, c2 = st.columns(2)
    fig_map = px.pie(df, names='County', title="National Reach", hole=0.4, template="plotly_dark")
    c1.plotly_chart(fig_map, use_container_width=True)
    
    top_skills = df['Skill'].value_counts().head(10).reset_index()
    top_skills.columns = ['Skill', 'Count']
    fig_skill = px.bar(top_skills, x='Count', y='Skill', orientation='h', title="Top Skills", template="plotly_dark", color='Count', color_continuous_scale='Bluyl')
    fig_skill.update_layout(yaxis={'categoryorder':'total ascending'})
    c2.plotly_chart(fig_skill, use_container_width=True)

with tab_ops:
    st.subheader("Operational Data Vault")
    
    if "admin_unlocked" not in st.session_state:
        st.session_state.admin_unlocked = False

    if st.session_state.admin_unlocked:
        st.success("🔓 Admin Access Granted")
        
        st.write("### 🏆 Top Performing Agents")
        
        agent_stats = df.groupby('Agent Name').agg(
            Total_Calls=('Timestamp', 'count')
        )
        
        # Calculate Success based on Lead Status (Interested OR Registered)
        success_counts = df[interested_mask].groupby('Agent Name').size()
        agent_stats['Successful_Reg'] = success_counts
        agent_stats['Successful_Reg'] = agent_stats['Successful_Reg'].fillna(0).astype(int)
        agent_stats['Conversion_Rate'] = (agent_stats['Successful_Reg'] / agent_stats['Total_Calls'] * 100).round(1)
        
        leaderboard = agent_stats.sort_values(by=['Successful_Reg', 'Conversion_Rate'], ascending=False).reset_index()
        leaderboard.index += 1
        
        st.dataframe(
            leaderboard,
            use_container_width=True,
            column_config={
                "Agent Name": "Agent",
                "Total_Calls": st.column_config.NumberColumn("Inbound Calls"),
                "Successful_Reg": st.column_config.ProgressColumn(
                    "Interested/Reg", 
                    format="%d", 
                    min_value=0, 
                    max_value=int(leaderboard['Successful_Reg'].max()) if not leaderboard.empty else 10
                ),
                "Conversion_Rate": st.column_config.NumberColumn("Success Rate", format="%.1f%%")
            }
        )
        
        st.write("### 📂 Raw Call Logs")
        with st.expander("Expand to view Excel Data", expanded=True):
            # Sort by Timestamp descending so newest calls are first
            st.dataframe(df.sort_values('Timestamp', ascending=False), use_container_width=True)
            
        if st.button("🔒 Lock Data"):
            st.session_state.admin_unlocked = False
            st.rerun()
            
    else:
        st.warning("⚠️ Restricted Access")
        password_input = st.text_input("Enter Admin Password", type="password")
        if st.button("Unlock Logs"):
            if password_input == ADMIN_PASSWORD:
                st.session_state.admin_unlocked = True
                st.rerun()
            else:
                st.error("❌ Incorrect Password")

time.sleep(30) # Auto-refresh every 30s
st.rerun()
