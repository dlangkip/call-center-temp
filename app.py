import streamlit as st
import pandas as pd
import gspread
import plotly.express as px
import datetime
import time
from google.oauth2.service_account import Credentials

# --- CONFIGURATION ---
st.set_page_config(
    page_title="M-AJIRA Intelligence", 
    layout="wide", 
    page_icon="🇰🇪",
    initial_sidebar_state="collapsed"
)

# --- 1. SETUP AUTHENTICATION (The Streamlit Cloud Way) ---
# We look for the credentials inside Streamlit's "Secrets"
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

@st.cache_resource
def get_client():
    # Load credentials from Streamlit Secrets
    creds_dict = st.secrets["gcp_service_account"]
    creds = Credentials.from_service_account_info(
        creds_dict, scopes=SCOPES
    )
    return gspread.authorize(creds)

SHEET_NAME = 'M-ajira_Logs'
ADMIN_PASSWORD = st.secrets["general"]["admin_password"]

# --- CUSTOM CSS ---
st.markdown("""
<style>
    div[data-testid="metric-container"] {
        background-color: #262730; 
        border: 1px solid #3d3f4e; 
        padding: 15px; 
        border-radius: 8px;
    }
    h1 { font-size: 1.8rem !important; }
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    .css-1d391kg { padding-top: 1rem; }
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
        
        # EAT Timezone Adjustment (UTC+3)
        df['Timestamp'] = pd.to_datetime(df['Timestamp'], dayfirst=True, errors='coerce')
        df['Hour'] = df['Timestamp'].dt.hour
        return df
    except Exception as e:
        st.error(f"Connection Error: {str(e)}")
        return pd.DataFrame()

# --- MAIN DASHBOARD ---
c_head, c_status = st.columns([3, 1])
c_head.title("🇰🇪 M-AJIRA Intelligence")

# Live Clock (EAT)
utc_now = datetime.datetime.utcnow()
eat_now = utc_now + datetime.timedelta(hours=3)
c_status.caption(f"Live Feed • {eat_now.strftime('%H:%M')} EAT")

df = load_data()

if df.empty:
    st.info("⏳ Waiting for data stream...")
    time.sleep(10)
    st.rerun()

# KPI ROW
col1, col2, col3, col4 = st.columns(4)
total_calls = len(df)
success_reg = len(df[df['Disposition'] == 'Successfully Registered'])
top_source = df['Source'].mode()[0] if not df.empty else "N/A"
top_skill = df['Skill'].mode()[0] if not df.empty else "N/A"

col1.metric("Total Inquiries", total_calls)
col2.metric("Successful Registrations", success_reg, delta=f"{round(success_reg/total_calls*100, 1)}%" if total_calls > 0 else "0%")
col3.metric("Top Traffic Source", top_source)
col4.metric("Top Requested Skill", top_skill)

st.markdown("---")

# TABS
tab_market, tab_talent, tab_ops = st.tabs(["📻 Marketing & ROI", "🛠️ Talent & Geography", "🔒 Operations & Logs"])

with tab_market:
    c1, c2 = st.columns(2)
    fig_roi = px.bar(df, x='Source', color='Disposition', title="Conversion Quality", barmode='group', template="plotly_dark", color_discrete_sequence=px.colors.qualitative.Safe)
    c1.plotly_chart(fig_roi, use_container_width=True)
    
    traffic_counts = df['Hour'].value_counts().sort_index().reset_index()
    traffic_counts.columns = ['Hour', 'Calls']
    fig_time = px.line(traffic_counts, x='Hour', y='Calls', title="Hourly Volume", markers=True, template="plotly_dark", color_discrete_sequence=['#3366ff'])
    c2.plotly_chart(fig_time, use_container_width=True)

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
        st.write("### 👨‍💼 Agent Performance")
        st.dataframe(df.groupby('Agent Name')['Disposition'].value_counts().unstack().fillna(0), use_container_width=True)
        
        st.write("### 📂 Raw Call Logs")
        with st.expander("Expand to view Excel Data", expanded=True):
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

time.sleep(30) # Refresh every 30s to stay within quotas
st.rerun()
