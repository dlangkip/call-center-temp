import streamlit as st
import pandas as pd
import gspread
import plotly.express as px
import plotly.graph_objects as go
import datetime
import time
from google.oauth2.service_account import Credentials

# ==========================================
# 1. CONFIGURATION
# ==========================================
st.set_page_config(page_title="M-AJIRA COMMAND CENTER", layout="wide", page_icon="🇰🇪", initial_sidebar_state="collapsed")

# SHEET NAME (Change to 'M-ajira_Logs_Test' if testing)
SHEET_NAME = 'M-ajira_Logs' 
ADMIN_PASSWORD = st.secrets["general"]["admin_password"]

SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

@st.cache_resource
def get_client():
    creds_dict = st.secrets["gcp_service_account"]
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)

# --- CUSTOM CSS ---
st.markdown("""
<style>
    header, footer, div[data-testid="stToolbar"] {visibility: hidden !important;}
    .block-container {padding-top: 1rem !important;}
    div[data-testid="metric-container"] {
        background-color: #1e2130; 
        border: 1px solid #3d3f4e; 
        padding: 15px; 
        border-radius: 8px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    .stRadio > div {
        display: flex;
        justify-content: center;
        gap: 20px;
        font-weight: bold;
    }
    .big-stat { font-size: 24px; font-weight: bold; }
</style>
""", unsafe_allow_html=True)

# ==========================================
# 2. DATA ENGINE
# ==========================================
def load_data():
    try:
        gc = get_client()
        sh = gc.open(SHEET_NAME)
        
        # A. AGENT LOGS (Original Logic + Fixes)
        ws_agents = sh.sheet1
        rows_agents = ws_agents.get_all_values()
        if len(rows_agents) > 1:
            df_agents = pd.DataFrame(rows_agents[1:], columns=[h.strip() for h in rows_agents[0]])
            # FIX 1: DATE PARSING (Day First)
            df_agents['Timestamp'] = pd.to_datetime(df_agents['Timestamp'], dayfirst=True, errors='coerce')
            df_agents['Date'] = df_agents['Timestamp'].dt.date
            df_agents['Hour'] = df_agents['Timestamp'].dt.hour
            
            # FIX 2: MISSING COLUMNS
            if 'Call Status' not in df_agents.columns: df_agents['Call Status'] = 'Answered'
            if 'Category' not in df_agents.columns: df_agents['Category'] = df_agents.get('Disposition', 'General Inquiry')
            if 'Specific Reason' not in df_agents.columns: df_agents['Specific Reason'] = 'N/A'
            if 'Lead Status' not in df_agents.columns: df_agents['Lead Status'] = 'Inquiry Only'
            if 'Skill' not in df_agents.columns: df_agents['Skill'] = 'N/A'
            if 'County' not in df_agents.columns: df_agents['County'] = 'N/A'
        else:
            df_agents = pd.DataFrame(columns=["Timestamp", "Date", "Phone", "Lead Status", "Source", "Category", "Skill", "County", "Hour"])

        # B. SYSTEM LOGS (New Traffic Data)
        try:
            ws_system = sh.worksheet("System_Logs")
            rows_system = ws_system.get_all_values()
            if len(rows_system) > 1:
                df_system = pd.DataFrame(rows_system[1:], columns=[h.strip() for h in rows_system[0]])
                df_system['Timestamp'] = pd.to_datetime(df_system['Timestamp'], errors='coerce')
                df_system['Date'] = df_system['Timestamp'].dt.date
            else:
                df_system = pd.DataFrame()
        except:
            df_system = pd.DataFrame()

        # C. RECOVERY QUEUES (Factory Line)
        queues_data = []
        for q_name in ["Queue_MAjira", "Queue_General"]:
            try:
                ws_q = sh.worksheet(q_name)
                rows_q = ws_q.get_all_values()
                if len(rows_q) > 1:
                    temp_df = pd.DataFrame(rows_q[1:], columns=[h.strip() for h in rows_q[0]])
                    temp_df['Queue Origin'] = q_name
                    queues_data.append(temp_df)
            except:
                pass
        
        if queues_data:
            df_queues = pd.concat(queues_data)
        else:
            df_queues = pd.DataFrame(columns=["Status", "Assigned Agent", "Queue Origin"])

        return df_agents, df_system, df_queues

    except Exception as e:
        st.error(f"Connection Error: {str(e)}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

# ==========================================
# 3. DASHBOARD CONTROLLER
# ==========================================
df_agents, df_system, df_queues = load_data()

c_head, c_nav = st.columns([1, 2])
c_head.title("🇰🇪 M-AJIRA")

# NAVIGATION (Switches Views)
view_mode = c_nav.radio(
    "Select View:", 
    ["📡 Live Traffic & Recovery", "📊 Marketing & ROI", "🛠️ Talent & Operations"], 
    horizontal=True,
    label_visibility="collapsed"
)

# Global Date (Defaults to Today)
today = datetime.datetime.now().date()
# UNCOMMENT TO TEST OLD DATA: 
# today = datetime.date(2025, 12, 20)

# ==========================================
# VIEW 1: LIVE TRAFFIC & RECOVERY (New)
# ==========================================
if view_mode == "📡 Live Traffic & Recovery":
    
    daily_system = df_system[df_system['Date'] == today] if not df_system.empty else df_system
    daily_agents = df_agents[df_agents['Date'] == today] if not df_agents.empty else df_agents
    
    # METRICS
    total_hits = len(daily_system)
    total_handled = len(daily_agents)
    total_missed = 0
    if not daily_system.empty:
        missed_mask = daily_system['Call Status'].astype(str).str.contains('Missed|Abandoned|Lost', case=False, na=False)
        total_missed = len(daily_system[missed_mask])

    ready_count = len(df_queues[df_queues['Status'] == 'Ready'])
    assigned_count = len(df_queues[df_queues['Status'] == 'Assigned'])

    st.markdown(f"### 🚦 TRAFFIC CONTROL ({today})")
    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("1. Incoming Hits", total_hits)
    m2.metric("2. Answered", total_handled, delta=f"{round(total_handled/total_hits*100)}%" if total_hits>0 else "")
    m3.metric("3. Failed/Missed", total_missed, delta="-Gap", delta_color="inverse")
    m4.metric("4. In Progress", assigned_count, delta="Active")
    m5.metric("5. Pending Queue", ready_count, delta="Waiting", delta_color="inverse")
    
    st.markdown("---")
    
    c1, c2 = st.columns([2, 1])
    with c1:
        # Sankey Flow
        label = ["Incoming Calls", "Answered", "Missed Gap", "Assigned (Calling)", "Waiting in Queue"]
        source = [0, 0, 2, 2] 
        target = [1, 2, 3, 4]
        value = [total_handled, total_missed, assigned_count, ready_count]
        
        if total_hits > 0:
            fig_sankey = go.Figure(data=[go.Sankey(
                node = dict(pad = 15, thickness = 20, line = dict(color = "black", width = 0.5), label = label, color = ["#2b2d42", "#00cc96", "#EF553B", "#FFA15A", "#8d99ae"]),
                link = dict(source = source, target = target, value = value)
            )])
            fig_sankey.update_layout(title_text="Daily Traffic Flow", font_size=12, height=350, margin=dict(l=0,r=0,t=40,b=0))
            st.plotly_chart(fig_sankey, use_container_width=True)
        else:
            st.info("No traffic data for today.")

    with c2:
        st.subheader("🗂️ Active Queues")
        tab_ajira, tab_gen = st.tabs(["🔥 M-Ajira", "📞 General"])
        with tab_ajira:
            if not df_queues.empty:
                df_ajira = df_queues[df_queues['Queue Origin'] == 'Queue_MAjira']
                st.dataframe(df_ajira[['Phone', 'Status']], use_container_width=True, height=250)
        with tab_gen:
            if not df_queues.empty:
                df_gen = df_queues[df_queues['Queue Origin'] == 'Queue_General']
                st.dataframe(df_gen[['Phone', 'Status']], use_container_width=True, height=250)

# ==========================================
# VIEW 2: MARKETING & ROI (Original Logic)
# ==========================================
elif view_mode == "📊 Marketing & ROI":
    
    # CALCULATIONS (Preserved from original)
    total_calls = len(df_agents)
    interested_mask = df_agents['Lead Status'].isin(['Interested', 'Registered', 'Registered (Paid Activation)'])
    interested_count = len(df_agents[interested_mask])
    
    top_source = df_agents['Source'].mode()[0] if not df_agents.empty else "N/A"
    top_skill = df_agents['Skill'].mode()[0] if not df_agents.empty else "N/A"
    
    # METRICS ROW
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Inquiries", total_calls)
    col2.metric("Interested Candidates", interested_count, delta=f"{round(interested_count/total_calls*100, 1)}%" if total_calls > 0 else "0%")
    col3.metric("Top Traffic Source", top_source)
    col4.metric("Top Requested Skill", top_skill)
    
    st.markdown("---")
    
    c1, c2 = st.columns(2)
    
    # 1. Source Quality (Preserved: Color by Category)
    if not df_agents.empty:
        fig_roi = px.bar(df_agents, x='Source', color='Category', title="Source Quality (By Category)", barmode='group', template="plotly_dark", color_discrete_sequence=px.colors.qualitative.Safe)
        c1.plotly_chart(fig_roi, use_container_width=True)
    
    # 2. Lead Status Breakdown (Preserved)
    if 'Lead Status' in df_agents.columns and not df_agents.empty:
        lead_counts = df_agents['Lead Status'].value_counts().reset_index()
        lead_counts.columns = ['Status', 'Count']
        color_map = {'Interested': '#00cc96', 'Registered': '#FFA15A', 'Registered (Paid Activation)': '#FFA15A', 'Inquiry Only': '#636efa', 'Not Interested': '#EF553B'}
        fig_status = px.pie(lead_counts, names='Status', values='Count', title="Lead Conversion (Interest Level)", hole=0.5, template="plotly_dark", color='Status', color_discrete_map=color_map)
        c2.plotly_chart(fig_status, use_container_width=True)
    
    c3, c4 = st.columns(2)
    
    # 3. Sunburst (Preserved)
    df_clean = df_agents[df_agents['Category'] != 'Unknown']
    if not df_clean.empty:
        fig_sun = px.sunburst(df_clean, path=['Category', 'Specific Reason'], title="Inquiry Breakdown", template="plotly_dark")
        c3.plotly_chart(fig_sun, use_container_width=True)

    # 4. Hourly Volume (Preserved)
    if not df_agents.empty:
        traffic_counts = df_agents['Hour'].value_counts().sort_index().reset_index()
        traffic_counts.columns = ['Hour', 'Calls']
        fig_time = px.line(traffic_counts, x='Hour', y='Calls', title="Hourly Volume", markers=True, template="plotly_dark", color_discrete_sequence=['#3366ff'])
        c4.plotly_chart(fig_time, use_container_width=True)

# ==========================================
# VIEW 3: TALENT & OPERATIONS (Original Logic)
# ==========================================
elif view_mode == "🛠️ Talent & Operations":
    
    tab_geo, tab_team, tab_logs = st.tabs(["🌍 Geography", "🏆 Team Performance", "🔒 Operations Vault"])
    
    with tab_geo:
        c1, c2 = st.columns(2)
        if not df_agents.empty:
            fig_map = px.pie(df_agents, names='County', title="National Reach", hole=0.4, template="plotly_dark")
            c1.plotly_chart(fig_map, use_container_width=True)
            
            top_skills = df_agents['Skill'].value_counts().head(10).reset_index()
            top_skills.columns = ['Skill', 'Count']
            fig_skill = px.bar(top_skills, x='Count', y='Skill', orientation='h', title="Top Skills", template="plotly_dark", color='Count', color_continuous_scale='Bluyl')
            fig_skill.update_layout(yaxis={'categoryorder':'total ascending'})
            c2.plotly_chart(fig_skill, use_container_width=True)

    with tab_team:
        if not df_agents.empty:
            st.write("### 🏆 Top Performing Agents (Sorted)")
            agent_stats = df_agents.groupby('Agent Name').agg(Total_Calls=('Timestamp', 'count'))
            
            # Logic: Interested OR Registered = Success
            interested_mask = df_agents['Lead Status'].isin(['Interested', 'Registered', 'Registered (Paid Activation)'])
            success_counts = df_agents[interested_mask].groupby('Agent Name').size()
            
            agent_stats['Successful_Reg'] = success_counts
            agent_stats['Successful_Reg'] = agent_stats['Successful_Reg'].fillna(0).astype(int)
            agent_stats['Conversion_Rate'] = (agent_stats['Successful_Reg'] / agent_stats['Total_Calls'] * 100).round(1)
            
            # SORTING (Preserved)
            leaderboard = agent_stats.sort_values(by=['Successful_Reg', 'Conversion_Rate'], ascending=False).reset_index()
            leaderboard.index += 1
            
            st.dataframe(
                leaderboard, use_container_width=True,
                column_config={
                    "Agent Name": "Agent",
                    "Total_Calls": st.column_config.NumberColumn("Inbound Calls"),
                    "Successful_Reg": st.column_config.ProgressColumn("Interested/Reg", format="%d", min_value=0, max_value=int(leaderboard['Successful_Reg'].max()) if not leaderboard.empty else 10),
                    "Conversion_Rate": st.column_config.NumberColumn("Success Rate", format="%.1f%%")
                }
            )
            
    with tab_logs:
        if "admin_unlocked" not in st.session_state:
            st.session_state.admin_unlocked = False

        if st.session_state.admin_unlocked:
            st.success("🔓 Admin Access Granted")
            
            st.write("### 📂 Agent Logs (Sorted Newest First)")
            st.dataframe(df_agents.sort_values('Timestamp', ascending=False), use_container_width=True)
            
            st.write("### 🤖 System Logs")
            if not df_system.empty:
                st.dataframe(df_system.sort_values('Timestamp', ascending=False), use_container_width=True)
            
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

# Auto-refresh
time.sleep(30)
st.rerun()
