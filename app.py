import streamlit as st
import pandas as pd
import gspread
import plotly.express as px
import plotly.graph_objects as go
import datetime
import time
from google.oauth2.service_account import Credentials


# ==============================================================================
# SECTION 1: CONFIGURATION & SETUP
# ==============================================================================

st.set_page_config(
    page_title="M-AJIRA COMMAND CENTER",
    layout="wide",
    page_icon="🇰🇪",
    initial_sidebar_state="collapsed"
)

# --- Constants ---
SHEET_NAME = 'M-ajira_Logs'  # Change to 'M-ajira_Logs_Test' for testing
ADMIN_PASSWORD = st.secrets["general"]["admin_password"]
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

# --- Color Schemes ---
LEAD_STATUS_COLORS = {
    'Interested': '#00cc96',
    'Registered': '#FFA15A',
    'Registered (Paid Activation)': '#FFA15A',
    'Inquiry Only': '#636efa',
    'Not Interested': '#EF553B'
}

TRAFFIC_FLOW_COLORS = ["#2b2d42", "#00cc96", "#EF553B", "#FFA15A", "#8d99ae"]


# ==============================================================================
# SECTION 2: STYLING
# ==============================================================================

st.markdown("""
<style>
    /* Hide Streamlit branding */
    header, footer, div[data-testid="stToolbar"] {visibility: hidden !important;}
    .block-container {padding-top: 1rem !important;}
    
    /* Metric cards */
    div[data-testid="metric-container"] {
        background-color: #1e2130;
        border: 1px solid #3d3f4e;
        padding: 15px;
        border-radius: 8px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    
    /* Navigation radio buttons */
    .stRadio > div {
        display: flex;
        justify-content: center;
        gap: 20px;
        font-weight: bold;
    }
    
    .big-stat { font-size: 24px; font-weight: bold; }
</style>
""", unsafe_allow_html=True)


# ==============================================================================
# SECTION 3: DATA ENGINE
# ==============================================================================

@st.cache_resource
def get_gsheet_client():
    """Initialize and cache Google Sheets client."""
    creds_dict = st.secrets["gcp_service_account"]
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)


def load_agent_logs(sheet) -> pd.DataFrame:
    """Load and process agent logs from Sheet1."""
    ws = sheet.sheet1
    rows = ws.get_all_values()
    
    if len(rows) <= 1:
        return pd.DataFrame(columns=[
            "Timestamp", "Date", "Phone", "Lead Status", "Source",
            "Category", "Skill", "County", "Hour", "Agent Name"
        ])
    
    df = pd.DataFrame(rows[1:], columns=[h.strip() for h in rows[0]])
    
    # Parse timestamps (day first for DD/MM/YYYY format)
    df['Timestamp'] = pd.to_datetime(df['Timestamp'], dayfirst=True, errors='coerce')
    df['Date'] = df['Timestamp'].dt.date
    df['Hour'] = df['Timestamp'].dt.hour
    
    # Ensure required columns exist with defaults
    column_defaults = {
        'Call Status': 'Answered',
        'Category': df.get('Disposition', 'General Inquiry'),
        'Specific Reason': 'N/A',
        'Lead Status': 'Inquiry Only',
        'Skill': 'N/A',
        'County': 'N/A',
        'Source': 'Unknown',
        'Agent Name': 'Unknown'
    }
    
    for col, default in column_defaults.items():
        if col not in df.columns:
            df[col] = default
    
    return df


def load_system_logs(sheet) -> pd.DataFrame:
    """Load system logs (raw webhook data)."""
    try:
        ws = sheet.worksheet("System_Logs")
        rows = ws.get_all_values()
        
        if len(rows) <= 1:
            return pd.DataFrame()
        
        df = pd.DataFrame(rows[1:], columns=[h.strip() for h in rows[0]])
        df['Timestamp'] = pd.to_datetime(df['Timestamp'], errors='coerce')
        df['Date'] = df['Timestamp'].dt.date
        return df
    except Exception:
        return pd.DataFrame()


def load_recovery_queues(sheet) -> pd.DataFrame:
    """Load recovery queue data from both queue sheets."""
    queue_names = ["Queue_MAjira", "Queue_General"]
    queues_data = []
    
    for q_name in queue_names:
        try:
            ws = sheet.worksheet(q_name)
            rows = ws.get_all_values()
            
            if len(rows) > 1:
                temp_df = pd.DataFrame(rows[1:], columns=[h.strip() for h in rows[0]])
                temp_df['Queue Origin'] = q_name
                queues_data.append(temp_df)
        except Exception:
            pass
    
    if queues_data:
        return pd.concat(queues_data, ignore_index=True)
    
    return pd.DataFrame(columns=["Phone", "Status", "Assigned Agent", "Queue Origin"])


@st.cache_data(ttl=30)
def load_all_data():
    """Master data loader - loads all sheets."""
    try:
        gc = get_gsheet_client()
        sheet = gc.open(SHEET_NAME)
        
        df_agents = load_agent_logs(sheet)
        df_system = load_system_logs(sheet)
        df_queues = load_recovery_queues(sheet)
        
        return df_agents, df_system, df_queues
    
    except Exception as e:
        st.error(f"🔴 Connection Error: {str(e)}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()


# ==============================================================================
# SECTION 4: VIEW COMPONENTS
# ==============================================================================

def render_marketing_roi(df_agents: pd.DataFrame):
    """
    MARKETING & ROI VIEW
    Shows cumulative performance metrics, source quality, and conversion analytics.
    Uses ALL-TIME data (not filtered by date).
    """
    
    # --- Calculations ---
    total_calls = len(df_agents)
    
    interested_statuses = ['Interested', 'Registered', 'Registered (Paid Activation)']
    interested_mask = df_agents['Lead Status'].isin(interested_statuses)
    interested_count = len(df_agents[interested_mask])
    
    top_source = df_agents['Source'].mode()[0] if not df_agents.empty else "N/A"
    top_skill = df_agents['Skill'].mode()[0] if not df_agents.empty else "N/A"
    
    # --- Top Metrics Row ---
    col1, col2, col3, col4 = st.columns(4)
    
    col1.metric("Total Inquiries", f"{total_calls:,}")
    col2.metric(
        "Interested Candidates",
        f"{interested_count:,}",
        delta=f"{round(interested_count/total_calls*100, 1)}%" if total_calls > 0 else "0%"
    )
    col3.metric("Top Traffic Source", top_source)
    col4.metric("Top Requested Skill", top_skill)
    
    st.markdown("---")
    
    # --- Row 1: Source Quality & Lead Conversion ---
    c1, c2 = st.columns(2)
    
    with c1:
        if not df_agents.empty:
            fig_roi = px.bar(
                df_agents,
                x='Source',
                color='Category',
                title="Source Quality (By Category)",
                barmode='group',
                template="plotly_dark",
                color_discrete_sequence=px.colors.qualitative.Safe
            )
            st.plotly_chart(fig_roi, use_container_width=True)
    
    with c2:
        if 'Lead Status' in df_agents.columns and not df_agents.empty:
            lead_counts = df_agents['Lead Status'].value_counts().reset_index()
            lead_counts.columns = ['Status', 'Count']
            
            fig_status = px.pie(
                lead_counts,
                names='Status',
                values='Count',
                title="Lead Conversion (Interest Level)",
                hole=0.5,
                template="plotly_dark",
                color='Status',
                color_discrete_map=LEAD_STATUS_COLORS
            )
            st.plotly_chart(fig_status, use_container_width=True)
    
    # --- Row 2: Inquiry Breakdown & Hourly Volume ---
    c3, c4 = st.columns(2)
    
    with c3:
        df_clean = df_agents[df_agents['Category'] != 'Unknown']
        if not df_clean.empty:
            fig_sun = px.sunburst(
                df_clean,
                path=['Category', 'Specific Reason'],
                title="Inquiry Breakdown",
                template="plotly_dark"
            )
            st.plotly_chart(fig_sun, use_container_width=True)
    
    with c4:
        if not df_agents.empty:
            traffic_counts = df_agents['Hour'].value_counts().sort_index().reset_index()
            traffic_counts.columns = ['Hour', 'Calls']
            
            fig_time = px.line(
                traffic_counts,
                x='Hour',
                y='Calls',
                title="Hourly Volume",
                markers=True,
                template="plotly_dark",
                color_discrete_sequence=['#3366ff']
            )
            st.plotly_chart(fig_time, use_container_width=True)


def render_live_traffic(df_agents: pd.DataFrame, df_system: pd.DataFrame, df_queues: pd.DataFrame, selected_date):
    """
    LIVE TRAFFIC & RECOVERY VIEW
    Shows real-time traffic flow, queue status, and recovery operations.
    Filtered by selected date.
    """
    
    # --- Filter data by date ---
    daily_system = df_system[df_system['Date'] == selected_date] if not df_system.empty else df_system
    daily_agents = df_agents[df_agents['Date'] == selected_date] if not df_agents.empty else df_agents
    
    # --- Calculate metrics ---
    total_hits = len(daily_system)
    total_handled = len(daily_agents)
    
    # Missed calls calculation
    total_missed = 0
    if not daily_system.empty and 'Call Status' in daily_system.columns:
        missed_mask = daily_system['Call Status'].astype(str).str.contains(
            'Missed|Abandoned|Lost', case=False, na=False
        )
        total_missed = len(daily_system[missed_mask])
    
    # Queue status
    ready_count = len(df_queues[df_queues['Status'] == 'Ready']) if not df_queues.empty else 0
    assigned_count = len(df_queues[df_queues['Status'] == 'Assigned']) if not df_queues.empty else 0
    
    # --- Header with date ---
    st.markdown(f"### 🚦 TRAFFIC CONTROL ({selected_date})")
    
    # --- Metrics Row ---
    m1, m2, m3, m4, m5 = st.columns(5)
    
    m1.metric("1. Incoming Hits", total_hits, help="Total calls hitting the PBX")
    m2.metric(
        "2. Answered",
        total_handled,
        delta=f"{round(total_handled/total_hits*100)}%" if total_hits > 0 else "",
        help="Calls logged by agents"
    )
    m3.metric(
        "3. Failed/Missed",
        total_missed,
        delta="-Gap",
        delta_color="inverse",
        help="Calls dropped in queue"
    )
    m4.metric(
        "4. In Progress",
        assigned_count,
        delta="Active",
        help="Leads currently dispatched to agents"
    )
    m5.metric(
        "5. Pending Queue",
        ready_count,
        delta="Waiting",
        delta_color="inverse",
        help="Waiting for 'Get Lead' click"
    )
    
    st.markdown("---")
    
    # --- Sankey Flow & Queues ---
    c1, c2 = st.columns([2, 1])
    
    with c1:
        if total_hits > 0:
            fig_sankey = go.Figure(data=[go.Sankey(
                node=dict(
                    pad=15,
                    thickness=20,
                    line=dict(color="black", width=0.5),
                    label=["Incoming Calls", "Answered", "Missed Gap", "Assigned (Calling)", "Waiting in Queue"],
                    color=TRAFFIC_FLOW_COLORS
                ),
                link=dict(
                    source=[0, 0, 2, 2],
                    target=[1, 2, 3, 4],
                    value=[total_handled, total_missed, assigned_count, ready_count]
                )
            )])
            fig_sankey.update_layout(
                title_text="Daily Traffic Flow",
                font_size=12,
                height=350,
                margin=dict(l=0, r=0, t=40, b=0)
            )
            st.plotly_chart(fig_sankey, use_container_width=True)
        else:
            st.info("📭 No traffic data for this date.")
    
    with c2:
        st.subheader("🗂️ Active Queues")
        tab_ajira, tab_gen = st.tabs(["🔥 M-Ajira", "📞 General"])
        
        with tab_ajira:
            if not df_queues.empty:
                df_ajira = df_queues[df_queues['Queue Origin'] == 'Queue_MAjira']
                if not df_ajira.empty:
                    st.dataframe(
                        df_ajira[['Phone', 'Status', 'Assigned Agent']],
                        use_container_width=True,
                        height=250
                    )
                else:
                    st.caption("Queue empty")
            else:
                st.caption("No queue data")
        
        with tab_gen:
            if not df_queues.empty:
                df_gen = df_queues[df_queues['Queue Origin'] == 'Queue_General']
                if not df_gen.empty:
                    st.dataframe(
                        df_gen[['Phone', 'Status', 'Assigned Agent']],
                        use_container_width=True,
                        height=250
                    )
                else:
                    st.caption("Queue empty")
            else:
                st.caption("No queue data")
    
    # --- Recovery Squad (if callbacks in progress) ---
    if assigned_count > 0 and 'Assigned Agent' in df_queues.columns:
        st.markdown("### 📞 Recovery Squad - Active Callbacks")
        active_recovery = df_queues[df_queues['Status'] == 'Assigned']
        agent_counts = active_recovery['Assigned Agent'].value_counts().reset_index()
        agent_counts.columns = ['Agent', 'Active Calls']
        
        fig_bar = px.bar(
            agent_counts,
            x='Agent',
            y='Active Calls',
            title="Callbacks Assigned by Agent",
            color='Active Calls',
            template="plotly_dark"
        )
        st.plotly_chart(fig_bar, use_container_width=True)


def render_talent_operations(df_agents: pd.DataFrame, df_system: pd.DataFrame):
    """
    TALENT & OPERATIONS VIEW
    Shows geographic distribution, skills analysis, team performance, and admin logs.
    """
    
    tab_geo, tab_team, tab_logs = st.tabs([
        "🌍 Geography & Skills",
        "🏆 Team Performance",
        "🔒 Operations Vault"
    ])
    
    # --- Geography & Skills Tab ---
    with tab_geo:
        c1, c2 = st.columns(2)
        
        with c1:
            if not df_agents.empty:
                fig_map = px.pie(
                    df_agents,
                    names='County',
                    title="National Reach (By County)",
                    hole=0.4,
                    template="plotly_dark"
                )
                st.plotly_chart(fig_map, use_container_width=True)
            else:
                st.info("No geographic data available.")
        
        with c2:
            if not df_agents.empty:
                top_skills = df_agents['Skill'].value_counts().head(10).reset_index()
                top_skills.columns = ['Skill', 'Count']
                
                fig_skill = px.bar(
                    top_skills,
                    x='Count',
                    y='Skill',
                    orientation='h',
                    title="Top 10 Skills Requested",
                    template="plotly_dark",
                    color='Count',
                    color_continuous_scale='Bluyl'
                )
                fig_skill.update_layout(yaxis={'categoryorder': 'total ascending'})
                st.plotly_chart(fig_skill, use_container_width=True)
            else:
                st.info("No skills data available.")
    
    # --- Team Performance Tab ---
    with tab_team:
        if not df_agents.empty:
            st.write("### 🏆 Agent Leaderboard")
            
            # Calculate agent stats
            agent_stats = df_agents.groupby('Agent Name').agg(
                Total_Calls=('Timestamp', 'count')
            )
            
            # Success = Interested OR Registered
            interested_statuses = ['Interested', 'Registered', 'Registered (Paid Activation)']
            interested_mask = df_agents['Lead Status'].isin(interested_statuses)
            success_counts = df_agents[interested_mask].groupby('Agent Name').size()
            
            agent_stats['Successful_Reg'] = success_counts
            agent_stats['Successful_Reg'] = agent_stats['Successful_Reg'].fillna(0).astype(int)
            agent_stats['Conversion_Rate'] = (
                agent_stats['Successful_Reg'] / agent_stats['Total_Calls'] * 100
            ).round(1)
            
            # Sort by success
            leaderboard = agent_stats.sort_values(
                by=['Successful_Reg', 'Conversion_Rate'],
                ascending=False
            ).reset_index()
            leaderboard.index += 1
            
            # Display with formatting
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
                    "Conversion_Rate": st.column_config.NumberColumn(
                        "Success Rate",
                        format="%.1f%%"
                    )
                }
            )
        else:
            st.info("No agent performance data available.")
    
    # --- Operations Vault Tab (Admin Protected) ---
    with tab_logs:
        render_admin_vault(df_agents, df_system)


def render_admin_vault(df_agents: pd.DataFrame, df_system: pd.DataFrame):
    """Admin-protected logs section."""
    
    if "admin_unlocked" not in st.session_state:
        st.session_state.admin_unlocked = False
    
    if st.session_state.admin_unlocked:
        st.success("🔓 Admin Access Granted")
        
        # Agent Logs
        st.write("### 📂 Agent Logs (Newest First)")
        if not df_agents.empty:
            st.dataframe(
                df_agents.sort_values('Timestamp', ascending=False),
                use_container_width=True
            )
        else:
            st.caption("No agent logs available.")
        
        # System Logs
        st.write("### 🤖 System Logs (Raw Webhook Data)")
        if not df_system.empty:
            st.dataframe(
                df_system.sort_values('Timestamp', ascending=False),
                use_container_width=True
            )
        else:
            st.caption("No system logs available.")
        
        # Lock button
        if st.button("🔒 Lock Data"):
            st.session_state.admin_unlocked = False
            st.rerun()
    
    else:
        st.warning("⚠️ Restricted Access - Admin Only")
        password_input = st.text_input("Enter Admin Password", type="password")
        
        if st.button("Unlock Logs"):
            if password_input == ADMIN_PASSWORD:
                st.session_state.admin_unlocked = True
                st.rerun()
            else:
                st.error("❌ Incorrect Password")


# ==============================================================================
# SECTION 5: MAIN APPLICATION
# ==============================================================================

def main():
    """Main application entry point."""
    
    # --- Load Data ---
    df_agents, df_system, df_queues = load_all_data()
    
    # --- Header & Navigation ---
    c_head, c_nav = st.columns([1, 2])
    c_head.title("🇰🇪 M-AJIRA")
    
    view_mode = c_nav.radio(
        "Select View:",
        ["📊 Marketing & ROI", "📡 Live Traffic & Recovery", "🛠️ Talent & Operations"],
        horizontal=True,
        label_visibility="collapsed"
    )
    
    # --- Date Selection (for Live Traffic view) ---
    today = datetime.datetime.now().date()
    # UNCOMMENT TO TEST WITH OLDER DATA:
    # today = datetime.date(2025, 12, 20)
    
    # --- Render Selected View ---
    if view_mode == "📊 Marketing & ROI":
        render_marketing_roi(df_agents)
    
    elif view_mode == "📡 Live Traffic & Recovery":
        # Date picker for traffic view
        selected_date = st.date_input("View Traffic For:", today, label_visibility="collapsed")
        render_live_traffic(df_agents, df_system, df_queues, selected_date)
    
    elif view_mode == "🛠️ Talent & Operations":
        render_talent_operations(df_agents, df_system)
    
    # --- Auto-refresh ---
    time.sleep(30)
    st.rerun()


# ==============================================================================
# ENTRY POINT
# ==============================================================================

if __name__ == "__main__":
    main()
