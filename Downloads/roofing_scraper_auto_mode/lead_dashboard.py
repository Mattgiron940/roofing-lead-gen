#!/usr/bin/env python3
"""
Streamlit Lead Dashboard
Real-time lead management and analytics dashboard
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import json
from typing import Dict, List, Any
import logging

import sys
sys.path.append('.')
from supabase_client import supabase

# Configure page
st.set_page_config(
    page_title="Roofing Lead Dashboard",
    page_icon="🏠",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LeadDashboard:
    def __init__(self):
        self.supabase_conn = SupabaseConnection()
        
        if not self.supabase_conn.supabase:
            st.error("❌ Unable to connect to Supabase. Please check your configuration.")
            st.stop()
    
    @st.cache_data(ttl=300)  # Cache for 5 minutes
    def get_leads_data(_self, table_name: str, days_back: int = 30) -> pd.DataFrame:
        """Get leads data from Supabase with caching"""
        try:
            cutoff_date = (datetime.now() - timedelta(days=days_back)).isoformat()
            
            result = _self.supabase_conn.supabase.table(table_name)\
                .select("*")\
                .gte('created_at', cutoff_date)\
                .order('created_at', desc=True)\
                .execute()
            
            if result.data:
                df = pd.DataFrame(result.data)
                df['created_at'] = pd.to_datetime(df['created_at'])
                df['source'] = table_name.replace('_leads', '')
                return df
            else:
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Error fetching {table_name}: {e}")
            return pd.DataFrame()
    
    @st.cache_data(ttl=300)
    def get_all_leads(_self, days_back: int = 30) -> pd.DataFrame:
        """Get all leads from all sources"""
        tables = ['zillow_leads', 'redfin_leads', 'cad_leads', 'permit_leads']
        all_leads = []
        
        for table in tables:
            df = _self.get_leads_data(table, days_back)
            if not df.empty:
                all_leads.append(df)
        
        if all_leads:
            combined_df = pd.concat(all_leads, ignore_index=True)
            
            # Standardize common fields
            combined_df['price'] = combined_df['price'].fillna(combined_df.get('appraised_value', 0))
            combined_df['lead_score'] = combined_df['lead_score'].fillna(combined_df.get('lead_priority', 5))
            
            return combined_df
        else:
            return pd.DataFrame()
    
    @st.cache_data(ttl=300)
    def get_lead_metrics(_self, df: pd.DataFrame) -> Dict[str, Any]:
        """Calculate key lead metrics"""
        if df.empty:
            return {}
        
        metrics = {
            'total_leads': len(df),
            'leads_today': len(df[df['created_at'].dt.date == datetime.now().date()]),
            'leads_this_week': len(df[df['created_at'] >= (datetime.now() - timedelta(days=7))]),
            'high_priority_leads': len(df[df['lead_score'] >= 8]),
            'avg_lead_score': df['lead_score'].mean(),
            'total_property_value': df['price'].sum(),
            'avg_property_value': df['price'].mean(),
            'conversion_rate': 0,  # Would need contact tracking
            'top_zip_codes': df['zip_code'].value_counts().head(5).to_dict(),
            'source_breakdown': df['source'].value_counts().to_dict(),
            'status_breakdown': df['lead_status'].value_counts().to_dict() if 'lead_status' in df.columns else {},
            'priority_breakdown': df['priority'].value_counts().to_dict() if 'priority' in df.columns else {}
        }
        
        return metrics
    
    def render_header(self):
        """Render dashboard header"""
        st.title("🏠 Roofing Lead Dashboard")
        st.markdown("Real-time lead management and analytics")
        
        # Last updated indicator
        st.sidebar.markdown(f"**Last Updated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    def render_sidebar_filters(self) -> Dict[str, Any]:
        """Render sidebar filters"""
        st.sidebar.header("🔍 Filters")
        
        filters = {}
        
        # Date range
        filters['days_back'] = st.sidebar.selectbox(
            "Time Period",
            options=[7, 14, 30, 60, 90],
            index=2,
            help="Number of days back to analyze"
        )
        
        # Source filter
        filters['sources'] = st.sidebar.multiselect(
            "Lead Sources",
            options=['zillow', 'redfin', 'cad', 'permit'],
            default=['zillow', 'redfin', 'cad', 'permit'],
            help="Select lead sources to include"
        )
        
        # Lead score filter
        filters['min_score'] = st.sidebar.slider(
            "Minimum Lead Score",
            min_value=1,
            max_value=10,
            value=1,
            help="Filter leads by minimum score"
        )
        
        # Priority filter
        filters['priorities'] = st.sidebar.multiselect(
            "Priority Levels",
            options=['urgent', 'high', 'medium', 'low'],
            default=['urgent', 'high', 'medium', 'low'],
            help="Select priority levels to include"
        )
        
        return filters
    
    def render_metrics_cards(self, metrics: Dict[str, Any]):
        """Render key metrics cards"""
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                label="Total Leads",
                value=f"{metrics.get('total_leads', 0):,}",
                delta=f"+{metrics.get('leads_today', 0)} today"
            )
        
        with col2:
            st.metric(
                label="High Priority",
                value=f"{metrics.get('high_priority_leads', 0):,}",
                delta=f"{metrics.get('high_priority_leads', 0) / max(metrics.get('total_leads', 1), 1) * 100:.1f}%"
            )
        
        with col3:
            st.metric(
                label="Avg Lead Score",
                value=f"{metrics.get('avg_lead_score', 0):.1f}/10",
                delta=None
            )
        
        with col4:
            st.metric(
                label="Total Property Value",
                value=f"${metrics.get('total_property_value', 0):,.0f}",
                delta=f"${metrics.get('avg_property_value', 0):,.0f} avg"
            )
    
    def render_charts(self, df: pd.DataFrame, metrics: Dict[str, Any]):
        """Render main charts"""
        if df.empty:
            st.warning("No data available for the selected filters.")
            return
        
        # Lead trends over time
        st.subheader("📈 Lead Trends")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Daily lead volume
            daily_leads = df.groupby(df['created_at'].dt.date).size().reset_index()
            daily_leads.columns = ['date', 'count']
            
            fig_daily = px.line(
                daily_leads,
                x='date',
                y='count',
                title='Daily Lead Volume',
                labels={'count': 'Number of Leads', 'date': 'Date'}
            )
            fig_daily.update_layout(height=400)
            st.plotly_chart(fig_daily, use_container_width=True)
        
        with col2:
            # Source breakdown
            source_counts = metrics.get('source_breakdown', {})
            if source_counts:
                fig_sources = px.pie(
                    values=list(source_counts.values()),
                    names=list(source_counts.keys()),
                    title='Leads by Source'
                )
                fig_sources.update_layout(height=400)
                st.plotly_chart(fig_sources, use_container_width=True)
        
        # Lead scoring distribution
        st.subheader("🎯 Lead Quality Analysis")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Lead score histogram
            fig_scores = px.histogram(
                df,
                x='lead_score',
                nbins=10,
                title='Lead Score Distribution',
                labels={'lead_score': 'Lead Score', 'count': 'Number of Leads'}
            )
            fig_scores.update_layout(height=400)
            st.plotly_chart(fig_scores, use_container_width=True)
        
        with col2:
            # Price vs Lead Score scatter
            if 'price' in df.columns and df['price'].notna().sum() > 0:
                fig_scatter = px.scatter(
                    df[df['price'] > 0],
                    x='price',
                    y='lead_score',
                    color='source',
                    title='Property Value vs Lead Score',
                    labels={'price': 'Property Value ($)', 'lead_score': 'Lead Score'}
                )
                fig_scatter.update_layout(height=400)
                st.plotly_chart(fig_scatter, use_container_width=True)
        
        # Geographic analysis
        st.subheader("🗺️ Geographic Distribution")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Top ZIP codes
            top_zips = metrics.get('top_zip_codes', {})
            if top_zips:
                fig_zips = px.bar(
                    x=list(top_zips.keys()),
                    y=list(top_zips.values()),
                    title='Top ZIP Codes by Lead Count',
                    labels={'x': 'ZIP Code', 'y': 'Number of Leads'}
                )
                fig_zips.update_layout(height=400)
                st.plotly_chart(fig_zips, use_container_width=True)
        
        with col2:
            # City breakdown
            city_counts = df['city'].value_counts().head(10)
            if not city_counts.empty:
                fig_cities = px.bar(
                    x=city_counts.values,
                    y=city_counts.index,
                    orientation='h',
                    title='Top Cities by Lead Count',
                    labels={'x': 'Number of Leads', 'y': 'City'}
                )
                fig_cities.update_layout(height=400)
                st.plotly_chart(fig_cities, use_container_width=True)
    
    def render_lead_status_analysis(self, df: pd.DataFrame, metrics: Dict[str, Any]):
        """Render lead status and routing analysis"""
        if 'lead_status' not in df.columns:
            st.info("Lead routing data not available. Run lead_router.py to enable this feature.")
            return
        
        st.subheader("🧠 Lead Routing & Status")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            # Status breakdown
            status_counts = metrics.get('status_breakdown', {})
            if status_counts:
                fig_status = px.pie(
                    values=list(status_counts.values()),
                    names=list(status_counts.keys()),
                    title='Leads by Status'
                )
                st.plotly_chart(fig_status, use_container_width=True)
        
        with col2:
            # Priority breakdown
            priority_counts = metrics.get('priority_breakdown', {})
            if priority_counts:
                fig_priority = px.pie(
                    values=list(priority_counts.values()),
                    names=list(priority_counts.keys()),
                    title='Leads by Priority'
                )
                st.plotly_chart(fig_priority, use_container_width=True)
        
        with col3:
            # Assignment breakdown
            if 'assigned_to' in df.columns:
                assignment_counts = df['assigned_to'].value_counts().head(5)
                if not assignment_counts.empty:
                    fig_assigned = px.bar(
                        x=assignment_counts.index,
                        y=assignment_counts.values,
                        title='Leads by Assignment'
                    )
                    st.plotly_chart(fig_assigned, use_container_width=True)
    
    def render_lead_table(self, df: pd.DataFrame, filters: Dict[str, Any]):
        """Render filterable lead table"""
        st.subheader("📋 Lead Details")
        
        if df.empty:
            st.warning("No leads found matching your criteria.")
            return
        
        # Apply filters
        filtered_df = df.copy()
        
        # Source filter
        if filters['sources']:
            filtered_df = filtered_df[filtered_df['source'].isin(filters['sources'])]
        
        # Score filter
        filtered_df = filtered_df[filtered_df['lead_score'] >= filters['min_score']]
        
        # Priority filter
        if 'priority' in filtered_df.columns and filters['priorities']:
            filtered_df = filtered_df[filtered_df['priority'].isin(filters['priorities'])]
        
        # Select columns to display
        display_columns = [
            'created_at', 'source', 'address_text', 'city', 'zip_code',
            'price', 'lead_score', 'lead_status', 'priority', 'next_action'
        ]
        
        # Only include columns that exist
        available_columns = [col for col in display_columns if col in filtered_df.columns]
        display_df = filtered_df[available_columns].copy()
        
        # Format columns
        if 'created_at' in display_df.columns:
            display_df['created_at'] = display_df['created_at'].dt.strftime('%Y-%m-%d %H:%M')
        
        if 'price' in display_df.columns:
            display_df['price'] = display_df['price'].apply(lambda x: f"${x:,.0f}" if pd.notna(x) and x > 0 else "N/A")
        
        # Display table with pagination
        st.dataframe(
            display_df,
            use_container_width=True,
            height=400
        )
        
        # Export options
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("📥 Export to CSV"):
                csv = filtered_df.to_csv(index=False)
                st.download_button(
                    label="Download CSV",
                    data=csv,
                    file_name=f"leads_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
        
        with col2:
            st.metric("Filtered Results", f"{len(filtered_df):,} leads")
    
    def render_action_buttons(self):
        """Render action buttons"""
        st.subheader("⚡ Quick Actions")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            if st.button("🔄 Refresh Data"):
                st.cache_data.clear()
                st.experimental_rerun()
        
        with col2:
            if st.button("🧠 Route Leads"):
                st.info("Run `python lead_router.py` to route unprocessed leads")
        
        with col3:
            if st.button("📊 Export Report"):
                st.info("Run `python lead_export.py` to generate daily export")
        
        with col4:
            if st.button("🔗 Test Webhooks"):
                st.info("Run `python webhook_integration.py --test` to test integrations")
    
    def run(self):
        """Main dashboard runner"""
        self.render_header()
        
        # Get filters
        filters = self.render_sidebar_filters()
        
        # Load data
        with st.spinner("Loading lead data..."):
            df = self.get_all_leads(filters['days_back'])
            metrics = self.get_lead_metrics(df)
        
        if df.empty:
            st.warning("No lead data found. Make sure your scrapers are running and data is being collected.")
            return
        
        # Render dashboard sections
        self.render_metrics_cards(metrics)
        st.divider()
        
        self.render_charts(df, metrics)
        st.divider()
        
        self.render_lead_status_analysis(df, metrics)
        st.divider()
        
        self.render_lead_table(df, filters)
        st.divider()
        
        self.render_action_buttons()
        
        # Sidebar info
        st.sidebar.markdown("---")
        st.sidebar.markdown("### 📊 Dashboard Info")
        st.sidebar.info(f"""
        **Total Leads:** {metrics.get('total_leads', 0):,}
        
        **Sources:** {', '.join(metrics.get('source_breakdown', {}).keys())}
        
        **Date Range:** Last {filters['days_back']} days
        
        **Auto-refresh:** Every 5 minutes
        """)

def main():
    """Main Streamlit app"""
    dashboard = LeadDashboard()
    dashboard.run()

if __name__ == "__main__":
    main()