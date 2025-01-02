import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import pandas as pd
from typing import Dict, Any
from pathlib import Path

class PerformanceVisualizer:
    """Generate visualizations for performance test results"""
    
    def __init__(self, results_df: pd.DataFrame):
        """
        Initialize visualizer with results
        
        Args:
            results_df: DataFrame containing performance metrics
        """
        self.results_df = results_df
    
    def create_latency_distribution(self) -> go.Figure:
        """Create box plot of query latencies by type"""
        fig = go.Figure()
        
        for query_type in self.results_df['Query Type'].unique():
            query_data = self.results_df[self.results_df['Query Type'] == query_type]
            
            fig.add_trace(go.Box(
                y=query_data['Avg Time (ms)'],
                name=query_type,
                boxpoints='all',
                jitter=0.3,
                pointpos=-1.8
            ))
        
        fig.update_layout(
            title='Query Latency Distribution by Type',
            yaxis_title='Latency (ms)',
            showlegend=True,
            height=600
        )
        
        return fig
    
    def create_success_rate_chart(self) -> go.Figure:
        """Create bar chart of success rates"""
        fig = go.Figure(data=[
            go.Bar(
                x=self.results_df['Query Type'],
                y=self.results_df['Success Rate'],
                text=self.results_df['Success Rate'].round(2),
                textposition='auto',
            )
        ])
        
        fig.update_layout(
            title='Query Success Rates',
            xaxis_title='Query Type',
            yaxis_title='Success Rate (%)',
            height=400
        )
        
        return fig
    
    def create_percentile_comparison(self) -> go.Figure:
        """Create line chart comparing different latency percentiles"""
        fig = go.Figure()
        
        percentiles = ['Median Time (ms)', 'P95 Time (ms)', 'P99 Time (ms)']
        
        for percentile in percentiles:
            fig.add_trace(go.Scatter(
                x=self.results_df['Query Type'],
                y=self.results_df[percentile],
                mode='lines+markers',
                name=percentile
            ))
        
        fig.update_layout(
            title='Latency Percentiles by Query Type',
            xaxis_title='Query Type',
            yaxis_title='Latency (ms)',
            height=500
        )
        
        return fig
    
    def create_dashboard(self, output_dir: str) -> None:
        """
        Create and save a comprehensive performance dashboard
        
        Args:
            output_dir: Directory to save the dashboard HTML
        """
        # Create subplot figure
        fig = make_subplots(
            rows=3, cols=1,
            subplot_titles=(
                'Query Latency Distribution',
                'Success Rates',
                'Latency Percentiles'
            ),
            vertical_spacing=0.1,
            heights=[0.4, 0.3, 0.3]
        )
        
        # Add latency distribution
        latency_fig = self.create_latency_distribution()
        for trace in latency_fig.data:
            fig.add_trace(trace, row=1, col=1)
        
        # Add success rates
        success_fig = self.create_success_rate_chart()
        for trace in success_fig.data:
            fig.add_trace(trace, row=2, col=1)
        
        # Add percentile comparison
        percentile_fig = self.create_percentile_comparison()
        for trace in percentile_fig.data:
            fig.add_trace(trace, row=3, col=1)
        
        # Update layout
        fig.update_layout(
            height=1200,
            showlegend=True,
            title_text="BigTable Query Performance Dashboard"
        )
        
        # Save dashboard
        output_path = Path(output_dir) / "performance_dashboard.html"
        fig.write_html(str(output_path))

def create_visualizations(results_df: pd.DataFrame) -> Dict[str, Any]:
    """
    Create and return all visualizations
    
    Args:
        results_df: DataFrame containing performance metrics
        
    Returns:
        Dictionary containing visualization figures
    """
    visualizer = PerformanceVisualizer(results_df)
    
    return {
        'latency_distribution': visualizer.create_latency_distribution(),
        'success_rates': visualizer.create_success_rate_chart(),
        'percentile_comparison': visualizer.create_percentile_comparison()
    }

def generate_performance_report(results_df: pd.DataFrame, output_dir: str) -> None:
    """
    Generate a comprehensive performance report with visualizations
    
    Args:
        results_df: DataFrame containing performance metrics
        output_dir: Directory to save the report
    """
    visualizer = PerformanceVisualizer(results_df)
    
    # Create dashboard
    visualizer.create_dashboard(output_dir)
    
    # Generate numerical summary
    summary = pd.DataFrame({
        'Metric': [
            'Total Queries Executed',
            'Overall Success Rate',
            'Average Latency (ms)',
            'Median Latency (ms)',
            'P95 Latency (ms)',
            'P99 Latency (ms)',
            'Best Performing Query Type',
            'Worst Performing Query Type'
        ],
        'Value': [
            results_df['Total Queries'].sum(),
            f"{results_df['Success Rate'].mean():.2f}%",
            f"{results_df['Avg Time (ms)'].mean():.2f}",
            f"{results_df['Median Time (ms)'].median():.2f}",
            f"{results_df['P95 Time (ms)'].mean():.2f}",
            f"{results_df['P99 Time (ms)'].mean():.2f}",
            results_df.loc[results_df['Avg Time (ms)'].idxmin(), 'Query Type'],
            results_df.loc[results_df['Avg Time (ms)'].idxmax(), 'Query Type']
        ]
    })
    
    # Save summary to CSV
    summary.to_csv(f"{output_dir}/performance_summary.csv", index=False)