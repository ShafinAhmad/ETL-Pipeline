import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path

root_dir = Path(__file__).resolve().parent.parent


def create_scatter_with_polynomial_trend(ax, x, y, xlabel, ylabel, title, degree=2):
    ax.scatter(x, y, alpha=0.5, s=20)
    
    # Calculate polynomial trend line
    z = np.polyfit(x, y, degree)
    p = np.poly1d(z)
    x_trend = np.linspace(x.min(), x.max(), 100)
    ax.plot(x_trend, p(x_trend), "r--", linewidth=2, label=f"Polynomial fit (degree {degree})")
    
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.legend()
    ax.grid(True, alpha=0.3)


def create_line_plot(ax, df_grouped, x_col, y_col, xlabel, ylabel, title):
    ax.plot(df_grouped[x_col], df_grouped[y_col], marker='o', linewidth=2, markersize=6)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.grid(True, alpha=0.3)


def remove_outliers_iqr(data, column):
    Q1 = data[column].quantile(0.25)
    Q3 = data[column].quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    return data[(data[column] >= lower_bound) & (data[column] <= upper_bound)]


def visualize(df: pd.DataFrame) -> None:
    # Remove Gone with the Wind, it's an outlier
    df = df[df['Series_Title'] != 'Gone with the Wind'].copy()
    
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle("Movie Data Analysis: Relationships with Inflation-Adjusted Gross", fontsize=14, fontweight='bold')
    
    # 1. Decade vs AGI - Line plot (grouped by decade)
    decade_grouped = df.groupby('Decade')['Gross_Inflation_Adjusted'].mean().reset_index()
    create_line_plot(
        axes[0, 0],
        decade_grouped,
        'Decade',
        'Gross_Inflation_Adjusted',
        'Decade',
        'Avg AGI ($)',
        'Decade vs AGI (Line)'
    )
    
    # 2. Movie Length vs AGI - Scatter with polynomial trend (all data)
    create_scatter_with_polynomial_trend(
        axes[0, 1],
        df['Runtime'],
        df['Gross_Inflation_Adjusted'],
        'Runtime (minutes)',
        'AGI ($)',
        'Movie Length vs AGI (Polynomial Trend)'
    )
    
    # 3. IMDB Rating vs AGI - Line plot
    rating_grouped = df.groupby(pd.cut(df['IMDB_Rating'], bins=10))['Gross_Inflation_Adjusted'].mean().reset_index()
    rating_grouped['IMDB_Rating'] = rating_grouped['IMDB_Rating'].apply(lambda x: x.mid)
    axes[1, 0].plot(rating_grouped['IMDB_Rating'], rating_grouped['Gross_Inflation_Adjusted'], marker='o', linewidth=2, markersize=6)
    axes[1, 0].set_xlabel('IMDB Rating')
    axes[1, 0].set_ylabel('Avg AGI ($)')
    axes[1, 0].set_title('IMDB Rating vs AGI (Line)')
    axes[1, 0].grid(True, alpha=0.3)
    
    # 4. Popularity Score vs AGI - Scatter with polynomial trend (all data)
    create_scatter_with_polynomial_trend(
        axes[1, 1],
        df['Popularity_Score'],
        df['Gross_Inflation_Adjusted'],
        'Popularity Score',
        'AGI ($)',
        'Popularity vs AGI (Polynomial Trend)'
    )
    
    plt.tight_layout()
    
    # Save to plots directory
    plots_dir = root_dir / "plots"
    plots_dir.mkdir(exist_ok=True)
    output_path = plots_dir / "analysis.png"
    
    plt.savefig(output_path, dpi=100, bbox_inches='tight')
    print(f"Plots saved to {output_path}")
    plt.close()

