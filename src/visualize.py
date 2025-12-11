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


def genre_intersection_union(df: pd.DataFrame) -> None:
    # Get all unique genres
    all_genres = set()
    for genres_list in df['Genre']:
        all_genres.update(genres_list)
    
    all_genres = sorted(list(all_genres))
    
    # For each pair of genres, compute union and intersection counts
    union_counts = []
    intersection_counts = []
    labels = []
    
    for i, genre1 in enumerate(all_genres):
        for genre2 in all_genres[i+1:]:
            # Count movies with genre1 AND genre2 (intersection)
            intersection = sum(df['Genre'].apply(lambda g: genre1 in g and genre2 in g))
            
            if intersection > 0:
                union = sum(df['Genre'].apply(lambda g: genre1 in g or genre2 in g))
                
                if union >= 280:
                    union_counts.append(union)
                    intersection_counts.append(intersection)
                    labels.append(f"{genre1}-{genre2}")
    
    fig, ax = plt.subplots(figsize=(12, 8))
    ax.scatter(union_counts, intersection_counts, alpha=0.6, s=50)
    
    for i, label in enumerate(labels):
        ax.annotate(label, (union_counts[i], intersection_counts[i]), 
                   fontsize=8, alpha=0.7, xytext=(5, 5), textcoords='offset points')
    
    ax.set_xlabel('Movies in Either Genre (Union)')
    ax.set_ylabel('Movies in Both Genres (Intersection)')
    ax.set_title('Genre Intersection vs Union (Intersection > 0)')
    ax.grid(True, alpha=0.3)
    
    plt.tight_layout()
    
    plots_dir = root_dir / "plots"
    plots_dir.mkdir(exist_ok=True)
    output_path = plots_dir / "genre_intersection.png"
    
    plt.savefig(output_path, dpi=100, bbox_inches='tight')
    print(f"Genre intersection plot saved to {output_path}")
    plt.close()
