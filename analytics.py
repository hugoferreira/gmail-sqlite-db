import datetime
import os
import subprocess
import shutil
import tempfile

# Local imports
from db import DatabaseManager # For type hinting
from queries import METRIC_QUERIES # Needs access to METRIC_QUERIES

async def analytics_email_density(db_manager: DatabaseManager, year=None, metric='emails'):
    """Show a monthly density chart for a given year and metric using termgraph."""
    if year is None:
        year = datetime.datetime.now().year
    metric_info = METRIC_QUERIES.get(metric, METRIC_QUERIES['emails']) 
    sql = metric_info['monthly_sql']
    # Query monthly counts
    async with db_manager.db.execute(sql, (str(year),)) as cursor:
        data = await cursor.fetchall()
    # Ensure all months are present
    counts = [0]*12
    for period, count_val in data:
        counts[int(period)-1] = count_val if count_val is not None else 0
    labels = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    # Prepare data for termgraph
    with tempfile.NamedTemporaryFile('w+', delete=False) as f:
        for label, count_val_inner in zip(labels, counts): 
            f.write(f"{label} {count_val_inner}\n")
        temp_path = f.name
    if not shutil.which('termgraph'):
        print("termgraph is not installed. Please install it with 'pip install termgraph'.")
        # It's good practice to remove the temp file if termgraph is not found and we can't use it.
        if os.path.exists(temp_path):
            os.unlink(temp_path)
        return
    print(f"\n{metric_info['label']} for {year} (monthly):")
    subprocess.run(['termgraph', temp_path, '--color', 'blue', '--width', '50', '--format', '{:.0f}'])
    print()
    if os.path.exists(temp_path): # Ensure temp file is cleaned up
        os.unlink(temp_path)

async def analytics_email_calendar_heatmap(db_manager: DatabaseManager, year=None, metric='emails'):
    if year is None:
        year = datetime.datetime.now().year
    metric_info = METRIC_QUERIES.get(metric, METRIC_QUERIES['emails']) 
    sql = metric_info['calendar_sql']
    async with db_manager.db.execute(sql, (str(year),)) as cursor:
        data = await cursor.fetchall()
    with tempfile.NamedTemporaryFile('w+', delete=False) as f:
        for period, count_val in data: 
            f.write(f"{period} {count_val}\n")
        temp_path = f.name
    if not shutil.which('termgraph'):
        print("termgraph is not installed. Please install it with 'pip install termgraph'.")
        if os.path.exists(temp_path):
            os.unlink(temp_path)
        return
    print(f"\n{metric_info['label']} calendar heatmap for {year}:")
    subprocess.run([
        'termgraph', temp_path, '--calendar', '--start-dt', f'{year}-01-01', '--color', 'blue'
    ])
    print()
    if os.path.exists(temp_path): # Ensure temp file is cleaned up
        os.unlink(temp_path)

async def run_analytics(db_manager: DatabaseManager, args):
    """Main dispatcher for analytics mode."""
    metric = getattr(args, 'metric', 'emails')
    year = getattr(args, 'year', datetime.datetime.now().year) # Ensure year is defaulted if not present in args for some reason

    if getattr(args, 'calendar', False):
        await analytics_email_calendar_heatmap(db_manager, year=year, metric=metric)
    else:
        await analytics_email_density(db_manager, year=year, metric=metric) 