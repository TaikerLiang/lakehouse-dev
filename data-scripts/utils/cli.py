"""
Command Line Interface for lakehouse data scripts
"""
import click
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
import sys
import os

# Add the parent directory to the path to import settings
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from settings import settings
from utils.clients import clients

console = Console()

@click.group()
def cli():
    """Lakehouse Data Scripts CLI"""
    pass

@cli.command()
def health_check():
    """Check the health of all services"""
    console.print(Panel.fit("🏥 Service Health Check", style="bold blue"))
    
    try:
        health_results = clients.health_check()
        
        # Create a table for results
        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("Service", style="cyan", no_wrap=True)
        table.add_column("Status", style="green")
        table.add_column("Endpoint", style="yellow")
        
        # Add rows for each service
        services_info = {
            'trino': f"{settings.trino_host}:{settings.trino_port}",
            'postgres': f"{settings.postgres_host}:{settings.postgres_port}",
            'redis': f"{settings.redis_host}:{settings.redis_port}",
            'minio': settings.minio_endpoint
        }
        
        for service, healthy in health_results.items():
            status = "✅ Healthy" if healthy else "❌ Unhealthy"
            endpoint = services_info.get(service, "N/A")
            table.add_row(service.title(), status, endpoint)
        
        console.print(table)
        
        # Summary
        healthy_count = sum(health_results.values())
        total_count = len(health_results)
        
        if healthy_count == total_count:
            console.print(f"\n✅ All services are healthy ({healthy_count}/{total_count})", style="bold green")
        else:
            console.print(f"\n⚠️  {healthy_count}/{total_count} services are healthy", style="bold yellow")
            
    except Exception as e:
        console.print(f"❌ Health check failed: {e}", style="bold red")
        sys.exit(1)

@cli.command()
@click.option('--source', '-s', type=click.Choice(['csv', 'json', 'parquet']), 
              help='Source data format')
@click.option('--target', '-t', help='Target table name')
def ingest(source, target):
    """Run data ingestion (placeholder for future implementation)"""
    console.print(Panel.fit("🚀 Data Ingestion", style="bold green"))
    
    if not source or not target:
        console.print("❌ Please specify both --source and --target", style="bold red")
        console.print("Example: lakehouse-ingest --source=csv --target=my_table")
        sys.exit(1)
    
    console.print(f"📊 Source format: {source}")
    console.print(f"🎯 Target table: {target}")
    console.print("⚠️  Ingestion functionality not yet implemented", style="yellow")

@cli.command()
def info():
    """Show configuration information"""
    console.print(Panel.fit("ℹ️  Configuration Info", style="bold cyan"))
    
    # Application info
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Setting", style="cyan", no_wrap=True)
    table.add_column("Value", style="green")
    
    table.add_row("App Name", settings.app_name)
    table.add_row("Environment", settings.environment)
    table.add_row("Debug Mode", str(settings.debug))
    table.add_row("Log Level", settings.log_level)
    table.add_row("Batch Size", str(settings.batch_size))
    table.add_row("Max Retries", str(settings.max_retries))
    
    console.print(table)
    
    # Feature flags
    console.print("\n🚩 Feature Flags:", style="bold yellow")
    flags = [
        ("Send Email Alerts", settings.send_email_alerts),
        ("Data Validation", settings.enable_data_validation),
        ("Dry Run Mode", settings.dry_run_mode),
        ("Auto Create Tables", settings.auto_create_tables),
    ]
    
    for flag_name, flag_value in flags:
        status = "✅ Enabled" if flag_value else "❌ Disabled"
        console.print(f"  • {flag_name}: {status}")

# Command functions that can be called directly by Poetry scripts
def health_check_command():
    """Entry point for poetry script"""
    health_check.callback()

def ingest_command():
    """Entry point for poetry script"""
    ingest.callback(None, None)

if __name__ == '__main__':
    cli()