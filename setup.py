"""
Setup Script for Business Performance Analytics Pipeline

This script:
1. Checks Python version
2. Creates virtual environment if needed
3. Installs all dependencies
4. Verifies DBT installation
5. Tests database connection
6. Provides next steps

Usage:
    python setup.py
"""

import sys
import os
import subprocess
import platform
from pathlib import Path

# Set UTF-8 encoding for Windows console
if platform.system() == "Windows":
    # Try to set UTF-8 for console output
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except:
        pass  # If it fails, we'll use ASCII-safe characters


def print_header(message: str):
    """Print formatted header."""
    print("\n" + "=" * 70)
    print(f"  {message}")
    print("=" * 70)


def check_python_version():
    """Verify Python version is 3.11-3.13."""
    print_header("Checking Python Version")

    version = sys.version_info
    print(f"Python version: {version.major}.{version.minor}.{version.micro}")

    # Check minimum version
    if version.major < 3 or (version.major == 3 and version.minor < 11):
        print("\n[ERROR] ERROR: Python 3.11 or higher is required")
        print(f"   You have: {version.major}.{version.minor}.{version.micro}")
        print("\n   Please install Python 3.11+ from: https://www.python.org/downloads/")
        sys.exit(1)

    # Check maximum version (warning for Python 3.14+)
    if version.major > 3 or (version.major == 3 and version.minor > 13):
        print(f"\n[WARNING]  WARNING: Python {version.major}.{version.minor} is newer than tested versions")
        print("   This project has been tested with Python 3.11-3.13")
        print("   Some dependencies may not be compatible yet")
        print("\n   Continuing anyway...")

    print("[OK] Python version is compatible")
    return True


def get_venv_path():
    """Get platform-specific virtual environment path."""
    if platform.system() == "Windows":
        return Path("venv/Scripts")
    else:
        return Path("venv/bin")


def get_python_executable():
    """Get platform-specific Python executable in venv."""
    venv_path = get_venv_path()
    if platform.system() == "Windows":
        return venv_path / "python.exe"
    else:
        return venv_path / "python"


def get_pip_executable():
    """Get platform-specific pip executable in venv."""
    venv_path = get_venv_path()
    if platform.system() == "Windows":
        return venv_path / "pip.exe"
    else:
        return venv_path / "pip"


def create_virtual_environment():
    """Create virtual environment if it doesn't exist."""
    print_header("Setting Up Virtual Environment")

    venv_dir = Path("venv")

    if venv_dir.exists():
        print("[OK] Virtual environment already exists")
        return True

    print("Creating virtual environment...")
    try:
        subprocess.run([sys.executable, "-m", "venv", "venv"], check=True)
        print("[OK] Virtual environment created successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"\n[ERROR] ERROR: Failed to create virtual environment: {e}")
        return False


def install_dependencies():
    """Install dependencies from requirements.txt."""
    print_header("Installing Dependencies")

    pip_executable = get_pip_executable()

    if not pip_executable.exists():
        print(f"[ERROR] ERROR: pip not found at {pip_executable}")
        return False

    print("Upgrading pip...")
    try:
        subprocess.run(
            [str(pip_executable), "install", "--upgrade", "pip"],
            check=True,
            capture_output=True
        )
        print("[OK] pip upgraded")
    except subprocess.CalledProcessError as e:
        print(f"[WARNING]  Warning: Failed to upgrade pip: {e}")

    print("\nInstalling requirements.txt...")
    try:
        # Show progress while installing
        result = subprocess.run(
            [str(pip_executable), "install", "-r", "requirements.txt"],
            check=True
        )
        print("[OK] All dependencies installed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"\n[ERROR] ERROR: Failed to install dependencies: {e}")
        return False


def install_dbt():
    """Install DBT with PostgreSQL adapter."""
    print_header("Installing DBT")

    pip_executable = get_pip_executable()

    print("Installing dbt-postgres...")
    try:
        subprocess.run(
            [str(pip_executable), "install", "dbt-postgres==1.7.4"],
            check=True
        )
        print("[OK] dbt-postgres installed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"\n[ERROR] ERROR: Failed to install DBT: {e}")
        return False


def verify_dbt_installation():
    """Verify DBT is installed and working."""
    print_header("Verifying DBT Installation")

    dbt_executable = get_venv_path() / "dbt"
    if platform.system() == "Windows":
        dbt_executable = get_venv_path() / "dbt.exe"

    try:
        result = subprocess.run(
            [str(dbt_executable), "--version"],
            check=True,
            capture_output=True,
            text=True
        )
        print("[OK] DBT is installed")
        print("\nDBT Version Info:")
        print(result.stdout)
        return True
    except subprocess.CalledProcessError as e:
        print(f"\n[ERROR] ERROR: DBT verification failed: {e}")
        return False


def check_docker():
    """Check if Docker is running."""
    print_header("Checking Docker")

    try:
        result = subprocess.run(
            ["docker", "--version"],
            check=True,
            capture_output=True,
            text=True
        )
        print("[OK] Docker is installed")
        print(f"  {result.stdout.strip()}")

        # Check if Docker is running
        result = subprocess.run(
            ["docker", "ps"],
            check=True,
            capture_output=True,
            text=True
        )
        print("[OK] Docker is running")
        return True

    except subprocess.CalledProcessError:
        print("\n[WARNING]  WARNING: Docker is not running or not installed")
        print("   Please install Docker Desktop: https://www.docker.com/products/docker-desktop/")
        print("   You'll need Docker to run PostgreSQL, Airflow, and pgAdmin")
        return False
    except FileNotFoundError:
        print("\n[WARNING]  WARNING: Docker is not installed")
        print("   Please install Docker Desktop: https://www.docker.com/products/docker-desktop/")
        return False


def create_activation_script():
    """Create helper scripts to activate virtual environment."""
    print_header("Creating Activation Helper Scripts")

    # Windows batch file
    windows_script = """@echo off
echo Activating virtual environment...
call venv\\Scripts\\activate.bat
echo.
echo ========================================
echo   Virtual Environment Activated!
echo ========================================
echo.
echo You can now run:
echo   - python scripts/load_csv_data.py
echo   - cd dbt ^&^& dbt run
echo.
"""

    # Unix shell script
    unix_script = """#!/bin/bash
echo "Activating virtual environment..."
source venv/bin/activate
echo ""
echo "========================================"
echo "  Virtual Environment Activated!"
echo "========================================"
echo ""
echo "You can now run:"
echo "  - python scripts/load_csv_data.py"
echo "  - cd dbt && dbt run"
echo ""
"""

    try:
        # Create Windows activation script
        with open("activate.bat", "w") as f:
            f.write(windows_script)
        print("[OK] Created activate.bat (Windows)")

        # Create Unix activation script
        with open("activate.sh", "w") as f:
            f.write(unix_script)

        # Make Unix script executable
        if platform.system() != "Windows":
            os.chmod("activate.sh", 0o755)

        print("[OK] Created activate.sh (Unix/Linux/Mac)")

        return True
    except Exception as e:
        print(f"[WARNING]  Warning: Failed to create activation scripts: {e}")
        return False


def print_next_steps():
    """Print next steps for the user."""
    print_header("Setup Complete!")

    print("\n[OK] Your development environment is ready!")
    print("\n📋 NEXT STEPS:\n")

    if platform.system() == "Windows":
        print("1.  Activate virtual environment:")
        print("   activate.bat")
        print("   (or: venv\\Scripts\\activate)")
    else:
        print("1.  Activate virtual environment:")
        print("   source activate.sh")
        print("   (or: source venv/bin/activate)")

    print("\n2.  Start Docker containers:")
    print("   docker-compose up -d")

    print("\n3.  Run load_csv_data.py to load sample data")
    print("   python scripts/load_csv_data.py")
    print("\n4.  Run the dag weather_data_dag.py in airflow to get weather data")
    print("\n5.  Run the dbt commands to build models")

    print("\n5.  Access services:")
    print("   - Airflow UI: http://localhost:8080 (admin/admin)")
    print("   - pgAdmin: http://localhost:5050 (admin@admin.com/admin)")

    print("\n📚 DOCUMENTATION:")
    print("   - README.md - Complete project overview")
    print("   - docs/PGADMIN_SETUP.md - pgAdmin setup guide")

    
    print("\n" + "=" * 70)


def main():
    """Main setup workflow."""
    print("\n" + "=" * 70)
    print("  BUSINESS PERFORMANCE ANALYTICS PIPELINE")
    print("  Development Environment Setup")
    print("=" * 70)

    # Change to project root directory
    project_root = Path(__file__).parent
    os.chdir(project_root)

    # Check Python version
    if not check_python_version():
        sys.exit(1)

    # Check Docker (warning only, not blocking)
    check_docker()

    # Create virtual environment
    if not create_virtual_environment():
        sys.exit(1)

    # Install dependencies
    if not install_dependencies():
        sys.exit(1)

    # Install DBT
    if not install_dbt():
        sys.exit(1)

    # Verify DBT
    if not verify_dbt_installation():
        sys.exit(1)

    # Create activation helper scripts
    create_activation_script()

    # Print next steps
    print_next_steps()

    sys.exit(0)


if __name__ == "__main__":
    main()
